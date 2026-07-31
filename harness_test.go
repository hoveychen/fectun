package main

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// lossyProxy 在两端之间转发 UDP,并按给定概率丢包。
// 用来在本地复现真实链路的随机丢包 —— 这些 bug 只有在丢包时才暴露。
type lossyProxy struct {
	conn     *net.UDPConn
	lossRate float64
	mu       sync.Mutex
	addrs    []*net.UDPAddr // 最多两个端点,互相转发
	rnd      *rand.Rand
	stopped  bool

	// 确定性丢包:丢弃 seq 落在 [dropFrom, dropTo] 的数据分片(含校验片所属组),
	// 且每个分片只丢一次 —— 重传可通过,用于构造"尾部连续丢失"场景。
	dropFrom, dropTo uint32
	dropEnabled      bool
	dropped          map[string]bool
}

func newLossyProxy(t *testing.T, loss float64, seed int64) *lossyProxy {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("proxy bind: %v", err)
	}
	p := &lossyProxy{conn: c, lossRate: loss, rnd: rand.New(rand.NewSource(seed)),
		dropped: make(map[string]bool)}
	go p.run()
	return p
}

func (p *lossyProxy) port() int { return p.conn.LocalAddr().(*net.UDPAddr).Port }

func (p *lossyProxy) run() {
	buf := make([]byte, 2048)
	for {
		n, src, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p.mu.Lock()
		known := false
		for _, a := range p.addrs {
			if a.String() == src.String() {
				known = true
			}
		}
		if !known && len(p.addrs) < 2 {
			cp := *src
			p.addrs = append(p.addrs, &cp)
		}
		var dst *net.UDPAddr
		for _, a := range p.addrs {
			if a.String() != src.String() {
				dst = a
			}
		}
		drop := p.rnd.Float64() < p.lossRate
		if p.dropEnabled {
			if h, ok := parseHeader(buf[:n]); ok && h.typ == pktData {
				// 校验片没有 seq,用 group 推算其覆盖范围的首个 seq
				sq := h.seq
				if int(h.shardIdx) >= int(h.k) {
					sq = h.group * uint32(h.k)
				}
				key := fmt.Sprintf("%d-%d", h.group, h.shardIdx)
				if sq >= p.dropFrom && sq <= p.dropTo && !p.dropped[key] {
					p.dropped[key] = true
					drop = true
				}
			}
		}
		p.mu.Unlock()
		if dst == nil || drop {
			continue
		}
		out := make([]byte, n)
		copy(out, buf[:n])
		p.conn.WriteToUDP(out, dst)
	}
}

func (p *lossyProxy) stop() { p.conn.Close() }

// newTestSession 建一个指向 proxy 的 session
func newTestSession(t *testing.T, proxyPort, k, m int, rate float64) *session {
	return newTestSessionOn(t, 0, proxyPort, k, m, rate)
}

// newTestSessionOn 可指定本地端口。localPort=0 表示随机。
// 重启场景必须复用同一端口 —— 真实部署中服务重启后 UDP 端口不变。
func newTestSessionOn(t *testing.T, localPort, proxyPort, k, m int, rate float64) *session {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: localPort})
	if err != nil {
		t.Fatalf("session bind: %v", err)
	}
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: proxyPort}
	s := newSession(c, peer, k, m, rate)
	go s.readLoop()
	return s
}

// collect 从 deliver 收集 want 字节,超时返回已收到的部分
func collect(s *session, want int, timeout time.Duration) []byte {
	var out []byte
	deadline := time.After(timeout)
	for len(out) < want {
		select {
		case p := <-s.deliver:
			out = append(out, p...)
		case <-deadline:
			return out
		}
	}
	return out
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

func TestHeaderRoundTrip(t *testing.T) {
	in := header{typ: pktData, shardIdx: 7, k: 20, m: 15, group: 123456, seq: 987654, epoch: 0xDEADBEEF}
	b := make([]byte, hdrSize)
	in.marshal(b)
	out, ok := parseHeader(b)
	if !ok {
		t.Fatal("parseHeader 失败")
	}
	if out != in {
		t.Fatalf("往返不一致\n got: %+v\nwant: %+v", out, in)
	}
	if _, ok := parseHeader(b[:hdrSize-1]); ok {
		t.Fatal("过短的包头应当被拒绝")
	}
}

// 回归:空载(双方都没发过任何数据分片)时不得发 NACK。
// 曾经的 bug —— NACK 扫描区间写成 [expected, maxRecvSeq] 闭区间,而两者初值都是 0,
// 于是 seq 0 被当作"已知缺口",每 120ms 请求重传一个还不存在的包,永不停止。
func TestIdleSessionSendsNoNack(t *testing.T) {
	px := newLossyProxy(t, 0, 21)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 500)
	b := newTestSession(t, px.port(), 20, 15, 500)
	defer a.close()
	defer b.close()

	// 只跑心跳,不写任何业务数据。nackLoop 每 40ms 一跳、缺口 120ms 后才发,
	// 1s 足够让 bug 版本发出多个 NACK。
	time.Sleep(1 * time.Second)

	for name, s := range map[string]*session{"a": a, "b": b} {
		s.stats.Lock()
		n := s.stats.nackSent
		s.stats.Unlock()
		if n != 0 {
			t.Fatalf("session %s 在零流量下发了 %d 个 NACK(空载无效 NACK 回归)", name, n)
		}
	}
}

// heartbeatPkt 拼一个心跳包。seq 字段携带发送端已发出的分片总数。
func heartbeatPkt(seq, epoch uint32) []byte {
	b := make([]byte, hdrSize)
	header{typ: pktHeartbeat, seq: seq, epoch: epoch}.marshal(b)
	return b
}

// 回归:对端 nextSeq 在高位时,本端的 NACK 记账不得爆炸。
// 这是 2026-07-31 生产事故的复现 —— 单端重启后重启方 expected=0,而对端已跑
// 4 天、nextSeq 在几百万的高位。对端心跳把 recvHigh 一下顶到高位,nackLoop
// 首个 tick 就从 0 一路扫到 recvHigh,给 firstSeen 建了几百万条目
// (map[uint32]time.Time 约 40 B/条),实测吃掉 202 MB 常驻并每秒发 780 个 NACK。
func TestHugePeerSeqDoesNotExplodeNackState(t *testing.T) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, 20, 15, 25)
	defer s.close()

	// 对端心跳声称已发出 50 万个分片,而本端 expected 还是 0
	const peerNext = 500000
	s.onPacket(heartbeatPkt(peerNext, 7))

	// 让 nackLoop(40ms 一跳)跑几轮
	time.Sleep(250 * time.Millisecond)

	s.recvMu.Lock()
	seen, high := len(s.firstSeen), s.recvHigh
	s.recvMu.Unlock()

	if high == 0 {
		t.Fatal("前置条件不成立:心跳没把 recvHigh 顶起来,测试无效")
	}
	// 记账条目数必须与"最近的缺口窗口"同阶,不能与对端 seq 同阶
	if seen > 8192 {
		t.Fatalf("firstSeen 爆炸:%d 条(对端 seq=%d)—— 单次扫描没有窗口上限", seen, peerNext)
	}
}

// 回归:对端序号空间回退时,本端必须跟着重置。
// 关键时序 —— A 重启(换新 epoch),B 收到新 epoch 后重置自己的 nextSeq 归零,
// 但 B 进程没重启、B 自己的 epoch 不变。于是 A 的 checkEpoch 永远察觉不到
// B 已经重置,recvHigh 单调递增停在高位,nackLoop 就永久对一段 B 根本没发过的
// 高位 seq 发 NACK(实测 780 个/秒)。
func TestPeerSeqRollbackResetsRecvState(t *testing.T) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, 20, 15, 25)
	defer s.close()

	// 对端在高位(epoch 固定为 7,全程不变 —— 对端进程并没有重启)
	s.onPacket(heartbeatPkt(300000, 7))
	s.recvMu.Lock()
	highBefore := s.recvHigh
	s.recvMu.Unlock()
	if highBefore != 300000 {
		t.Fatalf("前置条件不成立:recvHigh=%d,应为 300000", highBefore)
	}

	// 对端重置了序号空间,心跳回到低位。epoch 没变。
	s.onPacket(heartbeatPkt(3, 7))

	s.recvMu.Lock()
	high, exp := s.recvHigh, s.expected
	s.recvMu.Unlock()

	if high > 3 {
		t.Fatalf("recvHigh 没跟随对端回退:%d(对端已回到 3)—— 会永久对高位 seq 发 NACK", high)
	}
	if exp != 0 {
		t.Fatalf("对端序号空间已重置,本端 expected 应归零,实为 %d", exp)
	}
}

// dataShardPkt 拼一个合法的数据分片包,payload 前两字节是有效长度。
func dataShardPkt(group uint32, idx, k, m int, epoch uint32) []byte {
	pkt := make([]byte, hdrSize+shardPayload)
	header{typ: pktData, shardIdx: byte(idx), k: byte(k), m: byte(m),
		group: group, seq: group*uint32(k) + uint32(idx), epoch: epoch}.marshal(pkt)
	body := pkt[hdrSize:]
	body[0], body[1] = 0, 8 // 每片承载 8 字节有效数据
	for i := 0; i < 8; i++ {
		body[2+i] = byte(i)
	}
	return pkt
}

// 回归:FEC 组的记账 map 必须有界。
// 曾经的 bug —— groupDone 每个组留一条记录且永不删除,groups 里卡住的组也不淘汰。
// 一条组承载 k*(shardPayload-2) 字节,长期运行累计几百 GB 就是上百 MB 的常驻内存。
func TestGroupBookkeepingIsBounded(t *testing.T) {
	const k, m = 20, 15
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, k, m, 1e6)
	defer s.close()

	// 消费 deliver,否则 drain 会在 channel 满时阻塞
	go func() {
		for {
			select {
			case <-s.deliver:
			case <-s.closed:
				return
			}
		}
	}()

	// 3000 个组全部完整到达 —— 每个组都会走"完成"路径
	const nGroups = 3000
	for g := uint32(0); g < nGroups; g++ {
		for i := 0; i < k; i++ {
			s.onPacket(dataShardPkt(g, i, k, m, 1))
		}
	}

	s.recvMu.Lock()
	done, groups, expected := len(s.groupDone), len(s.groups), s.expected
	s.recvMu.Unlock()

	if expected != nGroups*k {
		t.Fatalf("前置条件不成立:expected=%d,应为 %d(数据没被正常交付,测试无效)", expected, nGroups*k)
	}
	// 交付完成的组不需要再记账,留一个小窗口容纳迟到重传即可
	if done > 512 {
		t.Fatalf("groupDone 无界增长:%d 个组已全部交付完毕却仍在记账中(上限应为常数)", done)
	}
	if groups > 512 {
		t.Fatalf("groups 无界增长:残留 %d 条组槽位", groups)
	}
}

// 回归:重传缓冲必须按字节封顶。
// 曾经的 bug —— 上限写成"16384 个分片",而每片 1184 字节,等于 19.4 MB 常驻;
// 裁剪后仍保 8192 片 ≈ 9.7 MB。GOGC=100 下这一项就把发送侧 RSS 推到 40 MB 量级。
func TestSendBufferRespectsByteBudget(t *testing.T) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	// peer 指向没人监听的端口:只关心发送侧缓冲的账,不需要对端。
	// rate 用生产默认值 25 Mbps —— 预算是跟着 rate 算的,必须按真实档位验。
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, 20, 15, 25)
	defer s.close()

	const written = 8 << 20 // 远超 25 Mbps 档的预算,足以触发淘汰
	s.write(payload(written))

	s.sendMu.Lock()
	var held int
	for _, sh := range s.sendBuf {
		held += len(sh)
	}
	shards := len(s.sendBuf)
	s.sendMu.Unlock()

	if shards == 0 {
		t.Fatal("前置条件不成立:sendBuf 是空的,写入路径没跑到(测试无效)")
	}
	// ARQ 只需覆盖一个 RTT 内的在途数据,几 MB 足够。
	const limit = 6 << 20
	if held > limit {
		t.Fatalf("重传缓冲占 %.1f MB(%d 片),超出 %.0f MB 预算 —— 上限是按包数而非字节封顶的",
			float64(held)/(1<<20), shards, float64(limit)/(1<<20))
	}
}

// 回归:FEC 应能在真实丢包下恢复字节流
func TestFECRecoversUnderLoss(t *testing.T) {
	px := newLossyProxy(t, 0.15, 1)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 500)
	b := newTestSession(t, px.port(), 20, 15, 500)
	defer a.close()
	defer b.close()
	time.Sleep(300 * time.Millisecond) // 让心跳建立双向路径

	data := payload(400 * 1024)
	go a.write(data)
	got := collect(b, len(data), 20*time.Second)
	if len(got) != len(data) {
		t.Fatalf("15%% 丢包下收到 %d/%d 字节", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("字节 %d 不一致: got %d want %d", i, got[i], data[i])
		}
	}
}

// 回归:尾部连续丢包必须能被检测并重传。
// 曾经的 bug —— NACK 只在 [expected, recvHigh] 区间扫描,而尾部分片全丢时
// recvHigh 停在缺口之前,循环根本不执行,永久死锁。
// 修复靠心跳携带发送端 nextSeq 把 recvHigh 顶上去。
func TestTailLossIsRecovered(t *testing.T) {
	px := newLossyProxy(t, 0, 7)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 500)
	b := newTestSession(t, px.port(), 20, 15, 500)
	defer a.close()
	defer b.close()
	time.Sleep(300 * time.Millisecond)

	// 先让一批数据正常通过,把 expected 推进到高位
	head := payload(shardPayload * 6)
	go a.write(head)
	if got := collect(b, len(head), 15*time.Second); len(got) != len(head) {
		t.Fatalf("前置数据未通过: %d/%d", len(got), len(head))
	}

	// 构造尾部连续丢失:后续分片首发全部丢弃(重传放行)
	a.sendMu.Lock()
	from := a.nextSeq
	a.sendMu.Unlock()
	px.mu.Lock()
	px.dropEnabled = true
	px.dropFrom, px.dropTo = from, from+9
	px.mu.Unlock()

	// 这批数据的分片首发会被全丢,且之后没有任何新数据来触发缺口检测
	tail := payload(shardPayload * 3)
	go a.write(tail)

	got := collect(b, len(tail), 20*time.Second)
	if len(got) != len(tail) {
		t.Fatalf("尾部连续丢包未恢复: 收到 %d/%d 字节(尾部丢包死锁回归)", len(got), len(tail))
	}
}

// 回归:对端重启后必须自愈。
// 曾经的 bug —— 重启方 nextSeq 归零而对端 expected 停在高位,新包被当旧包丢弃,永久死锁。
func TestPeerRestartRecovery(t *testing.T) {
	px := newLossyProxy(t, 0, 3)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 500)
	aPort := a.conn.LocalAddr().(*net.UDPAddr).Port
	b := newTestSession(t, px.port(), 20, 15, 500)
	defer b.close()
	time.Sleep(300 * time.Millisecond)

	// 先推进一批数据,把双方序号推到高位
	first := payload(200 * 1024)
	go a.write(first)
	if got := collect(b, len(first), 15*time.Second); len(got) != len(first) {
		t.Fatalf("重启前传输就失败: %d/%d", len(got), len(first))
	}
	a.sendMu.Lock()
	seqBefore := a.nextSeq
	a.sendMu.Unlock()
	if seqBefore == 0 {
		t.Fatal("前置数据未推进 seq,测试无效")
	}

	// 模拟 a 重启:关掉换一个新 session(epoch 会不同,nextSeq 归零)
	a.close()
	time.Sleep(200 * time.Millisecond)
	a2 := newTestSessionOn(t, aPort, px.port(), 20, 15, 500) // 同端口重启
	defer a2.close()

	// 等 b 通过 epoch 变化察觉对端重启并重置
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b.recvMu.Lock()
		exp := b.expected
		b.recvMu.Unlock()
		if exp == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	second := payload(100 * 1024)
	go a2.write(second)
	got := collect(b, len(second), 15*time.Second)
	if len(got) != len(second) {
		t.Fatalf("对端重启后未自愈: 收到 %d/%d 字节(序号失同步死锁回归)", len(got), len(second))
	}
	for i := range second {
		if got[i] != second[i] {
			t.Fatalf("重启后字节 %d 不一致", i)
		}
	}
}

// epoch 变化应重置接收与发送状态
func TestEpochChangeResetsState(t *testing.T) {
	c, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, 20, 15, 100)
	defer s.close()

	var resetCalled bool
	var mu sync.Mutex
	s.onReset = func() { mu.Lock(); resetCalled = true; mu.Unlock() }

	s.checkEpoch(1000) // 首次记录,不应重置
	s.recvMu.Lock()
	s.expected = 5000
	s.recvHigh = 5001
	s.recvBuf[5000] = []byte("x")
	s.recvMu.Unlock()
	s.sendMu.Lock()
	s.nextSeq = 5000
	s.sendMu.Unlock()

	s.checkEpoch(1000) // 同一 epoch,不应重置
	s.recvMu.Lock()
	stillThere := s.expected == 5000
	s.recvMu.Unlock()
	if !stillThere {
		t.Fatal("相同 epoch 不应触发重置")
	}

	s.checkEpoch(2000) // epoch 变化,必须重置
	s.recvMu.Lock()
	exp, mx, bufLen := s.expected, s.recvHigh, len(s.recvBuf)
	s.recvMu.Unlock()
	s.sendMu.Lock()
	next := s.nextSeq
	s.sendMu.Unlock()
	if exp != 0 || mx != 0 || bufLen != 0 {
		t.Fatalf("接收状态未重置: expected=%d recvHigh=%d recvBuf=%d", exp, mx, bufLen)
	}
	if next != 0 {
		t.Fatalf("发送状态未重置: nextSeq=%d", next)
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if !resetCalled {
		t.Fatal("onReset 回调未被调用(上层 stream 不会被清理)")
	}
}

// mux 帧必须能跨分片边界正确解析
func TestMuxFrameSpansShards(t *testing.T) {
	px := newLossyProxy(t, 0, 11)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 500)
	b := newTestSession(t, px.port(), 20, 15, 500)
	defer a.close()
	defer b.close()
	time.Sleep(300 * time.Millisecond)

	// 单帧远大于一个分片(shardPayload-2),必然跨多个分片
	big := payload(shardPayload * 5)
	go func() {
		a.write([]byte{0, 0, 0, 9, cmdData, byte(len(big) >> 8), byte(len(big))})
		a.write(big)
	}()
	got := collect(b, frameHdr+len(big), 20*time.Second)
	if len(got) != frameHdr+len(big) {
		t.Fatalf("跨分片帧收到 %d/%d 字节", len(got), frameHdr+len(big))
	}
	if fmt.Sprintf("%v", got[frameHdr:]) != fmt.Sprintf("%v", big) {
		t.Fatal("跨分片帧内容不一致")
	}
}
