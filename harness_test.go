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
