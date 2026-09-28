package main

import (
	"io"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
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
	// 且每个分片只丢 dropTimes 次 —— 之后的重传可通过,用于构造"尾部连续丢失"场景。
	//
	// dropTimes 默认(0)等价于 1。设 >1 可让同一分片连续多轮 ARQ 都补不上,
	// 用来构造"某段 seq 长时间留空洞"的场景 —— 队头阻塞只有在空洞持续存在时才显形。
	dropFrom, dropTo uint32
	dropEnabled      bool
	dropTimes        int
	dropCount        map[string]int

	// 突发丢包(Gilbert 模型):链路在 good/bad 两态间跳转,bad 态整段全丢。
	// lossRate 那个独立同分布模型丢不出真实链路的成组丢包 —— 而 RS 组是
	// 连续 k+m 个 seq、中间不交织,所以"丢 40% 但均匀散开"和"丢 40% 但
	// 集中在一段"对 FEC 是两回事:前者每组丢 ~16 片(<m,能救),后者整组
	// 一次丢光(>m,救不回)。要评估 k/m 就必须能造出后者。
	//
	// 每个方向一条独立的链:真实链路两个方向的拥塞不同步,共用一条状态
	// 会让"去程丢的同时回程也丢"变成必然,把 ARQ 的表现压得比实际更差。
	// 突发丢包按**时间**推进,不是按包。
	//
	// 最初写成逐包转移,结果是发包多的配置(补零 flush、冗余重传、大分组)会让
	// 链路状态切换得更快 —— 各个配置面对的根本不是同一条链路,配对实验因此
	// 自相矛盾:同一组 seed 下逐 seed 比较说 A 赢 5:0,合并样本却说 B 赢。
	// 真实链路的好坏是时间的函数,与我们发多少包无关,所以状态机也必须按时间走。
	burstOn        bool
	goodDur, badDur time.Duration        // good / bad 两态的平均持续时长
	chain          map[string]*geState // 源地址 → 该方向的链路状态

	// seen 是流过 proxy 的包总数(不论是否被丢)。高冗余配置在低流量下会把
	// 发包量放大十几倍,这个数用来确认放大后仍在带宽预算内 —— 否则"用带宽
	// 换可靠性"就成了空头支票。
	seen int
}

// geState 是单个方向的链路时间线:一串预生成的好/坏交替时段。
//
// 预生成而不是边跑边抽,是为了让链路行为**完全独立于我们发了多少包**。
// 边跑边抽的话,抽签次数取决于包的到达时刻,而不同配置(补零多寡、重传多寡)
// 的发包时刻不同,于是同一个 seed 在不同配置下会长出不同的链路 —— 那就不是
// 受控实验了,配对比较会自相矛盾。
type geState struct {
	start time.Time
	flips []time.Duration // 第 i 次状态翻转的相对时刻,起始为 good
}

// badAt 回答 now 这一刻链路是否处于坏态。
// 段号为奇数即坏态(第 0 段是 good)。
func (g *geState) badAt(now time.Time) bool {
	el := now.Sub(g.start)
	lo, hi := 0, len(g.flips)
	for lo < hi {
		mid := (lo + hi) / 2
		if g.flips[mid] <= el {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo%2 == 1
}

// setBurst 按"平均丢包率 loss、平均一次坏态持续 badDur"配置模型。
//
// 稳态 bad 占比 badDur/(badDur+goodDur) 要等于 loss,解出 goodDur。
// 生产上一次突发丢 ~30 个包、当时约 150 包/s,合到时间尺度就是 200ms 量级。
func (p *lossyProxy) setBurst(loss float64, badDur time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.burstOn = true
	p.lossRate = 0 // 突发模型接管,不再叠加独立丢包
	p.badDur = badDur
	p.goodDur = time.Duration(float64(badDur) * (1 - loss) / loss)
	p.chain = make(map[string]*geState)
}

// burstDrop 回答 src 方向的这一包是否落在坏态里。调用者必须持有 p.mu。
// 首次见到某个方向时为它铺一条时间线,起点就是该方向第一个包的时刻。
func (p *lossyProxy) burstDrop(src string, now time.Time) bool {
	st := p.chain[src]
	if st == nil {
		st = p.buildTimeline(now)
		p.chain[src] = st
	}
	return st.badAt(now)
}

// buildTimeline 预生成一条好/坏交替的时间线,长度够覆盖最长的一轮试验。
// 每段时长取指数分布(无记忆性),均值按 good/bad 分别取。
func (p *lossyProxy) buildTimeline(start time.Time) *geState {
	const span = 150 * time.Second
	g := &geState{start: start}
	var at time.Duration
	bad := false
	for at < span {
		mean := p.goodDur
		if bad {
			mean = p.badDur
		}
		at += time.Duration(p.rnd.ExpFloat64() * float64(mean))
		g.flips = append(g.flips, at)
		bad = !bad
	}
	return g
}

func newLossyProxy(t *testing.T, loss float64, seed int64) *lossyProxy {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("proxy bind: %v", err)
	}
	p := &lossyProxy{conn: c, lossRate: loss, rnd: rand.New(rand.NewSource(seed)),
		dropCount: make(map[string]int)}
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
		p.seen++
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
		if p.burstOn {
			drop = p.burstDrop(src.String(), time.Now())
		}
		if p.dropEnabled {
			if h, ok := parseHeader(buf[:n]); ok && h.typ == pktData {
				// 校验片没有 seq,用 group 推算其覆盖范围的首个 seq
				sq := h.seq
				if int(h.shardIdx) >= int(h.k) {
					sq = h.group * uint32(h.k)
				}
				key := fmt.Sprintf("%d-%d", h.group, h.shardIdx)
				limit := p.dropTimes
				if limit == 0 {
					limit = 1
				}
				if sq >= p.dropFrom && sq <= p.dropTo && p.dropCount[key] < limit {
					p.dropCount[key]++
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
			out = append(out, p.data...)
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

	// 对端重置了序号空间,心跳持续回到低位。epoch 没变。
	// 判定需要连续多个心跳都报更低值(单个低心跳可能只是乱序)。
	for i := 0; i < 3; i++ {
		s.onPacket(heartbeatPkt(3, 7))
	}

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

// 回归:小幅度的序号回退也必须能接住。
// 这是 2026-07-31 第二次生产故障的复现 —— 我给回退判据定了个"幅度要超过
// nackWindow(4096)"的阈值,而抓包看到的真实回退幅度只有 65 → 0,判据永远不
// 触发,两端各自死等一段对方已经不存在的 seq,持续发无效 NACK。
// 回退幅度取决于对端重启前发了多少分片,可以小到几十 —— 判据不能依赖幅度。
func TestSmallSeqRollbackIsDetected(t *testing.T) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, 20, 15, 25)
	defer s.close()

	// 对端只发过 65 个分片就重置了 —— 这是生产上的真实数值
	s.onPacket(heartbeatPkt(65, 7))
	s.recvMu.Lock()
	highBefore := s.recvHigh
	s.recvMu.Unlock()
	if highBefore != 65 {
		t.Fatalf("前置条件不成立:recvHigh=%d,应为 65", highBefore)
	}

	for i := 0; i < 3; i++ {
		s.onPacket(heartbeatPkt(0, 7))
	}

	s.recvMu.Lock()
	high, exp := s.recvHigh, s.expected
	s.recvMu.Unlock()

	if high != 0 {
		t.Fatalf("小幅回退没被接住:recvHigh=%d(对端已回到 0)—— 判据依赖了回退幅度", high)
	}
	if exp != 0 {
		t.Fatalf("expected 应归零,实为 %d", exp)
	}
}

// 单个迟到的旧心跳不得触发重置 —— 否则一次 UDP 乱序就会白拆一条正常连接。
func TestSingleLowHeartbeatDoesNotReset(t *testing.T) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, 20, 15, 25)
	defer s.close()

	s.onPacket(heartbeatPkt(300000, 7))
	// 一个迟到的旧心跳(幅度再大也只是乱序),随后正常心跳继续
	s.onPacket(heartbeatPkt(3, 7))

	s.recvMu.Lock()
	high := s.recvHigh
	s.recvMu.Unlock()

	if high != 300000 {
		t.Fatalf("单个低心跳就触发了重置:recvHigh=%d —— 一次 UDP 乱序会白拆连接", high)
	}
}

// 探针:deliver channel 满时,drain 是否持着 recvMu 阻塞?
// 怀疑来自 2026-07-31 落地侧的现场 —— NACK 计数卡在 229600 不动、收包 2.5 分钟
// 只涨 4 个、却仍在正常发心跳(心跳循环只用 sendMu)。若 drain 持 recvMu 阻塞在
// deliver 写入,onPacket 与 nackLoop 会一起被拘死,症状正好吻合。
// 失败时打印 goroutine 现场作为直接证据。
func TestDrainDoesNotHoldRecvMuWhenDeliverFull(t *testing.T) {
	const k, m = 20, 15
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, k, m, 1e6)
	defer s.close()

	// 故意不消费 deliver —— 模拟 mux.recvLoop 卡在下游 TCP 写(慢速对端)
	go func() {
		for g := uint32(0); g < 400; g++ { // 400*20 = 8000 个分片,远超 deliver 的 4096
			for i := 0; i < k; i++ {
				s.onPacket(dataShardPkt(g, i, k, m, 1))
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)

	if len(s.deliver) < cap(s.deliver) {
		t.Fatalf("前置条件不成立:deliver 只有 %d/%d,没填满(测试无效)", len(s.deliver), cap(s.deliver))
	}

	// nackLoop 每 40ms 就要拿一次 recvMu。这里模拟它去抢锁。
	locked := make(chan struct{})
	go func() {
		s.recvMu.Lock()
		s.recvMu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("recvMu 被持有超过 2 秒 —— deliver 满时 drain 持锁阻塞在 channel 写,\n"+
			"onPacket 与 nackLoop 会一起被拘死。goroutine 现场:\n%s", buf[:n])
	}
}

// dataShardPkt 拼一个合法的数据分片包。
// 载荷走 marshalShard:sid 固定 1、streamSeq 跟全局 seq 同步递增,
// 接收侧才能连续交付。用裸字节手拼载荷的话 parseShard 会当成越界长度拒收。
func dataShardPkt(group uint32, idx, k, m int, epoch uint32) []byte {
	seq := group*uint32(k) + uint32(idx)
	return dataShardPktOn(group, idx, k, m, epoch, 1, seq)
}

// dataShardPktOn 同 dataShardPkt,但可单独指定分片归属的 stream 与 stream 内序号。
// 全局 seq 仍由 (group, idx) 决定 —— 要在全局 seq 上留洞、同时让 stream 内序号
// 保持连续(这样空洞只影响全局记账、不影响交付)就得把两者解耦。
func dataShardPktOn(group uint32, idx, k, m int, epoch, sid, sseq uint32) []byte {
	pkt := make([]byte, hdrSize+shardPayload)
	seq := group*uint32(k) + uint32(idx)
	header{typ: pktData, shardIdx: byte(idx), k: byte(k), m: byte(m),
		group: group, seq: seq, epoch: epoch}.marshal(pkt)
	body := make([]byte, 8) // 每片承载 8 字节有效数据
	for i := range body {
		body[i] = byte(i)
	}
	marshalShard(pkt[hdrSize:], sid, sseq, body)
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
	s.writeStream(1, payload(written))

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
	go a.writeStream(1, data)
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
	go a.writeStream(1, head)
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
	go a.writeStream(1, tail)

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
	go a.writeStream(1, first)
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
	go a2.writeStream(1, second)
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
	s.recvSeen[5000] = true
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
	exp, mx, bufLen := s.expected, s.recvHigh, len(s.recvSeen)
	s.recvMu.Unlock()
	s.sendMu.Lock()
	next := s.nextSeq
	s.sendMu.Unlock()
	if exp != 0 || mx != 0 || bufLen != 0 {
		t.Fatalf("接收状态未重置: expected=%d recvHigh=%d recvSeen=%d", exp, mx, bufLen)
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

	// 单帧远大于一个分片(shardPayload-shardHdr),必然跨多个分片。
	// 帧头与数据一次性交给 writeStream —— 分开写会落进不同的 streamSeq,
	// 而接收侧现在按 stream 独立重组。
	big := payload(shardPayload * 5)
	frame := append([]byte{0, 0, 0, 9, cmdData, byte(len(big) >> 8), byte(len(big))}, big...)
	go a.writeStream(9, frame)
	got := collect(b, frameHdr+len(big), 20*time.Second)
	if len(got) != frameHdr+len(big) {
		t.Fatalf("跨分片帧收到 %d/%d 字节", len(got), frameHdr+len(big))
	}
	if fmt.Sprintf("%v", got[frameHdr:]) != fmt.Sprintf("%v", big) {
		t.Fatal("跨分片帧内容不一致")
	}
}

func TestShardRoundTrip(t *testing.T) {
	buf := make([]byte, shardPayload)
	want := payload(300)
	marshalShard(buf, 7, 12345, want)
	sid, sseq, got, ok := parseShard(buf)
	if !ok {
		t.Fatalf("parseShard 失败")
	}
	if sid != 7 || sseq != 12345 {
		t.Fatalf("归属解析错:sid=%d sseq=%d", sid, sseq)
	}
	if string(got) != string(want) {
		t.Fatalf("数据不一致:%d 字节 vs %d 字节", len(got), len(want))
	}
	// 截断的载荷必须被拒,不能读出越界数据
	if _, _, _, ok := parseShard(buf[:shardHdr-1]); ok {
		t.Fatalf("过短的载荷应当解析失败")
	}
	bad := make([]byte, shardHdr+10)
	binary.BigEndian.PutUint16(bad[8:10], 9999)
	if _, _, _, ok := parseShard(bad); ok {
		t.Fatalf("dataLen 超出载荷长度时应当解析失败")
	}
}

// 回归:高丢包 + 持续流量下,接收侧各记账结构必须有界。
//
// per-stream 重组拆掉了一个隐式自限 —— 从前 expected 卡住时交付也停,上层 TCP
// 收不到数据就不再发,流量自然降下来。现在交付不再受全局 expected 约束,而
// recvSeen 仍按全局 seq 记账,所以它必须自己守住有界,不能再指望背压兜底。
//
// 2026-08-01 实测(15% 丢包满载):recvSeen 峰值仅 14,groupDone 稳定在
// groupKeepWindow(128)。同日生产上一度观察到 RSS 18 分钟涨到 75 MB,查明是
// 25 并发压测(2056 包/s,稳态 38 包/s 的 54 倍)抬高的 Go 堆水位,不是泄漏 ——
// 撤掉压测后稳态回到 16 MB。这个测试守的就是"别真的变成泄漏"。
func TestReceiverStateStaysBoundedUnderLoss(t *testing.T) {
	px := newLossyProxy(t, 0.15, 5)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 100)
	b := newTestSession(t, px.port(), 20, 15, 100)
	defer a.close()
	defer b.close()
	time.Sleep(300 * time.Millisecond)

	// 上层持续消费 —— 新版的关键前提:交付不再被全局 expected 阻塞,
	// 所以流量不会因为某个 seq 卡住而自然停下来。
	go func() {
		for {
			select {
			case <-b.deliver:
			case <-b.closed:
				return
			}
		}
	}()

	stop := make(chan struct{})
	go func() {
		data := payload(32 << 10)
		for {
			select {
			case <-stop:
				return
			default:
				a.writeStream(1, data)
			}
		}
	}()

	var peakSeen int
	snap := func(label string) {
		b.recvMu.Lock()
		seen, groups, gdone, first := len(b.recvSeen), len(b.groups), len(b.groupDone), len(b.firstSeen)
		buffered := 0
		for _, r := range b.streamRecv {
			buffered += len(r.buf)
		}
		nstream, exp, high := len(b.streamRecv), b.expected, b.recvHigh
		b.recvMu.Unlock()
		a.sendMu.Lock()
		sbuf, sbytes := len(a.sendBuf), a.sendBufBytes
		a.sendMu.Unlock()
		if seen > peakSeen {
			peakSeen = seen
		}
		t.Logf("%s recvSeen=%-7d streams=%-3d streamBuf=%-6d groups=%-5d groupDone=%-6d firstSeen=%-6d gap=%-7d sendBuf=%d(%.1fMB)",
			label, seen, nstream, buffered, groups, gdone, first, high-exp, sbuf, float64(sbytes)/(1<<20))
	}

	for i := 1; i <= 8; i++ {
		time.Sleep(2 * time.Second)
		snap(fmt.Sprintf("t=%2ds", i*2))
	}
	close(stop)

	// recvSeen 是最可疑的:它只在 expected 连续推进时才删,没有窗口上限。
	// NACK 扫描窗口随收包速率缩放,记账结构理应与它可能达到的最大值同阶。
	if w := int(b.maxNackWin()); peakSeen > 4*w {
		t.Fatalf("recvSeen 峰值 %d 条,远超缺口窗口上限(%d)的量级 —— 按全局 seq 的记账失去上限",
			peakSeen, w)
	}
}

// 回归:nackLoop 发现的缺口必须被记账,且同一缺口只记一次。
//
// rawLost 从一开始就声明在 stats 结构里,却没有任何一处写它 —— 于是"有多少
// 分片没能被 FEC 当场救回"这个数在生产上完全不可观测。2026-08-01 排查队头
// 阻塞时,真实丢包率只能从 fecRecovered 与 nackSent 反推,就是因为缺这个数。
//
// 语义要说准:rawLost 记的是"nackLoop 扫描时仍缺失的 seq",这里面**包含
// 乱序造成的短暂缺口** —— 它们会在 120ms 内自愈,压根不会触发 NACK。
// 所以 rawLost 既不等于链路丢包(FEC 当场救回的不进统计),也不等于 ARQ 负担
// (自愈的那些不发 NACK)。2026-08-01 生产实测 缺口=6321 而 NACK=1291,
// 约八成缺口是自愈的。
func TestLostShardsAreCounted(t *testing.T) {
	px := newLossyProxy(t, 0, 33)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 500)
	b := newTestSession(t, px.port(), 20, 15, 500)
	defer a.close()
	defer b.close()
	time.Sleep(300 * time.Millisecond)

	// 先让一批正常通过,把 expected 推上去
	head := payload(shardPayload * 6)
	go a.writeStream(1, head)
	if got := collect(b, len(head), 15*time.Second); len(got) != len(head) {
		t.Fatalf("前置数据未通过: %d/%d", len(got), len(head))
	}

	// 构造整组丢失且连丢多轮。要丢够 18 片(> m=15)FEC 才真的救不回 ——
	// 自从有了 groupFlushAfter 的补零 flush,校验片不再依赖组自然攒满,
	// 少量丢失会被 FEC 当场接住、压根走不到 ARQ,这个测试就失去前置条件了。
	// dropTimes>1 是另一个关键:只有缺口持续存在,才能验证"反复 NACK 但只记一次账"。
	a.sendMu.Lock()
	from := a.nextSeq
	a.sendMu.Unlock()
	px.mu.Lock()
	px.dropEnabled = true
	px.dropTimes = 3
	px.dropFrom, px.dropTo = from, from+17
	px.mu.Unlock()

	tail := payload((shardPayload - shardHdr) * 20)
	go a.writeStream(1, tail)
	if got := collect(b, len(tail), 20*time.Second); len(got) != len(tail) {
		t.Fatalf("尾部数据未恢复: %d/%d", len(got), len(tail))
	}

	b.stats.Lock()
	lost, nacks, fec := b.stats.rawLost, b.stats.nackSent, b.stats.fecRecovered
	b.stats.Unlock()

	if nacks == 0 {
		t.Fatal("前置条件不成立:一个 NACK 都没发,没走到 ARQ 路径(测试无效)")
	}
	if lost == 0 {
		t.Fatalf("发了 %d 个 NACK、FEC 恢复 %d 片,rawLost 却是 0 —— 缺口没有被记账", nacks, fec)
	}
	// 去重校验:dropTimes=3 让每个洞至少被请求三轮,nackSent 会数倍于洞的个数,
	// 而 rawLost 只在首次发现时 +1。所以持久缺口下 rawLost 必须严格小于 nackSent。
	//
	// 注意不能反过来断言 rawLost <= nackSent 就算过 —— 那个不变量根本不成立:
	// 乱序造成的短暂缺口会计入 rawLost 却从不发 NACK,生产上实测 6321 vs 1291。
	if lost >= nacks {
		t.Fatalf("rawLost=%d 不小于 nackSent=%d —— 每个洞被反复 NACK 了三轮,"+
			"rawLost 却没有去重", lost, nacks)
	}
}

// 回归:低流量下 FEC 必须赶在 ARQ 前面。
//
// 校验片要等整组 k 片攒齐才发。低流量时攒不满,校验片就永远发不出去,FEC 形同
// 虚设 —— 丢一片只能靠 ARQ 的 120ms 判定加一个重传往返。
//
// 2026-08-01 生产实测:927 包/s 时攒一组 22ms,NACK 率 0.17%;流量掉到
// 110 包/s 后攒一组要 182ms、超过 120ms 判定,NACK 率升到 1.9% —— 那些重传
// 全是白跑的,校验片其实还在发送侧攒着。
//
// 断言用 fecRecovered 而非耗时:ARQ 最终也能把数据补回来,区别只在"谁救的"。
// 用时间阈值区分会在慢机器上 flaky,用语义就不会。
func TestFECCoversPartialGroupUnderLowTraffic(t *testing.T) {
	px := newLossyProxy(t, 0, 71)
	defer px.stop()
	a := newTestSession(t, px.port(), 20, 15, 500)
	b := newTestSession(t, px.port(), 20, 15, 500)
	defer a.close()
	defer b.close()
	time.Sleep(300 * time.Millisecond)

	// 只发 8 片 —— 不足 k=20 但超过 k/4 的 flush 门槛,靠自然攒永远满不了
	a.sendMu.Lock()
	from := a.nextSeq
	a.sendMu.Unlock()
	px.mu.Lock()
	px.dropEnabled = true
	px.dropFrom, px.dropTo = from+1, from+1 // 只丢第二片
	px.mu.Unlock()

	data := payload((shardPayload - shardHdr) * 8)
	go a.writeStream(1, data)

	got := collect(b, len(data), 5*time.Second)
	if len(got) != len(data) {
		t.Fatalf("数据未送达: %d/%d 字节", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("字节 %d 不一致", i)
		}
	}

	b.stats.Lock()
	fec, nacks := b.stats.fecRecovered, b.stats.nackSent
	b.stats.Unlock()

	if fec == 0 {
		t.Fatalf("丢了一片却没有任何 FEC 恢复(nackSent=%d)—— 组攒不满,"+
			"校验片压根没发出来,只能靠 ARQ 兜底", nacks)
	}
}

// 回归:撞上不可填补的空洞时,全局 expected 必须能继续推进。
//
// 2026-08-01 生产实测(落地侧 own-api-ko):待补涨到 40865 —— nackWindow 的 10 倍
// 且单调增长(约 +120 seq/s,正好是数据片到达速率),缺口计数冻在 154369 不动,
// 却仍以 76 NACK/s 往外发,而入口侧「重传」冻着不动 —— 这些 NACK 打的全是
// 对端 sendBuf 已淘汰的 seq,无人应答,那个空洞永远填不上。
//
// 后果有两条:recvSeen/firstSeen 永不回收(内存无界增长),以及 NACK 扫描窗口
// [expected, expected+nackWindow) 停在一段死区上空转,新缺口不再进入视野
// (缺口这个指标就此失真)。per-stream 交付把功能影响掩掉了,所以没人发现。
func TestExpectedAdvancesPastUnfillableHole(t *testing.T) {
	const k, m = 20, 15
	// 喂满 1200 组 = 24000 个 seq,是这个 session 缺口窗口上限(rate=100 下约
	// 11900)的 2 倍,足以把"待补无界"和"待补被钉在窗口内"两种行为区分开。
	// 同步注入会让实测收包速率瞬间顶到上限,所以必须按上限而不是下限来喂。
	const groups = 1200

	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	// peer 指向一个没人监听的端口:NACK 发得出去但永远没人应答,
	// 这正是生产里那个空洞的处境(对端 sendBuf 已经淘汰了那一片)。
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, k, m, 100)
	defer s.close()

	// 持续排空交付,否则 deliver(4096)填满后 onPacket 会阻塞在背压上。
	var delivered int64
	go func() {
		for {
			select {
			case p := <-s.deliver:
				atomic.AddInt64(&delivered, int64(len(p.data)))
			case <-s.closed:
				return
			}
		}
	}()

	// stream 内序号必须连续,空洞只留在全局 seq 上 —— 否则卡住的是 stream
	// 重组而不是全局 expected,测不到本例要测的东西。
	var sseq uint32
	feed := func(group uint32, idx int) {
		s.onPacket(dataShardPktOn(group, idx, k, m, 0, 2, sseq))
		sseq++
	}

	// group 0 只喂后 10 片:数据片 0..9 缺失,且到手分片数(10)不足 k,
	// FEC 也救不回来 —— 一个既补不上、也恢复不了的永久空洞。
	for idx := 10; idx < k; idx++ {
		feed(0, idx)
	}
	// 其余组全喂满,让 recvHigh 一路爬到远超 nackWindow 的位置。
	for g := 1; g <= groups; g++ {
		for idx := 0; idx < k; idx++ {
			feed(uint32(g), idx)
		}
	}

	// 给 nackLoop(40ms 一跳)几次机会,让它把该发的 NACK 发完。
	time.Sleep(300 * time.Millisecond)

	s.recvMu.Lock()
	gap := s.recvHigh - s.expected
	seen := len(s.recvSeen)
	first := len(s.firstSeen)
	win := s.nackWin
	s.recvMu.Unlock()

	if fed := uint32(groups * k); fed <= win {
		t.Fatalf("前置条件不成立:只喂了 %d 个 seq,没超过当前窗口 %d(测试无效)", fed, win)
	}
	if gap > win {
		t.Errorf("待补=%d 超出缺口窗口=%d:expected 卡死在填不上的空洞前,"+
			"NACK 扫描窗口停在死区空转", gap, win)
	}
	if seen > int(win) {
		t.Errorf("recvSeen=%d 条超出缺口窗口=%d:expected 不推进,"+
			"记账永不回收 —— 这就是生产上那份随流量线性增长的常驻内存", seen, win)
	}
	if first > int(win) {
		t.Errorf("firstSeen=%d 条超出缺口窗口=%d", first, win)
	}
	// 反向保险:修复不能是"干脆不收包了"。
	if n := atomic.LoadInt64(&delivered); n == 0 {
		t.Errorf("一个字节都没交付,空洞之后的分片本该照常送达")
	}
}

// shardPktFrom 把一个**已经编好码**的分片包成 UDP 包。
// 与 dataShardPkt 的区别是载荷由调用方给 —— 只有送进去的校验片是真的,
// Reconstruct 重构出来的数据片才是原样。喂假校验片的话重构结果是垃圾,
// parseShard 会解出随机的 streamID,污染 streamRecv 把测量搅浑。
func shardPktFrom(group uint32, idx, k, m int, epoch uint32, shard []byte) []byte {
	pkt := make([]byte, hdrSize+shardPayload)
	seq := group*uint32(k) + uint32(idx)
	header{typ: pktData, shardIdx: byte(idx), k: byte(k), m: byte(m),
		group: group, seq: seq, epoch: epoch}.marshal(pkt)
	copy(pkt[hdrSize:], shard)
	return pkt
}

// 回归:持续的 FEC 恢复不得留下常驻内存。
//
// reedsolomon 的逆矩阵缓存(inversionTree)按"本组缺了哪几片"的索引组合做 key
// 缓存求好的逆矩阵,**只插不删、没有任何容量上限**。它是为 RAID 设计的 ——
// 那里坏的是固定那几块盘,key 反复命中;而跨境链路每组丢的是随机的几片,
// C(k+m, d) 的模式空间近乎无穷,命中率约等于零。于是每恢复一组就永久多占
// 一份内存:每个树节点带 k+m 个 children 指针,叶子再挂一个 k×k 的矩阵。
//
// 2026-08-24 生产实测(入口侧 own-api-sz,k/m=40/40):9 天累计 FEC 恢复 235340
// 次,RSS 从 2.4 MB 涨到 214 MB(峰值 334 MB),合每次恢复约 900 B。同期 stream
// 新建/关闭 51673:51673 完全配对、对端 0 重启、待补峰值 32 —— fectun 自己那套
// 记账全在界内,内存不在本仓库的代码里,而在这个库的缓存里。机器总内存 894 MB
// 且无 swap,这一项就吃掉四分之一,且 MemoryMax=infinity 时会直接引来 OOM killer。
//
// k/m 从 20/20 调到 40/40 让每次恢复的代价从 950 B 涨到 3336 B(微基准实测):
// 矩阵 400 B → 1600 B,children 40 个指针 → 80 个。
func TestFECRecoveryMemoryIsBounded(t *testing.T) {
	// 生产参数
	const k, m = 40, 40
	// 每组丢 nMissing 片数据片,丢的位置逐组不同 —— 这正是真实链路的样子,
	// 也正是逆矩阵缓存永远命中不了的原因。
	const nMissing = 8
	const warmup = 200
	const measured = 2000

	enc, err := reedsolomon.New(k, m)
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, k, m, 1e6)
	defer s.close()

	// 消费 deliver,否则交付会在 channel 满时阻塞
	go func() {
		for {
			select {
			case <-s.deliver:
			case <-s.closed:
				return
			}
		}
	}()

	// feed 造一组真实编码过的分片,只投递其中 k 片(缺 nMissing 个数据片、
	// 补等量校验片),迫使接收侧走一次 Reconstruct。
	feed := func(g uint32) {
		shards := make([][]byte, k+m)
		for i := 0; i < k; i++ {
			sh := make([]byte, shardPayload)
			marshalShard(sh, 1, g*uint32(k)+uint32(i), []byte{1, 2, 3, 4, 5, 6, 7, 8})
			shards[i] = sh
		}
		for i := k; i < k+m; i++ {
			shards[i] = make([]byte, shardPayload)
		}
		if err := enc.Encode(shards); err != nil {
			t.Fatalf("encode group %d: %v", g, err)
		}
		// 每组一个不同的丢包模式:C(40,8) 有 7600 万种,2000 组几乎不会撞
		miss := rand.New(rand.NewSource(int64(g) + 1)).Perm(k)[:nMissing]
		gone := make(map[int]bool, nMissing)
		for _, i := range miss {
			gone[i] = true
		}
		for i := 0; i < k; i++ {
			if !gone[i] {
				s.onPacket(shardPktFrom(g, i, k, m, 1, shards[i]))
			}
		}
		// 补上等量校验片,凑够 k 片才够重构
		for i := k; i < k+nMissing; i++ {
			s.onPacket(shardPktFrom(g, i, k, m, 1, shards[i]))
		}
	}

	// 先热身,把一次性开销(编码表、map 初始桶、goroutine 栈)排除在测量之外
	for g := uint32(0); g < warmup; g++ {
		feed(g)
	}
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for g := uint32(warmup); g < warmup+measured; g++ {
		feed(g)
	}
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	s.stats.Lock()
	recovered := s.stats.fecRecovered
	s.stats.Unlock()
	s.recvMu.Lock()
	expected, groups, gdone, nstream := s.expected, len(s.groups), len(s.groupDone), len(s.streamRecv)
	s.recvMu.Unlock()

	// 前置条件:恢复必须真的发生过,否则这个测试什么都没测
	wantRecovered := uint64((warmup + measured) * nMissing)
	if recovered != wantRecovered {
		t.Fatalf("前置条件不成立:FEC 恢复 %d 次,应为 %d —— 重构没走到,测试无效", recovered, wantRecovered)
	}
	if expected != (warmup+measured)*k {
		t.Fatalf("前置条件不成立:expected=%d,应为 %d —— 数据没被正常交付,测试无效",
			expected, (warmup+measured)*k)
	}
	// 前置条件:fectun 自己那几个记账 map 必须是平的,否则涨的是它们、不是缓存
	if groups > 512 || gdone > 512 || nstream > 4 {
		t.Fatalf("前置条件不成立:groups=%d groupDone=%d streamRecv=%d 自身记账已失界,测量不可归因",
			groups, gdone, nstream)
	}

	grown := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	perRecovery := float64(grown) / float64(measured)
	t.Logf("%d 组恢复后常驻堆增量 %.1f MB,合每组 %.0f B(HeapAlloc %.1f → %.1f MB,存活对象 %d → %d)",
		measured, float64(grown)/1e6, perRecovery,
		float64(before.HeapAlloc)/1e6, float64(after.HeapAlloc)/1e6,
		before.HeapObjects, after.HeapObjects)

	// 恢复一组之后不该留下任何按组累积的常驻内存。留 256 B 的余量给测量噪声 ——
	// 生产上这一项是 900 B/组,微基准在同参数下是 3336 B/组,余量足够宽。
	if perRecovery > 256 {
		t.Fatalf("FEC 恢复留下常驻内存:每组 %.0f B,%d 组共 %.1f MB 且随恢复次数线性增长 —— "+
			"照生产上 9 天 23.5 万次恢复的量级,这就是 200 MB 常驻",
			perRecovery, measured, float64(grown)/1e6)
	}
}

// saturate 经 mux 用 4 条 stream 满负载写 dur,返回写入字节数、误判重置次数和首个写错误。
//
// 必须走 mux:直接调 writeStream 会绕过流控窗口,接收侧一有缺口重组缓冲就无界
// 增长 —— 那个版本常驻堆 241 MB,2026-09-28 放到 894 MB 的生产机上跑直接把机器
// 打爆重启。这些测试只在本机(或容器)里跑。
func saturate(t *testing.T, rateMbps, ccFloorMbps, burstLoss float64, dur time.Duration) (sent int64, resets int32, werr error) {
	t.Helper()
	sink, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("sink listen: %v", err)
	}
	defer sink.Close()
	go func() {
		for {
			c, err := sink.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(io.Discard, c); c.Close() }()
		}
	}()

	px := newLossyProxy(t, 0, 91)
	defer px.stop()
	if burstLoss > 0 {
		px.setBurst(burstLoss, 200*time.Millisecond)
	}
	cli := newTestSession(t, px.port(), 40, 40, rateMbps)
	srv := newTestSession(t, px.port(), 40, 40, rateMbps)
	defer cli.close()
	defer srv.close()
	if ccFloorMbps > 0 {
		cli.enableCC(ccFloorMbps)
		srv.enableCC(ccFloorMbps)
	}
	time.Sleep(300 * time.Millisecond)

	cliMux := newMuxer(cli, false, "")
	newMuxer(srv, true, sink.Addr().String())
	var nReset atomic.Int32
	for _, s := range []*session{cli, srv} {
		orig := s.onReset
		s.onReset = func() { nReset.Add(1); orig() }
	}

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			cliMux.openStream(c)
		}
	}()

	chunk := payload(64 << 10)
	end := time.Now().Add(dur)
	var wg sync.WaitGroup
	var total atomic.Int64
	var firstErr atomic.Value
	for i := 0; i < 4; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(end) && nReset.Load() == 0 {
				c.SetWriteDeadline(time.Now().Add(8 * time.Second))
				n, err := c.Write(chunk)
				total.Add(int64(n))
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("rate=%.0f ccFloor=%.0f burst=%.0f%%: %s 写入 %d MB | cli %s | srv %s", rateMbps, ccFloorMbps,
		burstLoss*100, dur, total.Load()>>20, cli.statsLine(), srv.statsLine())
	if e, ok := firstErr.Load().(error); ok {
		werr = e
	}
	return total.Load(), nReset.Load(), werr
}

// 满负载下不得误判对端重置,也不得卡死。
//
// 两个历史问题都在这里撞:
//   - 心跳读完 nextSeq 放锁后才发,被数据片抢先,接收端连续 3 次看到"报低"就误判
//     对端重置。2026-09-28 实测固定 rate=200、k=m=40 时 2~7 秒必现。
//   - 固定 rate≥200 时突发丢包压垮 ARQ,缺片在 NACK 到达前就被挤出重传缓冲,
//     stream 永久挂住。拥塞控制把速率压回链路(这里是 2 核接收端)承载得了的量。
func TestSaturatedFlowDoesNotFalseReset(t *testing.T) {
	_, resets, err := saturate(t, 200, 10, 0, 10*time.Second)
	if resets != 0 {
		t.Fatalf("对端没有重启,满负载 10 秒内却误判了 %d 次重置", resets)
	}
	if err != nil {
		t.Fatalf("写入中途失败:%v —— 隧道卡死或 stream 被断开", err)
	}
}

// 验收:突发丢包下,拥塞控制(上限 200)的吞吐不低于原来的固定 rate=50。
// 丢包模型取评估台的 10%、坏态 200ms。
func TestCCBeatsFixedRateUnderBurstLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("对照实验要跑 20 秒")
	}
	fixed, _, ferr := saturate(t, 50, 0, 0.1, 10*time.Second)
	cc, resets, err := saturate(t, 200, 10, 0.1, 10*time.Second)
	if ferr != nil {
		t.Fatalf("固定 rate=50 的对照组就失败了:%v", ferr)
	}
	if err != nil || resets != 0 {
		t.Fatalf("拥塞控制组失败:err=%v resets=%d", err, resets)
	}
	if float64(cc) < float64(fixed)*0.95 {
		t.Fatalf("拥塞控制 %d MB 不如固定 rate=50 的 %d MB", cc>>20, fixed>>20)
	}
}
