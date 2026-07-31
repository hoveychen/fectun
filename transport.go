package main

import (
	"net"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

const shardPayload = maxShard - hdrSize // 每分片可承载的字节数

// groupKeepWindow:即使一个组的全部 seq 都已交付,仍保留这么多组的记账,
// 用来吸收迟到的重传与重复包(它们只会落在最近几个组里)。
// 超出窗口的组记录必须删除 —— 否则 groupDone 每组一条、永不释放。
const groupKeepWindow = 128

// nackWindow:单次 NACK 缺口扫描最多往 expected 之后看这么多 seq。
//
// 没有这个上限,单端重启后 recvHigh 会被对端的旧 nextSeq 一下顶到几百万,
// nackLoop 首个 tick 就从 expected 一路扫到 recvHigh,给 firstSeen 建几百万条
// 记录(map[uint32]time.Time 约 40 B/条)—— 2026-07-31 的生产事故里这一项
// 吃掉 202 MB 常驻内存。
//
// 限制窗口不损失能力:ARQ 只需要修补最近的缺口,远处的缺口等 expected 推进
// 上来后自然进入窗口。
const nackWindow = 4096

// lowHbResetStreak:连续多少个心跳报告更低的 nextSeq,才判定对端重置了序号空间。
//
// 判据不能用"回退幅度"—— 幅度取决于对端重置前发过多少分片,2026-07-31 生产上
// 实测只有 65 → 0。之前拿 nackWindow(4096) 当幅度阈值,导致判据永不触发,两端
// 各自死等一段对方已不存在的 seq。
//
// 改用连续次数:心跳 100ms 一个,3 次 = 300ms。UDP 乱序不会持续这么久(一个迟到
// 的旧心跳只会偶发报低值),而真正的重置会一直报低值。
const lowHbResetStreak = 3

// sendBufBudgetFor 算出重传缓冲的字节预算:取 1 秒的在途数据量,足以覆盖
// NACK 的 120ms 缺口判定加一个重传往返。
//
// 必须按字节而非包数封顶:原先的"16384 个分片"在 1184 B/片下等于 19.4 MB,
// 是发送侧常驻内存的主要来源,且这个数字会随 maxShard 变化悄悄漂移。
// 也必须跟着 -rate 走而不是写死字节数 —— 写死的话高 rate 下预算不足一个
// NACK 往返的数据量,被请求重传的分片在请求到达前就已淘汰,ARQ 兜底失效。
func sendBufBudgetFor(rateBps float64) int {
	b := int(rateBps) // 1 秒
	if b < 512<<10 {
		b = 512 << 10
	}
	if b > 16<<20 {
		b = 16 << 20
	}
	return b
}

type session struct {
	conn *net.UDPConn
	peer *net.UDPAddr
	k, m int
	enc  reedsolomon.Encoder

	// 会话 epoch:启动时随机生成。对端重启后 epoch 变化,
	// 收到新 epoch 即重置序号空间 —— 否则双方 seq 对不上会永久死锁。
	myEpoch   uint32
	peerEpoch uint32
	onReset   func() // 通知上层(mux)关闭所有 stream

	// ---- 发送侧 ----
	sendMu   sync.Mutex
	nextSeq  uint32
	curGroup []([]byte) // 当前 FEC 组累积的数据分片
	sendBuf  map[uint32][]byte
	// 重传缓冲的字节账 + 淘汰游标(最老的仍可能在册的 seq)。
	// 记着字节数才能按 sendBufBudget 封顶;记着游标才能 O(1) 淘汰最老的,
	// 不必像原先那样每次超限就遍历整个 map。
	sendBufBytes  int
	sendBufTail   uint32
	sendBufBudget int
	tokens        float64 // 令牌桶
	lastFill      time.Time
	rateBps       float64

	// ---- 接收侧 ----
	recvMu   sync.Mutex
	expected uint32
	// recvHigh 是"已知对端发出过的 seq"的开区间上界(= 最大已见 seq + 1)。
	// 用开区间而非"最大已见 seq":后者初值 0 与"真的见过 seq 0"无法区分,
	// 会让 NACK 循环把还不存在的 seq 0 当成缺口,空载时无限重传请求。
	recvHigh uint32
	// lowHbStreak 是"连续收到多少个报告更低 nextSeq 的心跳"。见 lowHbResetStreak。
	lowHbStreak int
	// prunedGroup 是记账淘汰游标:组号 < 该值的组已全部交付完毕,
	// 其 groups/groupDone 记录已被删除,后续再收到这些组的重复包一律忽略。
	prunedGroup uint32
	recvBuf     map[uint32][]byte   // seq -> 已到达的数据分片
	groups      map[uint32][][]byte // group -> 分片槽位(含校验片)
	groupDone   map[uint32]bool
	firstSeen   map[uint32]time.Time // seq 缺口首次发现时间,用于 NACK 定时
	deliver     chan []byte

	stats struct {
		sync.Mutex
		rawRecv, rawLost, fecRecovered, nackSent, retransSent uint64
	}
	closed chan struct{}
}

func newSession(conn *net.UDPConn, peer *net.UDPAddr, k, m int, rateMbps float64) *session {
	enc, _ := reedsolomon.New(k, m)
	s := &session{
		conn: conn, peer: peer, k: k, m: m, enc: enc,
		sendBuf:   make(map[uint32][]byte),
		recvBuf:   make(map[uint32][]byte),
		groups:    make(map[uint32][][]byte),
		groupDone: make(map[uint32]bool),
		firstSeen: make(map[uint32]time.Time),
		deliver:   make(chan []byte, 4096),
		closed:    make(chan struct{}),
		rateBps:   rateMbps * 1e6 / 8,
		lastFill:  time.Now(),
	}
	s.sendBufBudget = sendBufBudgetFor(s.rateBps)
	s.myEpoch = uint32(time.Now().UnixNano())
	if s.myEpoch == 0 {
		s.myEpoch = 1
	}
	s.tokens = s.rateBps * 0.05
	go s.heartbeatLoop()
	go s.nackLoop()
	return s
}

// 令牌桶:实测持续满负载会把链路丢包从 10% 推到 30%,必须限速
func (s *session) acquire(n int) {
	for {
		s.sendMu.Lock()
		now := time.Now()
		s.tokens += s.rateBps * now.Sub(s.lastFill).Seconds()
		s.lastFill = now
		if cap := s.rateBps * 0.1; s.tokens > cap {
			s.tokens = cap
		}
		if s.tokens >= float64(n) {
			s.tokens -= float64(n)
			s.sendMu.Unlock()
			return
		}
		need := (float64(n) - s.tokens) / s.rateBps
		s.sendMu.Unlock()
		time.Sleep(time.Duration(need * float64(time.Second)))
	}
}

// 心跳:维持 NAT/安全组 conntrack。实测不发心跳则单向 UDP 被全丢。
func (s *session) heartbeatLoop() {
	// 100ms 一次:既维持 conntrack,又让对端及时发现尾部缺口。
	// 16 字节 * 10/s ≈ 1.3 kbps,开销可忽略。
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	buf := make([]byte, hdrSize)
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
			s.sendMu.Lock()
			next := s.nextSeq
			s.sendMu.Unlock()
			// seq 携带"我已发出的分片总数",对端据此判断是否有尾部丢失
			header{typ: pktHeartbeat, seq: next, epoch: s.myEpoch}.marshal(buf)
			s.conn.WriteToUDP(buf, s.peer)
		}
	}
}

// 写入一段字节流:切片 → 攒够 k 片做 FEC → 发送 k+m 包
func (s *session) write(p []byte) {
	for len(p) > 0 {
		n := len(p)
		if n > shardPayload-2 {
			n = shardPayload - 2
		}
		shard := make([]byte, shardPayload)
		shard[0] = byte(n >> 8)
		shard[1] = byte(n)
		copy(shard[2:], p[:n])
		p = p[n:]
		s.pushShard(shard)
	}
}

func (s *session) pushShard(shard []byte) {
	s.sendMu.Lock()
	seq := s.nextSeq
	s.nextSeq++
	s.curGroup = append(s.curGroup, shard)
	s.sendBuf[seq] = shard
	s.sendBufBytes += len(shard)
	// 按字节预算淘汰最老的分片。游标只往前走,所以摊还成本是 O(1)。
	for s.sendBufBytes > s.sendBufBudget && s.sendBufTail < seq {
		if old, ok := s.sendBuf[s.sendBufTail]; ok {
			s.sendBufBytes -= len(old)
			delete(s.sendBuf, s.sendBufTail)
		}
		s.sendBufTail++
	}
	group := seq / uint32(s.k)
	full := len(s.curGroup) == s.k
	var batch [][]byte
	if full {
		batch = s.curGroup
		s.curGroup = nil
	}
	s.sendMu.Unlock()

	// 数据片立即发出(不等整组,降低延迟)
	pkt := make([]byte, hdrSize+shardPayload)
	header{typ: pktData, shardIdx: byte(seq % uint32(s.k)), k: byte(s.k), m: byte(s.m),
		group: group, seq: seq, epoch: s.myEpoch}.marshal(pkt)
	copy(pkt[hdrSize:], shard)
	s.acquire(len(pkt))
	s.conn.WriteToUDP(pkt, s.peer)

	if !full {
		return
	}
	// 整组攒齐 → 生成校验片
	shards := make([][]byte, s.k+s.m)
	copy(shards, batch)
	for i := s.k; i < s.k+s.m; i++ {
		shards[i] = make([]byte, shardPayload)
	}
	if err := s.enc.Encode(shards); err != nil {
		return
	}
	for i := s.k; i < s.k+s.m; i++ {
		p2 := make([]byte, hdrSize+shardPayload)
		header{typ: pktData, shardIdx: byte(i), k: byte(s.k), m: byte(s.m), group: group, epoch: s.myEpoch}.marshal(p2)
		copy(p2[hdrSize:], shards[i])
		s.acquire(len(p2))
		s.conn.WriteToUDP(p2, s.peer)
	}
}

// readLoop 持续从 UDP 读包并投递给 onPacket。
// 提取为方法以便测试复用(生产由 main 启动,测试直接调用)。
func (s *session) readLoop() {
	buf := make([]byte, 2048)
	for {
		n, _, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p := make([]byte, n)
		copy(p, buf[:n])
		s.onPacket(p)
	}
}

// close 停止后台协程(心跳/NACK),测试清理用。
func (s *session) close() {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	s.conn.Close()
}
