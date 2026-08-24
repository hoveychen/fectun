package main

import (
	"fmt"
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

// groupFlushAfter:一组攒不满时,最多等这么久就补零片凑满、把校验片发出去。
//
// 必须显著小于 NACK 的 120ms 缺口判定 —— 否则校验片到得比重传请求还晚,
// FEC 白攒。2026-08-01 生产实测:927 包/s 时攒满一组只要 22ms,FEC 轻松赶在
// 前面(NACK 率 0.17%);流量掉到 110 包/s 后攒一组要 182ms,远超 120ms 判定,
// NACK 率升到 1.9%,那些重传全是白跑的。
const groupFlushAfter = 40 * time.Millisecond

// groupFlushMinShards 的分母:组内至少攒到 k/该值 片才值得补零 flush。
//
// 补零是有代价的 —— 攒了 j 片就要补 (k-j) 个空片再加 m 个校验片,发包量
// 放大 (k+m)/j 倍。极端情况(每秒才一个包)下 j=1,放大 40 倍,纯属自残。
// 取 k/4 是个折中:110 包/s 下攒 5 片约 45ms,放大 7 倍,而低流量时带宽本就
// 富余;真到了每秒几个包的量级就不再 flush,退回 ARQ 兜底。
//
// 标注:这个分母是权衡取的,不是实测最优值。要调优得先有低流量档的压测数据。
const groupFlushMinDiv = 4

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

// streamChunk 是交付给上层的一段数据,带上它属于哪条 stream。
// 从前 deliver 是裸的 chan []byte —— 一条全局字节流,顺序由全局 seq 决定,
// 那正是队头阻塞的根。现在归属随数据一起交付,上层按 stream 各自切帧。
type streamChunk struct {
	sid  uint32
	data []byte
}

// streamReasm 是单条 stream 的重组状态。
// 每条 stream 独立按 streamSeq 严格有序交付:TCP 字节流仍要求完整有序,
// 但"有序"的范围缩到了单条 stream 内,不再是整条隧道。
type streamReasm struct {
	expected uint32            // 下一个待交付的 streamSeq
	buf      map[uint32][]byte // 乱序先到的分片
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
	curGroup   []([]byte) // 当前 FEC 组累积的数据分片
	curGroupAt time.Time  // 本组第一片的入组时刻,用于超时 flush
	sendBuf  map[uint32][]byte
	// 重传缓冲的字节账 + 淘汰游标(最老的仍可能在册的 seq)。
	// 记着字节数才能按 sendBufBudget 封顶;记着游标才能 O(1) 淘汰最老的,
	// 不必像原先那样每次超限就遍历整个 map。
	sendBufBytes  int
	sendBufTail   uint32
	sendBufBudget int
	// flushMin / flushAfter 是补零 flush 的两个门槛,提成字段只为可调可测
	// (默认值仍是 groupFlushMinDiv / groupFlushAfter,行为不变)。
	// 低流量下这两个值决定 FEC 校验片能不能赶在 NACK 的 120ms 判定之前发出去 ——
	// 而 groupFlushAfter 的注释只算了"等待 40ms",漏算了"先攒够 flushMin 片"
	// 本身要多久:20 包/s 下攒够 k/4=5 片就要 250ms,校验片必然晚于重传请求。
	flushMin   int
	flushAfter time.Duration
	// retransCopies:应对端 NACK 时同一分片发几份。默认 1(行为不变)。见 onNack。
	retransCopies int
	// asyncRetrans:把重传发送挪出 readLoop 的同步链。默认 false(行为不变)。
	asyncRetrans bool
	retransCh    chan uint32
	// 每条 stream 独立的发送序号。接收侧按 (streamID, streamSeq) 重组,
	// 所以这个序号必须逐 stream 连续,不能跟全局 seq 混用。
	sendSeqOf map[uint32]uint32
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
	// recvSeen 只记"这个全局 seq 收到过",不再囤载荷。
	//
	// 从前这里是 map[uint32][]byte,分片要一直留到全局 expected 连续推进到它
	// 才交付 —— expected 处一出深空洞,其后所有分片就无上限堆积(2026-08-01
	// 入口侧实测 RSS 74.9 MB)。现在分片一到就按归属分发进 streamRecv,
	// 这里只留一个标记:全局 seq 的唯一用途是 NACK 缺口检测。
	recvSeen  map[uint32]bool
	groups    map[uint32][][]byte // group -> 分片槽位(含校验片)
	groupDone map[uint32]bool
	firstSeen map[uint32]time.Time // seq 缺口首次发现时间,用于 NACK 定时

	// streamRecv 是逐 stream 的重组缓冲。一条 stream 的空洞只挡它自己,
	// 别的 stream 照常交付 —— 这就是解开重组侧队头阻塞的地方。
	// 堆积量受该 stream 的流控窗口(最大 4 MB)约束,天然有界。
	streamRecv map[uint32]*streamReasm
	deliver    chan streamChunk

	stats struct {
		sync.Mutex
		rawRecv, rawLost, fecRecovered, nackSent, retransSent uint64
	}
	closed chan struct{}
}

func newSession(conn *net.UDPConn, peer *net.UDPAddr, k, m int, rateMbps float64) *session {
	// 必须关掉逆矩阵缓存 —— 它是 reedsolomon 里唯一一处只插不删、没有容量上限的
	// 结构。缓存按"本组缺了哪几片"的索引组合做 key 存求好的逆矩阵:每个树节点带
	// k+m 个 children 指针,叶子再挂一个 k×k 矩阵。这在 RAID 场景是划算的(坏的是
	// 固定那几块盘,key 反复命中),但跨境链路每组丢的是随机的几片,C(k+m,d) 的
	// 模式空间近乎无穷,命中率约等于零 —— 纯付内存不换任何东西。
	//
	// 2026-08-24 生产实测(入口侧 own-api-sz,k/m=40/40):9 天累计 FEC 恢复 235340
	// 次,RSS 从 2.4 MB 涨到 214 MB(峰值 334 MB),合每次约 900 B,占了 894 MB 机器
	// 的四分之一且无 swap 兜底。同期 stream 新建/关闭 51673:51673 完全配对、对端
	// 0 重启、待补峰值 32,本仓库自己那套记账全在界内 —— 内存全在这个缓存里。
	//
	// 代价是每次重构都要重求一次逆矩阵。微基准(k/m=40/40,每组丢 8 片,20000 次)
	// 实测无可测代价:开缓存 45.8/55.3/59.7 µs、关缓存 53.6/49.4/40.4 µs,区间重叠 ——
	// 不断膨胀的缓存树给 GC 的压力抵掉了省下的那次求逆。
	enc, _ := reedsolomon.New(k, m, reedsolomon.WithInversionCache(false))
	s := &session{
		conn: conn, peer: peer, k: k, m: m, enc: enc,
		sendBuf:    make(map[uint32][]byte),
		sendSeqOf:  make(map[uint32]uint32),
		recvSeen:   make(map[uint32]bool),
		groups:     make(map[uint32][][]byte),
		groupDone:  make(map[uint32]bool),
		firstSeen:  make(map[uint32]time.Time),
		streamRecv: make(map[uint32]*streamReasm),
		deliver:    make(chan streamChunk, 4096),
		closed:    make(chan struct{}),
		rateBps:   rateMbps * 1e6 / 8,
		lastFill:  time.Now(),
	}
	s.sendBufBudget = sendBufBudgetFor(s.rateBps)
	s.flushMin = k / groupFlushMinDiv
	if s.flushMin < 2 {
		s.flushMin = 2
	}
	s.flushAfter = groupFlushAfter
	s.retransCopies = 1
	s.retransCh = make(chan uint32, 1024)
	s.myEpoch = uint32(time.Now().UnixNano())
	if s.myEpoch == 0 {
		s.myEpoch = 1
	}
	s.tokens = s.rateBps * 0.05
	go s.heartbeatLoop()
	go s.nackLoop()
	go s.groupFlushLoop()
	go s.retransLoop()
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

// writeStream 写入一段属于 sid 的字节流:切片 → 攒够 k 片做 FEC → 发送 k+m 包。
//
// 分片不跨 stream:每一片只承载一条 stream 的数据,并在载荷里带上
// (streamID, streamSeq)。接收侧据此独立重组,一条 stream 的空洞不再挡住别人。
// 代价是小帧(窗口更新、open/close)会独占一片、填不满 —— 实测这类控制帧
// 占比不到 1%,换掉队头阻塞值得。
func (s *session) writeStream(sid uint32, p []byte) {
	for len(p) > 0 {
		n := len(p)
		if n > shardPayload-shardHdr {
			n = shardPayload - shardHdr
		}
		s.sendMu.Lock()
		sseq := s.sendSeqOf[sid]
		s.sendSeqOf[sid] = sseq + 1
		s.sendMu.Unlock()

		shard := make([]byte, shardPayload)
		marshalShard(shard, sid, sseq, p[:n])
		p = p[n:]
		s.pushShard(shard)
	}
}

// groupFlushLoop 定期把攒了一半、迟迟满不了的 FEC 组补零发出。
func (s *session) groupFlushLoop() {
	t := time.NewTicker(groupFlushAfter / 2)
	defer t.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
			s.flushStaleGroup()
		}
	}
}

// flushStaleGroup 把超时未满的组补零片凑满 k,好让校验片发得出去。
//
// 必须补零片,不能跳 seq:发送侧跳过的 seq 在接收侧是永远补不上的空洞 ——
// sendBuf 里根本没有那一片,对端 NACK 过来 onNack 也重传不出来,
// expected 就此卡死。补零片让 seq 保持连续,接收侧一行都不用改;
// 零片的载荷 dataLen=0,acceptShardLocked 那边自然不会交付任何字节。
func (s *session) flushStaleGroup() {
	for {
		s.sendMu.Lock()
		n := len(s.curGroup)
		stale := n >= s.flushMin && n < s.k && time.Since(s.curGroupAt) >= s.flushAfter
		s.sendMu.Unlock()
		if !stale {
			return
		}
		pad := make([]byte, shardPayload)
		marshalShard(pad, 0, 0, nil)
		s.pushShard(pad) // 攒满那一刻 pushShard 自己会把校验片发出去
	}
}

// forgetStream 清掉某条 stream 的收发记账。
// 不清就是每条 stream 一条、随累计连接数线性增长的常驻内存 —— 入口侧 4422
// 暴露在公网被 ~2 次/秒 高频连接,2026-07-31 实测 40 分钟积压 2626 条 stream。
func (s *session) forgetStream(sid uint32) {
	s.sendMu.Lock()
	delete(s.sendSeqOf, sid)
	s.sendMu.Unlock()

	s.recvMu.Lock()
	delete(s.streamRecv, sid)
	s.recvMu.Unlock()
}

func (s *session) pushShard(shard []byte) {
	s.sendMu.Lock()
	seq := s.nextSeq
	s.nextSeq++
	if len(s.curGroup) == 0 {
		s.curGroupAt = time.Now()
	}
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

// statsLine 拼一行运行统计。
//
// 分两次取锁而非嵌套:onPacket 持 recvMu 期间会去拿 stats 锁,这里若反序
// 嵌套(先 stats 再 recvMu)就是经典的锁顺序反转,压力下会死锁。
//
// 各字段语义:
//   收包   本端收到的数据分片总数
//   缺口   FEC 没能当场救回、进了 NACK 视野的分片数(每个缺口只计一次)
//   FEC恢复 靠校验片重构回来的分片数
//   NACK   本端发出的重传请求数(同一缺口退避后会重发,故 ≥ 缺口数)
//   重传   本端应对端请求发出的重传数
//   待补   此刻仍在等的 seq 跨度(recvHigh-expected),持续不落零说明有洞补不上
//   stream 当前有重组状态的 stream 数
func (s *session) statsLine() string {
	s.recvMu.Lock()
	gap := s.recvHigh - s.expected
	streams := len(s.streamRecv)
	s.recvMu.Unlock()

	s.stats.Lock()
	defer s.stats.Unlock()
	return fmt.Sprintf("收包=%d 缺口=%d FEC恢复=%d NACK=%d 重传=%d 待补=%d stream=%d",
		s.stats.rawRecv, s.stats.rawLost, s.stats.fecRecovered,
		s.stats.nackSent, s.stats.retransSent, gap, streams)
}
