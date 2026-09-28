package fectun

import (
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// 在可靠字节流之上做帧,实现多连接复用。
// 帧格式: [4B streamID][1B cmd][2B payloadLen][payload...]
const (
	cmdData     = 0
	cmdOpen     = 1
	cmdClose    = 2
	cmdShutdown = 3 // 半关闭:我不再发数据,但仍要接收(TCP 允许单向关闭)
	cmdWindow   = 4 // 逐 stream 流控:payload 4 字节 = 累计已消费字节数
	frameHdr    = 7
)

// 每条 stream 的流控窗口 = "已发出但对端尚未消费"的字节上限。
//
// 有了这个上限,接收侧的写队列就永远不会溢出 —— 这是"可靠字节流 + 内存有界 +
// 一条慢 stream 不影响其他 stream"三者同时成立的唯一办法。少了流控,三者必舍其一:
// 队列满了阻塞就卡住别人,满了丢弃就撕坏字节流,不设上限就是无界内存。
//
// 窗口必须跟着 -rate 走,不能写死。单条 stream 的吞吐上限 = window / RTT:
// 实测 rate=25、RTT=170ms 时,固定 256 KB 窗口把吞吐压到 12.0 Mbps
// (理论 256KB/170ms = 12.3),而净上限是 14.3 —— 已经开始约束。rate 调高后
// 固定窗口就会变成严重瓶颈(rate=60 净 34 Mbps,256KB/170ms 只够 12 Mbps)。
const (
	// windowSpan:窗口按"多少秒的在途数据"来定。覆盖到 RTT 200ms 的链路,
	// 同时不让每条 stream 的接收队列上限失控。
	windowSpan   = 0.2
	minStreamWin = 128 << 10
	maxStreamWin = 4 << 20
	// frameChunk:pumpToTunnel 的切帧大小,队列深度按它换算。
	frameChunk = 8192
)

// streamWindowFor 按线路速率与 FEC 冗余度算出每条 stream 的窗口字节数。
// 用净速率(扣掉 m/k 冗余)而非线路速率 —— 冗余分片不承载用户数据。
func streamWindowFor(rateBps float64, k, m int) uint32 {
	netBps := rateBps
	if k > 0 {
		netBps = rateBps / (1 + float64(m)/float64(k))
	}
	w := int(netBps * windowSpan)
	if w < minStreamWin {
		w = minStreamWin
	}
	if w > maxStreamWin {
		w = maxStreamWin
	}
	return uint32(w)
}

// halfCloseLinger:本端已读完(sentEOF)、但对端迟迟不回 cmdShutdown 时,最多再等多久。
//
// 半关闭是合法语义(发完请求等响应),所以不能太短;但没有上限就会积压 ——
// 2026-07-31 实测:入口侧 4422 暴露在公网被以 ~2 次/秒 高频连接,每条"连上就断"
// 的连接都留下一条 stream 及其 goroutine,40 分钟积压 2626 条、约 42 MB 且不回落。
// 落地侧的 sshd 在 LoginGraceTime 内只是静静等待,不会回 EOF,所以对端的
// cmdShutdown 永远不来。
//
// 是变量而非常量:测试要把它调短。用原子量是因为前一个测试残留的 stream 协程
// 可能恰在此时读它(-race 实测)。存的是纳秒数。
var halfCloseLinger atomic.Int64

func init() { halfCloseLinger.Store(int64(60 * time.Second)) }

// 累计消费超过窗口的一半就回一个窗口更新帧:既不会每帧都回(白占隧道带宽),
// 又能让发送方始终有额度。

// 写队列元素。关闭动作也当成队列元素排进去,这样它一定排在此前的数据之后 ——
// 直接关连接会把队列里尚未写出的数据丢掉(加队列后踩过:半关闭恰好在 48 帧
// 队列深度上截断了数据)。
const (
	wqData     = 0 // 普通数据
	wqShutdown = 1 // 排空后关写方向(半关闭)
	wqClose    = 2 // 排空后整条关闭
)

type wqItem struct {
	kind byte
	data []byte
}

type muxer struct {
	sess     *session
	isServer bool
	target   string

	mu      sync.Mutex
	streams map[uint32]*stream
	nextID  uint32
	// 发送侧串行化:可靠层要求帧字节连续不交错
	writeMu sync.Mutex

	// 流控参数,由 session 的速率与冗余度算出(见 streamWindowFor)
	window     uint32
	ackEvery   uint32 // 累计消费多少字节就回一次窗口更新
	queueDepth int    // 接收侧写队列深度(帧数)

	// recvLoop 帧缓冲的代际。resetAll 递增它,recvLoop 据此丢掉卡在半路的
	// 残帧 —— 缓冲是 recvLoop 的局部变量,除此之外没有别的办法够到它。
	resetGen atomic.Uint64
}

type stream struct {
	id      uint32
	conn    net.Conn
	mux     *muxer
	closed  bool
	sentEOF bool // 本端已读完,已通知对端
	recvEOF bool // 对端已读完
	mu      sync.Mutex

	// ---- 接收侧:写队列 ----
	// dispatch 只入队,由 writeLoop 落到下游 TCP。这样某条 stream 的下游写阻塞
	// 不会卡住 mux.recvLoop,其他 stream 照常交付。
	wq   chan wqItem   // 数据与关闭动作都走这里,保证按序、且关闭前队列已排空
	done chan struct{} // stream 已关闭,唤醒 writeLoop 与等窗口的发送方

	// ---- 流控记账 ----
	// 全是"累计字节数",允许 uint32 绕回:in-flight 由 sent-acked 的 uint32 减法
	// 算出,只要它小于 2^31 就正确 —— 而它受 window(最大 4 MB)约束。
	wmu      sync.Mutex
	sent     uint32        // 本端已发出的字节数
	acked    uint32        // 对端已消费的字节数(由 cmdWindow 更新)
	wnd      chan struct{} // 窗口出现额度时唤醒等待者(容量 1)
	consumed uint32        // 本端已写入下游的字节数
	lastAck  uint32        // 上次通告出去的 consumed
	// 从 muxer 复制过来,让 stream 自包含(测试可直接构造)
	window   uint32
	ackEvery uint32
}

func newMuxer(s *session, isServer bool, target string) *muxer {
	w := streamWindowFor(s.rateBps, s.k, s.m)
	m := &muxer{sess: s, isServer: isServer, target: target,
		streams: make(map[uint32]*stream), nextID: 1,
		window: w, ackEvery: w / 2,
		// 队列要装得下一整窗的帧,才保证对端守窗口时永不溢出;+16 留余量
		queueDepth: int(w)/frameChunk + 16,
	}
	// 对端重启后序号空间已重置,旧 stream 全部失效,必须清掉
	s.onReset = m.resetAll
	s.setOnStreamDead(m.killStream)
	go m.recvLoop()
	return m
}

func newStream(m *muxer, sid uint32, c net.Conn) *stream {
	st := &stream{id: sid, conn: c, mux: m,
		wq:       make(chan wqItem, m.queueDepth),
		done:     make(chan struct{}),
		wnd:      make(chan struct{}, 1),
		window:   m.window,
		ackEvery: m.ackEvery,
	}
	go st.writeLoop()
	return st
}

func (m *muxer) sendFrame(sid uint32, cmd byte, data []byte) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	// 帧头与数据必须一次性交给 writeStream:分开写会让帧头和数据落进
	// 不同的 streamSeq 甚至不同分片,而接收侧现在按 stream 独立重组。
	frame := make([]byte, frameHdr+len(data))
	binary.BigEndian.PutUint32(frame[0:4], sid)
	frame[4] = cmd
	binary.BigEndian.PutUint16(frame[5:7], uint16(len(data)))
	copy(frame[frameHdr:], data)
	m.sess.writeStream(sid, frame)
}

// 从可靠层持续读数据,按 stream 各自切帧并分发。
//
// 帧缓冲必须逐 stream 分开:可靠层现在按 (streamID, streamSeq) 独立交付,
// 各 stream 的字节之间不再有全局顺序。合用一个 buf 会把 A 的半个帧和 B 的
// 帧头拼在一起,帧边界当场就错。
func (m *muxer) recvLoop() {
	bufs := make(map[uint32][]byte)
	gen := m.resetGen.Load()
	for {
		var c streamChunk
		select {
		case c = <-m.sess.deliver:
			// 对端重启后字节流从头开始,此前卡在 buf 里的半个帧已经没有下文了。
			// 不丢掉的话新字节会被拼到残帧尾部,此后每个帧头都从错位的偏移解析。
			if g := m.resetGen.Load(); g != gen {
				gen = g
				bufs = make(map[uint32][]byte)
			}
			bufs[c.sid] = append(bufs[c.sid], c.data...)
		case <-m.sess.closed:
			return
		}
		buf := bufs[c.sid]
		gone := false
		for {
			if len(buf) < frameHdr {
				break
			}
			sid := binary.BigEndian.Uint32(buf[0:4])
			cmd := buf[4]
			n := int(binary.BigEndian.Uint16(buf[5:7]))
			if len(buf) < frameHdr+n {
				break
			}
			payload := make([]byte, n)
			copy(payload, buf[frameHdr:frameHdr+n])
			buf = buf[frameHdr+n:]
			m.dispatch(sid, cmd, payload)
			if cmd == cmdClose {
				gone = true
			}
		}
		// 帧边界对齐(或 stream 已关)就不留空条目 —— 否则每条来过的 stream
		// 都在这里留一条,又是一份随累计连接数线性增长的常驻内存。
		if gone || len(buf) == 0 {
			delete(bufs, c.sid)
		} else {
			bufs[c.sid] = buf
		}
	}
}

func (m *muxer) lookup(sid uint32) *stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[sid]
}

func (m *muxer) dispatch(sid uint32, cmd byte, payload []byte) {
	switch cmd {
	case cmdOpen:
		if !m.isServer {
			return
		}
		c, err := net.Dial("tcp", m.target)
		if err != nil {
			logf("[mux] stream %d 连接 target 失败: %v", sid, err)
			m.sendFrame(sid, cmdClose, nil)
			return
		}
		st := newStream(m, sid, c)
		m.mu.Lock()
		m.streams[sid] = st
		m.mu.Unlock()
		logf("[mux] stream %d 已连接 %s", sid, m.target)
		go st.pumpToTunnel()
	case cmdData:
		// 只入队,绝不在这里同步 Write —— 那会让 recvLoop 停摆,
		// 一条 stream 的下游写阻塞就拖垮整条隧道的所有 stream。
		if st := m.lookup(sid); st != nil {
			st.enqueue(wqItem{kind: wqData, data: payload})
		}
	case cmdWindow:
		if len(payload) < 4 {
			return
		}
		if st := m.lookup(sid); st != nil {
			st.onWindowUpdate(binary.BigEndian.Uint32(payload))
		}
	case cmdShutdown:
		st := m.lookup(sid)
		if st == nil {
			return
		}
		// 先记下 recvEOF,再排入哨兵 —— writeLoop 取到哨兵时要读这个标志
		// 来判断两个方向是否都结束了。
		st.mu.Lock()
		st.recvEOF = true
		st.mu.Unlock()
		// 排队而不是立刻 CloseWrite/close:队列里可能还有没写出的数据,
		// 直接动连接会把它们丢掉。收尾由 writeLoop 在排空后做。
		st.enqueue(wqItem{kind: wqShutdown})
	case cmdClose:
		m.mu.Lock()
		st := m.streams[sid]
		delete(m.streams, sid)
		m.mu.Unlock()
		if st != nil {
			st.close(false)
		}
	}
}

// enqueue 把一个元素交给该 stream 的写队列。
// 对端守窗口时数据不会把队列填满;真满了说明对端没守(例如未升级的旧版本),
// 此时宁可阻塞也不能丢 —— 隧道承载的是可靠字节流,丢一帧就撕坏整条流。
func (s *stream) enqueue(it wqItem) {
	select {
	case s.wq <- it:
	case <-s.done:
	}
}

// writeLoop 把队列里的数据落到下游 TCP,并回送窗口更新。
// 独立 goroutine 是关键:下游写阻塞时只停这一条 stream,mux.recvLoop 不受影响。
// 关闭动作也从队列里取,所以关闭一定发生在此前所有数据都写出之后。
func (s *stream) writeLoop() {
	for {
		var it wqItem
		select {
		case it = <-s.wq:
		case <-s.done:
			return
		}
		switch it.kind {
		case wqShutdown:
			// 队列已排到这里,说明此前的数据都写出去了,现在才关写方向
			if tc, ok := s.conn.(*net.TCPConn); ok {
				tc.CloseWrite()
			}
			// 若本端也早已读完,两个方向都结束了,整条可以收掉
			s.mu.Lock()
			both := s.sentEOF && s.recvEOF
			s.mu.Unlock()
			if both {
				s.close(false)
				return
			}
		case wqClose:
			s.close(false)
			return
		default:
			if _, err := s.conn.Write(it.data); err != nil {
				s.close(true)
				return
			}
			s.ackConsumed(len(it.data))
		}
	}
}

// ackConsumed 累计已写入下游的字节数,过阈值就通告对端,让它继续有额度。
func (s *stream) ackConsumed(n int) {
	s.wmu.Lock()
	s.consumed += uint32(n)
	cur := s.consumed
	need := cur-s.lastAck >= s.ackEvery
	if need {
		s.lastAck = cur
	}
	s.wmu.Unlock()
	if need {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], cur)
		s.mux.sendFrame(s.id, cmdWindow, b[:])
	}
}

// reserve 为即将发出的 n 字节申请窗口额度,额度不足就等对端的窗口更新。
// 返回 false 表示 stream 已关闭,调用方应停止发送。
func (s *stream) reserve(n int) bool {
	for {
		s.wmu.Lock()
		if s.sent-s.acked+uint32(n) <= s.window {
			s.sent += uint32(n)
			s.wmu.Unlock()
			return true
		}
		s.wmu.Unlock()
		select {
		case <-s.wnd:
		case <-s.done:
			return false
		}
	}
}

// onWindowUpdate 记下对端已消费到哪,并唤醒可能在等额度的发送方。
func (s *stream) onWindowUpdate(consumed uint32) {
	s.wmu.Lock()
	// uint32 减法判断是否前进,天然处理绕回;倒退的通告(乱序)忽略
	if consumed-s.acked < 1<<31 {
		s.acked = consumed
	}
	s.wmu.Unlock()
	select {
	case s.wnd <- struct{}{}:
	default:
	}
}

// 本地 TCP → 隧道
func (s *stream) pumpToTunnel() {
	buf := make([]byte, 16*1024)
	for {
		n, err := s.conn.Read(buf)
		if n > 0 {
			// 单帧上限 65535,按 8K 切
			for off := 0; off < n; off += frameChunk {
				end := off + frameChunk
				if end > n {
					end = n
				}
				// 先申请窗口额度:不超发,对端的写队列就不会溢出
				if !s.reserve(end - off) {
					return
				}
				s.mux.sendFrame(s.id, cmdData, buf[off:end])
			}
		}
		if err != nil {
			// 只声明"我这边读完了",不拆整条 stream —— 对端可能还有数据要回
			s.mu.Lock()
			already := s.sentEOF
			s.sentEOF = true
			both := s.sentEOF && s.recvEOF
			s.mu.Unlock()
			if !already {
				s.mux.sendFrame(s.id, cmdShutdown, nil)
				// 对端可能永远不回 cmdShutdown(落地侧 sshd 在 LoginGraceTime 内
				// 只是静静等待,不会回 EOF)。没有上限的话这条 stream 及其两个
				// goroutine 就一直挂着 —— 公网端口被高频连接时会无限积压。
				s.lingerAfterEOF()
			}
			if both {
				// 走队列:对端的数据可能还排在写队列里没落地,
				// 直接 close 会把它们丢掉。
				s.enqueue(wqItem{kind: wqClose})
			}
			return
		}
	}
}

// lingerAfterEOF 给"已发出 cmdShutdown、等对端回应"的状态设一个上限。
// 超时仍未收到对端的 cmdShutdown 就主动拆掉,并用 cmdClose 通知对端一起回收 ——
// 否则对端那一侧也会挂着同样一条 stream。
// 用 time.AfterFunc 而不是起 goroutine:定时器由 runtime 管,不额外占栈。
func (s *stream) lingerAfterEOF() {
	time.AfterFunc(time.Duration(halfCloseLinger.Load()), func() {
		s.mu.Lock()
		stuck := !s.recvEOF && !s.closed
		s.mu.Unlock()
		if stuck {
			s.close(true)
		}
	})
}

func (s *stream) close(notify bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.done) // 唤醒 writeLoop 和所有在等窗口额度的发送方
	s.mu.Unlock()
	s.conn.Close()
	s.mux.mu.Lock()
	delete(s.mux.streams, s.id)
	s.mux.mu.Unlock()
	if notify {
		s.mux.sendFrame(s.id, cmdClose, nil)
	}
	// 序号记账可以清了。必须排在 sendFrame 之后 —— 清早了这条 cmdClose
	// 会拿到一个重置回 0 的 streamSeq,对端排不出它相对于此前数据的顺序。
	s.mux.sess.forgetStream(s.id)
	logf("[mux] stream %d 关闭", s.id)
}

// killStream 断开一条空洞补不回的 stream(见 holeGiveUp),并通知对端一起关。
// 不在 sess 的回调里同步做:close 会 sendFrame,而那要走发送侧的令牌桶。
func (m *muxer) killStream(sid uint32) {
	go func() {
		logf("[mux] stream %d 有补不回的空洞超过 %s,断开", sid, holeGiveUp)
		if st := m.lookup(sid); st != nil {
			st.close(true)
			return
		}
		// 本端已没有这条 stream(比如已先行关闭),对端那侧可能还挂着。
		m.sendFrame(sid, cmdClose, nil)
	}()
}

// client 侧:接受一个本地 TCP 连接,分配 stream
func (m *muxer) openStream(c net.Conn) {
	m.mu.Lock()
	sid := m.nextID
	m.nextID++
	st := newStream(m, sid, c)
	m.streams[sid] = st
	m.mu.Unlock()
	logf("[mux] stream %d 新建(来自 %s)", sid, c.RemoteAddr())
	m.sendFrame(sid, cmdOpen, nil)
	go st.pumpToTunnel()
}

// 对端重启:关闭全部 stream。上层(ssh 等)会看到连接断开并自行重连。
//
// 递增 resetGen 让 recvLoop 丢掉半路的残帧。少了这一步,光关 stream 是不够的:
// 2026-07-31 生产事故里 ko 侧重启后,sz 侧照常打印"已重置 N 条 stream"、新
// stream 也照常建立,隧道却再没通过 —— 因为残帧还卡在 recvLoop 的局部缓冲里,
// 重启后的新字节被拼在它后面,帧边界永久错位。局部变量只有进程重启才会消失,
// 这就是当时"必须手工 restart 才恢复"的原因。
func (m *muxer) resetAll() {
	m.resetGen.Add(1)
	m.mu.Lock()
	old := m.streams
	m.streams = make(map[uint32]*stream)
	m.mu.Unlock()
	for _, st := range old {
		st.mu.Lock()
		if !st.closed {
			st.closed = true
			close(st.done) // 必须关:否则 writeLoop 与等窗口的发送方永久泄漏
		}
		st.mu.Unlock()
		st.conn.Close()
	}
	if len(old) > 0 {
		logf("[mux] 对端重启,已重置 %d 条 stream", len(old))
	}
}
