package main

import (
	"io"
	"fmt"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

// startEcho 起一个回显服务:读到 EOF 后把收到的内容原样送回再关闭。
// 这种"先收完再回"的服务专门用来暴露半关闭处理错误。
func startEcho(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				var buf []byte
				tmp := make([]byte, 32*1024)
				for {
					n, err := c.Read(tmp)
					buf = append(buf, tmp[:n]...)
					if err != nil {
						break // 对端半关闭 → 读到 EOF
					}
				}
				c.Write(buf)
			}(c)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// 回归:半关闭语义。
// 曾经的 bug —— 把"本端读到 EOF"当成整条连接结束,直接发 cmdClose,
// 对端还没把回显数据送回就被拆掉,数据丢失。
func TestHalfCloseKeepsReadDirection(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()

	px := newLossyProxy(t, 0, 21)
	defer px.stop()
	cliSess := newTestSession(t, px.port(), 20, 15, 500)
	srvSess := newTestSession(t, px.port(), 20, 15, 500)
	defer cliSess.close()
	defer srvSess.close()
	time.Sleep(300 * time.Millisecond)

	cliMux := newMuxer(cliSess, false, "")
	newMuxer(srvSess, true, echoAddr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
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

	data := payload(120 * 1024)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// 发完就关写方向 —— echo 服务要读到 EOF 才会回数据
	go func() {
		c.Write(data)
		c.(*net.TCPConn).CloseWrite()
	}()

	got := make([]byte, 0, len(data))
	c.SetReadDeadline(time.Now().Add(25 * time.Second))
	tmp := make([]byte, 32*1024)
	for len(got) < len(data) {
		n, err := c.Read(tmp)
		got = append(got, tmp[:n]...)
		if err != nil {
			break
		}
	}
	if len(got) != len(data) {
		t.Fatalf("半关闭后读方向被误关: 收到 %d/%d 字节(半关闭语义回归)", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("回显字节 %d 不一致", i)
		}
	}
}

// startBlackhole 起一个收下连接但既不读也不关的服务。
// 模拟落地侧 sshd 在 LoginGraceTime 内等待客户端 —— 它不会回 EOF,
// 所以对端不会发 cmdShutdown 过来。
func startBlackhole(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blackhole listen: %v", err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c) // 收下,既不读也不关
			mu.Unlock()
		}
	}()
	return ln.Addr().String(), func() {
		ln.Close()
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	}
}

// 回归:本端已读完但对端迟迟不回 cmdShutdown 的 stream 必须被回收。
// 2026-07-31 实测:入口侧 4422 暴露在公网被 5.231.242.176 以 ~2 次/秒 高频连接,
// 每条"连上就断"的连接都留下一条 stream —— 40 分钟内新建 4306 条、关闭仅 1680 条,
// 积压 2626 条,每条带 2 个 goroutine(约 16 KB 栈),把 RSS 顶到 46 MB 且不回落。
func TestHalfClosedStreamIsReclaimed(t *testing.T) {
	old := halfCloseLinger
	halfCloseLinger = 600 * time.Millisecond
	defer func() { halfCloseLinger = old }()

	bhAddr, stopBH := startBlackhole(t)
	defer stopBH()

	px := newLossyProxy(t, 0, 61)
	defer px.stop()
	cliSess := newTestSession(t, px.port(), 20, 15, 500)
	srvSess := newTestSession(t, px.port(), 20, 15, 500)
	defer cliSess.close()
	defer srvSess.close()
	time.Sleep(300 * time.Millisecond)

	cliMux := newMuxer(cliSess, false, "")
	newMuxer(srvSess, true, bhAddr)

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

	// 模拟扫描器:连上立刻断开
	for i := 0; i < 5; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		c.Close()
	}
	time.Sleep(400 * time.Millisecond)

	cliMux.mu.Lock()
	before := len(cliMux.streams)
	cliMux.mu.Unlock()
	if before == 0 {
		t.Fatal("前置条件不成立:stream 没建起来,测试无效")
	}

	// 等过 linger 时限
	time.Sleep(2 * time.Second)
	cliMux.mu.Lock()
	after := len(cliMux.streams)
	cliMux.mu.Unlock()
	if after != 0 {
		t.Fatalf("半关闭的 stream 未被回收:%d 条仍在册(峰值 %d)—— 连上就断的连接会无限积压",
			after, before)
	}
}

// startSpew 起一个"猛发"服务:accept 后往回灌 n 字节。
// 用来把某条 stream 的下游 TCP 缓冲填满,制造写阻塞。
func startSpew(t *testing.T, n int) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("spew listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 32*1024)
				for i := range buf {
					buf[i] = byte(i)
				}
				for sent := 0; sent < n; sent += len(buf) {
					if _, err := c.Write(buf); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// 回归:一条 stream 的下游不读,不得拖垮其他 stream。
// mux.recvLoop 在 dispatch 里同步调 st.conn.Write —— 某条 stream 的下游 TCP
// 接收窗口一满,recvLoop 就停在那里,整条隧道所有 stream 的数据交付一起停摆。
// 2026-07-31 落地侧卡死的结构性成因。
func TestSlowStreamDoesNotStallOthers(t *testing.T) {
	spewAddr, stopSpew := startSpew(t, 8<<20)
	defer stopSpew()

	px := newLossyProxy(t, 0, 41)
	defer px.stop()
	cliSess := newTestSession(t, px.port(), 20, 15, 800)
	srvSess := newTestSession(t, px.port(), 20, 15, 800)
	defer cliSess.close()
	defer srvSess.close()
	time.Sleep(300 * time.Millisecond)

	cliMux := newMuxer(cliSess, false, "")
	newMuxer(srvSess, true, spewAddr)

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

	// stream A:连上但从不 Read —— 下游 TCP 缓冲会被 spew 灌满
	slow, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("slow dial: %v", err)
	}
	defer slow.Close()

	// 给它时间把缓冲灌满、让 mux 的 Write 卡住
	time.Sleep(4 * time.Second)

	// stream B:正常读。它和 A 毫无关系,必须能拿到数据。
	fast, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("fast dial: %v", err)
	}
	defer fast.Close()

	fast.SetReadDeadline(time.Now().Add(20 * time.Second))
	got := 0
	tmp := make([]byte, 32*1024)
	for got < 1<<20 {
		n, err := fast.Read(tmp)
		got += n
		if err != nil {
			break
		}
	}
	if got < 1<<20 {
		t.Fatalf("另一条 stream 只收到 %d 字节(要求 ≥ 1 MB)—— 慢 stream 拖停了整条隧道", got)
	}
}

// 流控窗口必须能撑住远超一个窗口的传输。
// 其他 mux 测试的数据量都小于窗口(rate=800 档约 4 MB 上限),窗口机制不会被触发 ——
// 这个测试双向各推 4 MB,会跨越 30+ 次窗口更新。窗口更新一旦漏发或算错,
// 发送方就会永久等在 reserve 里,表现为传输卡死。
func TestFlowControlHandlesLargeTransfer(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()

	px := newLossyProxy(t, 0, 51)
	defer px.stop()
	cliSess := newTestSession(t, px.port(), 20, 15, 800)
	srvSess := newTestSession(t, px.port(), 20, 15, 800)
	defer cliSess.close()
	defer srvSess.close()
	time.Sleep(300 * time.Millisecond)

	cliMux := newMuxer(cliSess, false, "")
	newMuxer(srvSess, true, echoAddr)

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

	const size = 4 << 20 // 远超窗口,必然反复触发窗口更新
	data := payload(size)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	go func() {
		c.Write(data)
		c.(*net.TCPConn).CloseWrite()
	}()

	c.SetReadDeadline(time.Now().Add(60 * time.Second))
	got := make([]byte, 0, size)
	tmp := make([]byte, 64*1024)
	for len(got) < size {
		n, err := c.Read(tmp)
		got = append(got, tmp[:n]...)
		if err != nil {
			break
		}
	}
	if len(got) != size {
		t.Fatalf("4 MB 回显只收到 %d/%d 字节 —— 流控窗口在大流量下卡死或丢数据", len(got), size)
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("回显字节 %d 不一致", i)
		}
	}
}

// 窗口必须跟着 -rate 走。单条 stream 的吞吐上限 = window / RTT,写死的窗口在
// 高 rate 下会成为瓶颈:实测 rate=25、RTT=170ms 时 256 KB 固定窗口把吞吐压到
// 12.0 Mbps(净上限 14.3);若 rate 调到 60(净 34 Mbps),256 KB 只够 12 Mbps。
func TestStreamWindowScalesWithRate(t *testing.T) {
	const k, m = 20, 15
	bps := func(mbps float64) float64 { return mbps * 1e6 / 8 }

	lo := streamWindowFor(bps(25), k, m)
	hi := streamWindowFor(bps(200), k, m)
	if hi <= lo {
		t.Fatalf("窗口没跟着 rate 涨:rate=25 → %d,rate=200 → %d", lo, hi)
	}

	// 窗口应覆盖 windowSpan 秒的净在途数据(净速率 = 线路速率 / (1+m/k))
	wantLo := uint32(bps(25) / (1 + float64(m)/float64(k)) * windowSpan)
	if lo != wantLo {
		t.Fatalf("rate=25 的窗口 %d,按净速率×%.1fs 应为 %d", lo, windowSpan, wantLo)
	}

	// 上下限必须封住:极低 rate 不能让窗口小到卡死,极高 rate 不能让它吃光内存
	if got := streamWindowFor(bps(0.1), k, m); got != minStreamWin {
		t.Fatalf("极低 rate 未落到下限:got %d want %d", got, minStreamWin)
	}
	if got := streamWindowFor(bps(10000), k, m); got != maxStreamWin {
		t.Fatalf("极高 rate 未封到上限:got %d want %d", got, maxStreamWin)
	}

	// 冗余度要扣掉:同样线路速率下,冗余越高净速率越低,窗口越小
	if lowRedundancy := streamWindowFor(bps(200), 20, 2); lowRedundancy <= hi {
		t.Fatalf("冗余度没参与计算:m=15 → %d,m=2 → %d(后者净速率更高,窗口应更大)", hi, lowRedundancy)
	}
}

// 流控记账用 uint32 累计字节数,传够 4 GB 就会绕回。
// in-flight 是 sent-acked 的 uint32 减法,数学上能正确跨越绕回点 —— 这里直接
// 把计数推到上限附近验证,不然这条路径要传 4 GB 才会走到。
func TestFlowControlWrapsAroundUint32(t *testing.T) {
	const win = 256 << 10
	st := &stream{done: make(chan struct{}), wnd: make(chan struct{}, 1),
		window: win, ackEvery: win / 2}
	const start = ^uint32(0) - 1000 // 起点距上限 1000 字节,后续申请必然绕回
	st.sent, st.acked = start, start

	const chunk = 8192
	for i := 0; i < win/chunk; i++ { // 正好占满一个窗口
		if !st.reserve(chunk) {
			t.Fatalf("窗口未满,第 %d 次 reserve 却被拒", i)
		}
	}
	st.wmu.Lock()
	cur := st.sent
	st.wmu.Unlock()
	if cur >= start {
		t.Fatalf("前置条件不成立:sent=%d 没跨过绕回点(start=%d),测试无效", cur, start)
	}

	// 窗口已占满,reserve 必须阻塞
	blocked := make(chan bool, 1)
	go func() { blocked <- st.reserve(chunk) }()
	select {
	case <-blocked:
		t.Fatal("窗口已满,reserve 不该立刻放行")
	case <-time.After(200 * time.Millisecond):
	}

	// 对端通告已消费完本端发出的全部字节(cur 就是绕回后的 sent),应唤醒并放行
	st.onWindowUpdate(cur)
	select {
	case ok := <-blocked:
		if !ok {
			t.Fatal("stream 未关闭,reserve 不该返回 false")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("窗口更新后 reserve 未被唤醒 —— uint32 绕回处理有误")
	}
}

// 多条 stream 并发时数据不得串流
func TestMuxConcurrentStreamsIsolated(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()

	px := newLossyProxy(t, 0.05, 33)
	defer px.stop()
	cliSess := newTestSession(t, px.port(), 20, 15, 800)
	srvSess := newTestSession(t, px.port(), 20, 15, 800)
	defer cliSess.close()
	defer srvSess.close()
	time.Sleep(300 * time.Millisecond)

	cliMux := newMuxer(cliSess, false, "")
	newMuxer(srvSess, true, echoAddr)

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

	sizes := []int{40 * 1024, 70 * 1024, 25 * 1024}
	var wg sync.WaitGroup
	errs := make([]string, len(sizes))
	for i, sz := range sizes {
		wg.Add(1)
		go func(idx, size int) {
			defer wg.Done()
			// 每条流用不同内容,串流会立刻被发现
			d := make([]byte, size)
			for j := range d {
				d[j] = byte(idx*97 + j)
			}
			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				errs[idx] = "dial 失败"
				return
			}
			defer c.Close()
			go func() { c.Write(d); c.(*net.TCPConn).CloseWrite() }()
			c.SetReadDeadline(time.Now().Add(30 * time.Second))
			got := make([]byte, 0, size)
			tmp := make([]byte, 32*1024)
			for len(got) < size {
				n, err := c.Read(tmp)
				got = append(got, tmp[:n]...)
				if err != nil {
					break
				}
			}
			if len(got) != size {
				errs[idx] = "长度不符"
				return
			}
			for j := range d {
				if got[j] != d[j] {
					errs[idx] = "内容串流"
					return
				}
			}
		}(i, sz)
	}
	wg.Wait()
	for i, e := range errs {
		if e != "" {
			t.Fatalf("stream %d 失败: %s", i, e)
		}
	}
}

// 回归:对端重启时,recvLoop 的帧缓冲必须一并清空。
//
// 2026-07-31 生产事故 —— ko 侧 fectun 重启后,sz 侧确实打印了
// "[mux] 对端重启,已重置 N 条 stream",新 stream 也照常建立,隧道却再也不通,
// 实测 >=2 分钟未自愈,最后靠手工重启进程才恢复。
//
// 根因:recvLoop 的 buf 是它自己的局部变量,resetAll 够不着。对端重启的瞬间
// buf 里若卡着半个帧(帧头 7 字节没收全,或声明的 payload 还没到齐),重启后
// 从干净字节流重发的第一个帧就会被拼到这段残帧尾部 —— 此后每个帧头都从错
// 位的偏移解析,sid/cmd/length 全是垃圾,字节流被永久撕坏。而局部变量只有
// 进程重启才会消失,这正是"必须手工 restart 才恢复"的原因。
func TestResetClearsFrameBuffer(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()

	px := newLossyProxy(t, 0, 77)
	defer px.stop()
	srvSess := newTestSession(t, px.port(), 20, 15, 500)
	defer srvSess.close()
	srvMux := newMuxer(srvSess, true, echoAddr)

	// 1. 对端重启前:一个被截断的帧卡在 recvLoop 里该 stream 的 buf 中。
	//    帧头要 7 字节,这里只送 3 字节,recvLoop 会 break 出内层循环等后续字节。
	//    残帧必须和后续的新帧同属一个 sid,否则它们落进不同的 buf,污染不到。
	const wantSID = 42
	srvSess.deliver <- streamChunk{sid: wantSID, data: []byte{0xAA, 0xBB, 0xCC}}
	time.Sleep(100 * time.Millisecond)

	// 2. 对端重启 → session 层重置序号空间 → onReset → resetAll
	srvMux.resetAll()
	time.Sleep(50 * time.Millisecond)

	// 3. 重启后对端从干净的字节流重新开始:一个完整的 cmdOpen(sid=42)
	frame := make([]byte, frameHdr)
	binary.BigEndian.PutUint32(frame[0:4], wantSID)
	frame[4] = cmdOpen
	binary.BigEndian.PutUint16(frame[5:7], 0)
	srvSess.deliver <- streamChunk{sid: wantSID, data: frame}

	// 4. server 侧应据此连上 target 并登记 stream 42
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srvMux.lookup(wantSID) != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("对端重启后 cmdOpen(sid=%d) 未被正确解析 —— 残留的半帧把帧边界撕错了", wantSID)
}

// nextSeqOf 读发送侧当前序号(测试用,需持锁)。
func nextSeqOf(s *session) uint32 {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.nextSeq
}

// 回归:一条 stream 的分片丢失,不得拖停其他 stream。
//
// 这是"重组侧队头阻塞",与 TestSlowStreamDoesNotStallOthers 覆盖的"消费侧队头
// 阻塞"是两回事:那个是下游 TCP 写阻塞卡住 recvLoop,已由 per-stream 写队列解决;
// 这个发生在更下面一层 —— session.drainLocked 严格按全局 expected 递增交付,
// 任何一个分片 FEC 救不回,deliver 就一字不吐,整条隧道所有 stream 一起停摆,
// 直到 ARQ 把那个空洞补上。流控窗口和写队列都在 dispatch 之后,救不了上游。
//
// 2026-08-01 生产实测:入口侧 NACK 3.3/s,每次空洞停摆 120ms(NACK 判定)+69ms
// (重传往返),量级上每秒有大半秒隧道是停的。
func TestLostShardDoesNotStallOtherStreams(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()

	px := newLossyProxy(t, 0, 91)
	defer px.stop()
	cliSess := newTestSession(t, px.port(), 20, 15, 800)
	srvSess := newTestSession(t, px.port(), 20, 15, 800)
	defer cliSess.close()
	defer srvSess.close()
	time.Sleep(300 * time.Millisecond)

	cliMux := newMuxer(cliSess, false, "")
	newMuxer(srvSess, true, echoAddr)

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

	// stream A:只写不读。echo 服务要读到 EOF 才回送,A 不半关闭,
	// 于是 A 的流量全部是 cli→srv 单向的 —— 丢包窗口才不会误伤 srv→cli 方向。
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("stream A dial: %v", err)
	}
	defer a.Close()

	// 先推高 cli 侧 seq,让后面设的丢包窗口落在 srv 侧 seq 触及不到的高位
	if _, err := a.Write(payload(640 << 10)); err != nil {
		t.Fatalf("stream A 预热写入失败: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for nextSeqOf(cliSess) < 400 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	// 开一个丢包窗口:A 接下来发的分片连丢 5 轮,ARQ 要 5 个 120ms 判定周期
	// 才补得上,空洞至少存在 600ms。
	from := nextSeqOf(cliSess)
	px.mu.Lock()
	px.dropFrom, px.dropTo = from, from+150
	px.dropTimes = 5
	px.dropEnabled = true
	px.mu.Unlock()

	if _, err := a.Write(payload(320 << 10)); err != nil {
		t.Fatalf("stream A 二次写入失败: %v", err)
	}
	// 等 A 这批数据全部发出(seq 越过丢包窗口),B 的分片才不会落进窗口里
	deadline = time.Now().Add(5 * time.Second)
	for nextSeqOf(cliSess) <= from+150 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := nextSeqOf(cliSess); got <= from+150 {
		t.Fatalf("stream A 的数据没能发满丢包窗口(seq=%d, 需 >%d)", got, from+150)
	}

	// stream B:全新连接,它的分片一个都没丢,必须能独立完成往返。
	b, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("stream B dial: %v", err)
	}
	defer b.Close()
	if _, err := b.Write([]byte("ping")); err != nil {
		t.Fatalf("stream B 写入失败: %v", err)
	}
	b.(*net.TCPConn).CloseWrite() // 触发 echo 回送

	start := time.Now()
	b.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	buf := make([]byte, 64)
	n, err := b.Read(buf)
	if err != nil || string(buf[:n]) != "ping" {
		srvSess.recvMu.Lock()
		detail := fmt.Sprintf("srv expected=%d recvHigh=%d seen=%d",
			srvSess.expected, srvSess.recvHigh, len(srvSess.recvSeen))
		for sid, r := range srvSess.streamRecv {
			detail += fmt.Sprintf(" | sid=%d exp=%d buffered=%d", sid, r.expected, len(r.buf))
		}
		srvSess.recvMu.Unlock()
		t.Fatalf("stream B 在 %v 内没拿到回显(收到 %q, err=%v)"+
			" —— stream A 的分片空洞把整条隧道的交付卡住了\n%s",
			time.Since(start), buf[:n], err, detail)
	}
}

// 有积压却超过 holeGiveUp 没推进的 stream 要被清掉;正常推进的、没有积压的不动。
func TestStuckStreamIsReaped(t *testing.T) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	s := newSession(c, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, 20, 15, 25)
	defer s.close()

	shard := func(sid, sseq uint32) []byte {
		b := make([]byte, shardPayload)
		marshalShard(b, sid, sseq, []byte("x"))
		return b
	}
	s.recvMu.Lock()
	s.acceptShardLocked(0, shard(5, 1)) // sid 5 缺 sseq 0,积压 1 片
	s.acceptShardLocked(1, shard(6, 0)) // sid 6 连续,已交付、无积压
	now := time.Now()
	early := s.reapStuckStreamsLocked(now.Add(holeGiveUp / 2))
	late := s.reapStuckStreamsLocked(now.Add(holeGiveUp + time.Second))
	_, left5 := s.streamRecv[5]
	_, left6 := s.streamRecv[6]
	s.recvMu.Unlock()

	if len(early) != 0 {
		t.Fatalf("还没到 holeGiveUp 就清掉了 %v", early)
	}
	if len(late) != 1 || late[0] != 5 || left5 {
		t.Fatalf("卡住的 sid 5 没被清掉:返回 %v,仍在册=%v", late, left5)
	}
	if !left6 {
		t.Fatal("没有积压的 sid 6 被误清")
	}
}

// 空洞永远补不回时(整组数据片连同校验片每次都丢),客户端的 TCP 连接必须被
// 断开,而不是永久挂住 —— 挂住时上层 ssh 什么都看不到,也不会重连。
func TestUnrecoverableHoleClosesStream(t *testing.T) {
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

	px := newLossyProxy(t, 0, 61)
	defer px.stop()
	// 第 3 组(seq 60~79)的数据片和校验片永远丢,FEC 与 ARQ 都救不回。
	px.mu.Lock()
	px.dropEnabled, px.dropFrom, px.dropTo, px.dropTimes = true, 60, 79, 1<<30
	px.mu.Unlock()
	cli := newTestSession(t, px.port(), 20, 20, 20)
	srv := newTestSession(t, px.port(), 20, 20, 20)
	defer cli.close()
	defer srv.close()
	time.Sleep(300 * time.Millisecond)
	cliMux := newMuxer(cli, false, "")
	newMuxer(srv, true, sink.Addr().String())

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
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	go c.Write(payload(1 << 20))

	start := time.Now()
	c.SetReadDeadline(start.Add(holeGiveUp + 5*time.Second))
	_, err = c.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("空洞补不回 %s 后连接仍挂着,没被断开", time.Since(start).Round(time.Second))
	}
	t.Logf("%s 后连接被断开:%v", time.Since(start).Round(100*time.Millisecond), err)
}
