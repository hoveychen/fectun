package main

import (
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
// 其他 mux 测试的数据量都小于 streamWindow(256 KB),窗口机制根本不会被触发 ——
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

	const size = 4 << 20 // 远超 streamWindow,必然反复触发窗口更新
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

// 流控记账用 uint32 累计字节数,传够 4 GB 就会绕回。
// in-flight 是 sent-acked 的 uint32 减法,数学上能正确跨越绕回点 —— 这里直接
// 把计数推到上限附近验证,不然这条路径要传 4 GB 才会走到。
func TestFlowControlWrapsAroundUint32(t *testing.T) {
	st := &stream{done: make(chan struct{}), wnd: make(chan struct{}, 1)}
	const start = ^uint32(0) - 1000 // 起点距上限 1000 字节,后续申请必然绕回
	st.sent, st.acked = start, start

	const chunk = 8192
	for i := 0; i < streamWindow/chunk; i++ { // 正好占满一个窗口
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
