package fectun

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func startServer(t *testing.T, target string, tweak func(*Server)) (*Server, *net.UDPAddr) {
	t.Helper()
	conn := listenLoopback(t)
	srv, err := NewServer(conn, target, Options{RateMbps: 200})
	if err != nil {
		t.Fatal(err)
	}
	if tweak != nil {
		tweak(srv)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv, conn.LocalAddr().(*net.UDPAddr)
}

// echoOnce 经 c 发 n 字节随机数据,要求原样回显。
func echoOnce(c net.Conn, n int, timeout time.Duration) error {
	want := make([]byte, n)
	rand.Read(want)
	go c.Write(want)
	got := make([]byte, n)
	c.SetReadDeadline(time.Now().Add(timeout))
	if _, err := io.ReadFull(c, got); err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// 两个客户端从不同源端口、用不同 k/m 同时连同一个 Server:
// 服务端必须按源地址各建会话、各用对端首包里的 k/m,互不串线。
func TestServerMultiplePeersWithOwnKM(t *testing.T) {
	echo := startStreamEcho(t)
	srv, addr := startServer(t, echo, nil)

	kms := [][2]int{{20, 15}, {10, 10}}
	var wg sync.WaitGroup
	errs := make([]error, len(kms))
	for i, km := range kms {
		cli, err := NewClient(listenLoopback(t), addr, Options{K: km[0], M: km[1], RateMbps: 200})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()
		wg.Add(1)
		go func(i int, cli *Client) {
			defer wg.Done()
			c := cli.OpenStream()
			defer c.Close()
			errs[i] = echoOnce(c, 128<<10, 15*time.Second)
		}(i, cli)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("客户端 %d (k/m=%v): %v\n%s", i, kms[i], err, srv.Stats())
		}
	}
	if n := srv.Peers(); n != 2 {
		t.Fatalf("Peers()=%d,想要 2", n)
	}
}

// 10% 丢包下经学到的地址回包,字节流仍须完整。
func TestServerLearnsPeerUnderLoss(t *testing.T) {
	echo := startStreamEcho(t)
	_, addr := startServer(t, echo, nil)

	// 链路:客户端 → lossyProxy → 转发器 → Server。
	// lossyProxy 靠"谁先发包"认识两端,而 Server 从不先发包,所以在它前面
	// 垫一个转发器,并把转发器预先登记为 proxy 的第二端。
	px := newLossyProxy(t, 0.10, 11)
	defer px.stop()
	fwd := newUDPForwarder(t, addr)
	px.mu.Lock()
	px.addrs = append(px.addrs, fwd.LocalAddr().(*net.UDPAddr))
	px.mu.Unlock()
	fwd.setReturn(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: px.port()})

	cli, err := NewClient(listenLoopback(t), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: px.port()},
		Options{K: 20, M: 15, RateMbps: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	c := cli.OpenStream()
	defer c.Close()
	if err := echoOnce(c, 256<<10, 20*time.Second); err != nil {
		t.Fatalf("丢包下回显失败: %v (%s)", err, cli.Stats())
	}
}

// 对端消失后会话必须按 IdleTimeout 回收。
func TestServerReapsIdlePeer(t *testing.T) {
	echo := startStreamEcho(t)
	srv, addr := startServer(t, echo, func(s *Server) { s.IdleTimeout = 400 * time.Millisecond })

	cli, _ := NewClient(listenLoopback(t), addr, Options{})
	c := cli.OpenStream()
	if err := echoOnce(c, 1024, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if srv.Peers() != 1 {
		t.Fatalf("Peers()=%d,想要 1", srv.Peers())
	}
	cli.Close()
	deadline := time.Now().Add(3 * time.Second)
	for srv.Peers() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("对端关闭 3s 后会话仍未回收")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// 超过 MaxPeers 的新对端不得建会话。
func TestServerMaxPeers(t *testing.T) {
	echo := startStreamEcho(t)
	srv, addr := startServer(t, echo, func(s *Server) { s.MaxPeers = 1 })

	a, _ := NewClient(listenLoopback(t), addr, Options{})
	defer a.Close()
	if err := echoOnce(a.OpenStream(), 1024, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	b, _ := NewClient(listenLoopback(t), addr, Options{})
	defer b.Close()
	if err := echoOnce(b.OpenStream(), 1024, 1500*time.Millisecond); err == nil {
		t.Fatal("超出 MaxPeers 的对端不该连通")
	}
	if srv.Peers() != 1 {
		t.Fatalf("Peers()=%d,想要 1", srv.Peers())
	}
}

// udpForwarder 把收到的包原样转给 dst,从 dst 回来的包转给 ret。
// 用来把 lossyProxy 接到一个不会主动发包的 Server 前面。
type udpForwarder struct {
	*net.UDPConn
	dst *net.UDPAddr
	mu  sync.Mutex
	ret *net.UDPAddr
}

func newUDPForwarder(t *testing.T, dst *net.UDPAddr) *udpForwarder {
	t.Helper()
	f := &udpForwarder{UDPConn: listenLoopback(t), dst: dst}
	t.Cleanup(func() { f.Close() })
	go func() {
		buf := make([]byte, 2048)
		for {
			n, src, err := f.ReadFromUDP(buf)
			if err != nil {
				return
			}
			f.mu.Lock()
			ret := f.ret
			f.mu.Unlock()
			if src.Port == dst.Port && src.IP.Equal(dst.IP) {
				if ret != nil {
					f.WriteToUDP(buf[:n], ret)
				}
			} else {
				f.WriteToUDP(buf[:n], dst)
			}
		}
	}()
	return f
}

func (f *udpForwarder) setReturn(a *net.UDPAddr) {
	f.mu.Lock()
	f.ret = a
	f.mu.Unlock()
}
