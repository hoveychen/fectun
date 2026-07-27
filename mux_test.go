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
