package fectun

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"
)

// startStreamEcho 起一个边读边回的回显服务(不依赖半关闭)。
func startStreamEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	return ln.Addr().String()
}

func listenLoopback(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("udp bind: %v", err)
	}
	return c
}

// 公开 API 的端到端:OpenStream 返回的 net.Conn 经 10% 丢包的隧道到 PeerServer,
// 再到回显服务,数据必须完整原样回来;RemoteAddr 必须是隧道对端的 UDP 地址。
func TestClientOpenStreamRoundTrip(t *testing.T) {
	echo := startStreamEcho(t)
	px := newLossyProxy(t, 0.10, 7)
	defer px.stop()
	proxy := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: px.port()}

	cli, err := NewClient(listenLoopback(t), proxy, Options{K: 20, M: 15, RateMbps: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	srv, err := NewPeerServer(listenLoopback(t), proxy, echo, Options{K: 20, M: 15, RateMbps: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	time.Sleep(300 * time.Millisecond) // 让 proxy 先认识两端

	c := cli.OpenStream()
	defer c.Close()
	if ua, ok := c.RemoteAddr().(*net.UDPAddr); !ok || !ua.IP.Equal(proxy.IP) || ua.Port != proxy.Port {
		t.Fatalf("RemoteAddr = %v, 想要 %v", c.RemoteAddr(), proxy)
	}

	want := make([]byte, 256<<10)
	rand.Read(want)
	go c.Write(want)
	got := make([]byte, len(want))
	c.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("读回显: %v (%s)", err, cli.Stats())
	}
	if !bytes.Equal(got, want) {
		t.Fatal("回显内容不符")
	}
}

// Close 之后已开的 stream 必须读到 EOF/错误,而不是永久挂起。
func TestClientCloseEndsStreams(t *testing.T) {
	echo := startStreamEcho(t)
	px := newLossyProxy(t, 0, 8)
	defer px.stop()
	proxy := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: px.port()}
	cli, _ := NewClient(listenLoopback(t), proxy, Options{})
	srv, _ := NewPeerServer(listenLoopback(t), proxy, echo, Options{})
	defer srv.Close()
	time.Sleep(300 * time.Millisecond)

	c := cli.OpenStream()
	cli.Close()
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err := c.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("Close 之后 stream 仍挂着")
	}
}

func TestOptionsRejectOversizedKM(t *testing.T) {
	if _, err := NewClient(listenLoopback(t), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}, Options{K: 200, M: 100}); err == nil {
		t.Fatal("k+m>255 应被拒绝")
	}
}
