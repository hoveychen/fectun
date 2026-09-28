package fectun

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestPacketAuthSealOpen(t *testing.T) {
	a := newPacketAuth([]byte("k1"))
	pkt := make([]byte, hdrSize+100)
	header{typ: pktHeartbeat, k: 20, m: 15, epoch: 9}.marshal(pkt)
	sealed := a.seal(pkt)
	if len(sealed) != len(pkt)+authTagSize {
		t.Fatalf("签名后长度 %d", len(sealed))
	}
	body, ok := a.open(sealed)
	if !ok || string(body) != string(pkt) {
		t.Fatal("正确签名的包验签失败")
	}
	sealed[5] ^= 1
	if _, ok := a.open(sealed); ok {
		t.Fatal("篡改过的包通过了验签")
	}
	if _, ok := newPacketAuth([]byte("k2")).open(a.seal(pkt)); ok {
		t.Fatal("换了密钥仍通过验签")
	}
	if newPacketAuth(nil) != nil {
		t.Fatal("空密钥应关闭鉴权")
	}
}

func startKeyedServer(t *testing.T, target string, key string) (*Server, *net.UDPAddr) {
	t.Helper()
	conn := listenLoopback(t)
	srv, err := NewServer(conn, target, Options{RateMbps: 200, Key: []byte(key)})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv, conn.LocalAddr().(*net.UDPAddr)
}

// 同一密钥:经 Server 与经 PeerServer(readLoop 路径)都要能回显。
func TestAuthMatchingKeyWorks(t *testing.T) {
	echo := startStreamEcho(t)
	_, addr := startKeyedServer(t, echo, "s3cret")
	cli, _ := NewClient(listenLoopback(t), addr, Options{RateMbps: 200, Key: []byte("s3cret")})
	defer cli.Close()
	if err := echoOnce(cli.OpenStream(), 64<<10, 10*time.Second); err != nil {
		t.Fatalf("Server: %v", err)
	}

	px := newLossyProxy(t, 0.05, 3)
	defer px.stop()
	proxy := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: px.port()}
	opt := Options{RateMbps: 200, Key: []byte("s3cret")}
	pc, _ := NewClient(listenLoopback(t), proxy, opt)
	defer pc.Close()
	ps, _ := NewPeerServer(listenLoopback(t), proxy, echo, opt)
	defer ps.Close()
	time.Sleep(300 * time.Millisecond)
	if err := echoOnce(pc.OpenStream(), 64<<10, 10*time.Second); err != nil {
		t.Fatalf("PeerServer: %v (%s)", err, pc.Stats())
	}
	if !strings.Contains(pc.Stats(), "鉴权失败=0") {
		t.Fatalf("同密钥不应有鉴权失败: %s", pc.Stats())
	}
}

// 密钥不符或没带密钥的客户端:连会话都建不起来,且计入鉴权失败。
func TestAuthRejectsWrongOrMissingKey(t *testing.T) {
	echo := startStreamEcho(t)
	srv, addr := startKeyedServer(t, echo, "s3cret")
	for _, key := range []string{"wrong", ""} {
		cli, _ := NewClient(listenLoopback(t), addr, Options{Key: []byte(key)})
		err := echoOnce(cli.OpenStream(), 1024, time.Second)
		cli.Close()
		if err == nil {
			t.Fatalf("密钥 %q 不该连通", key)
		}
	}
	if n := srv.Peers(); n != 0 {
		t.Fatalf("鉴权失败的对端建了 %d 个会话", n)
	}
	if srv.authFail.Load() == 0 {
		t.Fatal("鉴权失败没有计数")
	}
}
