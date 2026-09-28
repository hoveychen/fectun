package fectun

import (
	"fmt"
	"net"
)

// Logf 是库内所有日志的出口,默认打到 stdout。
// 嵌入方(例如 stdout 另有用途的进程)可以换成自己的 logger,或置为 nil 静默。
var Logf = func(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

func logf(format string, args ...any) {
	if f := Logf; f != nil {
		f(format, args...)
	}
}

// Options 是一端的 FEC 与限速参数。
//
// K/M 两端必须一致:接收侧按本端的 K 解组,对不上的包直接丢弃。
// RateMbps 是本端发送方向的线路限速(含冗余),净数据 ≈ RateMbps/(1+M/K)。
// 不要超过链路当时的实际容量 —— 超发只会加剧丢包,实测曾把同机 SSH 挤断。
type Options struct {
	K, M     int
	RateMbps float64
}

// 与 CLI 默认值保持一致。
const (
	DefaultK        = 20
	DefaultM        = 15
	DefaultRateMbps = 25
)

func (o Options) withDefaults() Options {
	if o.K <= 0 {
		o.K = DefaultK
	}
	if o.M <= 0 {
		o.M = DefaultM
	}
	if o.RateMbps <= 0 {
		o.RateMbps = DefaultRateMbps
	}
	return o
}

func (o Options) validate() error {
	// 分片序号与 k/m 在包头里各占 1 字节
	if o.K+o.M > 255 {
		return fmt.Errorf("fectun: k+m=%d 超过 255", o.K+o.M)
	}
	return nil
}

// Client 是入口侧:每个 OpenStream 在隧道里开一条 stream,
// 对端(Server)为它连一次 target。
//
// 一个 Client 独占一个 UDP socket 和一套限速。同一对端上的多条连接应共用
// 一个 Client,而不是各建一个 —— 否则每个 Client 各按 RateMbps 发,
// 总速率成倍超出限速。
type Client struct {
	sess *session
	mux  *muxer
}

// NewClient 在 conn 上建立到 peer 的隧道。conn 由调用方创建,这样调用方能在
// 交出之前对 socket 做平台相关处理(例如 Android 的 VpnService.protect)。
// Client 接管 conn 的所有权,Close 时一并关闭。
func NewClient(conn *net.UDPConn, peer *net.UDPAddr, o Options) (*Client, error) {
	o = o.withDefaults()
	if err := o.validate(); err != nil {
		return nil, err
	}
	s := newSession(conn, peer, o.K, o.M, o.RateMbps)
	go s.readLoop()
	return &Client{sess: s, mux: newMuxer(s, false, "")}, nil
}

// Dial 是 NewClient 的便捷形式:在本地临时端口上开 UDP socket 连到 peer。
func Dial(peer string, o Options) (*Client, error) {
	raddr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadBuffer(16 << 20)
	conn.SetWriteBuffer(16 << 20)
	c, err := NewClient(conn, raddr, o)
	if err != nil {
		conn.Close()
	}
	return c, err
}

// OpenStream 开一条新 stream,返回它的本地一端。
//
// 读写语义与一条 TCP 连接相同;Close 即结束该 stream。
// RemoteAddr 返回隧道对端的 UDP 地址。
func (c *Client) OpenStream() net.Conn {
	inner, outer := net.Pipe()
	c.mux.openStream(inner)
	return &streamConn{Conn: outer, remote: c.sess.peer}
}

// HandleConn 把一条现成的连接(例如 CLI 接受的 TCP 连接)接进隧道。
// 与 OpenStream 的区别是:下游是真 TCP 时半关闭会透传(CloseWrite)。
func (c *Client) HandleConn(conn net.Conn) {
	c.mux.openStream(conn)
}

// Peer 返回隧道对端地址。
func (c *Client) Peer() *net.UDPAddr { return c.sess.peer }

// Stats 返回一行运行统计,格式见 session.statsLine。
func (c *Client) Stats() string { return c.sess.statsLine() }

// Close 关闭隧道与底层 UDP socket,所有 stream 随之断开。
func (c *Client) Close() error {
	c.mux.resetAll()
	c.sess.close()
	return nil
}

// streamConn 只是把 pipe 的 RemoteAddr 换成隧道对端 ——
// 嵌入方常靠 RemoteAddr 拿到"真正在网络上说话的那个 IP"(例如给防火墙开例外)。
type streamConn struct {
	net.Conn
	remote net.Addr
}

func (c *streamConn) RemoteAddr() net.Addr { return c.remote }

// PeerServer 是落地侧的固定对端模式:只与 -peer 指定的一个对端通信。
// 两端都是云主机、安全组默认拒绝入站 UDP 时,靠双方同时发心跳打洞 —— 这要求
// 落地侧从一开始就知道对端地址,所以保留这一模式。
type PeerServer struct {
	sess *session
	mux  *muxer
}

// NewPeerServer 在 conn 上服务固定对端 peer,每条 stream 连一次 target。
func NewPeerServer(conn *net.UDPConn, peer *net.UDPAddr, target string, o Options) (*PeerServer, error) {
	o = o.withDefaults()
	if err := o.validate(); err != nil {
		return nil, err
	}
	s := newSession(conn, peer, o.K, o.M, o.RateMbps)
	go s.readLoop()
	return &PeerServer{sess: s, mux: newMuxer(s, true, target)}, nil
}

func (p *PeerServer) Stats() string { return p.sess.statsLine() }

func (p *PeerServer) Close() error {
	p.mux.resetAll()
	p.sess.close()
	return nil
}
