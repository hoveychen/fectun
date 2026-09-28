package fectun

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server 是落地侧的多对端模式:不预设对端地址,按 UDP 源地址为每个对端
// 各建一个会话。
//
// 这是给 NAT 后面、IP 会变的客户端(笔记本、手机)用的:客户端先发包,
// 服务端从源地址学到该往哪回。代价是落地侧的安全组必须放行入站 UDP ——
// 它没法像 PeerServer 那样靠双向心跳打洞,因为事先不知道对端是谁。
//
// k/m 由客户端决定:会话用对端第一个包头里的 k/m 建立(心跳与数据包都带),
// 所以 Options 里的 K/M 在这里不起作用,只有 RateMbps 是每个会话的发送限速。
// 注意限速是逐会话的,N 个对端同时满载时总发送量是 N×RateMbps。
//
// 客户端换了源地址(换网、NAT 映射过期)在服务端看来就是一个新对端:新会话
// 的 epoch 不同,客户端据此重置并断开旧 stream,上层重连即可;旧会话空闲超时
// 后回收。
type Server struct {
	conn   *net.UDPConn
	target string
	opts   Options

	// IdleTimeout:对端多久没有任何包(心跳 100ms 一个)就回收其会话。
	IdleTimeout time.Duration
	// MaxPeers:同时存在的会话上限。超出时新对端的包直接丢弃,
	// 防止伪造源地址的包把内存和 goroutine 撑爆。
	MaxPeers int

	mu     sync.Mutex
	peers  map[string]*serverPeer
	closed chan struct{}
	once   sync.Once
}

type serverPeer struct {
	addr *net.UDPAddr
	sess *session
	mux  *muxer
	in   chan []byte
	last atomic.Int64 // 最近一次收包的 UnixNano
}

// 单个对端的收包队列深度。队列满就丢包,交给 FEC/ARQ 补:
// 一个对端的交付背压不能拖住共用 socket 上的其他对端。
const serverPeerQueue = 4096

const (
	DefaultIdleTimeout = 30 * time.Second
	DefaultMaxPeers    = 64
)

// NewServer 在 conn 上服务任意对端,每条 stream 连一次 target。
// 调用 Serve 开始收包。
func NewServer(conn *net.UDPConn, target string, o Options) (*Server, error) {
	o = o.withDefaults()
	return &Server{
		conn: conn, target: target, opts: o,
		IdleTimeout: DefaultIdleTimeout,
		MaxPeers:    DefaultMaxPeers,
		peers:       make(map[string]*serverPeer),
		closed:      make(chan struct{}),
	}, nil
}

// Serve 阻塞收包直到 Close 或 socket 出错。
func (s *Server) Serve() error {
	go s.reapLoop()
	buf := make([]byte, 2048)
	for {
		n, src, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-s.closed:
				return nil
			default:
				return err
			}
		}
		p := s.peerFor(src, buf[:n])
		if p == nil {
			continue
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		p.last.Store(time.Now().UnixNano())
		select {
		case p.in <- pkt:
		default: // 队列满:当作链路丢包
		}
	}
}

// peerFor 找到 src 的会话,没有就按首包决定是否新建。
func (s *Server) peerFor(src *net.UDPAddr, first []byte) *serverPeer {
	key := src.String()
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.peers[key]; p != nil {
		return p
	}
	h, ok := parseHeader(first)
	if !ok || h.epoch == 0 {
		// 只有带 epoch 的包(心跳、数据片)才能开会话;NACK 不带 epoch,
		// 它只可能发给一个已存在的会话。
		return nil
	}
	k, m := int(h.k), int(h.m)
	if k < 1 || m < 1 || k+m > 255 {
		return nil
	}
	if len(s.peers) >= s.MaxPeers {
		return nil
	}
	addr := *src
	sess := newSession(s.conn, &addr, k, m, s.opts.RateMbps)
	sess.sharedConn = true
	p := &serverPeer{addr: &addr, sess: sess, in: make(chan []byte, serverPeerQueue)}
	p.mux = newMuxer(sess, true, s.target)
	s.peers[key] = p
	go p.recvLoop()
	logf("[server] 新对端 %s (k=%d m=%d)", key, k, m)
	return p
}

func (p *serverPeer) recvLoop() {
	for {
		select {
		case b := <-p.in:
			p.sess.onPacket(b)
		case <-p.sess.closed:
			return
		}
	}
}

func (p *serverPeer) close() {
	p.mux.resetAll()
	p.sess.close()
}

func (s *Server) reapLoop() {
	period := s.IdleTimeout / 4
	if period <= 0 {
		period = time.Second
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
		}
		cutoff := time.Now().Add(-s.IdleTimeout).UnixNano()
		var dead []*serverPeer
		s.mu.Lock()
		for key, p := range s.peers {
			if p.last.Load() < cutoff {
				delete(s.peers, key)
				dead = append(dead, p)
			}
		}
		s.mu.Unlock()
		for _, p := range dead {
			logf("[server] 对端 %s 空闲超时,回收会话", p.addr)
			p.close()
		}
	}
}

// Peers 返回当前会话数。
func (s *Server) Peers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.peers)
}

// Stats 返回每个对端一行的运行统计。
func (s *Server) Stats() string {
	s.mu.Lock()
	lines := make([]string, 0, len(s.peers))
	for key, p := range s.peers {
		lines = append(lines, fmt.Sprintf("%s %s", key, p.sess.statsLine()))
	}
	s.mu.Unlock()
	if len(lines) == 0 {
		return "无对端"
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// Close 关闭所有会话与 socket,Serve 随之返回。
func (s *Server) Close() error {
	s.once.Do(func() { close(s.closed) })
	s.mu.Lock()
	peers := s.peers
	s.peers = make(map[string]*serverPeer)
	s.mu.Unlock()
	for _, p := range peers {
		p.close()
	}
	return s.conn.Close()
}
