package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

// 在可靠字节流之上做帧,实现多连接复用。
// 帧格式: [4B streamID][1B cmd][2B payloadLen][payload...]
const (
	cmdData     = 0
	cmdOpen     = 1
	cmdClose    = 2
	cmdShutdown = 3 // 半关闭:我不再发数据,但仍要接收(TCP 允许单向关闭)
	frameHdr    = 7
)

type muxer struct {
	sess     *session
	isServer bool
	target   string

	mu      sync.Mutex
	streams map[uint32]*stream
	nextID  uint32
	// 发送侧串行化:可靠层要求帧字节连续不交错
	writeMu sync.Mutex
}

type stream struct {
	id      uint32
	conn    net.Conn
	mux     *muxer
	closed  bool
	sentEOF bool // 本端已读完,已通知对端
	recvEOF bool // 对端已读完
	mu      sync.Mutex
}

func newMuxer(s *session, isServer bool, target string) *muxer {
	m := &muxer{sess: s, isServer: isServer, target: target,
		streams: make(map[uint32]*stream), nextID: 1}
	// 对端重启后序号空间已重置,旧 stream 全部失效,必须清掉
	s.onReset = m.resetAll
	go m.recvLoop()
	return m
}

func (m *muxer) sendFrame(sid uint32, cmd byte, data []byte) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	hdr := make([]byte, frameHdr)
	binary.BigEndian.PutUint32(hdr[0:4], sid)
	hdr[4] = cmd
	binary.BigEndian.PutUint16(hdr[5:7], uint16(len(data)))
	m.sess.write(hdr)
	if len(data) > 0 {
		m.sess.write(data)
	}
}

// 从可靠层持续读字节流,切出帧并分发
func (m *muxer) recvLoop() {
	var buf []byte
	for {
		select {
		case p := <-m.sess.deliver:
			buf = append(buf, p...)
		case <-m.sess.closed:
			return
		}
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
		}
	}
}

func (m *muxer) dispatch(sid uint32, cmd byte, payload []byte) {
	switch cmd {
	case cmdOpen:
		if !m.isServer {
			return
		}
		c, err := net.Dial("tcp", m.target)
		if err != nil {
			fmt.Printf("[mux] stream %d 连接 target 失败: %v\n", sid, err)
			m.sendFrame(sid, cmdClose, nil)
			return
		}
		st := &stream{id: sid, conn: c, mux: m}
		m.mu.Lock()
		m.streams[sid] = st
		m.mu.Unlock()
		fmt.Printf("[mux] stream %d 已连接 %s\n", sid, m.target)
		go st.pumpToTunnel()
	case cmdData:
		m.mu.Lock()
		st := m.streams[sid]
		m.mu.Unlock()
		if st != nil {
			st.conn.Write(payload)
		}
	case cmdShutdown:
		m.mu.Lock()
		st := m.streams[sid]
		m.mu.Unlock()
		if st == nil {
			return
		}
		// 对端不再发数据 → 关闭本地连接的写方向,让下游看到 EOF
		if tc, ok := st.conn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		st.mu.Lock()
		st.recvEOF = true
		both := st.sentEOF && st.recvEOF
		st.mu.Unlock()
		if both {
			st.close(false)
		}
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

// 本地 TCP → 隧道
func (s *stream) pumpToTunnel() {
	buf := make([]byte, 16*1024)
	for {
		n, err := s.conn.Read(buf)
		if n > 0 {
			// 单帧上限 65535,按 8K 切
			for off := 0; off < n; off += 8192 {
				end := off + 8192
				if end > n {
					end = n
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
			}
			if both {
				s.close(false)
			}
			return
		}
	}
}

func (s *stream) close(notify bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	s.conn.Close()
	s.mux.mu.Lock()
	delete(s.mux.streams, s.id)
	s.mux.mu.Unlock()
	if notify {
		s.mux.sendFrame(s.id, cmdClose, nil)
	}
	fmt.Printf("[mux] stream %d 关闭\n", s.id)
}

// client 侧:接受一个本地 TCP 连接,分配 stream
func (m *muxer) openStream(c net.Conn) {
	m.mu.Lock()
	sid := m.nextID
	m.nextID++
	st := &stream{id: sid, conn: c, mux: m}
	m.streams[sid] = st
	m.mu.Unlock()
	fmt.Printf("[mux] stream %d 新建(来自 %s)\n", sid, c.RemoteAddr())
	m.sendFrame(sid, cmdOpen, nil)
	go st.pumpToTunnel()
}

// 对端重启:关闭全部 stream。上层(ssh 等)会看到连接断开并自行重连。
func (m *muxer) resetAll() {
	m.mu.Lock()
	old := m.streams
	m.streams = make(map[uint32]*stream)
	m.mu.Unlock()
	for _, st := range old {
		st.mu.Lock()
		st.closed = true
		st.mu.Unlock()
		st.conn.Close()
	}
	if len(old) > 0 {
		fmt.Printf("[mux] 对端重启,已重置 %d 条 stream\n", len(old))
	}
}
