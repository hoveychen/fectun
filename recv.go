package main

import (
	"time"
)

// 收到一个 UDP 包
func (s *session) onPacket(b []byte) {
	h, ok := parseHeader(b)
	if !ok {
		return
	}
	s.checkEpoch(h.epoch)
	switch h.typ {
	case pktHeartbeat:
		// 对端已发出 h.seq 个分片 → 最大 seq 应为 h.seq-1。
		// 没有这一步,尾部丢包时 maxRecvSeq 永远追不上,NACK 不会触发(死锁)。
		if h.seq > 0 {
			s.recvMu.Lock()
			if h.seq-1 > s.maxRecvSeq {
				s.maxRecvSeq = h.seq - 1
			}
			s.recvMu.Unlock()
		}
		return
	case pktNack:
		s.onNack(h.seq)
		return
	case pktData:
	default:
		return
	}
	payload := make([]byte, len(b)-hdrSize)
	copy(payload, b[hdrSize:])

	s.recvMu.Lock()
	s.stats.Lock()
	s.stats.rawRecv++
	s.stats.Unlock()

	k, m := int(h.k), int(h.m)
	if k == 0 || k != s.k {
		s.recvMu.Unlock()
		return
	}
	// 归入 FEC 组
	if !s.groupDone[h.group] {
		g := s.groups[h.group]
		if g == nil {
			g = make([][]byte, k+m)
			s.groups[h.group] = g
		}
		if int(h.shardIdx) < k+m && g[h.shardIdx] == nil {
			g[h.shardIdx] = payload
		}
	}
	// 数据片直接入重组缓冲
	if int(h.shardIdx) < k {
		seq := h.group*uint32(k) + uint32(h.shardIdx)
		if seq > s.maxRecvSeq {
			s.maxRecvSeq = seq
		}
		if seq >= s.expected && s.recvBuf[seq] == nil {
			s.recvBuf[seq] = payload
		}
	}
	s.tryRecover(h.group)
	s.drain()
	s.recvMu.Unlock()
}

// FEC 恢复:组内收到 >= k 片即可重构全部数据片
func (s *session) tryRecover(group uint32) {
	if s.groupDone[group] {
		return
	}
	g := s.groups[group]
	if g == nil {
		return
	}
	present, dataMissing := 0, 0
	for i, sh := range g {
		if sh != nil {
			present++
		} else if i < s.k {
			dataMissing++
		}
	}
	if dataMissing == 0 {
		s.groupDone[group] = true
		delete(s.groups, group)
		return
	}
	if present < s.k {
		return // 还不够,等更多分片或走 NACK
	}
	if err := s.enc.Reconstruct(g); err != nil {
		return
	}
	recovered := 0
	for i := 0; i < s.k; i++ {
		seq := group*uint32(s.k) + uint32(i)
		if seq > s.maxRecvSeq {
			s.maxRecvSeq = seq
		}
		if seq >= s.expected && s.recvBuf[seq] == nil && g[i] != nil {
			s.recvBuf[seq] = g[i]
			recovered++
		}
	}
	s.groupDone[group] = true
	delete(s.groups, group)
	if recovered > 0 {
		s.stats.Lock()
		s.stats.fecRecovered += uint64(recovered)
		s.stats.Unlock()
	}
}

// 按序交付连续到达的分片
func (s *session) drain() {
	for {
		sh := s.recvBuf[s.expected]
		if sh == nil {
			return
		}
		delete(s.recvBuf, s.expected)
		delete(s.firstSeen, s.expected)
		s.expected++
		if len(sh) < 2 {
			continue
		}
		n := int(sh[0])<<8 | int(sh[1])
		if n <= 0 || n > len(sh)-2 {
			continue
		}
		out := make([]byte, n)
		copy(out, sh[2:2+n])
		select {
		case s.deliver <- out:
		case <-s.closed:
			return
		}
	}
}

// NACK:FEC 没救回来的缺口,超时后请求重传(TCP 字节流必须完整有序)
func (s *session) nackLoop() {
	t := time.NewTicker(40 * time.Millisecond)
	defer t.Stop()
	buf := make([]byte, hdrSize)
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
		}
		s.recvMu.Lock()
		maxSeq := s.maxRecvSeq
		var want []uint32
		now := time.Now()
		for q := s.expected; q <= maxSeq && len(want) < 32; q++ {
			if s.recvBuf[q] != nil {
				continue
			}
			if f, ok := s.firstSeen[q]; !ok {
				s.firstSeen[q] = now
			} else if now.Sub(f) > 120*time.Millisecond {
				want = append(want, q)
				s.firstSeen[q] = now // 退避后可再次请求
			}
		}
		s.recvMu.Unlock()
		for _, q := range want {
			header{typ: pktNack, seq: q}.marshal(buf)
			s.conn.WriteToUDP(buf, s.peer)
			s.stats.Lock()
			s.stats.nackSent++
			s.stats.Unlock()
		}
	}
}

// 对端请求重传
func (s *session) onNack(seq uint32) {
	s.sendMu.Lock()
	shard := s.sendBuf[seq]
	s.sendMu.Unlock()
	if shard == nil {
		return
	}
	pkt := make([]byte, hdrSize+shardPayload)
	header{typ: pktData, shardIdx: byte(seq % uint32(s.k)), k: byte(s.k), m: byte(s.m),
		group: seq / uint32(s.k), seq: seq}.marshal(pkt)
	copy(pkt[hdrSize:], shard)
	s.acquire(len(pkt))
	s.conn.WriteToUDP(pkt, s.peer)
	s.stats.Lock()
	s.stats.retransSent++
	s.stats.Unlock()
}

// 检测对端是否重启。对端重启后其 nextSeq 归零,而本端 expected 仍停在旧的高位,
// 新包会被当作"旧包"丢弃 —— 双方都在正常收发却永久死锁。
// 收到新 epoch 时,双向重置序号空间并让上层重建连接。
func (s *session) checkEpoch(e uint32) {
	if e == 0 {
		return
	}
	s.recvMu.Lock()
	if s.peerEpoch == 0 {
		s.peerEpoch = e
		s.recvMu.Unlock()
		return
	}
	if e == s.peerEpoch {
		s.recvMu.Unlock()
		return
	}
	// 对端换了 epoch = 对端重启过
	s.peerEpoch = e
	s.expected = 0
	s.maxRecvSeq = 0
	s.recvBuf = make(map[uint32][]byte)
	s.groups = make(map[uint32][][]byte)
	s.groupDone = make(map[uint32]bool)
	s.firstSeen = make(map[uint32]time.Time)
	s.recvMu.Unlock()

	// 发送侧同样归零,让对端(其 expected 已是 0)能收到我们的包
	s.sendMu.Lock()
	s.nextSeq = 0
	s.curGroup = nil
	s.sendBuf = make(map[uint32][]byte)
	s.sendMu.Unlock()

	if s.onReset != nil {
		go s.onReset()
	}
}
