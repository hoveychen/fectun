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
		// h.seq = 对端已发出的分片总数,正好就是开区间上界。
		// 没有这一步,尾部丢包时 recvHigh 永远追不上,NACK 不会触发(死锁)。
		// h.seq==0 表示对端一个分片都没发过,此时不该有任何缺口。
		s.recvMu.Lock()
		rolledBack := false
		if h.seq > s.recvHigh {
			s.recvHigh = h.seq
		} else if h.seq+nackWindow < s.recvHigh {
			// 对端 nextSeq 显著回退 = 对端重置了序号空间。
			//
			// 这条不能靠 checkEpoch 兜住:对端是因为收到"我"的新 epoch 才重置的,
			// 它自己的进程没重启、epoch 没变,所以本端永远看不到 epoch 变化。
			// 不跟着回退的话 recvHigh 单调停在高位,nackLoop 就永久对一段对端
			// 根本没发过的高位 seq 发 NACK —— 实测 780 个/秒。
			//
			// 阈值取 nackWindow 而非 0:心跳可能乱序到达,一个迟到的旧心跳不该
			// 触发重置。真正的重置会让差距远超一个窗口。
			s.resetRecvLocked()
			s.recvHigh = h.seq
			rolledBack = true
		}
		s.recvMu.Unlock()
		if rolledBack {
			s.resetSendAndNotify()
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
	// 已淘汰记账的组:整组早已交付,这是迟到的重传或重复包。必须在这里丢掉 ——
	// 否则它会重新往 groups 里塞一条,而淘汰游标已经走过、永远不会回头删它。
	if h.group < s.prunedGroup {
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
		if seq+1 > s.recvHigh {
			s.recvHigh = seq + 1
		}
		if seq >= s.expected && s.recvBuf[seq] == nil {
			s.recvBuf[seq] = payload
		}
	}
	s.tryRecover(h.group)
	s.drain()
	s.pruneGroups()
	s.recvMu.Unlock()
}

// pruneGroups 淘汰"整组 seq 都已低于 expected"的组记账。
// 这些组的数据早已交付,groups/groupDone 里的条目再无用途;不删就是每组一条、
// 随累计流量线性增长的常驻内存。
// 用游标推进而不是遍历整个 map:开销与本次新过期的组数成正比,不是 map 大小。
// 调用者必须持有 recvMu。
func (s *session) pruneGroups() {
	// expected 是下一个待交付的 seq,故 expected/k 之前的组已整组交付完毕
	safe := s.expected / uint32(s.k)
	if safe <= groupKeepWindow {
		return
	}
	cutoff := safe - groupKeepWindow
	for g := s.prunedGroup; g < cutoff; g++ {
		delete(s.groups, g)
		delete(s.groupDone, g)
	}
	if cutoff > s.prunedGroup {
		s.prunedGroup = cutoff
	}
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
		if seq+1 > s.recvHigh {
			s.recvHigh = seq + 1
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
		high := s.recvHigh
		// 窗口上限:对端 seq 再高也只扫最近 nackWindow 个,否则单端重启时
		// 这个循环会一次性给 firstSeen 建几百万条目(见 nackWindow 注释)。
		if high > s.expected+nackWindow {
			high = s.expected + nackWindow
		}
		var want []uint32
		now := time.Now()
		// 开区间:q < high。recvHigh==0 时循环不执行,空载不会误发 NACK。
		for q := s.expected; q < high && len(want) < 32; q++ {
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
	s.resetRecvLocked()
	s.recvMu.Unlock()

	s.resetSendAndNotify()
}

// resetRecvLocked 把接收侧序号空间与全部记账归零。调用者必须持有 recvMu。
func (s *session) resetRecvLocked() {
	s.expected = 0
	s.recvHigh = 0
	s.prunedGroup = 0
	s.recvBuf = make(map[uint32][]byte)
	s.groups = make(map[uint32][][]byte)
	s.groupDone = make(map[uint32]bool)
	s.firstSeen = make(map[uint32]time.Time)
}

// resetSendAndNotify 把发送侧归零并通知上层重建 stream。
// 发送侧也要归零,否则对端(其 expected 已是 0)收不到我们的包。
func (s *session) resetSendAndNotify() {
	s.sendMu.Lock()
	s.nextSeq = 0
	s.curGroup = nil
	s.sendBuf = make(map[uint32][]byte)
	s.sendBufBytes = 0
	s.sendBufTail = 0
	s.sendMu.Unlock()

	if s.onReset != nil {
		go s.onReset()
	}
}
