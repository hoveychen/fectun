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
		switch {
		case h.seq > s.recvHigh:
			s.recvHigh = h.seq
			s.lowHbStreak = 0
		case h.seq < s.recvHigh:
			// 对端报告的 nextSeq 低于本端已知上界。两种可能:心跳乱序(偶发),
			// 或对端重置了序号空间(持续)。用连续次数区分,不用回退幅度 ——
			// 幅度可以小到几十,见 lowHbResetStreak。
			//
			// 这条不能靠 checkEpoch 兜住:对端是因为收到"我"的新 epoch 才重置
			// 序号的,它自己的进程没重启、epoch 没变,所以本端永远看不到 epoch
			// 变化。不跟着回退的话 recvHigh 停在高位,nackLoop 就永久对一段对端
			// 根本没发过的 seq 发 NACK —— 实测 780 个/秒。
			s.lowHbStreak++
			if s.lowHbStreak >= lowHbResetStreak {
				s.resetRecvLocked()
				s.recvHigh = h.seq
				rolledBack = true
			}
		default:
			s.lowHbStreak = 0
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
	// 数据片:记下全局 seq(供 NACK 缺口检测),并立刻按归属分发。
	// 关键是"立刻"—— 不再等全局 expected 连续推进到它。
	var ready []streamChunk
	if int(h.shardIdx) < k {
		seq := h.group*uint32(k) + uint32(h.shardIdx)
		if seq+1 > s.recvHigh {
			s.recvHigh = seq + 1
		}
		ready = s.acceptShardLocked(seq, payload)
	}
	ready = append(ready, s.tryRecover(h.group)...)
	s.advanceExpectedLocked()
	s.pruneGroups()
	s.recvMu.Unlock()

	// 交付必须在锁外:deliver 满时这里会阻塞,而 nackLoop 还要拿 recvMu。
	// 从前 drain 持锁写 channel,一条下游 TCP 写阻塞就把整个接收侧拘死。
	s.deliverAll(ready)
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

// FEC 恢复:组内收到 >= k 片即可重构全部数据片。
// 恢复出的分片和直达分片走同一条路 —— 载荷里带着 (streamID, streamSeq),
// 所以恢复出来立刻就知道该归给哪条 stream。这正是归属信息必须放在载荷里、
// 不能放包头的原因:包头不参与 FEC 编码,重构不出来。
func (s *session) tryRecover(group uint32) []streamChunk {
	if s.groupDone[group] {
		return nil
	}
	g := s.groups[group]
	if g == nil {
		return nil
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
		return nil
	}
	if present < s.k {
		return nil // 还不够,等更多分片或走 NACK
	}
	if err := s.enc.Reconstruct(g); err != nil {
		return nil
	}
	recovered := 0
	var ready []streamChunk
	for i := 0; i < s.k; i++ {
		seq := group*uint32(s.k) + uint32(i)
		if seq+1 > s.recvHigh {
			s.recvHigh = seq + 1
		}
		if g[i] == nil {
			continue
		}
		if seq >= s.expected && !s.recvSeen[seq] {
			recovered++
		}
		ready = append(ready, s.acceptShardLocked(seq, g[i])...)
	}
	s.groupDone[group] = true
	delete(s.groups, group)
	if recovered > 0 {
		s.stats.Lock()
		s.stats.fecRecovered += uint64(recovered)
		s.stats.Unlock()
	}
	return ready
}

// acceptShardLocked 收下一个已到达的数据分片:记下它的全局 seq(供 NACK 检测),
// 解出归属,投进对应 stream 的重组队列,并把该 stream 因此变得连续的部分收集返回。
//
// 它自己不往 deliver 写 —— 写 channel 会阻塞,而这里持着 recvMu。
// 调用者必须持有 recvMu,并在释放锁之后调 deliverAll。
func (s *session) acceptShardLocked(seq uint32, shard []byte) []streamChunk {
	// seq < expected 说明这一片早已处理过并从 recvSeen 清掉了;recvSeen 命中
	// 则是重复到达(FEC 恢复与直达包撞车,或迟到的重传)。两种都不能重复交付。
	if seq < s.expected || s.recvSeen[seq] {
		return nil
	}
	s.recvSeen[seq] = true

	sid, sseq, data, ok := parseShard(shard)
	if !ok {
		return nil
	}
	r := s.streamRecv[sid]
	if r == nil {
		r = &streamReasm{buf: make(map[uint32][]byte)}
		s.streamRecv[sid] = r
	}
	if sseq < r.expected {
		return nil // 这一片该 stream 已经交付过了
	}
	if r.buf[sseq] == nil {
		out := make([]byte, len(data))
		copy(out, data)
		r.buf[sseq] = out
	}

	// 只取该 stream 连续的部分。别的 stream 有没有空洞,与这里无关 ——
	// 这一行就是队头阻塞被解开的地方。
	var ready []streamChunk
	for {
		d, ok := r.buf[r.expected]
		if !ok {
			break
		}
		delete(r.buf, r.expected)
		r.expected++
		if len(d) > 0 {
			ready = append(ready, streamChunk{sid: sid, data: d})
		}
	}
	return ready
}

// advanceExpectedLocked 推进全局 expected 并回收记账。
//
// 全局 seq 现在只剩一个用途:给 nackLoop 划定缺口扫描的下界。它卡住不再
// 阻塞任何 stream 的交付 —— 数据在 acceptShardLocked 里就已经分发出去了。
// 调用者必须持有 recvMu。
func (s *session) advanceExpectedLocked() {
	for s.recvSeen[s.expected] {
		delete(s.recvSeen, s.expected)
		delete(s.firstSeen, s.expected)
		s.expected++
	}
}

// deliverAll 按序把 drainLocked 收集的字节交给上层。必须在锁外调用。
// 顺序性由调用方保证:onPacket 只由单个 readLoop goroutine 串行调用,
// 所以收集与交付都是串行的,字节流顺序不会乱。
// 这里阻塞是正确的背压 —— UDP 包会在内核缓冲排队,而 recvMu 是空闲的。
func (s *session) deliverAll(ready []streamChunk) {
	for _, out := range ready {
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
		newGaps := 0
		now := time.Now()
		// 开区间:q < high。recvHigh==0 时循环不执行,空载不会误发 NACK。
		for q := s.expected; q < high && len(want) < 32; q++ {
			if s.recvSeen[q] {
				continue
			}
			if f, ok := s.firstSeen[q]; !ok {
				// 首次发现这个缺口。FEC 能当场救回的分片根本不会走到这里
				// (recvSeen 已置位),所以 rawLost 记的是"FEC 没接住、
				// 要靠 ARQ 兜底"的量,不是链路总丢包。
				// 只在首次计一次 —— 同一缺口退避后会被反复 NACK。
				s.firstSeen[q] = now
				newGaps++
			} else if now.Sub(f) > 120*time.Millisecond {
				want = append(want, q)
				s.firstSeen[q] = now // 退避后可再次请求
			}
		}
		s.recvMu.Unlock()

		if newGaps > 0 {
			s.stats.Lock()
			s.stats.rawLost += uint64(newGaps)
			s.stats.Unlock()
		}
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
	s.lowHbStreak = 0
	s.prunedGroup = 0
	s.recvSeen = make(map[uint32]bool)
	s.groups = make(map[uint32][][]byte)
	s.groupDone = make(map[uint32]bool)
	s.firstSeen = make(map[uint32]time.Time)
	// 对端重启后 stream 全部作废,重组队列里的残片没有下文了。
	// 留着它们的话,新 stream 若复用了同一个 sid,残片会被当成新数据接上去。
	s.streamRecv = make(map[uint32]*streamReasm)
}

// resetSendAndNotify 把发送侧归零并通知上层重建 stream。
// 发送侧也要归零,否则对端(其 expected 已是 0)收不到我们的包。
func (s *session) resetSendAndNotify() {
	s.sendMu.Lock()
	s.nextSeq = 0
	s.curGroup = nil
	s.sendBuf = make(map[uint32][]byte)
	s.sendSeqOf = make(map[uint32]uint32)
	s.sendBufBytes = 0
	s.sendBufTail = 0
	s.sendMu.Unlock()

	if s.onReset != nil {
		go s.onReset()
	}
}
