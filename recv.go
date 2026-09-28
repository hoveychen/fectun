package fectun

import (
	"fmt"
	"math"
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
	case pktFeedback:
		if s.cc != nil {
			s.cc.onFeedback(time.Now(), s.txData.Load(), h.seq)
		}
		return
	case pktData:
		s.rxData.Add(1)
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
	// 补位零片(flushStaleGroup 造的):dataLen=0、不属于任何 stream,
	// 只是占住 seq 让序号连续。这里必须提前返回 —— 否则每个补位片都会在
	// streamRecv 里留下一条 sid=0 的垃圾条目。mux 的 nextID 从 1 起,
	// sid=0 不会与真实 stream 撞号。
	if sid == 0 && len(data) == 0 {
		return nil
	}
	r := s.streamRecv[sid]
	if r == nil {
		r = &streamReasm{buf: make(map[uint32][]byte), since: time.Now(),
			bufGseq: make(map[uint32]uint32)}
		s.streamRecv[sid] = r
	}
	if sseq < r.expected {
		return nil // 这一片该 stream 已经交付过了
	}
	if r.buf[sseq] == nil {
		// 积压由空变非空 = 空洞刚出现:holeGiveUp 的计时从这一刻起算。
		// 只看"最后一次推进"的话,空闲超过 holeGiveUp 的 stream(ssh 常态)
		// 一出空洞就在下一个 nackLoop tick 被判死,首个 NACK 都来不及发 ——
		// 2026-09-28 线上 4422 的断开全是这样:缺 1 片、NACK 0 次。
		if len(r.buf) == 0 && sseq != r.expected {
			r.since = time.Now()
		}
		out := make([]byte, len(data))
		copy(out, data)
		r.buf[sseq] = out
		r.bufGseq[sseq] = seq
	}

	// 只取该 stream 连续的部分。别的 stream 有没有空洞,与这里无关 ——
	// 这一行就是队头阻塞被解开的地方。
	var ready []streamChunk
	advanced := false
	for {
		d, ok := r.buf[r.expected]
		if !ok {
			break
		}
		delete(r.buf, r.expected)
		r.lastGseq = r.bufGseq[r.expected]
		delete(r.bufGseq, r.expected)
		r.expected++
		advanced = true
		if len(d) > 0 {
			ready = append(ready, streamChunk{sid: sid, data: d})
		}
	}
	if advanced {
		r.since = time.Now()
	}
	return ready
}

// advanceExpectedLocked 推进全局 expected 并回收记账。
//
// 全局 seq 现在只剩一个用途:给 nackLoop 划定缺口扫描的下界。它卡住不再
// 阻塞任何 stream 的交付 —— 数据在 acceptShardLocked 里就已经分发出去了。
// 调用者必须持有 recvMu。
func (s *session) advanceExpectedLocked() {
	// 先放弃已经出了 ARQ 视野的空洞,再做正常的连续推进。
	s.abandonStaleGapsLocked()
	for s.recvSeen[s.expected] {
		delete(s.recvSeen, s.expected)
		delete(s.firstSeen, s.expected)
		delete(s.nackTries, s.expected)
		s.expected++
	}
}

// abandonStaleGapsLocked 把 expected 强行推到 recvHigh-nackWin,放弃那之前
// 一切还没到的 seq。调用者必须持有 recvMu。
//
// 为什么必须放弃:落后超过 nackWindow 的空洞已经不可能再补上。一是对端的
// sendBuf 只按字节预算留约 1 秒的在途数据(见 sendBufBudgetFor),落后几千个
// 分片时那一片早被淘汰,onNack 拿不到东西、重传不出来;二是 nackLoop 的扫描
// 窗口本身就只有 [expected, expected+nackWindow),expected 卡住时这个窗口会
// 停在一段永远填不上的死区上,既把新缺口挡在视野外(rawLost 从此失真),
// 又持续对死区里的 seq 空发 NACK。
//
// 2026-08-01 生产实测(落地侧 own-api-ko):待补涨到 40865(nackWindow 的 10 倍)
// 且以 +120 seq/s 单调增长,缺口计数冻在 154369,76 NACK/s 全打在入口侧已淘汰
// 的 seq 上无人应答,recvSeen 随之无界增长。per-stream 交付把功能影响掩掉了
// (别的 stream 照常收发),所以这条一直没被发现。
//
// 放弃空洞不会丢用户数据的账:那一片是真的没到,而它属于哪条 stream 都不知道
// (归属信息在载荷里,载荷本身丢了)。受影响的那条 stream 由它自己的重组队列
// 挡着,与全局 expected 无关 —— 这里放弃的只是全局记账,不是交付。
func (s *session) abandonStaleGapsLocked() {
	if s.recvHigh <= s.nackWin {
		return
	}
	floor := s.recvHigh - s.nackWin
	if s.expected >= floor {
		return
	}
	from, missing := s.expected, 0
	for ; s.expected < floor; s.expected++ {
		if !s.recvSeen[s.expected] {
			missing++
		}
		delete(s.recvSeen, s.expected)
		delete(s.firstSeen, s.expected)
		delete(s.nackTries, s.expected)
	}
	if missing > 0 {
		if len(s.abandoned) >= abandonLogMax {
			s.abandoned = s.abandoned[1:]
		}
		s.abandoned = append(s.abandoned, gapRange{from, floor})
		s.stats.Lock()
		s.stats.abandonedSeq += uint64(missing)
		s.stats.Unlock()
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
		now := time.Now()
		s.updateARQScaleLocked(now)
		high := s.recvHigh
		// 窗口上限:对端 seq 再高也只扫最近 nackWin 个,否则单端重启时
		// 这个循环会一次性给 firstSeen 建几百万条目(见 nackWindow 注释)。
		if high > s.expected+s.nackWin {
			high = s.expected + s.nackWin
		}
		var want []uint32
		newGaps := 0
		// 开区间:q < high。recvHigh==0 时循环不执行,空载不会误发 NACK。
		for q := s.expected; q < high && len(want) < s.nackBurst; q++ {
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
				s.nackTries[q]++
			}
		}
		dead := s.reapStuckStreamsLocked(now)
		s.recvMu.Unlock()

		if s.onStreamDead != nil {
			for _, sid := range dead {
				s.onStreamDead(sid)
			}
		}
		if newGaps > 0 {
			s.stats.Lock()
			s.stats.rawLost += uint64(newGaps)
			s.stats.Unlock()
		}
		for _, q := range want {
			header{typ: pktNack, seq: q}.marshal(buf)
			s.writePkt(buf)
			s.stats.Lock()
			s.stats.nackSent++
			s.stats.Unlock()
		}
	}
}

// updateARQScaleLocked 按实测的数据片 seq 推进速率,重算缺口窗口 nackWin 与
// 每 tick 的 NACK 上限 nackBurst。调用者必须持有 recvMu,且只由 nackLoop 调用。
//
// 用 recvHigh 的推进量而不是 rxData:rxData 连校验片一起数,而窗口和 NACK
// 都是按数据片 seq 算的,k=m 时两者差一倍。
// 估计值封顶在 arqMaxRate:recvHigh 会被心跳或同步注入一下顶高几千,
// 不封顶的话一个 tick 就能把窗口拉到上限。
func (s *session) updateARQScaleLocked(now time.Time) {
	cur := s.recvHigh
	if !s.arqLastAt.IsZero() {
		dt := now.Sub(s.arqLastAt).Seconds()
		var inst float64
		if cur >= s.arqLastHigh && dt > 0 {
			inst = float64(cur-s.arqLastHigh) / dt
		}
		if inst > s.arqMaxRate {
			inst = s.arqMaxRate
		}
		s.arqRate = math.Max(inst, s.arqRate*math.Exp(-dt/arqRateDecay.Seconds()))
	}
	s.arqLastHigh, s.arqLastAt = cur, now

	win := s.arqRate * nackSpan.Seconds()
	s.nackWin = uint32(math.Min(math.Max(win, nackWindow), nackWindowMax))
	burst := s.arqRate * nackBurstSpan.Seconds()
	s.nackBurst = int(math.Min(math.Max(burst, 32), 4096))
}

// maxNackWin 是本 session 的缺口窗口可能达到的最大值(测试用来对照)。
func (s *session) maxNackWin() uint32 {
	w := s.arqMaxRate * nackSpan.Seconds()
	return uint32(math.Min(math.Max(w, nackWindow), nackWindowMax))
}

// reapStuckStreamsLocked 找出空洞补不回的 stream(有积压但超过 holeGiveUp
// 未推进),清掉它们的重组状态并返回其 sid。调用者必须持有 recvMu。
func (s *session) reapStuckStreamsLocked(now time.Time) []uint32 {
	var dead []uint32
	for sid, r := range s.streamRecv {
		if len(r.buf) > 0 && now.Sub(r.since) > holeGiveUp {
			logf("[diag] stream %d 断开: %s", sid, s.diagStuckLocked(r, now))
			dead = append(dead, sid)
			delete(s.streamRecv, sid)
		}
	}
	return dead
}

// diagStuckLocked 描述一条卡住的 stream 当时的状态,供查"空洞为什么补不回"。
// 调用者必须持有 recvMu。
//
// 缺片的全局 seq 落在 (lastGseq, lo) 之间(lo = 积压里最小 streamSeq 那片的
// 全局 seq)。对这段区间分三类数:低于 expected 的(已收到或已放弃,看是否与
// 放弃记录重叠)、窗口内仍缺着的(及其被 NACK 的最多次数)、超出扫描窗口的。
func (s *session) diagStuckLocked(r *streamReasm, now time.Time) string {
	var minS, maxS uint32 = math.MaxUint32, 0
	for sseq := range r.buf {
		if sseq < minS {
			minS = sseq
		}
		if sseq > maxS {
			maxS = sseq
		}
	}
	lo := r.bufGseq[minS]
	from := r.lastGseq + 1
	if r.expected == 0 {
		from = 0 // 这条 stream 一片都没按序交付过
	}
	// 低于 expected 的不用逐个看;超出扫描窗口的只报跨度 —— 持着 recvMu,
	// 不能在这里扫几百万个 seq。
	var missingInWin, maxTries int
	scanTop := s.expected + s.nackWin
	// (不用内建 min/max:fecparam_test.go 里定义了一个 int 版的 min 把它遮住了)
	start, end := from, lo
	if start < s.expected {
		start = s.expected
	}
	var beyondWin uint32
	if end > scanTop {
		if start > scanTop {
			beyondWin = end - start
		} else {
			beyondWin = end - scanTop
		}
		end = scanTop
	}
	for q := start; q < end; q++ {
		if s.recvSeen[q] {
			continue
		}
		missingInWin++
		if t := int(s.nackTries[q]); t > maxTries {
			maxTries = t
		}
	}
	abandonedHit := 0
	for _, g := range s.abandoned {
		if g.from < lo && g.to > from {
			abandonedHit++
		}
	}
	return fmt.Sprintf("缺sseq=%d 积压=%d片[%d..%d] 停滞=%.1fs 缺片gseq∈[%d,%d) "+
		"窗口内仍缺=%d(最多NACK %d次) 超窗跨度=%d 与放弃区间重叠=%d段 "+
		"全局expected=%d recvHigh=%d nackWin=%d nackBurst=%d arqRate=%.0f/s",
		r.expected, len(r.buf), minS, maxS, now.Sub(r.since).Seconds(), from, lo,
		missingInWin, maxTries, beyondWin, abandonedHit,
		s.expected, s.recvHigh, s.nackWin, s.nackBurst, s.arqRate)
}

// 对端请求重传。
//
// 默认路径是同步发 —— 而 onNack 是在 readLoop → onPacket 的同步调用链上,
// sendRetrans 里的 acquire 会为等令牌睡眠。NACK 风暴时(生产 2026-08-03 实测
// 500 个/秒)接收循环就被自己的重传发送反复拘住,收包处理跟着变慢,超时的
// 缺口更多,对端 NACK 更凶 —— 一个自激回路。asyncRetrans 把发送挪出这条链,
// 用来验证这个假设。
func (s *session) onNack(seq uint32) {
	if s.asyncRetrans {
		select {
		case s.retransCh <- seq:
		default: // 队列满就丢,对端超时后会再 NACK
		}
		return
	}
	s.sendRetrans(seq)
}

func (s *session) retransLoop() {
	for {
		select {
		case <-s.closed:
			return
		case seq := <-s.retransCh:
			s.sendRetrans(seq)
		}
	}
}

func (s *session) sendRetrans(seq uint32) {
	s.sendMu.Lock()
	shard := s.sendBuf[seq]
	tail, next := s.sendBufTail, s.nextSeq
	s.sendMu.Unlock()
	if shard == nil {
		// 未命中分两种:seq < tail 是被字节预算淘汰了;seq >= next 是对端
		// 请求了本端还没发过的 seq(序号空间对不上)。日志限 1 条/秒。
		s.stats.Lock()
		s.stats.retransMiss++
		logIt := time.Since(s.stats.lastMissLog) >= time.Second
		if logIt {
			s.stats.lastMissLog = time.Now()
		}
		total := s.stats.retransMiss
		s.stats.Unlock()
		if logIt {
			logf("[diag] 重传未命中 seq=%d sendBufTail=%d nextSeq=%d 落后=%d 累计=%d",
				seq, tail, next, int64(next)-int64(seq), total)
		}
		return
	}
	pkt := make([]byte, hdrSize+shardPayload)
	header{typ: pktData, shardIdx: byte(seq % uint32(s.k)), k: byte(s.k), m: byte(s.m),
		group: seq / uint32(s.k), seq: seq}.marshal(pkt)
	copy(pkt[hdrSize:], shard)
	// 重传发 retransCopies 份:44% 丢包下单份重传自己还有 44% 概率再丢,
	// 于是又要等一个 120ms 判定加一个往返,交互延迟就是这么被堆到秒级的。
	// 发 3 份把"这一轮又白跑"的概率从 44% 压到 8.5%。默认 1 份,行为不变。
	n := s.retransCopies
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		s.acquire(len(pkt))
		s.writePkt(pkt)
		s.txData.Add(1)
	}
	s.stats.Lock()
	s.stats.retransSent += uint64(n)
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
	s.nackTries = make(map[uint32]uint16)
	s.abandoned = nil
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
