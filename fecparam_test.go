package fectun

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// 这个文件是"该不该调 k/m"的评估台,不是回归测试。
//
// 起因:2026-08-03 19:39-19:49 生产上 own-api-sz ↔ own-api-ko 丢包从 8% 跳到 44%,
// 而 FEC恢复 从 17/s 掉到 0.3/s —— 50% 的冗余一片都没救回来。均匀丢包解释不了
// 这个现象(45% 均匀丢包下每组 40 片平均丢 18 片,小于 m=20,几乎组组可救),
// 所以真实链路那段一定是成组突发丢的。这里用 Gilbert 模型把两种丢包形态都造出来,
// 量化不同 k/m 的表现差异。
//
// 跑法:go test -run 'TestBurstModelShape|TestFecParamMatrix' -v -timeout 30m

// TestBurstModelShape 自检 Gilbert 模型:实际丢包率和平均连续丢包长度
// 必须落在设定值附近,否则后面整张矩阵的结论都是假的。
func TestBurstModelShape(t *testing.T) {
	for _, tc := range []struct {
		loss   float64
		badDur time.Duration
	}{
		{0.10, 200 * time.Millisecond},
		{0.45, 200 * time.Millisecond},
		{0.45, 50 * time.Millisecond},
	} {
		p := &lossyProxy{rnd: rand.New(rand.NewSource(42))}
		p.setBurst(tc.loss, tc.badDur)

		// 以固定步长扫过一段虚拟时间,统计落在坏态里的比例与坏态平均长度。
		// 步长取 1ms:远小于 badDur,不会把整段坏态跳过去。
		const step = time.Millisecond
		// 必须短于 buildTimeline 铺的 150s —— 超出末尾后 badAt 只会一直返回
		// 最后一段的状态,统计立刻失真(第一版写 600s,实测坏态"平均"1.46s)。
		const span = 120 * time.Second
		base := time.Unix(0, 0)
		badTicks, runs := 0, 0
		wasBad := false
		for el := time.Duration(0); el < span; el += step {
			bad := p.burstDrop("src", base.Add(el))
			if bad {
				badTicks++
				if !wasBad {
					runs++
				}
			}
			wasBad = bad
		}
		gotLoss := float64(badTicks) / float64(span/step)
		gotBad := time.Duration(float64(badTicks) / float64(runs) * float64(step))
		t.Logf("目标 loss=%.2f badDur=%v → 实测 loss=%.3f badDur=%v",
			tc.loss, tc.badDur, gotLoss, gotBad.Round(time.Millisecond))

		if gotLoss < tc.loss*0.9 || gotLoss > tc.loss*1.1 {
			t.Errorf("丢包率偏离:目标 %.2f,实测 %.3f", tc.loss, gotLoss)
		}
		if gotBad < time.Duration(float64(tc.badDur)*0.85) || gotBad > time.Duration(float64(tc.badDur)*1.15) {
			t.Errorf("坏态平均时长偏离:目标 %v,实测 %v", tc.badDur, gotBad)
		}
	}
}

type trialResult struct {
	k, m      int
	elapsed   time.Duration
	done      bool
	rawRecv   uint64
	rawLost   uint64
	fecRecov  uint64
	nackSent  uint64
	retrans   uint64
	goodputMB float64
}

// fecRate 是 FEC 接住的比例:恢复的片 / (恢复的片 + 漏给 ARQ 的片)。
// 1.0 表示 ARQ 完全不用出手,0 表示 FEC 形同虚设。
func (r trialResult) fecRate() float64 {
	den := r.fecRecov + r.rawLost
	if den == 0 {
		return 1
	}
	return float64(r.fecRecov) / float64(den)
}

// runTrial 让 a 推 nBytes 字节给 b,回报耗时与两侧计数器。
// rate 固定 25 Mbps —— 与生产 -rate 25 一致,好让结论能直接对上线上表现。
func runTrial(t *testing.T, k, m int, loss float64, badDur time.Duration, nBytes int, seed int64) trialResult {
	t.Helper()
	p := newLossyProxy(t, 0, seed)
	defer p.stop()
	if badDur > 0 {
		p.setBurst(loss, badDur)
	} else {
		p.mu.Lock()
		p.lossRate = loss
		p.mu.Unlock()
	}

	a := newTestSession(t, p.port(), k, m, 25)
	defer a.close()
	b := newTestSession(t, p.port(), k, m, 25)
	defer b.close()
	// 等心跳让 proxy 学到两个端点,否则头几片会因为没有对端地址被直接丢掉。
	time.Sleep(400 * time.Millisecond)

	data := payload(nBytes)
	start := time.Now()
	go a.writeStream(1, data)
	got := collect(b, nBytes, 90*time.Second)
	elapsed := time.Since(start)

	r := trialResult{k: k, m: m, elapsed: elapsed, done: len(got) == nBytes}
	b.stats.Lock()
	r.rawRecv, r.rawLost, r.fecRecov, r.nackSent = b.stats.rawRecv, b.stats.rawLost, b.stats.fecRecovered, b.stats.nackSent
	b.stats.Unlock()
	a.stats.Lock()
	r.retrans = a.stats.retransSent
	a.stats.Unlock()
	r.goodputMB = float64(len(got)) / 1e6 / elapsed.Seconds()
	return r
}

// TestBurstLenSweep 扫突发长度,找"m 要多大才接得住"的临界点。
//
// 直觉是 m >= 突发长度才救得回一组:一次连续丢 B 个包全落在同一组时,
// 组内缺 B 片,RS 要求缺片数 <= m。所以这张表实质是在验证 m 与 B 的关系,
// 顺便看现状 m=20 到底扛得住多长的突发。
func TestBurstLenSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const nBytes = 1 << 21
	params := []struct{ k, m int }{{20, 20}, {40, 40}, {60, 60}}

	t.Logf("%-10s %-8s %8s %8s %8s %8s", "坏态时长", "k/m", "吞吐MB/s", "FEC接住", "漏给ARQ", "NACK")
	for _, bl := range []time.Duration{20 * time.Millisecond, 50 * time.Millisecond,
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond} {
		for _, pm := range params {
			r := runTrial(t, pm.k, pm.m, 0.44, bl, nBytes, 7)
			t.Logf("%-10v %-8s %8.2f %7.0f%% %8d %8d",
				bl, fmt.Sprintf("%d/%d", r.k, r.m), r.goodputMB, r.fecRate()*100, r.rawLost, r.nackSent)
		}
	}
}

// TestLowRateLatency 量大组在低流量下的延迟代价。
//
// 生产上这条隧道大部分时间是交互式 SSH(收包 100-200/s)和几乎空转的 HTTPS
// (1-6/s),不是满速传输。低流量下一组攒不满,校验片要等 groupFlushAfter 补零
// flush 才发得出去,而 flush 的门槛是攒够 k/groupFlushMinDiv 片 —— k 越大,
// 这个门槛越高、等得越久。丢一片就得干等这段时间(或退到 120ms 的 ARQ)。
// 吞吐矩阵完全看不见这个代价,所以单独测。
func TestLowRateLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const (
		msgs     = 120
		interval = 50 * time.Millisecond
		msgSize  = 100
	)
	t.Logf("%-8s %8s %8s %8s %8s", "k/m", "p50", "p95", "max", "丢失")
	for _, pm := range []struct{ k, m int }{{10, 10}, {20, 20}, {40, 40}, {60, 60}} {
		p := newLossyProxy(t, 0, 11)
		p.setBurst(0.08, 200*time.Millisecond)
		a := newTestSession(t, p.port(), pm.k, pm.m, 25)
		b := newTestSession(t, p.port(), pm.k, pm.m, 25)
		time.Sleep(400 * time.Millisecond)

		sentAt := make([]time.Time, msgs)
		done := make(chan []time.Duration, 1)
		go func() {
			var lat []time.Duration
			got := 0
			for got < msgs {
				select {
				case <-b.deliver:
					lat = append(lat, time.Since(sentAt[got]))
					got++
				case <-time.After(5 * time.Second):
					done <- lat
					return
				}
			}
			done <- lat
		}()
		for i := 0; i < msgs; i++ {
			sentAt[i] = time.Now()
			a.writeStream(1, payload(msgSize))
			time.Sleep(interval)
		}
		lat := <-done
		a.close()
		b.close()
		p.stop()

		if len(lat) == 0 {
			t.Logf("%-8s   (一条都没到)", fmt.Sprintf("%d/%d", pm.k, pm.m))
			continue
		}
		sortDur(lat)
		t.Logf("%-8s %8s %8s %8s %8d", fmt.Sprintf("%d/%d", pm.k, pm.m),
			lat[len(lat)*50/100].Round(time.Millisecond),
			lat[len(lat)*95/100].Round(time.Millisecond),
			lat[len(lat)-1].Round(time.Millisecond),
			msgs-len(lat))
	}
}

// TestFecParamRepeat 用多个 seed 重复关键对比。
//
// 单次试验的方差很大(突发丢包本身就是重尾的),BurstLenSweep 里 60 突发那行
// 20/20→40/40→60/60 的吞吐是 0.29/0.90/0.74,不单调 —— 那是噪声,不是趋势。
// 要把"大组更抗突发"从观察升格成结论,必须重复。
func TestFecParamRepeat(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const nBytes = 1 << 21
	const reps = 5
	scenarios := []struct {
		name   string
		loss   float64
		badDur time.Duration
	}{
		{"日常-突发8%/200ms", 0.08, 200 * time.Millisecond},
		{"灾难-突发44%/200ms", 0.44, 200 * time.Millisecond},
	}
	params := []struct{ k, m int }{{20, 20}, {40, 40}}

	for _, sc := range scenarios {
		t.Logf("=== %s (%d 次重复) ===", sc.name, reps)
		t.Logf("%-8s %10s %10s %10s %10s", "k/m", "吞吐均值", "吞吐最差", "FEC均值", "NACK均值")
		for _, pm := range params {
			var sumTP, worstTP, sumFec, sumNack float64
			worstTP = 1e9
			for i := 0; i < reps; i++ {
				r := runTrial(t, pm.k, pm.m, sc.loss, sc.badDur, nBytes, int64(100+i*17))
				sumTP += r.goodputMB
				if r.goodputMB < worstTP {
					worstTP = r.goodputMB
				}
				sumFec += r.fecRate()
				sumNack += float64(r.nackSent)
			}
			t.Logf("%-8s %10.2f %10.2f %9.0f%% %10.0f", fmt.Sprintf("%d/%d", pm.k, pm.m),
				sumTP/reps, worstTP, sumFec/reps*100, sumNack/reps)
		}
	}
}

// TestInteractiveSurvival 测的是老板真正关心的那件事:链路很烂的时候,
// 交互式 SSH 还通不通、敲一下要等多久 —— 不是吞吐。
//
// 场景照着生产的交互流量捏:每 50ms 一个小包(约 20 包/s,对得上线上
// SSH 那条隧道 100-200 包/s 的量级),链路给 44% 突发丢包,也就是 8 月 3 日
// 19:39-19:49 那段的强度。
//
// 参数上特意扫了 m > k 的高冗余档:老板说带宽不重要,那 25 Mbps 的预算在
// 交互流量下根本用不完(20 包/s × 1200 B ≈ 192 kbps),完全可以拿带宽换纠错。
// 之前那张矩阵只比了 m == k 和 m < k,把这一整类漏了。
func TestInteractiveSurvival(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	params := []struct{ k, m int }{
		{20, 20}, // 现状
		{40, 40}, // 吞吐矩阵推荐的大组
		{20, 40}, // 200% 冗余
		{10, 30}, // 小组 + 300% 冗余
		{20, 60}, // 300% 冗余
	}

	const reps = 5
	t.Logf("%-8s %8s %8s %8s %6s %8s %7s %6s  (%d 次重复取中位)", "k/m", "p50", "p95", "max", "未送达", "占用kbps", "FEC救回", "NACK", reps)
	for _, pm := range params {
		var p50s, p95s, maxs []time.Duration
		var sumKbps, sumFec, sumNack float64
		lost := 0
		for rep := 0; rep < reps; rep++ {
			r := runInteractive(t, pm.k, pm.m, int64(11+rep*13), nil)
			p50s = append(p50s, r.p50)
			p95s = append(p95s, r.p95)
			maxs = append(maxs, r.max)
			sumKbps += r.kbps
			sumFec += float64(r.fec)
			sumNack += float64(r.nack)
			lost += r.lost
		}
		sortDur(p50s)
		sortDur(p95s)
		sortDur(maxs)
		t.Logf("%-8s %8s %8s %8s %6d %8.0f %7.0f %6.0f", fmt.Sprintf("%d/%d", pm.k, pm.m),
			p50s[reps/2].Round(time.Millisecond), p95s[reps/2].Round(time.Millisecond),
			maxs[reps/2].Round(time.Millisecond), lost,
			sumKbps/reps, sumFec/reps, sumNack/reps)
	}
}

// TestFlushThreshold 测低流量下把补零 flush 的门槛调低,能不能让 FEC 赶在
// ARQ 前面。
//
// 现状 k=20 时 flushMin = k/4 = 5 片,而交互流量只有 20 包/s —— 攒够 5 片就要
// 250ms,再等 flushAfter 40ms 才 flush,校验片出门时早过了对端 120ms 的 NACK
// 判定。也就是说低流量下 FEC 结构性地永远晚于 ARQ,交互延迟只能吃 ARQ 的
// 重传往返,而重传本身还要再丢 44%。
func TestFlushThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const reps = 5
	tunes := []struct {
		name  string
		min   int
		after time.Duration
	}{
		{"现状 k/4+40ms", 0, 0},
		{"min2+40ms", 2, 40 * time.Millisecond},
		{"min2+15ms", 2, 15 * time.Millisecond},
	}
	for _, pm := range []struct{ k, m int }{{20, 20}, {40, 40}} {
		t.Logf("=== k/m = %d/%d ===", pm.k, pm.m)
		t.Logf("%-14s %8s %8s %8s %8s %7s %6s", "flush 门槛", "p50", "p95", "max", "占用kbps", "FEC救回", "NACK")
		for _, tn := range tunes {
			var p50s, p95s, maxs []time.Duration
			var sumKbps, sumFec, sumNack float64
			for rep := 0; rep < reps; rep++ {
				var tune func(*session)
				if tn.min > 0 {
					tune = func(s *session) { s.flushMin = tn.min; s.flushAfter = tn.after }
				}
				r := runInteractive(t, pm.k, pm.m, int64(11+rep*13), tune)
				p50s = append(p50s, r.p50)
				p95s = append(p95s, r.p95)
				maxs = append(maxs, r.max)
				sumKbps += r.kbps
				sumFec += float64(r.fec)
				sumNack += float64(r.nack)
			}
			sortDur(p50s)
			sortDur(p95s)
			sortDur(maxs)
			t.Logf("%-14s %8s %8s %8s %8.0f %7.0f %6.0f", tn.name,
				p50s[reps/2].Round(time.Millisecond), p95s[reps/2].Round(time.Millisecond),
				maxs[reps/2].Round(time.Millisecond), sumKbps/reps, sumFec/reps, sumNack/reps)
		}
	}
}

// TestRedundantRetrans 测冗余重传:交互延迟的秒级长尾来自 ARQ 多轮往返,
// 而不是 FEC 参数 —— 参数矩阵已经证明 20/20 在交互场景就是最优的那档。
//
// 44% 丢包下,一份重传有 44% 概率再丢,再等 120ms 判定 + 一个往返;两轮没中
// 就是 400ms 起步。发 3 份把单轮失手率压到 8.5%。交互流量本身只有 ~192 kbps,
// 而重传只发生在丢包的那些片上,所以多发几份的带宽代价在 25 Mbps 预算里
// 几乎看不见 —— 这正是老板"带宽不重要、通畅最重要"下该做的交换。
func TestRedundantRetrans(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const reps = 5
	t.Logf("%-10s %8s %8s %8s %8s %8s %7s", "重传份数", "p50", "p90", "p95", "p99", "占用kbps", "NACK")
	for _, copies := range []int{1, 2, 3} {
		var all []time.Duration
		var sumKbps, sumNack float64
		lost := 0
		for rep := 0; rep < reps; rep++ {
			c := copies
			r := runInteractiveN(t, 20, 20, int64(11+rep*13),
				func(s *session) { s.retransCopies = c }, 400)
			all = append(all, r.lat...)
			sumKbps += r.kbps
			sumNack += float64(r.nack)
			lost += r.lost
		}
		sortDur(all)
		q := func(p int) string {
			if len(all) == 0 {
				return "-"
			}
			return all[min(len(all)*p/100, len(all)-1)].Round(time.Millisecond).String()
		}
		t.Logf("%-10d %8s %8s %8s %8s %8.0f %7.0f  (n=%d, 未送达 %d)",
			copies, q(50), q(90), q(95), q(99), sumKbps/reps, sumNack/reps, len(all), lost)
	}
}

// TestAsyncRetrans 验证"接收循环被自己的重传发送拘住"这个假设。
//
// 前面三个方案(大分组 / 降 flush 门槛 / 冗余重传)全是负优化,共同点都是
// 多发包。如果瓶颈真在 onNack 同步阻塞 readLoop,那么什么都不多发、只是把
// 重传挪到独立 goroutine,延迟就该改善 —— 这是能把假设和"多发包有害"
// 区分开的判决性实验。
//
// 只需在发送侧开:本场景数据是 a→b 单向的,NACK 全打在 a 上,被拘住的也是
// a 的 readLoop —— 而它正是负责及时收下一批 NACK 的那条循环。
func TestAsyncRetrans(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const reps = 5
	// 配对比较:同一个 seed 下同步跑一遍、异步跑一遍。整体分位数差 19% 落在
	// 单轮波动(实测 ±18%)的边缘,分不清是真改善还是运气;逐 seed 配对能看出
	// 方向是否一致 —— 5 个 seed 全朝同一边才算数。
	t.Logf("--- 逐 seed 配对(p95) ---")
	wins := 0
	for rep := 0; rep < reps; rep++ {
		seed := int64(11 + rep*13)
		var p [2]time.Duration
		for i, as := range []bool{false, true} {
			a := as
			r := runInteractiveN(t, 20, 20, seed, func(s *session) { s.asyncRetrans = a }, 400)
			p[i] = r.p95
		}
		mark := "同步赢"
		if p[1] < p[0] {
			mark = "异步赢"
			wins++
		}
		t.Logf("seed %3d: 同步 %8s  异步 %8s  → %s", seed,
			p[0].Round(time.Millisecond), p[1].Round(time.Millisecond), mark)
	}
	t.Logf("异步在 %d/%d 个 seed 上更优", wins, reps)

	t.Logf("--- 合并样本 ---")
	t.Logf("%-12s %8s %8s %8s %8s %8s %7s", "重传路径", "p50", "p90", "p95", "p99", "占用kbps", "NACK")
	for _, async := range []bool{false, true} {
		var all []time.Duration
		var sumKbps, sumNack float64
		lost := 0
		for rep := 0; rep < reps; rep++ {
			as := async
			r := runInteractiveN(t, 20, 20, int64(11+rep*13),
				func(s *session) { s.asyncRetrans = as }, 400)
			all = append(all, r.lat...)
			sumKbps += r.kbps
			sumNack += float64(r.nack)
			lost += r.lost
		}
		sortDur(all)
		q := func(p int) string {
			if len(all) == 0 {
				return "-"
			}
			return all[min(len(all)*p/100, len(all)-1)].Round(time.Millisecond).String()
		}
		name := "同步(现状)"
		if async {
			name = "异步"
		}
		t.Logf("%-12s %8s %8s %8s %8s %8.0f %7.0f  (n=%d, 未送达 %d)",
			name, q(50), q(90), q(95), q(99), sumKbps/reps, sumNack/reps, len(all), lost)
	}
}

type interactiveResult struct {
	p50, p95, max time.Duration
	lat           []time.Duration // 原始样本,供跨轮合并后再算分位数
	lost          int
	kbps          float64
	fec, nack     uint64
}

// TestInteractiveDecisive 是 20/20 与 40/40 在交互场景下的定论实验。
//
// 前两轮测出来方向互相打架(一轮 40/40 的 p95 是 20/20 的一半,另一轮反过来),
// 原因是统计功效不够:每轮只有 120 条消息,p95 实际只由 6 个样本决定,再取
// 5 轮中位数也救不回来。这里每轮 400 条、5 轮合并成 2000 个样本后再算分位数,
// p95 由 100 个样本支撑。
func TestInteractiveDecisive(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const reps = 5
	t.Logf("%-8s %8s %8s %8s %8s %8s %7s %6s", "k/m", "p50", "p90", "p95", "p99", "占用kbps", "FEC救回", "NACK")
	for _, pm := range []struct{ k, m int }{{20, 20}, {40, 40}} {
		var all []time.Duration
		var sumKbps, sumFec, sumNack float64
		lost := 0
		for rep := 0; rep < reps; rep++ {
			r := runInteractiveN(t, pm.k, pm.m, int64(11+rep*13), nil, 400)
			all = append(all, r.lat...)
			sumKbps += r.kbps
			sumFec += float64(r.fec)
			sumNack += float64(r.nack)
			lost += r.lost
		}
		sortDur(all)
		q := func(p int) string {
			if len(all) == 0 {
				return "-"
			}
			return all[min(len(all)*p/100, len(all)-1)].Round(time.Millisecond).String()
		}
		t.Logf("%-8s %8s %8s %8s %8s %8.0f %7.0f %6.0f  (n=%d, 未送达 %d)",
			fmt.Sprintf("%d/%d", pm.k, pm.m), q(50), q(90), q(95), q(99),
			sumKbps/reps, sumFec/reps, sumNack/reps, len(all), lost)
	}
}

// runInteractive 跑一轮交互式小包场景,回报延迟分布与代价。
// tune 非空时用来在开跑前改发送侧的 flush 门槛。
func runInteractive(t *testing.T, k, m int, seed int64, tune func(*session)) interactiveResult {
	return runInteractiveN(t, k, m, seed, tune, 120)
}

func runInteractiveN(t *testing.T, k, m int, seed int64, tune func(*session), msgs int) interactiveResult {
	t.Helper()
	const (
		interval = 50 * time.Millisecond
		msgSize  = 100
	)
	p := newLossyProxy(t, 0, seed)
	p.setBurst(0.44, 200*time.Millisecond)
	a := newTestSession(t, p.port(), k, m, 25)
	b := newTestSession(t, p.port(), k, m, 25)
	if tune != nil {
		tune(a)
	}
	time.Sleep(400 * time.Millisecond)
	p.mu.Lock()
	p.seen = 0 // 不计握手期的心跳
	p.mu.Unlock()

	sentAt := make([]time.Time, msgs)
	done := make(chan []time.Duration, 1)
	go func() {
		var lat []time.Duration
		for len(lat) < msgs {
			select {
			case <-b.deliver:
				lat = append(lat, time.Since(sentAt[len(lat)]))
			case <-time.After(15 * time.Second):
				done <- lat
				return
			}
		}
		done <- lat
	}()
	start := time.Now()
	for i := 0; i < msgs; i++ {
		sentAt[i] = time.Now()
		a.writeStream(1, payload(msgSize))
		time.Sleep(interval)
	}
	lat := <-done
	wall := time.Since(start).Seconds()
	p.mu.Lock()
	pkts := p.seen
	p.mu.Unlock()
	b.stats.Lock()
	fec, nack := b.stats.fecRecovered, b.stats.nackSent
	b.stats.Unlock()
	a.close()
	b.close()
	p.stop()

	r := interactiveResult{
		lost: msgs - len(lat), fec: fec, nack: nack,
		kbps: float64(pkts) * float64(hdrSize+shardPayload) * 8 / wall / 1000,
	}
	if len(lat) == 0 {
		return r
	}
	r.lat = append([]time.Duration(nil), lat...)
	sortDur(lat)
	r.p50 = lat[len(lat)*50/100]
	r.p95 = lat[min(len(lat)*95/100, len(lat)-1)]
	r.max = lat[len(lat)-1]
	return r
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sortDur(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

// TestFecParamMatrix 跑参数矩阵:每种丢包形态下比较几组 k/m。
// 它不做断言 —— 输出的是决策依据,不是通过/失败。
func TestFecParamMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("评估台,-short 下跳过")
	}
	const nBytes = 1 << 21 // 2 MB

	params := []struct{ k, m int }{
		{20, 20}, // 现状:100% 冗余
		{20, 10}, // 半冗余,省一半带宽
		{10, 10}, // 小组 + 100% 冗余
		{10, 5},  // 小组 + 半冗余
		{40, 40}, // 大组 + 100% 冗余
	}
	scenarios := []struct {
		name   string
		loss   float64
		badDur time.Duration
	}{
		{"基线-均匀8%", 0.08, 0},
		{"基线-突发8%/200ms", 0.08, 200 * time.Millisecond},
		{"灾难-均匀44%", 0.44, 0},
		{"灾难-突发44%/200ms", 0.44, 200 * time.Millisecond},
	}

	for _, sc := range scenarios {
		t.Logf("=== %s ===", sc.name)
		t.Logf("%-8s %8s %7s %8s %8s %8s %8s", "k/m", "耗时", "完成", "吞吐MB/s", "FEC接住", "漏给ARQ", "NACK")
		for _, pm := range params {
			r := runTrial(t, pm.k, pm.m, sc.loss, sc.badDur, nBytes, 7)
			t.Logf("%-8s %8s %7v %8.2f %7.0f%% %8d %8d",
				fmt.Sprintf("%d/%d", r.k, r.m), r.elapsed.Round(10*time.Millisecond),
				r.done, r.goodputMB, r.fecRate()*100, r.rawLost, r.nackSent)
		}
	}
}
