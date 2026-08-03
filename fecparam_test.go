package main

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
	for _, tc := range []struct{ loss, burstLen float64 }{
		{0.10, 20}, {0.45, 30}, {0.45, 5},
	} {
		p := &lossyProxy{rnd: rand.New(rand.NewSource(42))}
		p.setBurst(tc.loss, tc.burstLen)

		const n = 200000
		dropped, runs, curRun := 0, 0, 0
		for i := 0; i < n; i++ {
			if p.burstDrop("src") {
				dropped++
				curRun++
			} else {
				if curRun > 0 {
					runs++
				}
				curRun = 0
			}
		}
		if curRun > 0 {
			runs++
		}
		gotLoss := float64(dropped) / n
		gotBurst := float64(dropped) / float64(runs)
		t.Logf("目标 loss=%.2f burst=%.0f → 实测 loss=%.3f burst=%.1f",
			tc.loss, tc.burstLen, gotLoss, gotBurst)

		if gotLoss < tc.loss*0.9 || gotLoss > tc.loss*1.1 {
			t.Errorf("丢包率偏离:目标 %.2f,实测 %.3f", tc.loss, gotLoss)
		}
		if gotBurst < tc.burstLen*0.85 || gotBurst > tc.burstLen*1.15 {
			t.Errorf("平均突发长度偏离:目标 %.0f,实测 %.1f", tc.burstLen, gotBurst)
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
func runTrial(t *testing.T, k, m int, loss, burstLen float64, nBytes int, seed int64) trialResult {
	t.Helper()
	p := newLossyProxy(t, 0, seed)
	defer p.stop()
	if burstLen > 0 {
		p.setBurst(loss, burstLen)
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

	t.Logf("%-10s %-8s %8s %8s %8s %8s", "突发长度", "k/m", "吞吐MB/s", "FEC接住", "漏给ARQ", "NACK")
	for _, bl := range []float64{5, 10, 20, 40, 60} {
		for _, pm := range params {
			r := runTrial(t, pm.k, pm.m, 0.44, bl, nBytes, 7)
			t.Logf("%-10.0f %-8s %8.2f %7.0f%% %8d %8d",
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
		p.setBurst(0.08, 30)
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
		name     string
		loss     float64
		burstLen float64
	}{
		{"日常-突发8%x30", 0.08, 30},
		{"灾难-突发44%x30", 0.44, 30},
	}
	params := []struct{ k, m int }{{20, 20}, {40, 40}}

	for _, sc := range scenarios {
		t.Logf("=== %s (%d 次重复) ===", sc.name, reps)
		t.Logf("%-8s %10s %10s %10s %10s", "k/m", "吞吐均值", "吞吐最差", "FEC均值", "NACK均值")
		for _, pm := range params {
			var sumTP, worstTP, sumFec, sumNack float64
			worstTP = 1e9
			for i := 0; i < reps; i++ {
				r := runTrial(t, pm.k, pm.m, sc.loss, sc.burstLen, nBytes, int64(100+i*17))
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
		name     string
		loss     float64
		burstLen float64
	}{
		{"基线-均匀8%", 0.08, 0},
		{"基线-突发8%x30", 0.08, 30},
		{"灾难-均匀44%", 0.44, 0},
		{"灾难-突发44%x30", 0.44, 30},
	}

	for _, sc := range scenarios {
		t.Logf("=== %s ===", sc.name)
		t.Logf("%-8s %8s %7s %8s %8s %8s %8s", "k/m", "耗时", "完成", "吞吐MB/s", "FEC接住", "漏给ARQ", "NACK")
		for _, pm := range params {
			r := runTrial(t, pm.k, pm.m, sc.loss, sc.burstLen, nBytes, 7)
			t.Logf("%-8s %8s %7v %8.2f %7.0f%% %8d %8d",
				fmt.Sprintf("%d/%d", r.k, r.m), r.elapsed.Round(10*time.Millisecond),
				r.done, r.goodputMB, r.fecRate()*100, r.rawLost, r.nackSent)
		}
	}
}
