package fectun

import (
	"math"
	"testing"
	"time"
)

// feedLoss 以 100ms 一个反馈的节奏喂 n 个周期,每周期发 perTick 个、对端收到 (1-loss) 比例。
func feedLoss(c *congCtl, at *time.Time, tx, rx *uint32, n int, perTick uint32, loss float64) {
	for i := 0; i < n; i++ {
		*at = at.Add(100 * time.Millisecond)
		*tx += perTick
		*rx += uint32(math.Round(float64(perTick) * (1 - loss)))
		c.onFeedback(*at, *tx, *rx)
	}
}

const mbps = 1e6 / 8

func TestCCMeasuresLoss(t *testing.T) {
	c := newCongCtl(200*mbps, 10*mbps)
	at := time.Unix(1000, 0)
	var tx, rx uint32
	feedLoss(c, &at, &tx, &rx, 20, 1000, 0.2)
	_, loss, _, active := c.snapshot(at)
	if !active {
		t.Fatal("持续有反馈却显示未激活")
	}
	if math.Abs(loss-0.2) > 0.01 {
		t.Fatalf("丢包率测成 %.3f,应为 0.2", loss)
	}
}

// 本底随机丢包不得压住速率:这条链路本底就有 10~20% 丢包。
func TestCCClimbsUnderBaselineLoss(t *testing.T) {
	c := newCongCtl(200*mbps, 10*mbps)
	at := time.Unix(1000, 0)
	var tx, rx uint32
	feedLoss(c, &at, &tx, &rx, 80, 1000, 0.15)
	if r := c.rate(at); r < 200*mbps*0.999 {
		t.Fatalf("15%% 恒定本底丢包下 8 秒只爬到 %.1f Mbps,应到上限 200", r/mbps)
	}
}

func TestCCBacksOffOnExcessLoss(t *testing.T) {
	c := newCongCtl(200*mbps, 10*mbps)
	at := time.Unix(1000, 0)
	var tx, rx uint32
	feedLoss(c, &at, &tx, &rx, 80, 1000, 0.15)
	feedLoss(c, &at, &tx, &rx, 20, 1000, 0.30)
	after := c.rate(at)
	if after > 200*mbps*0.8 {
		t.Fatalf("丢包从本底 15%% 涨到 30%% 持续 2 秒,速率仍有 %.1f Mbps,没降", after/mbps)
	}
	// 一个样本窗口只降一次:2 秒里最多降 4 次
	if min := 200 * mbps * math.Pow(ccDecrease, 5); after < min {
		t.Fatalf("2 秒内降到 %.1f Mbps,低于 %.1f —— 同一次拥塞被重叠样本连降了多次", after/mbps, min/mbps)
	}
	// 拥塞消退后回升
	feedLoss(c, &at, &tx, &rx, 60, 1000, 0.15)
	if r := c.rate(at); r <= after {
		t.Fatalf("丢包回到本底 6 秒后速率 %.1f Mbps,没从 %.1f 回升", r/mbps, after/mbps)
	}
}

func TestCCRespectsFloor(t *testing.T) {
	c := newCongCtl(200*mbps, 10*mbps)
	at := time.Unix(1000, 0)
	var tx, rx uint32
	feedLoss(c, &at, &tx, &rx, 40, 1000, 0.0)
	feedLoss(c, &at, &tx, &rx, 300, 1000, 0.9) // 本底 10 秒后追上来之前一直降
	if r := c.rate(at); r < 10*mbps {
		t.Fatalf("速率掉到 %.1f Mbps,低于下限 10", r/mbps)
	}
}

// 老版本对端不回反馈:必须退回按上限固定发,两端才能分开升级。
func TestCCFallsBackWithoutFeedback(t *testing.T) {
	c := newCongCtl(50*mbps, 10*mbps)
	at := time.Unix(1000, 0)
	if r := c.rate(at); r != 50*mbps {
		t.Fatalf("从没收到反馈时速率 %.1f,应为上限 50", r/mbps)
	}
	var tx, rx uint32
	feedLoss(c, &at, &tx, &rx, 3, 1000, 0)
	if r := c.rate(at); r >= 50*mbps {
		t.Fatalf("刚开始有反馈就该从下限探起,实际 %.1f", r/mbps)
	}
	if r := c.rate(at.Add(2 * time.Second)); r != 50*mbps {
		t.Fatalf("反馈断了 2 秒,速率 %.1f,应退回上限 50", r/mbps)
	}
}

// 对端重启后累计收包数归零,那一段样本不能算成负丢包或巨大丢包。
func TestCCIgnoresPeerCounterReset(t *testing.T) {
	c := newCongCtl(200*mbps, 10*mbps)
	at := time.Unix(1000, 0)
	var tx, rx uint32
	feedLoss(c, &at, &tx, &rx, 20, 1000, 0.1)
	before := c.rate(at)
	rx = 0
	feedLoss(c, &at, &tx, &rx, 3, 1000, 0.1)
	if r := c.rate(at); r < before {
		t.Fatalf("对端计数归零被当成拥塞:速率从 %.1f 降到 %.1f", before/mbps, r/mbps)
	}
}

// 两端真跑:经 20% 随机丢包的 proxy,发端测到的丢包率应落在 20% 附近。
// rate 只给 20:这里只验测量,不需要高速率(也别让接收侧囤太多)。
func TestCCMeasuresRealLinkLoss(t *testing.T) {
	px := newLossyProxy(t, 0.2, 7)
	defer px.stop()
	cli := newTestSession(t, px.port(), 20, 20, 20)
	srv := newTestSession(t, px.port(), 20, 20, 20)
	defer cli.close()
	defer srv.close()
	cli.cc = newCongCtl(cli.rateBps, 10*mbps)
	time.Sleep(300 * time.Millisecond)

	go func() {
		for {
			select {
			case <-srv.deliver:
			case <-srv.closed:
				return
			}
		}
	}()
	chunk := payload(16 << 10)
	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		cli.writeStream(1, chunk)
	}
	_, loss, _, active := cli.cc.snapshot(time.Now())
	if !active {
		t.Fatalf("2 秒满负载都没收到对端反馈(发出 %d 包,对端收到 %d)", cli.txData.Load(), srv.rxData.Load())
	}
	// 反向的反馈包本身也过 20% 丢包,样本窗口两端错位会带来几个点的偏差
	if loss < 0.14 || loss > 0.26 {
		t.Fatalf("经 20%% 丢包链路测得丢包率 %.3f", loss)
	}
}
