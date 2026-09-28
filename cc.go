package fectun

import (
	"sync"
	"time"
)

// 拥塞控制:-rate 从固定速率变成上限,实际发送速率按对端回报的丢包率自动调。
//
// 为什么需要:固定速率一旦高于链路当下能承载的量,丢包会先压垮 ARQ —— 每 40ms
// 至多 32 个 NACK,而重传缓冲只留约 1 秒,突发丢的片在请求到之前就被淘汰,
// 那条 stream 从此永久挂住。2026-09-28 本机 Docker(2 核)实测 rate≥200 必现。
//
// 判据是"丢包高于本底",不是"有丢包":这条跨境链路本底就有 10~20% 随机丢包
// (见 proto.go 头注释),照搬 TCP 的"一丢就降"会被本底丢包一直压在下限。
// 本底取最近 ccBaseSpan 内的最低丢包率 —— 拥塞造成的丢包会随降速消失,
// 随机丢包不会,所以窗口内的最低值就是随机那部分。
//
// 丢包率必须分方向测:ICMP 与 UDP 丢包无关且两个方向不对称,只有对端数到的
// 包才作数。收端每个心跳周期回一个 pktFeedback,带上累计收到的 pktData 数;
// 发端拿同一时段自己发出的数来比。
const (
	ccBaseSpan   = 10 * time.Second       // 本底丢包率的回看窗口
	ccExcess     = 0.05                   // 高出本底多少才算拥塞
	ccLossSpan   = 500 * time.Millisecond // 单个丢包率样本覆盖的时长
	ccMinPkts    = 50                     // 样本内发包少于这个数就不算(低流量没有信息量)
	ccDecrease   = 0.85                   // 拥塞时乘性降速
	ccIncreaseOf = 0.02                   // 每个样本加速上限的这个比例:从下限到上限约 5 秒
	ccStale      = time.Second            // 超过这么久没收到反馈,退回固定上限
)

type fbPoint struct {
	at     time.Time
	tx, rx uint32
}

type lossSample struct {
	at   time.Time
	loss float64
}

type congCtl struct {
	mu          sync.Mutex
	ceil, floor float64 // 字节/秒
	cur         float64

	fb           []fbPoint // 最近 ccLossSpan 多一点的反馈点,用来算样本
	hist         []lossSample
	lastFeedback time.Time
	lastDecrease time.Time
	lastLoss     float64
	lastBase     float64
}

func newCongCtl(ceilBps, floorBps float64) *congCtl {
	if floorBps > ceilBps {
		floorBps = ceilBps
	}
	return &congCtl{ceil: ceilBps, floor: floorBps, cur: floorBps}
}

// rate 返回当前该用的发送速率(字节/秒)。
//
// 从没收到过反馈、或反馈断了超过 ccStale:对端是不回反馈的老版本,
// 或者反向链路断了。此时退回原来的行为 —— 按上限固定发,两端才能分开升级。
func (c *congCtl) rate(now time.Time) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastFeedback.IsZero() || now.Sub(c.lastFeedback) > ccStale {
		return c.ceil
	}
	return c.cur
}

// onFeedback 处理对端回报的累计收包数 rx;tx 是本端此刻累计发出的 pktData 数。
// 两个计数都是 uint32 累计值,相减自然处理回绕。
func (c *congCtl) onFeedback(now time.Time, tx, rx uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastFeedback.IsZero() || now.Sub(c.lastFeedback) > ccStale {
		// 第一次有反馈(或断了很久刚恢复):旧的样本不再可信,从下限重新探。
		c.fb = c.fb[:0]
		c.hist = c.hist[:0]
		c.cur = c.floor
	}
	c.lastFeedback = now
	c.fb = append(c.fb, fbPoint{now, tx, rx})

	// 找最老的、距今已满 ccLossSpan 的点作为样本起点,更老的丢掉。
	start := -1
	for i := len(c.fb) - 1; i >= 0; i-- {
		if now.Sub(c.fb[i].at) >= ccLossSpan {
			start = i
			break
		}
	}
	if start < 0 {
		return
	}
	old := c.fb[start]
	c.fb = append(c.fb[:0], c.fb[start:]...)

	dtx, drx := tx-old.tx, rx-old.rx
	if drx > dtx+dtx/2 {
		// 对端计数回退(对端重启)或乱序得离谱,这一段没法用。
		c.fb = c.fb[:0]
		return
	}
	if dtx < ccMinPkts {
		return
	}
	loss := 1 - float64(drx)/float64(dtx)
	if loss < 0 {
		loss = 0 // 在途量变化造成的小幅负值
	}
	c.addSample(now, loss)
}

func (c *congCtl) addSample(now time.Time, loss float64) {
	c.hist = append(c.hist, lossSample{now, loss})
	i := 0
	for i < len(c.hist) && now.Sub(c.hist[i].at) > ccBaseSpan {
		i++
	}
	c.hist = c.hist[i:]
	base := loss
	for _, h := range c.hist {
		if h.loss < base {
			base = h.loss
		}
	}
	c.lastLoss, c.lastBase = loss, base

	if loss > base+ccExcess {
		// 一个样本窗口内只降一次:相邻样本的窗口是重叠的,降速后的头 ccLossSpan
		// 里仍会看到降速前的丢包,不挡的话同一次拥塞会被连降好几次。
		if now.Sub(c.lastDecrease) >= ccLossSpan {
			c.cur *= ccDecrease
			c.lastDecrease = now
		}
	} else {
		c.cur += c.ceil * ccIncreaseOf
	}
	if c.cur > c.ceil {
		c.cur = c.ceil
	}
	if c.cur < c.floor {
		c.cur = c.floor
	}
}

// snapshot 给统计行用:当前速率、最近样本的丢包率与本底。
func (c *congCtl) snapshot(now time.Time) (rateBps, loss, base float64, active bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	active = !c.lastFeedback.IsZero() && now.Sub(c.lastFeedback) <= ccStale
	rateBps = c.ceil
	if active {
		rateBps = c.cur
	}
	return rateBps, c.lastLoss, c.lastBase, active
}
