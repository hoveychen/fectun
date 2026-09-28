package fectun

// 一次性探针(hole-diag P2):复现线上 4422 的形态 —— ko→sz 单向重丢、sz→ko 几乎
// 不丢,下行大流量与 ssh 式交互流混跑,看交互流会不会被 holeGiveUp 断开、为什么。
// 只在设了 PROBE=1 时跑。参数全走环境变量,见 probeEnv。

import (
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func probeEnv(name string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil {
		return v
	}
	return def
}

// asymProxy 只对来自 lossySrc 的包施加丢包:好态按 goodLoss 独立丢,坏态按 badLoss 丢。
// 好/坏态按时间走(与 lossyProxy 同理由),时长取指数分布。
type asymProxy struct {
	conn               *net.UDPConn
	mu                 sync.Mutex
	addrs              []*net.UDPAddr
	lossySrc           string
	goodLoss, badLoss  float64
	goodDur, badDur    time.Duration
	rnd                *rand.Rand
	tl                 *geState
	seenLossy, dropped int
}

func (p *asymProxy) run() {
	buf := make([]byte, 2048)
	for {
		n, src, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p.mu.Lock()
		known := false
		for _, a := range p.addrs {
			if a.String() == src.String() {
				known = true
			}
		}
		if !known && len(p.addrs) < 2 {
			cp := *src
			p.addrs = append(p.addrs, &cp)
		}
		var dst *net.UDPAddr
		for _, a := range p.addrs {
			if a.String() != src.String() {
				dst = a
			}
		}
		drop := false
		if src.String() == p.lossySrc {
			now := time.Now()
			if p.tl == nil {
				p.tl = &geState{start: now}
				var at time.Duration
				bad := false
				for at < 300*time.Second {
					mean := p.goodDur
					if bad {
						mean = p.badDur
					}
					at += time.Duration(p.rnd.ExpFloat64() * float64(mean))
					p.tl.flips = append(p.tl.flips, at)
					bad = !bad
				}
			}
			loss := p.goodLoss
			if p.tl.badAt(now) {
				loss = p.badLoss
			}
			drop = p.rnd.Float64() < loss
			p.seenLossy++
			if drop {
				p.dropped++
			}
		}
		p.mu.Unlock()
		if dst == nil || drop {
			continue
		}
		out := make([]byte, n)
		copy(out, buf[:n])
		p.conn.WriteToUDP(out, dst)
	}
}

func TestProbeHoleUnderAsymLoss(t *testing.T) {
	if os.Getenv("PROBE") != "1" {
		t.Skip("探针,设 PROBE=1 才跑")
	}
	rate := probeEnv("PROBE_RATE", 200)
	floor := probeEnv("PROBE_FLOOR", 10)
	goodLoss := probeEnv("PROBE_GOOD_LOSS", 0.05)
	badLoss := probeEnv("PROBE_BAD_LOSS", 0.7)
	badDur := time.Duration(probeEnv("PROBE_BAD_MS", 1000)) * time.Millisecond
	badFrac := probeEnv("PROBE_BAD_FRAC", 0.1) // 坏态时间占比
	dur := time.Duration(probeEnv("PROBE_SEC", 40)) * time.Second
	nBulk := int(probeEnv("PROBE_BULK", 2))
	nInter := int(probeEnv("PROBE_INTER", 4))

	// target:首字节 'B' → 无限下行灌数据;'I' → 回显
	tgt, _ := net.Listen("tcp", "127.0.0.1:0")
	defer tgt.Close()
	go func() {
		for {
			c, err := tgt.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				var b [1]byte
				if _, err := io.ReadFull(c, b[:]); err != nil {
					return
				}
				if b[0] == 'B' {
					go io.Copy(io.Discard, c)
					chunk := payload(32 << 10)
					for {
						if _, err := c.Write(chunk); err != nil {
							return
						}
					}
				}
				io.Copy(c, c)
			}()
		}
	}()

	pc, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	px := &asymProxy{conn: pc, goodLoss: goodLoss, badLoss: badLoss, badDur: badDur,
		goodDur: time.Duration(float64(badDur) * (1 - badFrac) / badFrac),
		rnd:     rand.New(rand.NewSource(int64(probeEnv("PROBE_SEED", 7))))}
	go px.run()
	defer pc.Close()
	pport := pc.LocalAddr().(*net.UDPAddr).Port

	cli := newTestSession(t, pport, 40, 40, rate)
	srv := newTestSession(t, pport, 40, 40, rate)
	px.lossySrc = srv.conn.LocalAddr().String()
	defer cli.close()
	defer srv.close()
	if floor > 0 {
		cli.enableCC(floor)
		srv.enableCC(floor)
	}
	time.Sleep(300 * time.Millisecond)
	cliMux := newMuxer(cli, false, "")
	newMuxer(srv, true, tgt.Addr().String())
	var kills atomic.Int32
	for _, s := range []*session{cli, srv} {
		orig := s.onStreamDead
		s.onStreamDead = func(sid uint32) { kills.Add(1); orig(sid) }
	}

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			cliMux.openStream(c)
		}
	}()

	end := time.Now().Add(dur)
	var wg sync.WaitGroup
	var bulkBytes atomic.Int64
	var deadInter, deadBulk atomic.Int32
	var latMu sync.Mutex
	var lats []time.Duration
	for i := 0; i < nBulk; i++ {
		c, _ := net.Dial("tcp", ln.Addr().String())
		defer c.Close()
		c.Write([]byte{'B'})
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 64<<10)
			for time.Now().Before(end) {
				c.SetReadDeadline(time.Now().Add(10 * time.Second))
				n, err := c.Read(buf)
				bulkBytes.Add(int64(n))
				if err != nil {
					deadBulk.Add(1)
					t.Logf("bulk 断开: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < nInter; i++ {
		c, _ := net.Dial("tcp", ln.Addr().String())
		defer c.Close()
		c.Write([]byte{'I'})
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := payload(64)
			got := make([]byte, 64)
			for time.Now().Before(end) {
				t0 := time.Now()
				c.SetDeadline(time.Now().Add(15 * time.Second))
				if _, err := c.Write(msg); err != nil {
					deadInter.Add(1)
					t.Logf("交互 写断开: %v", err)
					return
				}
				if _, err := io.ReadFull(c, got); err != nil {
					deadInter.Add(1)
					t.Logf("交互 读断开: %v", err)
					return
				}
				latMu.Lock()
				lats = append(lats, time.Since(t0))
				latMu.Unlock()
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
	// 每 5 秒打一次两端统计
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(5 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				t.Logf("cli %s", cli.statsLine())
				t.Logf("srv %s", srv.statsLine())
			}
		}
	}()
	wg.Wait()
	close(stop)

	px.mu.Lock()
	lossPct := float64(px.dropped) / float64(max(px.seenLossy, 1)) * 100
	px.mu.Unlock()
	p := func(q float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return sortedDur(lats)[int(q*float64(len(lats)-1))]
	}
	t.Logf("结果: 下行丢包=%.1f%% bulk=%dMB 断开 bulk=%d 交互=%d holeGiveUp触发=%d 交互延迟 p50=%s p99=%s max=%s",
		lossPct, bulkBytes.Load()>>20, deadBulk.Load(), deadInter.Load(), kills.Load(),
		p(0.5), p(0.99), p(1))
}

func sortedDur(d []time.Duration) []time.Duration {
	out := append([]time.Duration(nil), d...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
