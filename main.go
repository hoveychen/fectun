package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	mode := flag.String("mode", "client", "client(监听TCP) | server(转发到target)")
	listen := flag.String("listen", "0.0.0.0:4422", "client 模式监听的 TCP 地址")
	target := flag.String("target", "127.0.0.1:22", "server 模式转发到的 TCP 地址")
	peer := flag.String("peer", "", "对端 IP")
	uport := flag.Int("uport", 55700, "对端 UDP 端口")
	lport := flag.Int("lport", 0, "本地 UDP 端口(默认同 uport;两端同端口便于打洞)")
	k := flag.Int("k", 20, "FEC 数据分片")
	m := flag.Int("m", 15, "FEC 校验分片")
	rate := flag.Float64("rate", 25, "发送速率上限 Mbps(含冗余);实际速率由拥塞控制在 [-rate-min, -rate] 内自动调")
	rateMin := flag.Float64("rate-min", 10, "拥塞控制的速率下限 Mbps;设 0 关闭拥塞控制,按 -rate 固定发")
	flag.Parse()

	if *peer == "" {
		fmt.Println("必须指定 -peer")
		os.Exit(1)
	}
	if *lport == 0 {
		*lport = *uport
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: *lport})
	if err != nil {
		fmt.Println("UDP bind 失败:", err)
		os.Exit(1)
	}
	conn.SetReadBuffer(16 << 20)
	conn.SetWriteBuffer(16 << 20)
	dst := &net.UDPAddr{IP: net.ParseIP(*peer), Port: *uport}
	sess := newSession(conn, dst, *k, *m, *rate)
	if *rateMin > 0 {
		sess.enableCC(*rateMin)
	}

	go sess.readLoop()

	go func() {
		t := time.NewTicker(30 * time.Second)
		for range t.C {
			fmt.Printf("[stat] %s\n", sess.statsLine())
		}
	}()

	mx := newMuxer(sess, *mode == "server", *target)

	if *mode == "server" {
		fmt.Printf("server 就绪: FEC/UDP :%d → TCP %s (k=%d m=%d rate=%.0fMbps rate-min=%.0fMbps)\n",
			*lport, *target, *k, *m, *rate, *rateMin)
		select {} // 常驻
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Println("TCP 监听失败:", err)
		os.Exit(1)
	}
	fmt.Printf("client 就绪: TCP %s → FEC/UDP → %s:%d (k=%d m=%d rate=%.0fMbps rate-min=%.0fMbps)\n",
		*listen, *peer, *uport, *k, *m, *rate, *rateMin)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		mx.openStream(c)
	}
}
