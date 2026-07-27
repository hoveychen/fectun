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
	rate := flag.Float64("rate", 25, "发送限速 Mbps(含冗余),防止把链路打劣")
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

	go func() {
		buf := make([]byte, 2048)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			p := make([]byte, n)
			copy(p, buf[:n])
			sess.onPacket(p)
		}
	}()

	go func() {
		t := time.NewTicker(30 * time.Second)
		for range t.C {
			sess.stats.Lock()
			fmt.Printf("[stat] 收包=%d FEC恢复=%d NACK=%d 重传=%d\n",
				sess.stats.rawRecv, sess.stats.fecRecovered, sess.stats.nackSent, sess.stats.retransSent)
			sess.stats.Unlock()
		}
	}()

	mx := newMuxer(sess, *mode == "server", *target)

	if *mode == "server" {
		fmt.Printf("server 就绪: FEC/UDP :%d → TCP %s (k=%d m=%d rate=%.0fMbps)\n",
			*lport, *target, *k, *m, *rate)
		select {} // 常驻
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Println("TCP 监听失败:", err)
		os.Exit(1)
	}
	fmt.Printf("client 就绪: TCP %s → FEC/UDP → %s:%d (k=%d m=%d rate=%.0fMbps)\n",
		*listen, *peer, *uport, *k, *m, *rate)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		mx.openStream(c)
	}
}
