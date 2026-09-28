package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hoveychen/fectun"
)

func main() {
	mode := flag.String("mode", "client", "client(监听TCP) | server(转发到target)")
	listen := flag.String("listen", "0.0.0.0:4422", "client 模式监听的 TCP 地址")
	target := flag.String("target", "127.0.0.1:22", "server 模式转发到的 TCP 地址")
	peer := flag.String("peer", "", "对端 IP")
	uport := flag.Int("uport", 55700, "对端 UDP 端口")
	lport := flag.Int("lport", 0, "本地 UDP 端口(默认同 uport;两端同端口便于打洞)")
	k := flag.Int("k", fectun.DefaultK, "FEC 数据分片")
	m := flag.Int("m", fectun.DefaultM, "FEC 校验分片")
	rate := flag.Float64("rate", fectun.DefaultRateMbps, "发送限速 Mbps(含冗余),防止把链路打劣")
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
	opts := fectun.Options{K: *k, M: *m, RateMbps: *rate}

	var stats func() string
	var cli *fectun.Client
	if *mode == "server" {
		srv, err := fectun.NewPeerServer(conn, dst, *target, opts)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		stats = srv.Stats
	} else {
		cli, err = fectun.NewClient(conn, dst, opts)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		stats = cli.Stats
	}

	go func() {
		t := time.NewTicker(30 * time.Second)
		for range t.C {
			fmt.Printf("[stat] %s\n", stats())
		}
	}()

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
		cli.HandleConn(c)
	}
}
