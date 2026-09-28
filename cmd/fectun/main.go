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
	peer := flag.String("peer", "", "对端 IP。server 模式留空 = 多对端,按源地址学对端(安全组需放行入站 UDP)")
	uport := flag.Int("uport", 55700, "对端 UDP 端口")
	lport := flag.Int("lport", 0, "本地 UDP 端口:0=同 uport(两端同端口便于打洞),-1=临时端口(NAT 后的客户端)")
	k := flag.Int("k", fectun.DefaultK, "FEC 数据分片")
	m := flag.Int("m", fectun.DefaultM, "FEC 校验分片")
	rate := flag.Float64("rate", fectun.DefaultRateMbps, "发送限速 Mbps(含冗余),防止把链路打劣")
	key := flag.String("key", "", "预共享密钥,两端必须一致;留空则读环境变量 FECTUN_KEY(避免密钥出现在 ps 里)")
	flag.Parse()

	if *key == "" {
		*key = os.Getenv("FECTUN_KEY")
	}
	multiPeer := *mode == "server" && *peer == ""
	if *peer == "" && !multiPeer {
		fmt.Println("client 模式必须指定 -peer")
		os.Exit(1)
	}
	switch *lport {
	case 0:
		*lport = *uport
	case -1:
		*lport = 0 // 交给内核挑
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: *lport})
	if err != nil {
		fmt.Println("UDP bind 失败:", err)
		os.Exit(1)
	}
	conn.SetReadBuffer(16 << 20)
	conn.SetWriteBuffer(16 << 20)
	dst := &net.UDPAddr{IP: net.ParseIP(*peer), Port: *uport}
	opts := fectun.Options{K: *k, M: *m, RateMbps: *rate, Key: []byte(*key)}
	auth := "无鉴权"
	if *key != "" {
		auth = "PSK 鉴权"
	}

	var stats func() string
	var cli *fectun.Client
	if multiPeer {
		srv, err := fectun.NewServer(conn, *target, opts)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		stats = srv.Stats
		go func() {
			if err := srv.Serve(); err != nil {
				fmt.Println("收包失败:", err)
				os.Exit(1)
			}
		}()
	} else if *mode == "server" {
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

	if multiPeer {
		// k/m 由各客户端的首包决定,这里的 -k/-m 不起作用
		fmt.Printf("server 就绪(多对端): FEC/UDP :%d → TCP %s (rate=%.0fMbps/对端, %s)\n",
			conn.LocalAddr().(*net.UDPAddr).Port, *target, *rate, auth)
		select {} // 常驻
	}
	if *mode == "server" {
		fmt.Printf("server 就绪: FEC/UDP :%d → TCP %s (k=%d m=%d rate=%.0fMbps, %s)\n",
			*lport, *target, *k, *m, *rate, auth)
		select {} // 常驻
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Println("TCP 监听失败:", err)
		os.Exit(1)
	}
	fmt.Printf("client 就绪: TCP %s → FEC/UDP → %s:%d (k=%d m=%d rate=%.0fMbps, %s)\n",
		*listen, *peer, *uport, *k, *m, *rate, auth)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		cli.HandleConn(c)
	}
}
