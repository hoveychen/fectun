package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// 进程级集成:真实二进制,多对端 server + 两个临时端口 client(k/m 各不相同)+
// 一个没带密钥的 client。密钥从环境变量传入,验证 FECTUN_KEY 通路。
func TestCLIMultiPeerWithKey(t *testing.T) {
	if testing.Short() {
		t.Skip("要编译二进制,-short 下跳过")
	}
	bin := filepath.Join(t.TempDir(), "fectun")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	echo := startEcho(t)
	uport := freeUDPPort(t)

	run(t, bin, []string{"FECTUN_KEY=k3y"}, "-mode", "server", "-uport", uport, "-target", echo, "-rate", "200")

	type cli struct {
		k, m, key string
		ok        bool
	}
	clis := []cli{{"20", "15", "k3y", true}, {"10", "10", "k3y", true}, {"20", "15", "", false}}
	listens := make([]string, len(clis))
	for i, c := range clis {
		listens[i] = "127.0.0.1:" + strconv.Itoa(freeTCPPort(t))
		run(t, bin, []string{"FECTUN_KEY=" + c.key}, "-mode", "client", "-peer", "127.0.0.1", "-uport", uport,
			"-lport", "-1", "-listen", listens[i], "-k", c.k, "-m", c.m, "-rate", "200")
	}

	for i, c := range clis {
		err := roundTrip(listens[i], 64<<10, 3*time.Second)
		if c.ok && err != nil {
			t.Errorf("client %d (k/m=%s/%s) 应连通: %v", i, c.k, c.m, err)
		}
		if !c.ok && err == nil {
			t.Errorf("client %d 没带密钥却连通了", i)
		}
	}
}

func run(t *testing.T, bin string, env []string, args ...string) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
		if t.Failed() {
			t.Logf("%v 输出:\n%s", args, out.String())
		}
	})
}

// roundTrip 连上 client 的 TCP 入口,发 n 字节,要求原样回显。client 进程
// 刚起来时入口可能还没监听,所以连接失败会重试到 deadline。
func roundTrip(addr string, n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var c net.Conn
	var err error
	for {
		c, err = net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	defer c.Close()
	want := make([]byte, n)
	rand.Read(want)
	go c.Write(want)
	got := make([]byte, n)
	c.SetReadDeadline(deadline.Add(2 * time.Second))
	if _, err := io.ReadFull(c, got); err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("回显内容不符")
	}
	return nil
}

func startEcho(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	return ln.Addr().String()
}

func freeUDPPort(t *testing.T) string {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return strconv.Itoa(c.LocalAddr().(*net.UDPAddr).Port)
}

func freeTCPPort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
