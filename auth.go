package fectun

import (
	"crypto/hmac"
	"crypto/sha256"
	"hash"
	"sync"
)

// authTagSize:每个包尾部追加的 HMAC-SHA256 截断长度。
// 8 字节 = 64 位伪造难度,在线暴力不可行;包长从 1200 变 1208,仍远低于 MTU。
const authTagSize = 8

// packetAuth 用预共享密钥给每个包签名/验签。
//
// 它只解决"谁能往落地侧开 stream":没有密钥的包在建会话之前就被丢弃,
// 公网上的扫描器碰不到 target。它不加密 —— 隧道里跑的 SSH 自己加密;
// 也不防重放 —— 重放的包要么是重复分片(被去重),要么从别的源地址来,
// 最多占一个 MaxPeers 名额直到空闲回收,拿不到任何 target 上的权限。
//
// 密钥为空时 packetAuth 为 nil,线格式与旧版完全一致。
type packetAuth struct {
	pool sync.Pool // hash.Hash,HMAC 状态不能并发共用
}

func newPacketAuth(key []byte) *packetAuth {
	if len(key) == 0 {
		return nil
	}
	k := append([]byte(nil), key...)
	a := &packetAuth{}
	a.pool.New = func() any { return hmac.New(sha256.New, k) }
	return a
}

func (a *packetAuth) tag(b []byte, out []byte) {
	h := a.pool.Get().(hash.Hash)
	h.Reset()
	h.Write(b)
	sum := h.Sum(nil)
	a.pool.Put(h)
	copy(out, sum[:authTagSize])
}

// seal 返回带签名尾巴的新包。
func (a *packetAuth) seal(b []byte) []byte {
	out := make([]byte, len(b)+authTagSize)
	copy(out, b)
	a.tag(b, out[len(b):])
	return out
}

// open 验签并返回去掉尾巴的包;验签失败返回 ok=false。
func (a *packetAuth) open(b []byte) ([]byte, bool) {
	if len(b) < hdrSize+authTagSize {
		return nil, false
	}
	body := b[:len(b)-authTagSize]
	var want [authTagSize]byte
	a.tag(body, want[:])
	if !hmac.Equal(want[:], b[len(body):]) {
		return nil, false
	}
	return body, true
}
