// fectun —— socat 式 TCP 隧道,跨境段走 FEC over UDP
// 设计要点:
//  1. FEC(Reed-Solomon)把链路 10~20% 丢包压到近零,大幅减少重传触发
//  2. NACK-based ARQ 兜底 FEC 未能恢复的残余丢包(TCP 字节流必须完整有序)
//  3. 内建心跳维持 NAT/安全组 conntrack —— 实测单向 UDP 会被阿里云安全组全丢
//  4. 令牌桶速率控制 —— 实测持续满负载会把链路丢包从 10% 推到 30%,FEC 会崩
package main

import (
	"encoding/binary"
)

const (
	pktData      = 1 // 数据分片(可能是 FEC 数据片或校验片)
	pktNack      = 2 // 请求重传
	pktHeartbeat = 3 // 维持 conntrack
	pktOpen      = 4 // 新建 stream
	pktClose     = 5 // 关闭 stream

	hdrSize   = 16
	maxShard  = 1200 // UDP payload 上限,留足 IP/UDP 头避免分片
)

// 包头布局(16 字节):
//
//	[0]     type
//	[1]     shardIdx   FEC 组内分片序号
//	[2]     k          数据分片数
//	[3]     m          校验分片数
//	[4:8]   group      FEC 组号
//	[8:12]  seq        数据分片全局序号(仅 pktData 的数据片有效)
//	[12:16] stream     stream id
type header struct {
	typ      byte
	shardIdx byte
	k, m     byte
	group    uint32
	seq      uint32
	stream   uint32
}

func (h header) marshal(b []byte) {
	b[0], b[1], b[2], b[3] = h.typ, h.shardIdx, h.k, h.m
	binary.BigEndian.PutUint32(b[4:8], h.group)
	binary.BigEndian.PutUint32(b[8:12], h.seq)
	binary.BigEndian.PutUint32(b[12:16], h.stream)
}

func parseHeader(b []byte) (h header, ok bool) {
	if len(b) < hdrSize {
		return h, false
	}
	h.typ, h.shardIdx, h.k, h.m = b[0], b[1], b[2], b[3]
	h.group = binary.BigEndian.Uint32(b[4:8])
	h.seq = binary.BigEndian.Uint32(b[8:12])
	h.stream = binary.BigEndian.Uint32(b[12:16])
	return h, true
}
