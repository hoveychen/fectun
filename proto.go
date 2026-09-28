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
	pktFeedback  = 6 // 收端回报累计收到的 pktData 数(seq 字段),供对端拥塞控制

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
//	[12:16] epoch      发送方会话 epoch(启动时随机),用于检测对端重启
type header struct {
	typ      byte
	shardIdx byte
	k, m     byte
	group    uint32
	seq      uint32
	epoch    uint32
}

func (h header) marshal(b []byte) {
	b[0], b[1], b[2], b[3] = h.typ, h.shardIdx, h.k, h.m
	binary.BigEndian.PutUint32(b[4:8], h.group)
	binary.BigEndian.PutUint32(b[8:12], h.seq)
	binary.BigEndian.PutUint32(b[12:16], h.epoch)
}

func parseHeader(b []byte) (h header, ok bool) {
	if len(b) < hdrSize {
		return h, false
	}
	h.typ, h.shardIdx, h.k, h.m = b[0], b[1], b[2], b[3]
	h.group = binary.BigEndian.Uint32(b[4:8])
	h.seq = binary.BigEndian.Uint32(b[8:12])
	h.epoch = binary.BigEndian.Uint32(b[12:16])
	return h, true
}

// 分片载荷布局(10 字节头 + 数据):
//
//	[0:4]   streamID   本片承载哪条 stream 的数据
//	[4:8]   streamSeq  该 stream 内的分片序号,接收侧据此独立重组
//	[8:10]  dataLen    本片实际承载的字节数(不足一片时补零填充)
//	[10:]   data
//
// 归属信息必须放在载荷里,不能放包头 —— 包头不参与 FEC 编码,
// 校验片重构出来的是载荷内容。归属信息若在包头,FEC 恢复出的分片
// 就认不出自己属于哪条 stream,per-stream 重组无从谈起。
//
// streamSeq 独立于全局 seq:全局 seq 仍然只服务 FEC 分组与 NACK 重传,
// 而交付顺序由 (streamID, streamSeq) 决定。这正是解开队头阻塞的关键 ——
// 一条 stream 的空洞只挡它自己的 streamSeq 队列,别的 stream 照常交付。
const shardHdr = 10

func marshalShard(dst []byte, sid, sseq uint32, data []byte) {
	binary.BigEndian.PutUint32(dst[0:4], sid)
	binary.BigEndian.PutUint32(dst[4:8], sseq)
	binary.BigEndian.PutUint16(dst[8:10], uint16(len(data)))
	copy(dst[shardHdr:], data)
}

func parseShard(b []byte) (sid, sseq uint32, data []byte, ok bool) {
	if len(b) < shardHdr {
		return 0, 0, nil, false
	}
	sid = binary.BigEndian.Uint32(b[0:4])
	sseq = binary.BigEndian.Uint32(b[4:8])
	n := int(binary.BigEndian.Uint16(b[8:10]))
	if n > len(b)-shardHdr {
		return 0, 0, nil, false
	}
	return sid, sseq, b[shardHdr : shardHdr+n], true
}
