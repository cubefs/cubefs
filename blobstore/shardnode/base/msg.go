// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package base

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
)

type MsgKeyGenerator struct {
	ts int64
}

func NewMsgKeyGenerator(lastTs Ts) *MsgKeyGenerator {
	now := NewTs(time.Now().Unix())
	if now.Compare(lastTs) > 0 {
		return &MsgKeyGenerator{ts: int64(now)}
	}
	return &MsgKeyGenerator{ts: int64(lastTs)}
}

func (g *MsgKeyGenerator) generateTs() Ts {
	now := time.Now().Unix() << 32
	lastTs := atomic.LoadInt64(&g.ts)
	if now <= lastTs {
		return Ts(atomic.AddInt64(&g.ts, 1))
	}
	if atomic.CompareAndSwapInt64(&g.ts, lastTs, now) {
		return Ts(now)
	}
	return Ts(atomic.AddInt64(&g.ts, 1))
}

func (g *MsgKeyGenerator) EncodeDelMsgKey(vid proto.Vid, bid proto.BlobID, shardKeys [][]byte) (Ts, []byte) {
	ts := g.generateTs()
	return ts, g.EncodeDelMsgKeyWithTs(ts, vid, bid, shardKeys)
}

func (g *MsgKeyGenerator) EncodeDelMsgKeyWithTimeUnix(timeUnix int64, vid proto.Vid, bid proto.BlobID, shardKeys [][]byte) []byte {
	ts := NewTs(timeUnix)
	return g.EncodeDelMsgKeyWithTs(ts, vid, bid, shardKeys)
}

func (g *MsgKeyGenerator) EncodeRawDelMsgKey(vid proto.Vid, bid proto.BlobID, tagNum int) (ts Ts, key []byte, shardKeys [][]byte) {
	key1 := make([]byte, 4)
	binary.BigEndian.PutUint32(key1, uint32(vid))
	key2 := make([]byte, 8)
	binary.BigEndian.PutUint64(key2, uint64(bid))
	shardKeys = [][]byte{key1, key2}
	if tagNum > 2 {
		for i := 0; i < tagNum-2; i++ {
			shardKeys = append(shardKeys, []byte(""))
		}
	}
	ts = g.generateTs()
	key = g.EncodeDelMsgKeyWithTs(ts, vid, bid, shardKeys)
	return ts, key, shardKeys
}

func (g *MsgKeyGenerator) EncodeDelMsgKeyWithTs(ts Ts, vid proto.Vid, bid proto.BlobID, shardKeys [][]byte) []byte {
	shardKeyLen := 0
	for _, sk := range shardKeys {
		shardKeyLen += len(sk)
	}

	buf := make([]byte, 1+8+4+8+2*len(shardKeys)+shardKeyLen)
	copy(buf, snproto.DeleteMsgPrefix)
	index := 1
	binary.BigEndian.PutUint64(buf[index:], uint64(ts))
	index += 8
	binary.BigEndian.PutUint32(buf[index:], uint32(vid))
	index += 4
	binary.BigEndian.PutUint64(buf[index:], uint64(bid))
	index += 8
	for _, sk := range shardKeys {
		buf[index] = proto.ShardingTagLeft
		index++
		copy(buf[index:], sk)
		index += len(sk)
		buf[index] = proto.ShardingTagRight
		index++
	}
	return buf
}

func (g *MsgKeyGenerator) decodeMsgKey(key []byte) (Ts, proto.Vid, proto.BlobID) {
	ts := Ts(binary.BigEndian.Uint64(key[1:9]))
	vid := proto.Vid(binary.BigEndian.Uint32(key[9:13]))
	bid := proto.BlobID(binary.BigEndian.Uint64(key[13:21]))
	return ts, vid, bid
}

func (g *MsgKeyGenerator) CurrentTs() Ts {
	return Ts(atomic.LoadInt64(&g.ts))
}
