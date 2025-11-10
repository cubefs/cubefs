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

package message

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

const minMsgKeyLen = 22 // 2(prefix) + 8(ts) + 4(vid) + 8(bid)

var (
	errInvalidMsgKey = errors.New("invalid msg key")
	msgKeyPool       = sync.Pool{
		New: func() interface{} {
			return &msgKey{}
		},
	}
)

func getMessagePrefix(msgType snproto.MessageType, priority snproto.MessageTier) []byte {
	switch msgType {
	case snproto.MessageTypeDelete:
		return genDeleteMsgPrefix(priority)
	case snproto.MessageTypeRepair:
		return genRepairMsgPrefix(priority)
	default:
		panic(fmt.Sprintf("unknown message type: %d", msgType))
	}
}

func genDeleteMsgPrefix(priority snproto.MessageTier) []byte {
	switch priority {
	case snproto.TierSingleIdx:
		return append(append([]byte(nil), snproto.DeleteMsgPrefix...), snproto.TierSingleIdxPrefix...)
	case snproto.TierPunish:
		return append(append([]byte(nil), snproto.DeleteMsgPrefix...), snproto.TierPunishPrefix...)
	default:
		panic(fmt.Sprintf("unknown priority: %d", priority))
	}
}

func genRepairMsgPrefix(priority snproto.MessageTier) []byte {
	switch priority {
	case snproto.TierSingleIdx:
		return append(append([]byte(nil), snproto.RepairMsgPrefix...), snproto.TierSingleIdxPrefix...)
	case snproto.TierMultiIdx:
		return append(append([]byte(nil), snproto.RepairMsgPrefix...), snproto.TierMultiIdxPrefix...)
	case snproto.TierPunish:
		return append(append([]byte(nil), snproto.RepairMsgPrefix...), snproto.TierPunishPrefix...)
	default:
		panic(fmt.Sprintf("unknown priority: %d", priority))
	}
}

type msgKey struct {
	msgType   snproto.MessageType
	tier      snproto.MessageTier
	ts        base.Ts
	vid       proto.Vid
	bid       proto.BlobID
	shardKeys []string

	key       []byte // encoded key buffer for reuse
	ownBuffer bool   // true if key is allocated from bytespool and should be freed
}

func newMsgKey() *msgKey {
	return msgKeyPool.Get().(*msgKey)
}

func (k *msgKey) release() {
	k.reset()
	msgKeyPool.Put(k)
}

// reset resets the msgKey for reuse
func (k *msgKey) reset() {
	k.setMsgType(0)
	k.setTier(0)
	k.setTs(0)
	k.setVid(0)
	k.setBid(0)
	k.setShardKeys(k.shardKeys[:0])
	if k.ownBuffer && k.key != nil {
		bytespool.Free(k.key)
		k.key = nil
	}
	k.ownBuffer = false
}

// set methods for msgKey fields
func (k *msgKey) setMsgType(msgType snproto.MessageType) {
	k.msgType = msgType
}

func (k *msgKey) setTier(tier snproto.MessageTier) {
	k.tier = tier
}

func (k *msgKey) setTs(ts base.Ts) {
	k.ts = ts
}

func (k *msgKey) setVid(vid proto.Vid) {
	k.vid = vid
}

func (k *msgKey) setBid(bid proto.BlobID) {
	k.bid = bid
}

func (k *msgKey) setShardKeys(shardKeys []string) {
	k.shardKeys = shardKeys
}

// setKey sets the key buffer from external source
func (k *msgKey) setKey(key []byte) {
	// free old buffer if we own it
	if k.ownBuffer && k.key != nil {
		bytespool.Free(k.key)
	}
	k.key = key
	k.ownBuffer = false
}

// encoded format is: prefix(2) + ts(8) + vid(4) + bid(8) + {shardKey1}{shardKey2}...
// copy the result if reuse the msgKey
func (k *msgKey) encode() []byte {
	prefix := getMessagePrefix(k.msgType, k.tier)
	if len(prefix) != 2 {
		panic(fmt.Sprintf("invalid prefix: %+v", prefix))
	}

	shardKeyLen := 0
	for _, sk := range k.shardKeys {
		shardKeyLen += len(sk)
	}

	requiredSize := minMsgKeyLen + 2*len(k.shardKeys) + shardKeyLen

	// free old buffer if we own it
	if k.ownBuffer && k.key != nil {
		bytespool.Free(k.key)
	}
	k.key = bytespool.Alloc(requiredSize)
	k.ownBuffer = true

	copy(k.key, prefix)
	index := 2
	binary.BigEndian.PutUint64(k.key[index:], uint64(k.ts))
	index += 8
	binary.BigEndian.PutUint32(k.key[index:], uint32(k.vid))
	index += 4
	binary.BigEndian.PutUint64(k.key[index:], uint64(k.bid))
	index += 8
	for _, sk := range k.shardKeys {
		k.key[index] = proto.ShardingTagLeft
		index++
		copy(k.key[index:], sk)
		index += len(sk)
		k.key[index] = proto.ShardingTagRight
		index++
	}
	return k.key
}

// decode decodes the msgKey from the key buffer
func (k *msgKey) decode(tagNum int) error {
	if len(k.key) < minMsgKeyLen+2*tagNum {
		return errInvalidMsgKey
	}
	k.setTs(base.Ts(binary.BigEndian.Uint64(k.key[2:10])))
	k.setVid(proto.Vid(binary.BigEndian.Uint32(k.key[10:14])))
	k.setBid(proto.BlobID(binary.BigEndian.Uint64(k.key[14:22])))
	k.setShardKeys(snapi.DecodeShardKeys(string(k.key[minMsgKeyLen:]), tagNum))
	return nil
}
