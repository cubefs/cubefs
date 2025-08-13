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
	"errors"
	"fmt"

	snapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
)

const minMsgKeyLen = 21 // 1(prefix) + 8(ts) + 4(vid) + 8(bid)

var errInvalidMsgKey = errors.New("invalid msg key")

func EncodeDelMsgKey(ts Ts, vid proto.Vid, bid proto.BlobID, shardKeys [][]byte) []byte {
	shardKeyLen := 0
	for _, sk := range shardKeys {
		shardKeyLen += len(sk)
	}

	// del_msg_key = d-ts-vid-bid-{shardKeys1}{shardKeys2}{...}
	buf := make([]byte, minMsgKeyLen+2*len(shardKeys)+shardKeyLen)
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

func EncodeRawDelMsgKey(ts Ts, vid proto.Vid, bid proto.BlobID, tagNum int) (key []byte, shardKeys [][]byte) {
	key1 := []byte(fmt.Sprintf("%d", uint32(vid)))
	key2 := []byte(fmt.Sprintf("%d", uint64(bid)))

	// raw msg shardKeys = {"vid"}{"bid"}{...}
	shardKeys = [][]byte{key1, key2}
	if tagNum > 2 {
		for i := 0; i < tagNum-2; i++ {
			shardKeys = append(shardKeys, []byte(""))
		}
	}

	// raw_del_msg_key = d-ts-vid-bid-{"vid"}{"bid"}{...}
	return EncodeDelMsgKey(ts, vid, bid, shardKeys), shardKeys
}

func DecodeDelMsgKey(key []byte, tagNum int) (Ts, proto.Vid, proto.BlobID, [][]byte, error) {
	if len(key) < minMsgKeyLen+2*tagNum {
		return Ts(0), proto.InvalidVid, proto.InValidBlobID, nil, errInvalidMsgKey
	}
	ts := Ts(binary.BigEndian.Uint64(key[1:9]))
	vid := proto.Vid(binary.BigEndian.Uint32(key[9:13]))
	bid := proto.BlobID(binary.BigEndian.Uint64(key[13:21]))
	return ts, vid, bid, snapi.ParseShardKeys(key[minMsgKeyLen:], tagNum), nil
}
