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

package shardnode

import (
	"bytes"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func ParseShardKeys(key []byte, tagNum int) [][]byte {
	if len(key) == 0 || tagNum < 1 {
		return [][]byte{}
	}

	var tags [][]byte
	start := 0
	for i := 0; i < tagNum; i++ {
		left := bytes.IndexByte(key[start:], proto.ShardingTagLeft)
		if left == -1 {
			break
		}
		left += start

		right := bytes.IndexByte(key[left+1:], proto.ShardingTagRight)
		if right == -1 {
			break
		}
		right += left + 1

		tags = append(tags, key[left+1:right])
		start = right + 1
	}

	// no tags, use key as tags
	if len(tags) < 1 {
		for i := 0; i < tagNum; i++ {
			tags = append(tags, key)
		}
		return tags
	}

	// tags not enough, add empty tags
	if len(tags) < tagNum {
		for i := 0; i < tagNum-len(tags); i++ {
			tags = append(tags, []byte(""))
		}
		return tags
	}
	return tags
}
