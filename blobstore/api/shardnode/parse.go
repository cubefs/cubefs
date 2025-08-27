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
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func DecodeShardKeys(keyStr string, tagNum int) []string {
	key := []byte(keyStr)
	if len(key) == 0 || tagNum < 1 {
		return []string{}
	}

	var tags []string
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

		tags = append(tags, string(key[left+1:right]))
		start = right + 1
	}

	// no tags, use key as tags
	if len(tags) < 1 {
		for i := 0; i < tagNum; i++ {
			tags = append(tags, string(key))
		}
		return tags
	}

	// tags not enough, add empty tags
	if len(tags) < tagNum {
		for i := 0; i < tagNum-len(tags); i++ {
			tags = append(tags, "")
		}
		return tags
	}
	return tags
}

// EncodeName encode shardkeys into formatStr, for example:
// formatStr = "s%s%sname1%sname2" keys=["key1", "key2", "key3"]
// encode to "s{key1}{key2}name1{key3}name2", and len(shardkeys) must equal with tagNum
func EncodeName(formatStr string, shardkeys []string, tagNum int) string {
	if len(shardkeys) != tagNum {
		panic("len(shardkeys) not equal with tagNum")
	}

	if formatStr == "" || len(shardkeys) == 0 || tagNum <= 0 {
		return formatStr
	}

	// Validate that formatStr only contains %s placeholders, and has no '{' or '}'
	validateFormatString(formatStr)
	containsUnsupportedBytes(formatStr)

	count := strings.Count(formatStr, "%s")
	if count != len(shardkeys) {
		panic("num of '%s' in formatStr no equal with len(shardkeys)")
	}

	encodedKeys := make([]interface{}, len(shardkeys))
	for i := range shardkeys {
		encodedKeys[i] = fmt.Sprintf("{%s}", shardkeys[i])
	}
	return fmt.Sprintf(formatStr, encodedKeys...)
}

// validateFormatString ensures that formatStr only contains %s placeholders
func validateFormatString(formatStr string) {
	for i := 0; i < len(formatStr); i++ {
		if i == len(formatStr)-1 && formatStr[i] == '%' {
			panic("unsupported format specifier: found '%' in last character")
		}
		if formatStr[i] == '%' && formatStr[i+1] != 's' {
			panic(fmt.Sprintf("unsupported format specifier: only %%s is allowed, found '%%%c'", formatStr[i+1]))
		}
	}
}

func containsUnsupportedBytes(s string) {
	raw := []byte(s)
	idx0, idx1 := bytes.IndexByte(raw, '{'), bytes.IndexByte(raw, '}')
	if idx0 > -1 || idx1 > -1 {
		panic("'{' or '}' is not supported")
	}
}
