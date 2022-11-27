// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
)

const indexItemSize = 8 + 8 + 4

type indexItem struct {
	logindex uint64 // 日志的index
	logterm  uint64 // 日志的term
	offset   uint32 // 日志在文件中的偏移
}

type logEntryIndex []indexItem

func (li logEntryIndex) First() uint64 {
	if len(li) == 0 {
		return 0
	}
	return li[0].logindex
}

func (li logEntryIndex) Last() uint64 {
	size := len(li)
	if size == 0 {
		return 0
	}

	return li[size-1].logindex
}

func (li logEntryIndex) Get(i uint64) (item indexItem, err error) {
	size := len(li)
	if size == 0 {
		err = fmt.Errorf("maybe index(%d) is out of bound lastindex(%d)", i, li.Last())
		return
	}

	ibegin := li[0].logindex
	iend := li[size-1].logindex
	if i < ibegin || i > iend {
		err = fmt.Errorf("maybe index(%d) is out of bound lastindex(%d)", i, li.Last())
		return
	}
	return li[i-ibegin], nil
}

func (li logEntryIndex) Append(offset uint32, entry *proto.Entry) logEntryIndex {
	return append(li, indexItem{
		logindex: entry.Index,
		logterm:  entry.Term,
		offset:   offset,
	})
}

func (li logEntryIndex) Truncate(i uint64) (logEntryIndex, error) {
	if _, err := li.Get(i); err != nil {
		return nil, err
	}

	return li[:i-li[0].logindex], nil
}

func (li logEntryIndex) Len() int {
	return len(li)
}

// 实现recordData接口Encode方法
func (li logEntryIndex) Encode(w io.Writer) (err error) {
	u32Buf := make([]byte, 4)
	u64Buf := make([]byte, 8)

	// write index items count
	binary.BigEndian.PutUint32(u32Buf, uint32(li.Len()))
	if _, err = w.Write(u32Buf); err != nil {
		return
	}

	// write indexs data
	for _, item := range li {
		// logindex
		binary.BigEndian.PutUint64(u64Buf, item.logindex)
		if _, err = w.Write(u64Buf); err != nil {
			return
		}
		// logitem
		binary.BigEndian.PutUint64(u64Buf, item.logterm)
		if _, err = w.Write(u64Buf); err != nil {
			return
		}
		// logoffset
		binary.BigEndian.PutUint32(u32Buf, item.offset)
		if _, err = w.Write(u32Buf); err != nil {
			return
		}
	}
	return
}

// 实现recordData接口Size方法
func (li logEntryIndex) Size() uint64 {
	return uint64(4 + li.Len()*indexItemSize)
}

func decodeLogIndex(data []byte) logEntryIndex {
	offset := 0

	nItems := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	li := make([]indexItem, nItems)

	for i := 0; i < int(nItems); i++ {
		li[i].logindex = binary.BigEndian.Uint64(data[offset:])
		offset += 8
		li[i].logterm = binary.BigEndian.Uint64(data[offset:])
		offset += 8
		li[i].offset = binary.BigEndian.Uint32(data[offset:])
		offset += 4
	}
	return li
}
