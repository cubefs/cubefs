// Copyright 2022 The CubeFS Authors.
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

package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const indexItemSize = 8 + 8 + 4

type indexItem struct {
	index  uint64
	term   uint64
	offset uint32
}

type logIndexSlice []indexItem

func (s logIndexSlice) First() uint64 {
	if len(s) == 0 {
		return 0
	}
	return s[0].index
}

func (s logIndexSlice) Last() uint64 {
	if len(s) == 0 {
		return 0
	}

	return s[len(s)-1].index
}

func (s logIndexSlice) Get(i uint64) (item indexItem, err error) {
	size := len(s)
	if size == 0 {
		err = fmt.Errorf("index=%d is out of bound lastindex=%d", i, s.Last())
		return
	}

	if i < s[0].index || i > s[size-1].index {
		err = fmt.Errorf("index=%d is out of bound lastindex=%d", i, s.Last())
		return
	}
	return s[i-s[0].index], nil
}

func (s logIndexSlice) Append(offset uint32, entry *pb.Entry) logIndexSlice {
	return append(s, indexItem{
		index:  entry.Index,
		term:   entry.Term,
		offset: offset,
	})
}

func (s logIndexSlice) Truncate(i uint64) (logIndexSlice, error) {
	if _, err := s.Get(i); err != nil {
		return nil, err
	}

	return s[:i-s[0].index], nil
}

func (s logIndexSlice) Len() int {
	return len(s)
}

func (s logIndexSlice) Encode(w io.Writer) (err error) {
	b := make([]byte, 8)

	// write index items count
	binary.BigEndian.PutUint32(b, uint32(s.Len()))
	if _, err = w.Write(b[0:4]); err != nil {
		return
	}

	for _, item := range s {
		binary.BigEndian.PutUint64(b, item.index)
		if _, err = w.Write(b); err != nil {
			return
		}
		binary.BigEndian.PutUint64(b, item.term)
		if _, err = w.Write(b); err != nil {
			return
		}
		binary.BigEndian.PutUint32(b, item.offset)
		if _, err = w.Write(b[0:4]); err != nil {
			return
		}
	}
	return
}

func (s logIndexSlice) Size() uint64 {
	return uint64(4 + s.Len()*indexItemSize)
}

func decodeIndex(data []byte) logIndexSlice {
	offset := 0
	nItems := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	li := make([]indexItem, nItems)

	for i := 0; i < int(nItems); i++ {
		li[i].index = binary.BigEndian.Uint64(data[offset:])
		offset += 8
		li[i].term = binary.BigEndian.Uint64(data[offset:])
		offset += 8
		li[i].offset = binary.BigEndian.Uint32(data[offset:])
		offset += 4
	}
	return li
}
