// Copyright 2018 The Containerfs Authors.
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

package metanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/tiglabs/containerfs/util/btree"
)

type MetaItem struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

// MarshalJson
func (s *MetaItem) MarshalJson() ([]byte, error) {
	return json.Marshal(s)
}

// MarshalBinary marshal this MetaItem entity to binary data.
// Binary frame structure:
//  +------+----+------+------+------+------+
//  | Item | Op | LenK |   K  | LenV |   V  |
//  +------+----+------+------+------+------+
//  | byte | 4  |  4   | LenK |  4   | LenV |
//  +------+----+------+------+------+------+
func (s *MetaItem) MarshalBinary() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(4 + len(s.K) + len(s.V))
	if err = binary.Write(buff, binary.BigEndian, s.Op); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(s.K))); err != nil {
		return
	}
	if _, err = buff.Write(s.K); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(s.V))); err != nil {
		return
	}
	if _, err = buff.Write(s.V); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// UnmarshalJson unmarshal binary data to MetaItem entity.
func (s *MetaItem) UnmarshalJson(data []byte) error {
	return json.Unmarshal(data, s)
}

// MarshalBinary unmarshal this MetaItem entity from binary data.
// Binary frame structure:
//  +------+----+------+------+------+------+
//  | Item | Op | LenK |   K  | LenV |   V  |
//  +------+----+------+------+------+------+
//  | byte | 4  |  4   | LenK |  4   | LenV |
//  +------+----+------+------+------+------+
func (s *MetaItem) UnmarshalBinary(raw []byte) (err error) {
	var (
		lenK uint32
		lenV uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &s.Op); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &lenK); err != nil {
		return
	}
	s.K = make([]byte, lenK)
	if _, err = buff.Read(s.K); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &lenV); err != nil {
		return
	}
	s.V = make([]byte, lenV)
	if _, err = buff.Read(s.V); err != nil {
		return
	}
	return
}

func NewMetaItem(op uint32, key, value []byte) *MetaItem {
	return &MetaItem{
		Op: op,
		K:  key,
		V:  value,
	}
}

type ItemIterator struct {
	applyID    uint64
	cur        int
	curItem    BtreeItem
	inoLen     int
	inodeTree  *BTree
	dentryLen  int
	dentryTree *BTree
	total      int
}

func NewMetaItemIterator(applyID uint64, ino, den *BTree) *ItemIterator {
	si := new(ItemIterator)
	si.applyID = applyID
	si.inodeTree = ino
	si.dentryTree = den
	si.cur = 0
	si.inoLen = ino.Len()
	si.dentryLen = den.Len()
	si.total = si.inoLen + si.dentryLen
	return si
}

func (si *ItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

func (si *ItemIterator) Close() {
	si.cur = si.total + 1
	return
}

func (si *ItemIterator) Next() (data []byte, err error) {
	// TODO: Redesign iterator to improve performance. [Mervin]
	if si.cur > si.total {
		err = io.EOF
		data = nil
		return
	}
	// First Send ApplyIndex
	if si.cur == 0 {
		appIdBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(appIdBuf, si.applyID)
		data = appIdBuf[:]
		si.cur++
		return
	}
	// ascend Inode tree
	if si.cur <= si.inoLen {
		si.inodeTree.AscendGreaterOrEqual(si.curItem, func(i btree.Item) bool {
			ino := i.(*Inode)
			if si.curItem == ino {
				return true
			}
			si.curItem = ino
			snap := NewMetaItem(opCreateInode, ino.MarshalKey(),
				ino.MarshalValue())
			data, err = snap.MarshalBinary()
			si.cur++
			return false
		})
		return
	}

	// ascend range dentry tree
	if si.cur == (si.inoLen + 1) {
		si.curItem = nil
	}
	si.dentryTree.AscendGreaterOrEqual(si.curItem, func(i btree.Item) bool {
		dentry := i.(*Dentry)
		if si.curItem == dentry {
			return true
		}
		si.curItem = dentry
		snap := NewMetaItem(opCreateDentry, dentry.MarshalKey(),
			dentry.MarshalValue())
		data, err = snap.MarshalBinary()
		si.cur++
		return false
	})
	return
}
