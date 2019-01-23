// Copyright 2018 The ChuBao Authors.
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
	"encoding/json"
	"github.com/tiglabs/containerfs/proto"
	"sync"
)

var (
	extentsPool = sync.Pool{
		New: func() interface{} {
			ek := make([]BtreeItem, 0, 10)
			return ek
		},
	}
)

func GetExtentSlice() []BtreeItem {
	eks := extentsPool.Get()
	return eks.([]BtreeItem)
}

func PutExtentSlice(eks []BtreeItem) {
	eks = eks[:0]
	extentsPool.Put(eks)
}

type ExtentsTree struct {
	*BTree
}

func (e *ExtentsTree) String() string {
	buff := bytes.NewBuffer(nil)
	buff.Grow(128)
	exts, _ := e.Marshal()
	buff.Write(exts)
	return buff.String()
}

func NewExtentsTree() *ExtentsTree {
	return &ExtentsTree{
		BTree: NewBtree(),
	}
}

func (e *ExtentsTree) Marshal() ([]byte, error) {
	eks := GetExtentSlice()
	defer PutExtentSlice(eks)
	if cap(eks) <= e.Len() {
		eks = make([]BtreeItem, 0, e.Len())
	}
	stepFunc := func(item BtreeItem) bool {
		eks = append(eks, item)
		return true
	}
	e.Ascend(stepFunc)
	return json.Marshal(eks)
}

func (e *ExtentsTree) MarshalJSON() ([]byte, error) {
	return e.Marshal()
}

func (e *ExtentsTree) Append(key BtreeItem) (items []BtreeItem) {
	var delItems []BtreeItem
	ext := key.(*proto.ExtentKey)
	lessFileOffset := ext.FileOffset + uint64(ext.Size)
	e.AscendRange(key, &proto.ExtentKey{FileOffset: lessFileOffset},
		func(item BtreeItem) bool {
			delItems = append(delItems, item)
			return true
		})

	// should Delete Items
	for _, item := range delItems {
		delKey := item.(*proto.ExtentKey)
		if delKey.PartitionId == ext.PartitionId && delKey.ExtentId == ext.
			ExtentId {
			continue
		}
		e.Delete(item)
		items = append(items, item)
	}
	// add Item to btree
	e.ReplaceOrInsert(key, true)
	return
}

func (e *ExtentsTree) Size() (size uint64) {
	item := e.MaxItem()
	if item == nil {
		size = 0
		return
	}
	ext := item.(*proto.ExtentKey)
	size = ext.FileOffset + uint64(ext.Size)
	return
}

// Range calls f sequentially for each exporterKey and value present in the extent exporterKey collection.
// If f returns false, range stops the iteration.
func (e *ExtentsTree) Range(f func(item BtreeItem) bool) {
	e.Ascend(f)
}

func (e *ExtentsTree) MarshalBinary() (data []byte, err error) {
	var binData []byte
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	stepFunc := func(item BtreeItem) bool {
		if item == nil {
			return false
		}
		ext := item.(*proto.ExtentKey)
		binData, err = ext.MarshalBinary()
		if err != nil {
			return false
		}
		buf.Write(binData)
		return true
	}
	e.Ascend(stepFunc)
	if err != nil {
		return
	}
	data = buf.Bytes()
	return
}

func (e *ExtentsTree) UnmarshalBinary(data []byte) (err error) {
	buf := bytes.NewBuffer(data)
	for {
		if buf.Len() == 0 {
			break
		}
		var ext proto.ExtentKey
		if err = ext.UnmarshalBinary(buf); err != nil {
			break
		}
		e.ReplaceOrInsert(&ext, true)
	}
	return
}

func (e *ExtentsTree) Clone() *ExtentsTree {
	return &ExtentsTree{
		BTree: e.GetTree(),
	}
}
