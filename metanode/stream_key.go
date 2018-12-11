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
	"fmt"
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

type StreamKey struct {
	Extents *BTree
}

func (sk *StreamKey) String() string {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.WriteString("{")
	exts, _ := sk.Marshal()
	buff.WriteString(fmt.Sprintf("Extents[%v]", exts))
	buff.WriteString("}")
	return buff.String()
}

func NewStreamKey() *StreamKey {
	return &StreamKey{
		Extents: NewBtree(),
	}
}

func (sk *StreamKey) Marshal() (data []byte, err error) {
	eks := GetExtentSlice()
	defer PutExtentSlice(eks)
	if cap(eks) <= sk.Extents.Len() {
		eks = make([]BtreeItem, 0, sk.Extents.Len())
	}
	stepFunc := func(item BtreeItem) bool {
		eks = append(eks, item)
		return true
	}
	sk.Extents.Ascend(stepFunc)
	m := make(map[string]interface{})
	m["extents"] = eks
	return json.Marshal(m)
}

func (sk *StreamKey) MarshalJSON() (data []byte, err error) {
	return sk.Marshal()
}

func (sk *StreamKey) Put(key BtreeItem) (items []BtreeItem) {
	var delItems []BtreeItem
	ext := key.(*proto.ExtentKey)
	lessFileOffset := ext.FileOffset + uint64(ext.Size)
	tx := sk.Extents.BeginTx()
	tx.TxAscendRange(key, &proto.ExtentKey{FileOffset: lessFileOffset},
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
		tx.TxDelete(item)
		items = append(items, item)
	}
	// add Item
	tx.TxReplaceOrInsert(key)
	tx.TxClose()
	return
}

func (sk *StreamKey) Size() (bytes uint64) {
	item := sk.Extents.MaxItem()
	if item == nil {
		bytes = 0
		return
	}
	ext := item.(*proto.ExtentKey)
	bytes = ext.FileOffset + uint64(ext.Size)
	return
}

func (sk *StreamKey) GetExtentLen() int {
	return sk.Extents.Len()
}

// Range calls f sequentially for each key and value present in the extent key collection.
// If f returns false, range stops the iteration.
func (sk *StreamKey) Range(f func(item BtreeItem) bool) {
	sk.Extents.Ascend(f)
}

func (sk *StreamKey) MarshalBinary() (data []byte, err error) {
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
	sk.Extents.Ascend(stepFunc)
	if err != nil {
		return
	}
	data = buf.Bytes()
	return
}

func (sk *StreamKey) UnmarshalBinary(data []byte) (err error) {
	buf := bytes.NewBuffer(data)
	for {
		if buf.Len() == 0 {
			break
		}
		var ext proto.ExtentKey
		if err = ext.UnmarshalBinary(buf); err != nil {
			break
		}
		sk.Extents.ReplaceOrInsert(&ext, true)
	}
	return
}

func (sk *StreamKey) Delete(item BtreeItem) {
	sk.Extents.Delete(item)
}

func (sk *StreamKey) Max() (item BtreeItem) {
	return sk.Extents.MaxItem()
}
