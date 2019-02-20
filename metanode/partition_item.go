// Copyright 2018 The Chubao Authors.
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
	"io/ioutil"
	"path"
)

// MetaItem defines the structure of the metadata operations.
type MetaItem struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

// MarshalJson
func (s *MetaItem) MarshalJson() ([]byte, error) {
	return json.Marshal(s)
}

// MarshalBinary marshals MetaItem to binary data.
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

// UnmarshalJson unmarshals binary data to MetaItem.
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

// NewMetaItem returns a new MetaItem.
func NewMetaItem(op uint32, key, value []byte) *MetaItem {
	return &MetaItem{
		Op: op,
		K:  key,
		V:  value,
	}
}

// MetaItemIterator defines the iterator of the MetaItem.
type MetaItemIterator struct {
	applyID     uint64
	cur         int
	curItem     BtreeItem
	inoLen      int
	inodeTree   *BTree
	dentryLen   int
	dentryTree  *BTree
	fileRootDir string
	fileList    []string
	total       int
}

// NewMetaItemIterator returns a new MetaItemIterator.
func NewMetaItemIterator(applyID uint64, ino, den *BTree,
	rootDir string, filelist []string) *MetaItemIterator {
	si := new(MetaItemIterator)
	si.applyID = applyID
	si.inodeTree = ino
	si.dentryTree = den
	si.cur = 0
	si.inoLen = ino.Len()
	si.dentryLen = den.Len()
	si.fileRootDir = rootDir
	si.fileList = filelist
	si.total = si.inoLen + si.dentryLen
	return si
}

// ApplyIndex returns the applyID of the iterator.
func (si *MetaItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

// Close closes the iterator.
func (si *MetaItemIterator) Close() {
	si.cur = si.total + 1
	return
}

// Next returns the next item.
func (si *MetaItemIterator) Next() (data []byte, err error) {
	// TODO: Redesign iterator to improve performance. [Mervin]
	// First Send ApplyIndex
	if si.cur == 0 {
		appIdBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(appIdBuf, si.applyID)
		data = appIdBuf[:]
		si.cur++
		return
	}

	if si.cur <= si.inoLen {
		si.inodeTree.AscendGreaterOrEqual(si.curItem, func(i BtreeItem) bool {
			ino := i.(*Inode)
			if si.curItem == ino {
				return true
			}
			si.curItem = ino
			snap := NewMetaItem(opFSMCreateInode, ino.MarshalKey(),
				ino.MarshalValue())
			data, err = snap.MarshalBinary()
			si.cur++
			return false
		})
		return
	}

	if si.cur == (si.inoLen + 1) {
		si.curItem = nil
	}

	if si.cur <= si.total {
		si.dentryTree.AscendGreaterOrEqual(si.curItem, func(i BtreeItem) bool {
			dentry := i.(*Dentry)
			if si.curItem == dentry {
				return true
			}
			si.curItem = dentry
			snap := NewMetaItem(opFSMCreateDentry, dentry.MarshalKey(),
				dentry.MarshalValue())
			data, err = snap.MarshalBinary()
			si.cur++
			return false
		})
	}
	if len(si.fileList) == 0 {
		err = io.EOF
		data = nil
		return
	}

	fileName := si.fileList[0]
	fileBody, err := ioutil.ReadFile(path.Join(si.fileRootDir, fileName))
	if err != nil {
		data = nil
		return
	}
	snap := NewMetaItem(opExtentFileSnapshot, []byte(fileName), fileBody)
	data, err = snap.MarshalBinary()
	if err != nil {
		si.fileList = si.fileList[1:]
	}
	return
}
