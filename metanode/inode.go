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
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"io"
	"sync"
)

const (
	DeleteMarkFlag = 1 << 0
)

// Inode wraps necessary properties of `Inode` information in the file system.
// Marshal exporterKey:
//  +-------+-------+
//  | item  | Inode |
//  +-------+-------+
//  | bytes |   8   |
//  +-------+-------+
// Marshal value:
//  +-------+------+------+-----+----+----+----+--------+------------------+
//  | item  | Type | Size | Gen | CT | AT | MT | ExtLen | MarshaledExtents |
//  +-------+------+------+-----+----+----+----+--------+------------------+
//  | bytes |  4   |  8   |  8  | 8  | 8  | 8  |   4    |      ExtLen      |
//  +-------+------+------+-----+----+----+----+--------+------------------+
// Marshal entity:
//  +-------+-----------+--------------+-----------+--------------+
//  | item  | KeyLength | MarshaledKey | ValLength | MarshaledVal |
//  +-------+-----------+--------------+-----------+--------------+
//  | bytes |     4     |   KeyLength  |     4     |   ValLength  |
//  +-------+-----------+--------------+-----------+--------------+
type Inode struct {
	sync.RWMutex
	Inode      uint64 // Inode ID
	Type       uint32
	Uid        uint32
	Gid        uint32
	Size       uint64
	Generation uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	LinkTarget []byte // SymLink target name
	NLink      uint32 // NodeLink counts
	Flag       int32
	Reserved   uint64 // reserved space
	Extents    *ExtentsTree
}

// String returns the string format of the inode.
func (i *Inode) String() string {
	i.RLock()
	defer i.RUnlock()
	buff := bytes.NewBuffer(nil)
	buff.Grow(128)
	buff.WriteString("Inode{")
	buff.WriteString(fmt.Sprintf("Inode[%d]", i.Inode))
	buff.WriteString(fmt.Sprintf("Type[%d]", i.Type))
	buff.WriteString(fmt.Sprintf("Uid[%d]", i.Uid))
	buff.WriteString(fmt.Sprintf("Gid[%d]", i.Gid))
	buff.WriteString(fmt.Sprintf("Size[%d]", i.Size))
	buff.WriteString(fmt.Sprintf("Gen[%d]", i.Generation))
	buff.WriteString(fmt.Sprintf("CT[%d]", i.CreateTime))
	buff.WriteString(fmt.Sprintf("AT[%d]", i.AccessTime))
	buff.WriteString(fmt.Sprintf("MT[%d]", i.ModifyTime))
	buff.WriteString(fmt.Sprintf("LinkT[%s]", i.LinkTarget))
	buff.WriteString(fmt.Sprintf("NLink[%d]", i.NLink))
	buff.WriteString(fmt.Sprintf("Flag[%d]", i.Flag))
	buff.WriteString(fmt.Sprintf("Reserved[%d]", i.Reserved))
	buff.WriteString(fmt.Sprintf("Extents[%s]", i.Extents))
	buff.WriteString("}")
	return buff.String()
}

// NewInode returns a new Inode instance with specified Inode ID, name and type.
// The AccessTime and ModifyTime will be set to the current time.
func NewInode(ino uint64, t uint32) *Inode {
	ts := Now.GetCurrentTime().Unix()
	i := &Inode{
		Inode:      ino,
		Type:       t,
		Generation: 1,
		CreateTime: ts,
		AccessTime: ts,
		ModifyTime: ts,
		NLink:      1,
		Extents:    NewExtentsTree(),
	}
	if proto.IsDir(t) {
		i.NLink = 2
	}
	return i
}

// Less tests whether the current Inode item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (i *Inode) Less(than BtreeItem) bool {
	ino, ok := than.(*Inode)
	return ok && i.Inode < ino.Inode
}

// Copy returns a copy of the inode.
func (i *Inode) Copy() BtreeItem {
	newIno := NewInode(i.Inode, i.Type)
	i.RLock()
	newIno.Uid = i.Uid
	newIno.Gid = i.Gid
	newIno.Size = i.Size
	newIno.Generation = i.Generation
	newIno.CreateTime = i.CreateTime
	newIno.ModifyTime = i.ModifyTime
	newIno.AccessTime = i.AccessTime
	if size := len(i.LinkTarget); size > 0 {
		newIno.LinkTarget = make([]byte, size)
		copy(newIno.LinkTarget, i.LinkTarget)
	}
	newIno.NLink = i.NLink
	newIno.Flag = i.Flag
	newIno.Reserved = i.Reserved
	newIno.Extents = i.Extents.Clone()
	i.RUnlock()
	return newIno
}

// MarshalToJSON is the wrapper of json.Marshal.
func (i *Inode) MarshalToJSON() ([]byte, error) {
	i.RLock()
	defer i.RUnlock()
	return json.Marshal(i)
}

// Marshal marshals the inode into a byte array.
func (i *Inode) Marshal() (result []byte, err error) {
	keyBytes := i.MarshalKey()
	valBytes := i.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(128)
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {

	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// Unmarshal unmarshals the inode.
func (i *Inode) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = i.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = i.UnmarshalValue(valBytes)
	return
}

// MarshalKey marshals the exporterKey to bytes.
func (i *Inode) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, i.Inode)
	return
}

// UnmarshalKey unmarshals the exporterKey from bytes.
func (i *Inode) UnmarshalKey(k []byte) (err error) {
	i.Inode = binary.BigEndian.Uint64(k)
	return
}

// MarshalValue marshals the value to bytes.
func (i *Inode) MarshalValue() (val []byte) {
	var err error
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)
	i.RLock()
	if err = binary.Write(buff, binary.BigEndian, &i.Type); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Uid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Gid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Size); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Generation); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.CreateTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.AccessTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		panic(err)
	}
	// write SymLink
	symSize := uint32(len(i.LinkTarget))
	if err = binary.Write(buff, binary.BigEndian, &symSize); err != nil {
		panic(err)
	}
	if _, err = buff.Write(i.LinkTarget); err != nil {
		panic(err)
	}

	if err = binary.Write(buff, binary.BigEndian, &i.NLink); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Flag); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Reserved); err != nil {
		panic(err)
	}
	// marshal ExtentsKey
	extData, err := i.Extents.MarshalBinary()
	if err != nil {
		panic(err)
	}
	if _, err = buff.Write(extData); err != nil {
		panic(err)
	}

	val = buff.Bytes()
	i.RUnlock()
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (i *Inode) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &i.Type); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Uid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Gid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Generation); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.AccessTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		return
	}
	// read symLink
	symSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &symSize); err != nil {
		return
	}
	if symSize > 0 {
		i.LinkTarget = make([]byte, symSize)
		if _, err = io.ReadFull(buff, i.LinkTarget); err != nil {
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &i.NLink); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Flag); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Reserved); err != nil {
		return
	}
	if buff.Len() == 0 {
		return
	}
	// unmarshal ExtentsKey
	if i.Extents == nil {
		i.Extents = NewExtentsTree()
	}
	if err = i.Extents.UnmarshalBinary(buff.Bytes()); err != nil {
		return
	}
	return
}

// AppendExtents append the extent to the btree.
func (i *Inode) AppendExtents(exts []BtreeItem, ct int64) (items []BtreeItem) {
	i.Lock()
	for _, ext := range exts {
		delItems := i.Extents.Append(ext)
		size := i.Extents.Size()
		if i.Size < size {
			i.Size = size
		}
		items = append(items, delItems...)
	}
	i.Generation++
	i.ModifyTime = ct
	i.Unlock()
	return
}

// ExtentsTruncate truncates the extents.
func (i *Inode) ExtentsTruncate(exts []BtreeItem, length uint64, ct int64) {
	i.Lock()
	for _, ext := range exts {
		i.Extents.Delete(ext)
	}
	// check the max item size
	item := i.Extents.MaxItem()
	if item != nil {
		ext := item.(*proto.ExtentKey)
		if (ext.FileOffset + uint64(ext.Size)) > length {
			ext.Size = uint32(length - ext.FileOffset)
		}
	}
	i.Size = length
	i.ModifyTime = ct
	i.Generation++
	i.Unlock()
}

// IncNLink increases the nLink value by one.
func (i *Inode) IncNLink() {
	i.Lock()
	i.NLink++
	i.Unlock()
}

// DecNLink decreases the nLink value by one.
func (i *Inode) DecNLink() {
	i.Lock()
	if proto.IsDir(i.Type) {
		if i.NLink > 2 {
			i.NLink--
		}
	} else {
		if i.NLink > 0 {
			i.NLink--
		}
	}
	i.Unlock()
}

// GetNLink returns the nLink value.
func (i *Inode) GetNLink() uint32 {
	i.RLock()
	defer i.RUnlock()
	return i.NLink
}

func (i *Inode) IsTempFile() bool {
	i.RLock()
	ok := i.NLink == 0
	i.RUnlock()
	return ok
}

func (i *Inode) IsEmptyDir() bool {
	i.RLock()
	ok := i.NLink == 2
	i.RUnlock()
	return ok
}

// SetDeleteMark set the deleteMark flag. TODO markDelete or deleteMark? markDelete has been used in datanode.
func (i *Inode) SetDeleteMark() {
	i.Lock()
	i.Flag |= DeleteMarkFlag
	i.Unlock()
}

// ShouldDelete returns if the inode has been marked as deleted.
func (i *Inode) ShouldDelete() bool {
	i.RLock()
	defer i.RUnlock()
	return i.Flag&DeleteMarkFlag == DeleteMarkFlag
}

// SetAttr sets the attributes of the inode.
func (i *Inode) SetAttr(valid, mode, uid, gid uint32) {
	i.Lock()
	if mode&proto.AttrMode != 0 {
		i.Type = mode
	}
	if valid&proto.AttrUid != 0 {
		i.Uid = uid
	}
	if valid&proto.AttrGid != 0 {
		i.Gid = gid
	}
	i.Unlock()
}

func (i *Inode) DoWriteFunc(fn func()) {
	i.Lock()
	fn()
	i.Unlock()
}

// DoFunc executes the given function.
func (i *Inode) DoReadFunc(fn func()) {
	i.RLock()
	fn()
	i.RUnlock()
}
