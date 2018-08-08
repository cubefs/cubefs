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

package storage

import (
	"encoding/binary"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"os"

	"github.com/chubaoio/cbfs/util/btree"
)

const (
	ObjectHeaderSize  = 20
	IndexBatchRead    = 1024
	TombstoneFileSize = math.MaxUint32
)

type Object struct {
	Oid    uint64
	Offset uint32
	Size   uint32
	Crc    uint32
}

func (o *Object) Less(than btree.Item) bool {
	that := than.(*Object)
	return o.Oid < that.Oid
}

func (o *Object) Marshal(out []byte) {
	binary.BigEndian.PutUint64(out[0:8], o.Oid)
	binary.BigEndian.PutUint32(out[8:12], o.Offset)
	binary.BigEndian.PutUint32(out[12:16], o.Size)
	binary.BigEndian.PutUint32(out[16:ObjectHeaderSize], o.Crc)
}

func (o *Object) Unmarshal(in []byte) {
	o.Oid = binary.BigEndian.Uint64(in[0:8])
	o.Offset = binary.BigEndian.Uint32(in[8:12])
	o.Size = binary.BigEndian.Uint32(in[12:16])
	o.Crc = binary.BigEndian.Uint32(in[16:ObjectHeaderSize])
	return
}

type treeStat struct {
	fileCount   uint32
	deleteCount uint32
	fileBytes   uint64
	deleteBytes uint64
}

type ObjectTree struct {
	idxFile *os.File
	idxLock sync.Mutex
	tree    *btree.BTree
	treeStat
}

func (tree *ObjectTree) FileBytes() uint64 {
	return atomic.LoadUint64(&tree.fileBytes)
}

func NewObjectTree(f *os.File) *ObjectTree {
	tree := &ObjectTree{
		tree: btree.New(32),
	}
	tree.idxFile = f
	return tree
}

// Needle map in this function is not protected, so callers should
// guarantee there is no write and delete operations on this needle map
func (tree *ObjectTree) Load() (maxOid uint64, err error) {
	f := tree.idxFile
	maxOid, err = WalkIndexFile(f, func(oid uint64, offset, size, crc uint32) error {
		o := &Object{Oid: oid, Offset: offset, Size: size, Crc: crc}
		if oid > 0 && size != TombstoneFileSize {
			tree.idxLock.Lock()
			found := tree.tree.ReplaceOrInsert(o)
			tree.idxLock.Unlock()
			if found != nil {
				oldNi := found.(*Object)
				tree.decreaseSize(oldNi.Size)
			}
			tree.increaseSize(size)
		} else {
			tree.idxLock.Lock()
			found := tree.tree.Delete(o)
			tree.idxLock.Unlock()
			if found != nil {
				oldNi := found.(*Object)
				tree.decreaseSize(oldNi.Size)
			}
		}
		return nil
	})

	return
}

func (o *Object) Check(offset, size, crc uint32) bool {
	return o.Oid != 0 && o.Offset == offset && o.Crc == crc &&
		(o.Size == size || size == TombstoneFileSize)
}

func WalkIndexFile(f *os.File, fn func(oid uint64, offset, size, crc uint32) error) (maxOid uint64, err error) {
	var (
		readOff int64
		count   int
		iter    int
	)
	bytes := make([]byte, ObjectHeaderSize*IndexBatchRead)
	count, err = f.ReadAt(bytes, readOff)
	readOff += int64(count)

	o := new(Object)

	for count > 0 && err == nil || err == io.EOF {
		for iter = 0; iter+ObjectHeaderSize <= count; iter += ObjectHeaderSize {
			o.Unmarshal(bytes[iter : iter+ObjectHeaderSize])
			if maxOid < o.Oid {
				maxOid = o.Oid
			}
			if e := fn(o.Oid, o.Offset, o.Size, o.Crc); e != nil {
				return maxOid, e
			}
		}

		// walk index file to an end
		if err == io.EOF {
			return maxOid, nil
		}

		count, err = f.ReadAt(bytes, readOff)
		readOff += int64(count)
	}

	return maxOid, err
}

func (tree *ObjectTree) set(oid uint64, offset, size, crc uint32) (oldOff, oldSize uint32, err error) {
	o := &Object{
		Oid:    oid,
		Offset: offset,
		Size:   size,
		Crc:    crc,
	}

	tree.idxLock.Lock()
	if found := tree.tree.ReplaceOrInsert(o); found != nil {
		object := found.(*Object)
		oldOff = object.Offset
		oldSize = object.Size
		tree.decreaseSize(oldSize)
	}
	tree.increaseSize(size)
	tree.idxLock.Unlock()
	err = tree.appendToIdxFile(o)

	return
}

func (tree *ObjectTree) get(oid uint64) (n *Object, exist bool) {
	defer func() {
		if r := recover(); r != nil {
			exist = false
		}
	}()
	found := tree.tree.Get(&Object{Oid: oid})
	if found != nil {
		o := found.(*Object)
		return o, true
	}

	return nil, false
}

func (tree *ObjectTree) delete(oid uint64) error {
	tree.idxLock.Lock()
	found := tree.tree.Delete(&Object{Oid: oid})
	if found == nil {
		tree.idxLock.Unlock()
		return nil
	}
	o := found.(*Object)
	tree.decreaseSize(o.Size)
	o.Size = TombstoneFileSize
	tree.idxLock.Unlock()

	return tree.appendToIdxFile(o)
}

func (tree *ObjectTree) checkConsistency(oid uint64, offset, size uint32) bool {
	o, ok := tree.get(oid)
	if !ok || o.Offset != offset || o.Size != size {
		return false
	}

	return true
}

func (tree *ObjectTree) increaseSize(size uint32) {
	tree.fileCount++
	tree.fileBytes += uint64(size)
}

func (tree *ObjectTree) decreaseSize(size uint32) {
	tree.deleteCount++
	tree.deleteBytes += uint64(size)
}

func (tree *ObjectTree) appendToIdxFile(o *Object) error {
	bytes := make([]byte, ObjectHeaderSize)
	o.Marshal(bytes)

	_, err := tree.idxFile.Write(bytes)
	return err
}

func (tree *ObjectTree) getTree() *btree.BTree {
	return tree.tree
}

func (o *Object) IsIdentical(that *Object) bool {
	return o.Oid == that.Oid && o.Offset == that.Offset && o.Size == that.Size && o.Crc == that.Crc
}
