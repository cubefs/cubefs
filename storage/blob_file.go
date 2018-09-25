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

package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

type BlobFile struct {
	file        *os.File
	tree        *ObjectTree
	lastOid     uint64
	syncLastOid uint64
	commitLock  sync.RWMutex
	compactLock util.TryMutexLock
}

func NewBlobFile(dataDir string, blobfileId int) (c *BlobFile, err error) {
	c = new(BlobFile)
	name := dataDir + "/" + strconv.Itoa(blobfileId)
	maxOid, err := c.loadTree(name)
	if err != nil {
		return nil, err
	}

	c.storeLastOid(maxOid)
	return c, nil
}

func (c *BlobFile) applyDelObjects(objects []uint64) (err error) {
	for _, needle := range objects {
		c.commitLock.RLock()
		c.tree.delete(needle)
		c.commitLock.RUnlock()
	}

	c.storeSyncLastOid(c.loadLastOid())
	return
}

func (c *BlobFile) loadTree(name string) (maxOid uint64, err error) {
	if c.file, err = os.OpenFile(name, BlobFileOpenOpt, 0666); err != nil {
		return
	}
	var idxFile *os.File
	idxName := name + ".idx"
	if idxFile, err = os.OpenFile(idxName, BlobFileOpenOpt, 0666); err != nil {
		c.file.Close()
		return
	}

	tree := NewObjectTree(idxFile)
	if maxOid, err = tree.Load(); err == nil {
		c.tree = tree
	} else {
		idxFile.Close()
		c.file.Close()
	}

	return
}

// returns count of valid objects calculated for CRC
func (c *BlobFile) getCheckSum() (fullCRC uint32, syncLastOid uint64, count int) {
	syncLastOid = c.loadLastOid()
	c.tree.idxFile.Sync()
	crcBuffer := make([]byte, 0)
	buf := make([]byte, 4)
	c.commitLock.RLock()
	LoopIndexFile(c.tree.idxFile, func(oid uint64, offset, size, crc uint32) error {
		if oid > syncLastOid {
			return nil
		}
		o, ok := c.tree.get(oid)
		if !ok {
			return nil
		}
		if !o.Check(offset, size, crc) {
			return nil
		}
		binary.BigEndian.PutUint32(buf, o.Crc)
		crcBuffer = append(crcBuffer, buf...)
		count++
		return nil
	})
	c.commitLock.RUnlock()

	fullCRC = crc32.ChecksumIEEE(crcBuffer)
	return
}

func (c *BlobFile) loadLastOid() uint64 {
	return atomic.LoadUint64(&c.lastOid)
}

func (c *BlobFile) storeLastOid(val uint64) {
	atomic.StoreUint64(&c.lastOid, val)
	return
}

func (c *BlobFile) incLastOid() uint64 {
	return atomic.AddUint64(&c.lastOid, uint64(1))
}

func (c *BlobFile) loadSyncLastOid() uint64 {
	return atomic.LoadUint64(&c.syncLastOid)
}

func (c *BlobFile) storeSyncLastOid(val uint64) {
	atomic.StoreUint64(&c.syncLastOid, val)
	return
}

func (c *BlobFile) doCompact() (err error) {
	var (
		newIdxFile *os.File
		newDatFile *os.File
		tree       *ObjectTree
	)

	name := c.file.Name()
	newIdxName := name + ".tmpIndex"
	newDatName := name + ".tmpData"
	if newIdxFile, err = os.OpenFile(newIdxName, BlobFileOpenOpt|os.O_TRUNC, 0644); err != nil {
		return err
	}
	defer newIdxFile.Close()

	if newDatFile, err = os.OpenFile(newDatName, BlobFileOpenOpt|os.O_TRUNC, 0644); err != nil {
		return err
	}
	defer newDatFile.Close()

	tree = NewObjectTree(newIdxFile)

	if err = c.copyValidData(tree, newDatFile); err != nil {
		return err
	}

	return nil
}

func (c *BlobFile) copyValidData(dstNm *ObjectTree, dstDatFile *os.File) (err error) {
	srcTree := c.tree
	srcDatFile := c.file
	srcIdxFile := srcTree.idxFile
	deletedSet := make(map[uint64]struct{})
	log.LogInfo("copyValidData start: ", c.tree.idxFile.Name())
	_, err = LoopIndexFile(srcIdxFile, func(oid uint64, offset, size, crc uint32) error {
		var (
			o *Object
			e error

			newOffset int64
		)

		_, ok := deletedSet[oid]
		if size == MarkDeleteObject && !ok {
			o = &Object{Oid: oid, Offset: offset, Size: size, Crc: crc}
			if e = dstNm.appendToIdxFile(o); e != nil {
				return e
			}
			deletedSet[oid] = struct{}{}
			return nil
		}

		o, ok = srcTree.get(oid)
		if !ok {
			return nil
		}

		if !o.Check(offset, size, crc) {
			return nil
		}

		realsize := o.Size
		if newOffset, e = dstDatFile.Seek(0, 2); e != nil {
			return e
		}

		dataInFile := make([]byte, realsize)
		if _, e = srcDatFile.ReadAt(dataInFile, int64(o.Offset)); e != nil {
			return e
		}

		if _, e = dstDatFile.Write(dataInFile); e != nil {
			return e
		}

		o.Offset = uint32(newOffset)
		if e = dstNm.appendToIdxFile(o); e != nil {
			return e
		}

		return nil
	})
	log.LogInfo("copyValidData end: ", c.tree.idxFile.Name(), "err = ", err)

	return err
}

func (c *BlobFile) doCommit() (err error) {
	name := c.file.Name()
	c.tree.idxFile.Close()
	c.file.Close()

	err = catchupDeleteIndex(name+".idx", name+".tmpIndex")
	if err != nil {
		return
	}

	err = os.Rename(name+".tmpData", name)
	if err != nil {
		return
	}
	err = os.Rename(name+".tmpIndex", name+".idx")
	if err != nil {
		return
	}

	maxOid, err := c.loadTree(name)
	if err == nil && maxOid > c.loadLastOid() {
		log.LogWarn("doCommit: maxOid = ", maxOid, "lastOid = ", c.loadLastOid())
		c.storeLastOid(maxOid)
	}
	return err
}

func catchupDeleteIndex(oldIdxName, newIdxName string) error {
	var (
		oldIdxFile, newIdxFile *os.File
		err                    error
	)
	if oldIdxFile, err = os.OpenFile(oldIdxName, os.O_RDONLY, 0644); err != nil {
		return err
	}
	defer oldIdxFile.Close()

	if newIdxFile, err = os.OpenFile(newIdxName, os.O_RDWR, 0644); err != nil {
		return err
	}
	defer newIdxFile.Close()

	newinfo, err := newIdxFile.Stat()
	if err != nil {
		return err
	}

	data := make([]byte, ObjectHeaderSize)
	_, err = newIdxFile.ReadAt(data, newinfo.Size()-ObjectHeaderSize)
	if err != nil {
		return err
	}

	lastIndexEntry := &Object{}
	lastIndexEntry.Unmarshal(data)

	oldInfo, err := oldIdxFile.Stat()
	if err != nil {
		return err
	}

	catchup := make([]byte, 0)
	for offset := oldInfo.Size() - ObjectHeaderSize; offset >= 0; offset -= ObjectHeaderSize {
		_, err = oldIdxFile.ReadAt(data, offset)
		if err != nil {
			return err
		}

		o := &Object{}
		o.Unmarshal(data)
		if o.Size != MarkDeleteObject || o.IsIdentical(lastIndexEntry) {
			break
		}
		result := make([]byte, len(catchup)+ObjectHeaderSize)
		copy(result, data)
		copy(result[ObjectHeaderSize:], catchup)
		catchup = result
	}

	_, err = newIdxFile.Seek(0, 2)
	if err != nil {
		return err
	}
	_, err = newIdxFile.Write(catchup)
	if err != nil {
		return err
	}

	return nil
}
