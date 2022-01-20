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
	"context"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
)

// MetaItemIteratorV2 defines the iterator of the MetaItem.
type MetaItemIteratorV2 struct {
	fileRootDir   string
	applyID       uint64

	filenames []string

	dataCh    chan interface{}
	errorCh   chan error
	err       error
	closeCh   chan struct{}
	closeOnce sync.Once

	dataTmpCh chan interface{}
	snapshotSign hash.Hash32
	snapshotCrcFlag bool
	treeSnap Snapshot
}

// newMetaItemIteratorV2 returns a new MetaItemIterator.
func newMetaItemIteratorV2(mp *metaPartition ) (si *MetaItemIteratorV2, err error) {
	si = new(MetaItemIteratorV2)
	si.fileRootDir = mp.config.RootDir
	si.applyID = mp.applyID
	si.dataCh = make(chan interface{})
	si.errorCh = make(chan error, 1)
	si.closeCh = make(chan struct{})
	si.dataTmpCh = make(chan interface{}, 1)
	si.snapshotSign = crc32.NewIEEE()
	si.treeSnap = NewSnapshot(mp)
	if si.treeSnap == nil {
		err = errors.NewErrorf("get mp[%v] tree snap failed", mp.config.PartitionId)
		return
	}
	// collect extend del files
	var filenames = make([]string, 0)
	var fileInfos []os.FileInfo
	if fileInfos, err = ioutil.ReadDir(mp.config.RootDir); err != nil {
		return
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtent) {
			filenames = append(filenames, fileInfo.Name())
		}
	}
	si.filenames = filenames

	// start data producer
	go func(iter *MetaItemIteratorV2) {
		defer func() {
			close(iter.dataCh)
			close(iter.errorCh)
		}()
		var ctx = context.Background()
		var produceItem = func(item interface{}) (success bool) {
			select {
			case iter.dataCh <- item:
				return true
			case <-iter.closeCh:
				return false
			}
		}
		var produceError = func(err error) {
			select {
			case iter.errorCh <- err:
			default:
			}
		}
		var checkClose = func() (closed bool) {
			select {
			case <-iter.closeCh:
				return true
			default:
				return false
			}
		}
		// process index ID
		produceItem(si.applyID)

		// process inodes
		if err = iter.treeSnap.Range(InodeType, func(v []byte) (bool, error) {
			inode := NewInode(0, 0)
			if e := inode.Unmarshal(ctx, v); e != nil {
				return false, e
			}
			if ok := produceItem(inode); !ok {
				return false, nil
			}
			return true, nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		//process deleted inode
		if err = iter.treeSnap.Range(DelInodeType, func(v []byte) (bool, error) {
			dino := NewDeletedInodeByID(0)
			if e := dino.Unmarshal(ctx, v); e != nil {
				return false, e
			}
			produceItem(dino)
			return true, nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process dentries
		if err = iter.treeSnap.Range(DentryType, func(v []byte) (bool, error) {
			dentry := new(Dentry)
			if e := dentry.Unmarshal(v); e != nil {
				return false, e
			}
			if ok := produceItem(dentry); !ok {
				return false, nil
			}
			return true, nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process deleted dentries
		if err = iter.treeSnap.Range(DelDentryType, func(v []byte) (bool, error) {
			dd := newPrimaryDeletedDentry(0, "", 0, 0)
			if e := dd.Unmarshal(v); e != nil {
				return false, e
			}
			if ok := produceItem(dd); !ok {
				return false, nil
			}
			return true, nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process extends
		if err = iter.treeSnap.Range(ExtendType, func(v []byte) (bool, error) {
			extend, e := NewExtendFromBytes(v)
			if e != nil {
				return false, e
			}
			if ok := produceItem(extend); !ok {
				return false, nil
			}
			return true, nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process multiparts
		if err = iter.treeSnap.Range(MultipartType, func(v []byte) (bool, error) {
			multipart := MultipartFromBytes
			if ok := produceItem(multipart); !ok {
				return false, nil
			}
			return true, nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process extent del files
		var err error
		var raw []byte
		for _, filename := range iter.filenames {
			if raw, err = ioutil.ReadFile(path.Join(iter.fileRootDir, filename)); err != nil {
				produceError(err)
				return
			}
			if !produceItem(&fileData{filename: filename, data: raw}) {
				return
			}
		}
	}(si)

	return
}

// ApplyIndex returns the applyID of the iterator.
func (si *MetaItemIteratorV2) ApplyIndex() uint64 {
	return si.applyID
}

// Close closes the iterator.
func (si *MetaItemIteratorV2) Close() {
	si.closeOnce.Do(func() {
		close(si.closeCh)
	})
	return
}

func (si *MetaItemIteratorV2) Next() (data []byte, err error) {
	if si.err != nil {
		if !si.snapshotCrcFlag && si.err == io.EOF {
			si.snapshotCrcFlag = true
			crcBuff := make([]byte, 4)
			binary.BigEndian.PutUint32(crcBuff, si.snapshotSign.Sum32())
			snap := NewMetaItem(opFSMSnapShotCrc, nil, crcBuff)
			if data, err = snap.MarshalBinary(); err != nil{
				si.err = err
				si.Close()
			}
			return
		}
		err = si.err
		return
	}
	if len(si.dataTmpCh) != 0 {
		return si.tmpChDataMarshal()
	}
	var item interface{}
	var open bool
	var snap *MetaItem

	batchSize := 128
	var (
		val        []byte
		mulItems  MulItems
	)
	for i := 0; i < batchSize;  i++{
		select {
		case item, open = <-si.dataCh:
		case err, open = <-si.errorCh:
		}
		if item == nil || !open {
			err, si.err = io.EOF, io.EOF
			si.Close()
			break
		}
		if err != nil {
			si.err = err
			si.Close()
			break
		}

		switch   item.(type) {
		case uint64:
			applyIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
			data = applyIDBuf
			lenData := make([]byte,4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = si.snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = si.snapshotSign.Write(data); err != nil{
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			return
		case *Inode:
			mulItems.InodeBatches = append(mulItems.InodeBatches, item.(*Inode))
			continue
		case *Dentry:
			mulItems.DentryBatches = append(mulItems.DentryBatches, item.(*Dentry))
			continue
		case *Extend:
			mulItems.ExtendBatches = append(mulItems.ExtendBatches, item.(*Extend))
			continue
		case *Multipart:
			mulItems.MultipartBatches = append(mulItems.MultipartBatches, item.(*Multipart))
			continue
		default:
			si.dataTmpCh <- item
			break
		}
	}

	val, err = mulItems.Marshal()
	if err != nil {
		si.err = err
		si.Close()
		return
	}
	snap = NewMetaItem(opFSMBatchCreate, nil, val)
	if data, err = snap.MarshalBinary(); err != nil {
		si.err = err
		si.Close()
		return
	}
	lenData := make([]byte,4)
	binary.BigEndian.PutUint32(lenData, uint32(len(data)))
	if _, err = si.snapshotSign.Write(lenData); err != nil {
		log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
	}
	if _, err = si.snapshotSign.Write(data); err != nil{
		log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
	}
	return
}

func (si *MetaItemIteratorV2)tmpChDataMarshal() (data []byte, err error) {
	item := <- si.dataTmpCh
	var snap *MetaItem
	switch typedItem := item.(type) {
	case *fileData:
		snap = NewMetaItem(opExtentFileSnapshot, []byte(typedItem.filename), typedItem.data)
	default:
		panic(fmt.Sprintf("unknown item type: %v", reflect.TypeOf(item).Name()))
	}
	if data, err = snap.MarshalBinary(); err != nil {
		si.err = err
		si.Close()
		return
	}
	lenData := make([]byte,4)
	binary.BigEndian.PutUint32(lenData, uint32(len(data)))
	if _, err = si.snapshotSign.Write(lenData); err != nil {
		log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
	}
	if _, err = si.snapshotSign.Write(data); err != nil{
		log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
	}
	return
}