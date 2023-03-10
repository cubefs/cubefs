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
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tecbot/gorocksdb"
	"hash"
	"hash/crc32"
	"io"
	"sync"
)

const DefaultBatchCount = 128

var MetaBatchSnapshotVersionMap = map[string]SnapshotVersion{
	RocksDBVersion:        BatchSnapshotV1,
	Version3_3_0:          BatchSnapshotV1,
	MPReuseVersion:        BatchSnapshotV2,
	MetaNodeLatestVersion: LatestSnapV,
}

type EkData struct {
	key     []byte
	data    []byte
}
func NewEkData(k, v []byte) *EkData {
	ek := &EkData{}
	ek.key = make([]byte, 0)
	ek.data = make([]byte, 0)

	ek.key = append(ek.key, k...)
	ek.data = append(ek.data, v...)
	return ek
}

func (ekInfo *EkData) Marshal() []byte {
	buff := make([]byte, 8 + len(ekInfo.key) + len(ekInfo.data))
	off := 0
	binary.BigEndian.PutUint32(buff[off : off + 4], uint32(len(ekInfo.key)))
	off += 4
	copy(buff[off: off + len(ekInfo.key)], ekInfo.key)
	off += len(ekInfo.key)
	binary.BigEndian.PutUint32(buff[off : off + 4], uint32(len(ekInfo.data)))
	off += 4
	copy(buff[off: off + len(ekInfo.data)], ekInfo.data)
	return buff
}

func (ekInfo *EkData) UnMarshal(raw []byte) (err error){
	if len(raw) < 8 {
		err = fmt.Errorf("ek raw data failed, len(%d) is below 8", len(raw))
		return
	}
	ekInfo.key  = make([]byte, 0)
	ekInfo.data = make([]byte, 0)

	off := 0
	keyLen := int(binary.BigEndian.Uint32(raw[off : off + 4]))
	off += 4
	if off + keyLen > len(raw) {
		err = fmt.Errorf("ek raw data failed, offset(%d) key len(%d) is beyond raw len(%d)", off, keyLen, len(raw))
		return
	}
	ekInfo.key = append(ekInfo.key, raw[off : off + keyLen]...)

	off += keyLen
	dataLen := int(binary.BigEndian.Uint32(raw[off : off + 4]))
	off += 4
	if off + dataLen > len(raw) {
		err = fmt.Errorf("ek raw data failed, offset(%d) data len(%d) is beyond raw len(%d)", off, dataLen, len(raw))
		return
	}
	ekInfo.data = append(ekInfo.data, raw[off : off + dataLen]...)
	return nil
}
type DelExtentBatch []*EkData

// BatchMetaItemIterator defines the iterator of the MetaItem.
type BatchMetaItemIterator struct {
	fileRootDir     string
	applyID         uint64
	db              *RocksDbInfo
	rocksDBSnap     *gorocksdb.Snapshot
	dataCh          chan interface{}
	errorCh         chan error
	err             error
	closeCh         chan struct{}
	closeOnce       sync.Once
	snapshotSign    hash.Hash32
	snapshotCrcFlag bool
	treeSnap        Snapshot
	batchSnapV      SnapshotVersion
	metaConf        MetaPartitionConfig
}

// newBatchMetaItemIterator returns a new MetaItemIterator.
func newBatchMetaItemIterator(mp *metaPartition, snapV SnapshotVersion) (si *BatchMetaItemIterator, err error) {
	mp.raftFSMLock.Lock()
	defer mp.raftFSMLock.Unlock()
	si = new(BatchMetaItemIterator)
	si.fileRootDir = mp.config.RootDir
	si.applyID = mp.applyID
	si.dataCh = make(chan interface{})
	si.errorCh = make(chan error, 1)
	si.closeCh = make(chan struct{})
	si.snapshotSign = crc32.NewIEEE()
	si.treeSnap = NewSnapshot(mp)
	if si.treeSnap == nil {
		err = errors.NewErrorf("get mp[%v] tree snap failed", mp.config.PartitionId)
		return
	}
	si.batchSnapV = snapV
	si.db = mp.db
	si.rocksDBSnap = mp.db.OpenSnap()
	si.metaConf = *mp.config

	// start data producer
	go func(iter *BatchMetaItemIterator) {
		defer func() {
			si.db.ReleaseSnap(si.rocksDBSnap)
			si.treeSnap.Close()
			close(iter.dataCh)
			close(iter.errorCh)
		}()
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

		if snapV >= BatchSnapshotV2 {
			ok := produceItem(&si.metaConf)
			if !ok {
				return
			}
		}

		// process inodes
		if err = iter.treeSnap.Range(InodeType, func(v interface{}) (bool, error) {
			if ok := produceItem(v.(*Inode)); !ok {
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
		// process dentries
		if err = iter.treeSnap.Range(DentryType, func(v interface{}) (bool, error) {
			if ok := produceItem(v.(*Dentry)); !ok {
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
		if err = iter.treeSnap.Range(ExtendType, func(v interface{}) (bool, error) {
			if ok := produceItem(v.(*Extend)); !ok {
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
		if err = iter.treeSnap.Range(MultipartType, func(v interface{}) (bool, error) {
			if ok := produceItem(v.(*Multipart)); !ok {
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
		if err = iter.treeSnap.Range(DelInodeType, func(v interface{}) (bool, error) {
			if ok := produceItem(v.(*DeletedINode)); !ok {
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
		if err = iter.treeSnap.Range(DelDentryType, func(v interface{}) (bool, error) {
			if ok := produceItem(v.(*DeletedDentry)); !ok {
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

		log.LogInfof("raft snap recover follower, send rocks db extent")
		stKey   := make([]byte, 1)
		endKey  := make([]byte, 1)

		stKey[0]  = byte(ExtentDelTable)
		endKey[0] = byte(ExtentDelTable + 1)
		_ = mp.db.RangeWithSnap(stKey, endKey, si.rocksDBSnap, func(k, v []byte) (bool, error){
			if k[0] !=  byte(ExtentDelTable) {
				return false, nil
			}
			ek := NewEkData(k, v)
			if !produceItem(ek) {
				return false, fmt.Errorf("gen snap:producet extent item failed")
			}
			return true, nil
		})
		if checkClose() {
			return
		}
	}(si)

	return
}

// ApplyIndex returns the applyID of the iterator.
func (si *BatchMetaItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

// Close closes the iterator.
func (si *BatchMetaItemIterator) Close() {
	si.closeOnce.Do(func() {
		close(si.closeCh)
	})
	return
}

func (si *BatchMetaItemIterator) Version() uint32 {
	return uint32(si.batchSnapV)
}

func (si *BatchMetaItemIterator) Next() (data []byte, err error) {
	if si.err != nil {
		if !si.snapshotCrcFlag && si.err == io.EOF {
			si.snapshotCrcFlag = true
			crcBuff := make([]byte, 4)
			binary.BigEndian.PutUint32(crcBuff, si.snapshotSign.Sum32())
			snap := NewMetaItem(opFSMSnapShotCrc, nil, crcBuff)
			if data, err = snap.MarshalBinary(); err != nil{
				log.LogErrorf("Item Iterator: snap marshal binary failed:%v, dir:%s", err, si.fileRootDir)
				si.err = err
				si.Close()
			}
			return
		}
		err = si.err
		return
	}

	var item interface{}
	var open bool
	var snap *MetaItem

	var (
		val        []byte
		mulItems  MulItems
	)
	for i := 0; i < DefaultBatchCount;  i++{
		select {
		case item, open = <-si.dataCh:
		case err, open = <-si.errorCh:
		}
		if item == nil || !open {
			err, si.err = io.EOF, io.EOF
			log.LogWarnf("Item Iterator: snap finish:%v, dir:%s", err, si.fileRootDir)
			si.Close()
			break
		}
		if err != nil {
			si.err = err
			log.LogErrorf("Item Iterator: receive error:%v, dir:%s", err, si.fileRootDir)
			si.Close()
			break
		}

		switch item.(type) {
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
		case *DeletedINode:
			mulItems.DeletedInodeBatches = append(mulItems.DeletedInodeBatches, item.(*DeletedINode))
			continue
		case *DeletedDentry:
			mulItems.DeletedDentryBatches = append(mulItems.DeletedDentryBatches, item.(*DeletedDentry))
			continue
		case *EkData:
			mulItems.DelExtents = append(mulItems.DelExtents, item.(*EkData))
			continue
		case *MetaPartitionConfig:
			var confJsonData []byte
			confJsonData, err = item.(*MetaPartitionConfig).marshalJson()
			if err != nil {
				si.err = err
				si.Close()
				return
			}
			snap = NewMetaItem(opFSMSyncMetaConf, nil, confJsonData)
			if data, err = snap.MarshalBinary(); err != nil{
				si.err = err
				si.Close()
			}
			return
		default:
			break
		}
	}

	val, err = mulItems.Marshal()
	if err != nil {
		si.err = err
		log.LogErrorf("Item Iterator: multi item marshal failed:%v, dir:%s", err, si.fileRootDir)
		si.Close()
		return
	}
	snap = NewMetaItem(opFSMBatchCreate, nil, val)
	if data, err = snap.MarshalBinary(); err != nil {
		log.LogErrorf("Item Iterator: snap marshal failed:%v, dir:%s", err, si.fileRootDir)
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
