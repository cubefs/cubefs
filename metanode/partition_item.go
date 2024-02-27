// Copyright 2018 The CubeFS Authors.
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
	"io"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/util/errors"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// MetaItem defines the structure of the metadata operations.
type MetaItem struct {
	Op uint32 `json:"Op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

// MarshalJson
func (s *MetaItem) MarshalJson() ([]byte, error) {
	return json.Marshal(s)
}

// MarshalBinary marshals MetaItem to binary data.
// Binary frame structure:
//
//	+------+----+------+------+------+------+
//	| Item | Op | LenK |   K  | LenV |   V  |
//	+------+----+------+------+------+------+
//	| byte | 4  |  4   | LenK |  4   | LenV |
//	+------+----+------+------+------+------+
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
//
//	+------+----+------+------+------+------+
//	| Item | Op | LenK |   K  | LenV |   V  |
//	+------+----+------+------+------+------+
//	| byte | 4  |  4   | LenK |  4   | LenV |
//	+------+----+------+------+------+------+
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

type fileData struct {
	filename string
	data     []byte
}

const (
	// initial version
	SnapFormatVersion_0 uint32 = iota

	// version since transaction feature, added formatVersion, txId and cursor in MetaItemIterator struct
	SnapFormatVersion_1
)

// MetaItemIterator defines the iterator of the MetaItem.
type MetaItemIterator struct {
	fileRootDir       string
	SnapFormatVersion uint32
	applyID           uint64
	uniqID            uint64
	txId              uint64
	cursor            uint64
	treeSnap          Snapshot
	uniqChecker       *uniqChecker
	verList           []*proto.VolVersionInfo
	deletedExtentsId  uint64

	filenames []string

	dataCh    chan interface{}
	errorCh   chan error
	err       error
	closeCh   chan struct{}
	closeOnce sync.Once
}

// SnapItemWrapper key definition
const (
	SiwKeySnapFormatVer uint32 = iota
	SiwKeyApplyId
	SiwKeyTxId
	SiwKeyCursor
	SiwKeyUniqId
	SiwKeyVerList
	SiwKeyDeletedExtentsId
)

type SnapItemWrapper struct {
	key   uint32
	value interface{}
}

func (siw *SnapItemWrapper) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint32(k, siw.key)
	return
}

func (siw *SnapItemWrapper) UnmarshalKey(k []byte) (err error) {
	siw.key = binary.BigEndian.Uint32(k)
	return
}

// newMetaItemIterator returns a new MetaItemIterator.
func newMetaItemIterator(mp *metaPartition) (si *MetaItemIterator, err error) {
	si = new(MetaItemIterator)
	si.fileRootDir = mp.config.RootDir
	si.SnapFormatVersion = mp.manager.metaNode.raftSyncSnapFormatVersion
	mp.nonIdempotent.Lock()
	si.applyID = mp.getApplyID()
	si.txId = mp.txProcessor.txManager.txIdAlloc.getTransactionID()
	si.cursor = mp.GetCursor()
	si.uniqID = mp.GetUniqId()
	si.treeSnap, err = mp.GetSnapShot()
	if err != nil {
		return
	}
	si.uniqChecker = mp.uniqChecker.clone()
	si.verList = mp.GetAllVerList()
	si.deletedExtentsId = mp.GetDeletedExtentId()
	mp.nonIdempotent.Unlock()

	if si.treeSnap == nil {
		return nil, errors.NewErrorf("get mp[%v] tree snap failed", mp.config.PartitionId)
	}

	si.dataCh = make(chan interface{})
	si.errorCh = make(chan error, 1)
	si.closeCh = make(chan struct{})

	// TODO(NaturalSelect): check need or not
	// collect extend del files
	filenames := make([]string, 0)
	var fileInfos []os.DirEntry
	if fileInfos, err = os.ReadDir(mp.config.RootDir); err != nil {
		return
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtent) {
			filenames = append(filenames, fileInfo.Name())
		}
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtentV2) {
			filenames = append(filenames, fileInfo.Name())
		}
	}
	si.filenames = filenames

	// start data producer
	go func(iter *MetaItemIterator) {
		defer func() {
			close(iter.dataCh)
			close(iter.errorCh)
			si.treeSnap.Close()
		}()
		produceItem := func(item interface{}) (success bool) {
			select {
			case iter.dataCh <- item:
				return true
			case <-iter.closeCh:
				return false
			}
		}
		produceError := func(err error) {
			select {
			case iter.errorCh <- err:
			default:
			}
		}
		checkClose := func() (closed bool) {
			select {
			case <-iter.closeCh:
				return true
			default:
				return false
			}
		}

		if si.SnapFormatVersion == SnapFormatVersion_0 {
			// process index ID
			produceItem(si.applyID)
			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_0, partitionId(%v), applyID(%v)",
				mp.config.PartitionId, si.applyID)
		} else if si.SnapFormatVersion == SnapFormatVersion_1 {
			// process snapshot format version
			snapFormatVerWrapper := SnapItemWrapper{SiwKeySnapFormatVer, si.SnapFormatVersion}
			produceItem(snapFormatVerWrapper)

			// process apply index ID
			applyIdWrapper := SnapItemWrapper{SiwKeyApplyId, si.applyID}
			produceItem(applyIdWrapper)

			// process txId
			txIdWrapper := SnapItemWrapper{SiwKeyTxId, si.txId}
			produceItem(txIdWrapper)

			// process cursor
			cursorWrapper := SnapItemWrapper{SiwKeyCursor, si.cursor}
			produceItem(cursorWrapper)

			verListWrapper := SnapItemWrapper{SiwKeyVerList, si.verList}
			produceItem(verListWrapper)

			// NOTE: process deleted extents id
			deletedExtentsIdWrapper := SnapItemWrapper{SiwKeyDeletedExtentsId, si.deletedExtentsId}
			produceItem(deletedExtentsIdWrapper)

			log.LogDebugf("newMetaItemIterator: SnapFormatVersion_1, partitionId(%v) applyID(%v) txId(%v) cursor(%v) uniqID(%v) verList(%v)",
				mp.config.PartitionId, si.applyID, si.txId, si.cursor, si.uniqID, si.verList)

			if si.uniqID != 0 {
				// process uniqId
				uniqIdWrapper := SnapItemWrapper{SiwKeyUniqId, si.uniqID}
				produceItem(uniqIdWrapper)
			}
		} else {
			panic(fmt.Sprintf("invalid raftSyncSnapFormatVersione: %v", si.SnapFormatVersion))
		}

		// NOTE: if using rocksdb, send base
		// process inodes
		if err = iter.treeSnap.Range(InodeType, func(v interface{}) (bool, error) {
			log.LogDebugf("[newMetaItemIterator] send inode")
			return produceItem(v.(*Inode)), nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process dentries
		if err = iter.treeSnap.Range(DentryType, func(v interface{}) (bool, error) {
			log.LogDebugf("[newMetaItemIterator] send dentries")
			return produceItem(v.(*Dentry)), nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process extends
		if err = iter.treeSnap.Range(ExtendType, func(v interface{}) (bool, error) {
			log.LogDebugf("[newMetaItemIterator] send extends")
			return produceItem(v.(*Extend)), nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}
		// process multiparts
		if err = iter.treeSnap.Range(MultipartType, func(v interface{}) (bool, error) {
			log.LogDebugf("[newMetaItemIterator] send multi parts")
			return produceItem(v.(*Multipart)), nil
		}); err != nil {
			produceError(err)
			return
		}
		if checkClose() {
			return
		}

		if si.SnapFormatVersion == SnapFormatVersion_1 {
			iter.treeSnap.Range(TransactionType, func(i interface{}) (bool, error) {
				log.LogDebugf("[newMetaItemIterator] send transaction")
				return produceItem(i), nil
			})
			if checkClose() {
				return
			}

			iter.treeSnap.Range(TransactionRollbackInodeType, func(i interface{}) (bool, error) {
				log.LogDebugf("[newMetaItemIterator] send rb inodes")
				return produceItem(i), nil
			})
			if checkClose() {
				return
			}

			iter.treeSnap.Range(TransactionRollbackDentryType, func(i interface{}) (bool, error) {
				log.LogDebugf("[newMetaItemIterator] send rb dentries")
				return produceItem(i), nil
			})
			if checkClose() {
				return
			}

			if si.uniqID != 0 {
				produceItem(si.uniqChecker)
				if checkClose() {
					return
				}
			}

			iter.treeSnap.Range(DeletedExtentsType, func(item interface{}) (bool, error) {
				log.LogDebugf("[newMetaItemIterator] send deleted extents")
				return produceItem(item), nil
			})
		}

		// process extent del files
		var err error
		var raw []byte
		for _, filename := range iter.filenames {
			if raw, err = os.ReadFile(path.Join(iter.fileRootDir, filename)); err != nil {
				produceError(err)
				return
			}
			if !produceItem(&fileData{filename: filename, data: raw}) {
				return
			}
		}
	}(si)

	log.LogDebugf("[newMetaItemIterator] send snapshot")
	return
}

// ApplyIndex returns the applyID of the iterator.
func (si *MetaItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

// Close closes the iterator.
func (si *MetaItemIterator) Close() {
	si.closeOnce.Do(func() {
		close(si.closeCh)
	})
	return
}

// Next returns the next item.
func (si *MetaItemIterator) Next() (data []byte, err error) {
	if si.err != nil {
		err = si.err
		return
	}
	var item interface{}
	var open bool
	select {
	case item, open = <-si.dataCh:
	case err, open = <-si.errorCh:
	}
	if item == nil || !open {
		err, si.err = io.EOF, io.EOF
		si.Close()
		return
	}
	if err != nil {
		si.err = err
		si.Close()
		return
	}

	var snap *MetaItem
	switch typedItem := item.(type) {
	case uint64:
		applyIDBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
		data = applyIDBuf
		return
	case SnapItemWrapper:
		if typedItem.key == SiwKeySnapFormatVer {
			snapFormatVerBuf := make([]byte, 8)
			binary.BigEndian.PutUint32(snapFormatVerBuf, si.SnapFormatVersion)
			snap = NewMetaItem(opFSMSnapFormatVersion, typedItem.MarshalKey(), snapFormatVerBuf)
		} else if typedItem.key == SiwKeyApplyId {
			applyIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(applyIDBuf, si.applyID)
			snap = NewMetaItem(opFSMApplyId, typedItem.MarshalKey(), applyIDBuf)
		} else if typedItem.key == SiwKeyTxId {
			txIDBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(txIDBuf, si.txId)
			snap = NewMetaItem(opFSMTxId, typedItem.MarshalKey(), txIDBuf)
		} else if typedItem.key == SiwKeyCursor {
			cursor := typedItem.value.(uint64)
			cursorBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(cursorBuf, cursor)
			snap = NewMetaItem(opFSMCursor, typedItem.MarshalKey(), cursorBuf)
		} else if typedItem.key == SiwKeyUniqId {
			uniqId := typedItem.value.(uint64)
			uniqIdBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(uniqIdBuf, uniqId)
			snap = NewMetaItem(opFSMUniqIDSnap, typedItem.MarshalKey(), uniqIdBuf)
		} else if typedItem.key == SiwKeyVerList {
			var verListBuf []byte
			if verListBuf, err = json.Marshal(typedItem.value.([]*proto.VolVersionInfo)); err != nil {
				return
			}
			snap = NewMetaItem(opFSMVerListSnapShot, typedItem.MarshalKey(), verListBuf)
			log.LogInfof("snapshot.fileRootDir %v verList %v", si.fileRootDir, verListBuf)
		} else if typedItem.key == SiwKeyDeletedExtentsId {
			deletedExtentsId := typedItem.value.(uint64)
			deletedExtentsIdBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(deletedExtentsIdBuf, deletedExtentsId)
			snap = NewMetaItem(opFSMDeletedExtentsId, typedItem.MarshalKey(), deletedExtentsIdBuf)
		} else {
			panic(fmt.Sprintf("MetaItemIterator.Next: unknown SnapItemWrapper key: %v", typedItem.key))
		}
	case *Inode:
		snap = NewMetaItem(opFSMCreateInode, typedItem.MarshalKey(), typedItem.MarshalValue())
	case *Dentry:
		snap = NewMetaItem(opFSMCreateDentry, typedItem.MarshalKey(), typedItem.MarshalValue())
	case *Extend:
		var raw []byte
		if raw, err = typedItem.Bytes(); err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMSetXAttr, nil, raw)
	case *Multipart:
		var raw []byte
		if raw, err = typedItem.Bytes(); err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMCreateMultipart, nil, raw)
	case *proto.TransactionInfo:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMTxSnapshot, []byte(typedItem.TxID), val)
	case *TxRollbackInode:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMTxRbInodeSnapshot, typedItem.inode.MarshalKey(), val)
	case *TxRollbackDentry:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMTxRbDentrySnapshot, []byte(typedItem.txDentryInfo.GetKey()), val)
	case *fileData:
		snap = NewMetaItem(opExtentFileSnapshot, []byte(typedItem.filename), typedItem.data)
	case *uniqChecker:
		var raw []byte
		if raw, _, err = typedItem.Marshal(); err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMUniqCheckerSnap, nil, raw)
	case *DeletedExtentKey:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMDeletedExtentsSnap, nil, val)
	case *DeletedObjExtentKey:
		var val []byte
		val, err = typedItem.Marshal()
		if err != nil {
			si.err = err
			si.Close()
			return
		}
		snap = NewMetaItem(opFSMDeletedObjExtentsSnap, nil, val)
	default:
		panic(fmt.Sprintf("unknown item type: %v", reflect.TypeOf(item).Name()))
	}

	if data, err = snap.MarshalBinary(); err != nil {
		si.err = err
		si.Close()
		return
	}
	return
}
