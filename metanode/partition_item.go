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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/tecbot/gorocksdb"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
)

// MetaItem defines the structure of the metadata operations.
type MetaItem struct {
	Op          uint32 `json:"op"`
	K           []byte `json:"k"`
	V           []byte `json:"v"`
	From        string `json:"frm"` // The address of the client that initiated the operation.
	Timestamp   int64  `json:"ts"`  // DeleteTime of operation
	TrashEnable bool   `json:"te"`  // enable trash
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
	buff.Grow(4 * 3 + len(s.K) + len(s.V))
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

func (s *MetaItem) String() string {
	if s == nil {
		return ""
	}
	var v string
	switch s.Op {
	case opFSMCreateDentry, opFSMDeleteDentry:
		den := &Dentry{}
		if err := den.Unmarshal(s.V); err != nil {
			break
		}
		v = fmt.Sprintf("parent: %v, name: %v, inode: %v, type: %v", den.ParentId, den.Name, den.Inode, den.Type)
	case opFSMCreateInode, opFSMUnlinkInode:
		ino := &Inode{}
		if err := ino.Unmarshal(context.Background(), s.V); err != nil {
			break
		}
		v = fmt.Sprintf("inode: %v", ino.Inode)
	}
	return fmt.Sprintf("Op: %v, K: %v, V: %v, From: %v, Timestamp: %v, trashEnable: %v", s.Op, string(s.K), v, s.From, s.Timestamp, s.TrashEnable)
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

// MetaItemIterator defines the iterator of the MetaItem.
type MetaItemIterator struct {
	fileRootDir       string
	applyID           uint64
	marshalVersion    uint32 //just test
	db                *RocksDbInfo
	snap              *gorocksdb.Snapshot
	treeSnap          Snapshot
	filenames []string
	dataCh    chan interface{}
	errorCh   chan error
	err       error
	closeCh   chan struct{}
	closeOnce sync.Once
}

// newMetaItemIterator returns a new MetaItemIterator.
func newMetaItemIterator(mp *metaPartition) (si *MetaItemIterator, err error) {
	mp.raftFSMLock.Lock()
	defer mp.raftFSMLock.Unlock()
	si = new(MetaItemIterator)
	si.fileRootDir = mp.config.RootDir
	si.applyID = mp.applyID
	si.dataCh = make(chan interface{})
	si.errorCh = make(chan error, 1)
	si.closeCh = make(chan struct{})
	si.marshalVersion = mp.marshalVersion
	si.db = mp.db
	si.snap = mp.db.OpenSnap()
	si.treeSnap = NewSnapshot(mp)
	if si.treeSnap == nil {
		return nil, errors.NewErrorf("get mp[%v] tree snap failed", mp.config.PartitionId)
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
	go func(iter *MetaItemIterator) {
		defer func() {
			si.db.ReleaseSnap(si.snap)
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
		// process extent del files
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

func (si *MetaItemIterator) Version() uint32 {
	return BaseSnapshotV
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
	case *Inode:
		if si.marshalVersion == MetaPartitionMarshVersion2 {
			inodeBuf, _:= typedItem.MarshalV2()
			snap = NewMetaItem(opFSMCreateInode, inodeBuf[BaseInodeKeyOffset : BaseInodeKeyOffset + BaseInodeKeyLen],
												 inodeBuf[BaseInodeValueOffset:])
		} else {
			snap = NewMetaItem(opFSMCreateInode, typedItem.MarshalKey(), typedItem.MarshalValue())
		}

	case *Dentry:
		if si.marshalVersion == MetaPartitionMarshVersion2 {
			dentryBuf, _:= typedItem.MarshalV2()
			snap = NewMetaItem(opFSMCreateDentry, dentryBuf[DentryKeyOffset : DentryKeyOffset + typedItem.DentryKeyLen()],
												  dentryBuf[len(dentryBuf) - DentryValueLen : ])
		} else {
			snap = NewMetaItem(opFSMCreateDentry, typedItem.MarshalKey(), typedItem.MarshalValue())
		}

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
	return
}
