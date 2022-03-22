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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tecbot/gorocksdb"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
)

type RaftCmdItemMarshalBinaryVersion byte

const (
	ItemMarshalBinaryMinVersion RaftCmdItemMarshalBinaryVersion = iota
	ItemMarshalBinaryV1
	ItemMarshalBinaryMaxVersion
	ItemMarshalJsonVersion = 123
)

const (
	RaftCmdItemMarshalBinaryBaseLen = 26
	MetaItemMarshalBinaryTimestampLen = 8
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

func (s *MetaItem) MarshalBinaryWithVersion(version RaftCmdItemMarshalBinaryVersion) (dataBytes []byte, err error) {
	if s == nil {
		return nil, nil
	}

	switch version {
	case ItemMarshalBinaryV1:
		return s.marshalBinaryV1()
	default:
		return nil, fmt.Errorf("error version")
	}
}

func (s *MetaItem) marshalBinaryV1() (dataBytes []byte, err error) {
	length := RaftCmdItemMarshalBinaryBaseLen + len(s.K) + len(s.V) + len([]byte(s.From))
	dataBytes = make([]byte, length)
	offset := 0
	dataBytes[0] = byte(ItemMarshalBinaryV1)
	offset += 1
	binary.BigEndian.PutUint32(dataBytes[offset:offset+4], s.Op)
	offset += 4
	binary.BigEndian.PutUint32(dataBytes[offset:offset+4], uint32(len(s.K)))
	offset += 4
	copy(dataBytes[offset:offset+len(s.K)], s.K)
	offset += len(s.K)
	binary.BigEndian.PutUint32(dataBytes[offset:offset+4], uint32(len(s.V)))
	offset += 4
	copy(dataBytes[offset:offset+len(s.V)], s.V)
	offset += len(s.V)
	binary.BigEndian.PutUint32(dataBytes[offset:offset+4], uint32(len([]byte(s.From))))
	offset += 4
	copy(dataBytes[offset:offset+len([]byte(s.From))], []byte(s.From))
	offset += len([]byte(s.From))
	binary.BigEndian.PutUint64(dataBytes[offset:offset+8], uint64(s.Timestamp))
	offset += 8
	if s.TrashEnable {
		dataBytes[offset] = 1
	} else {
		dataBytes[offset] = 0
	}
	return
}

func (s *MetaItem) UnmarshalBinaryWithVersion(version RaftCmdItemMarshalBinaryVersion, raw []byte) (err error) {
	switch version {
	case ItemMarshalBinaryV1:
		return s.unmarshalBinaryV1(raw)
	default:
		return fmt.Errorf("error version")
	}
}

func (s *MetaItem) unmarshalBinaryV1(raw []byte) (err error) {
	var lenK, lenV, lenFrom uint32
	if len(raw) < RaftCmdItemMarshalBinaryBaseLen {
		return fmt.Errorf("error data")
	}

	//skip version
	offset := 1

	s.Op = binary.BigEndian.Uint32(raw[offset : offset+4])
	offset += 4

	lenK = binary.BigEndian.Uint32(raw[offset : offset+4])
	offset += 4

	if len(raw) < int(lenK+RaftCmdItemMarshalBinaryBaseLen) {
		err = fmt.Errorf("decode item K failed, actual len:%d, execpt at least:%d", len(raw)-offset, lenK+RaftCmdItemMarshalBinaryBaseLen-uint32(offset))
		log.LogErrorf(err.Error())
		return
	}
	s.K = make([]byte, lenK)
	copy(s.K, raw[offset:])
	offset += int(lenK)

	lenV = binary.BigEndian.Uint32(raw[offset : offset+4])
	offset += 4

	if len(raw) < int(lenK+lenV+RaftCmdItemMarshalBinaryBaseLen) {
		err = fmt.Errorf("decode item V failed, actual len:%d, execpt at least:%d", len(raw)-offset, lenV+lenV+RaftCmdItemMarshalBinaryBaseLen-uint32(offset))
		log.LogErrorf(err.Error())
		return
	}
	s.V = make([]byte, lenV)
	copy(s.V, raw[offset:])
	offset += int(lenV)

	lenFrom = binary.BigEndian.Uint32(raw[offset : offset+4])
	offset += 4

	if len(raw) < int(lenFrom+lenK+lenV+RaftCmdItemMarshalBinaryBaseLen) {
		err = fmt.Errorf("decode item from failed, actual len:%d, execpt at least:%d", len(raw)-offset, lenFrom+lenK+lenV+RaftCmdItemMarshalBinaryBaseLen-uint32(offset))
		log.LogErrorf(err.Error())
		return
	}
	fromBuff := make([]byte, lenFrom)
	copy(fromBuff, raw[offset:])
	offset += int(lenFrom)
	s.From = string(fromBuff)

	s.Timestamp = int64(binary.BigEndian.Uint64(raw[offset : offset+8]))
	offset += 8

	if raw[offset] == 1 {
		s.TrashEnable = true
	}
	return
}

func (s *MetaItem) UnmarshalRaftMsg(command []byte) (err error) {
	if len(command) <= 0 {
		return fmt.Errorf("error raft command")
	}
	if command[0] == ItemMarshalJsonVersion {
		log.LogInfof("unmarshal by json")
		return s.UnmarshalJson(command)
	}

	var version = RaftCmdItemMarshalBinaryVersion(command[0])
	if version > ItemMarshalBinaryMinVersion && version < ItemMarshalBinaryMaxVersion {
		log.LogInfof("unmarshal by binary")
		return s.UnmarshalBinaryWithVersion(version, command)
	}
	return fmt.Errorf("error version:%v", version)
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
		si.db.ReleaseSnap(si.snap)
		si.treeSnap.Close()
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
