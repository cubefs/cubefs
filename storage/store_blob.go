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
	"os"
	"time"

	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/raft/util"
)

const (
	BlobFileFileCount = 10
	BlobFileOpenOpt   = os.O_CREATE | os.O_RDWR | os.O_APPEND
	CompactThreshold  = 40
	CompactMaxWait    = time.Second * 10
	ObjectIdLen       = 8
)

var (
	gMaxBlobFileSize uint64
)

// BlobStore is a store implement for blob file storage which container 40 blobfile files.
// This store will choose a available blobfile file and append data to it.
type BlobStore struct {
	dataDir           string
	blobfiles         map[int]*BlobFile
	availBlobFileCh   chan int
	unavailBlobFileCh chan int
	storeSize         int
	blobfileSize      int
}

func NewBlobStore(dataDir string, storeSize int) (s *BlobStore, err error) {
	s = new(BlobStore)
	s.dataDir = dataDir
	if err = CheckAndCreateSubdir(dataDir); err != nil {
		return nil, fmt.Errorf("NewBlobStore [%v] err[%v]", dataDir, err)
	}
	s.blobfiles = make(map[int]*BlobFile)
	if err = s.initBlobFileFile(); err != nil {
		return nil, fmt.Errorf("NewBlobStore [%v] err[%v]", dataDir, err)
	}

	s.availBlobFileCh = make(chan int, BlobFileFileCount+1)
	s.unavailBlobFileCh = make(chan int, BlobFileFileCount+1)
	for i := 1; i <= BlobFileFileCount; i++ {
		s.unavailBlobFileCh <- i
	}
	s.storeSize = storeSize
	s.blobfileSize = storeSize / BlobFileFileCount
	gMaxBlobFileSize = 2*util.MB

	return
}

func (s *BlobStore) DeleteStore() {
	for index, c := range s.blobfiles {
		c.file.Close()
		c.tree.idxFile.Close()
		delete(s.blobfiles, index)
	}
	os.RemoveAll(s.dataDir)
}

func (s *BlobStore) UseSize() (size int64) {
	// TODO: implement this
	return 0
}

func (s *BlobStore) initBlobFileFile() (err error) {
	for i := 1; i <= BlobFileFileCount; i++ {
		var c *BlobFile
		if c, err = NewBlobFile(s.dataDir, i); err != nil {
			return fmt.Errorf("initBlobFileFile Error %s", err.Error())
		}
		s.blobfiles[i] = c
	}

	return
}

func (s *BlobStore) blobfileExist(blobfileId uint32) (exist bool) {
	name := s.dataDir + "/" + strconv.Itoa(int(blobfileId))
	if _, err := os.Stat(name); err == nil {
		exist = true
	}

	return
}

func (s *BlobStore) WriteDeleteDentry(objectId uint64, blobfileId int, crc uint32) (err error) {
	var (
		fi os.FileInfo
	)
	c, ok := s.blobfiles[blobfileId]
	if !ok {
		return ErrorFileNotFound
	}
	if !c.compactLock.TryLock() {
		return ErrorAgain
	}
	defer c.compactLock.Unlock()
	if fi, err = c.file.Stat(); err != nil {
		return
	}
	o := &Object{Oid: objectId, Size: MarkDeleteObject, Offset: uint32(fi.Size()), Crc: crc}
	if err = c.tree.appendToIdxFile(o); err == nil {
		if c.loadLastOid() < objectId {
			c.storeLastOid(objectId)
		}
	}

	return
}

func (s *BlobStore) Write(fileId uint32, objectId uint64, size int64, data []byte, crc uint32) (err error) {
	var (
		fi os.FileInfo
	)
	blobfileId := int(fileId)
	c, ok := s.blobfiles[blobfileId]
	if !ok {
		return ErrorFileNotFound
	}

	if !c.compactLock.TryLock() {
		return ErrorAgain
	}
	defer c.compactLock.Unlock()

	if objectId < c.loadLastOid() {
		msg := fmt.Sprintf("Object id smaller than last oid. DataDir[%v] FileId[%v]"+
			" ObjectId[%v] Size[%v]", s.dataDir, blobfileId, objectId, c.loadLastOid())
		err = fmt.Errorf(msg)
		return ErrObjectSmaller
	}

	if fi, err = c.file.Stat(); err != nil {
		return
	}

	newOffset := fi.Size()
	if _, err = c.file.WriteAt(data[:size], newOffset); err != nil {
		return
	}

	if _, _, err = c.tree.set(objectId, uint32(newOffset), uint32(size), crc); err == nil {
		if c.loadLastOid() < objectId {
			c.storeLastOid(objectId)
		}
	}
	return
}

func (s *BlobStore) Read(fileId uint32, offset, size int64, nbuf []byte) (crc uint32, err error) {
	blobfileId := int(fileId)
	objectId := uint64(offset)
	c, ok := s.blobfiles[blobfileId]
	if !ok {
		return 0, ErrorFileNotFound
	}

	lastOid := c.loadLastOid()
	if lastOid < objectId {
		return 0, ErrorFileNotFound
	}

	c.commitLock.RLock()
	defer c.commitLock.RUnlock()

	var fi os.FileInfo
	if fi, err = c.file.Stat(); err != nil {
		return
	}

	o, ok := c.tree.get(objectId)
	if !ok {
		return 0, ErrorObjNotFound
	}

	if int64(o.Size) != size || int64(o.Offset)+size > fi.Size() {
		return 0, ErrorParamMismatch
	}

	if _, err = c.file.ReadAt(nbuf[:size], int64(o.Offset)); err != nil {
		return
	}
	crc = o.Crc

	return
}

func (s *BlobStore) Sync(fileId uint32) (err error) {
	blobfileId := (int)(fileId)
	c, ok := s.blobfiles[blobfileId]
	if !ok {
		return ErrorFileNotFound
	}

	err = c.tree.idxFile.Sync()
	if err != nil {
		return
	}

	return c.file.Sync()
}

func (s *BlobStore) GetAllWatermark() (blobfiles []*FileInfo, err error) {
	blobfiles = make([]*FileInfo, 0)
	for blobfileId, c := range s.blobfiles {
		ci := &FileInfo{FileId: blobfileId, Size: c.loadLastOid()}
		blobfiles = append(blobfiles, ci)
	}

	return
}

func (s *BlobStore) GetWatermark(fileId uint64) (blobfileInfo *FileInfo, err error) {
	blobfileId := (int)(fileId)
	c, ok := s.blobfiles[blobfileId]
	if !ok {
		return nil, ErrorFileNotFound
	}
	blobfileInfo = &FileInfo{FileId: blobfileId, Size: c.loadLastOid()}

	return
}

func (s *BlobStore) GetAvailBlobFile() (blobfileId int, err error) {
	select {
	case blobfileId = <-s.availBlobFileCh:
	default:
		err = ErrorNoAvaliFile
	}

	return
}

func (s *BlobStore) GetBlobFileForWrite() (blobfileId int, err error) {
	select {
	case blobfileId = <-s.availBlobFileCh:
		return blobfileId, nil
	default:
		return -1, ErrorNoAvaliFile
	}

	return
}

func (s *BlobStore) SyncAll() {
	for _, blobfileFp := range s.blobfiles {
		blobfileFp.tree.idxFile.Sync()
		blobfileFp.file.Sync()
	}
}
func (s *BlobStore) CloseAll() {
	for _, blobfileFp := range s.blobfiles {
		blobfileFp.tree.idxFile.Close()
		blobfileFp.file.Close()
	}
}

func (s *BlobStore) PutAvailBlobFile(blobfileId int) {
	s.availBlobFileCh <- blobfileId
}

func (s *BlobStore) GetUnAvailBlobFile() (blobfileId int, err error) {
	select {
	case blobfileId = <-s.unavailBlobFileCh:
	default:
		err = ErrorNoUnAvaliFile
	}

	return
}

func (s *BlobStore) PutUnAvailBlobFile(blobfileId int) {
	s.unavailBlobFileCh <- blobfileId
}

func (s *BlobStore) GetStoreBlobFileCount() (files int, err error) {
	return BlobFileFileCount, nil
}

func (s *BlobStore) MarkDelete(fileId uint32, offset, size int64) error {
	blobfileId := int(fileId)
	objectId := uint64(offset)
	c, ok := s.blobfiles[blobfileId]
	if !ok {
		return ErrorFileNotFound
	}
	c.commitLock.RLock()
	defer c.commitLock.RUnlock()
	return c.tree.delete(objectId)
}

func (s *BlobStore) GetUnAvailChanLen() (chanLen int) {
	return len(s.unavailBlobFileCh)
}

func (s *BlobStore) GetAvailChanLen() (chanLen int) {
	return len(s.availBlobFileCh)
}

func (s *BlobStore) AllocObjectId(fileId uint32) (uint64, error) {
	blobfileId := int(fileId)
	c, ok := s.blobfiles[blobfileId]
	if !ok {
		return 0, ErrorFileNotFound //0 is an invalid object id
	}
	return c.loadLastOid() + 1, nil
}

func (s *BlobStore) GetLastOid(fileId uint32) (objectId uint64, err error) {
	c, ok := s.blobfiles[int(fileId)]
	if !ok {
		return 0, ErrorFileNotFound
	}

	return c.loadLastOid(), nil
}

func (s *BlobStore) GetObject(fileId uint32, objectId uint64) (o *Object, err error) {
	c, ok := s.blobfiles[int(fileId)]
	if !ok {
		return nil, ErrorFileNotFound
	}

	o, ok = c.tree.get(objectId)
	if !ok {
		return nil, ErrorObjNotFound
	}

	return
}

func (s *BlobStore) GetDelObjects(fileId uint32) (objects []uint64) {
	objects = make([]uint64, 0)
	c, ok := s.blobfiles[int(fileId)]
	if !ok {
		return
	}

	syncLastOid := c.loadLastOid()
	c.storeSyncLastOid(syncLastOid)

	c.commitLock.RLock()
	LoopIndexFile(c.tree.idxFile, func(oid uint64, offset, size, crc uint32) error {
		if oid > syncLastOid {
			return errors.New("Exceed syncLastOid")
		}
		if size == MarkDeleteObject {
			objects = append(objects, oid)
		}
		return nil
	})
	c.commitLock.RUnlock()

	return
}

func (s *BlobStore) ApplyDelObjects(blobfileId uint32, objects []uint64) (err error) {
	c, ok := s.blobfiles[int(blobfileId)]
	if !ok {
		return ErrorFileNotFound
	}
	err = c.applyDelObjects(objects)
	return
}

// make sure blobfileID is valid
func (s *BlobStore) IsReadyToCompact(blobfileID, thresh int) (isready bool, fileBytes, deleteBytes uint64, deletePercent float64) {
	if thresh < 0 {
		thresh = CompactThreshold
	}

	c := s.blobfiles[blobfileID]
	objects := c.tree
	deletePercent = float64(objects.deleteBytes) / float64(objects.fileBytes)
	fileBytes = objects.fileBytes
	deleteBytes = objects.deleteBytes
	if objects.fileBytes < uint64(gMaxBlobFileSize)*CompactThreshold/100 {
		return false, fileBytes, deleteBytes, deletePercent
	}

	if objects.deleteBytes < objects.fileBytes*uint64(thresh)/100 {
		return false, fileBytes, deleteBytes, deletePercent
	}

	return true, fileBytes, deleteBytes, deletePercent
}

func (s *BlobStore) DoCompactWork(blobfileID int) (err error, released uint64) {
	_, ok := s.blobfiles[blobfileID]
	if !ok {
		return ErrorFileNotFound, 0
	}

	err, released = s.doCompactAndCommit(blobfileID)
	if err != nil {
		return err, 0
	}
	err = s.Sync(uint32(blobfileID))
	if err != nil {
		return err, 0
	}

	return nil, released
}

func (s *BlobStore) MoveBlobFileToUnavailChan() {
	if len(s.unavailBlobFileCh) >= 2 {
		return
	}
	for i := 0; i < 2; i++ {
		select {
		case blobfileId := <-s.availBlobFileCh:
			s.unavailBlobFileCh <- blobfileId
		default:
			return
		}
	}
}

func (s *BlobStore) doCompactAndCommit(blobfileID int) (err error, released uint64) {
	cc := s.blobfiles[blobfileID]
	// prevent write and delete operations
	if !cc.compactLock.TryLockTimed(CompactMaxWait) {
		return nil, 0
	}
	defer cc.compactLock.Unlock()

	sizeBeforeCompact := cc.tree.FileBytes()
	if err = cc.doCompact(); err != nil {
		return err, 0
	}

	cc.commitLock.Lock()
	defer cc.commitLock.Unlock()

	err = cc.doCommit()
	if err != nil {
		return err, 0
	}

	sizeAfterCompact := cc.tree.FileBytes()
	released = sizeBeforeCompact - sizeAfterCompact
	return nil, released
}

func CheckAndCreateSubdir(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func (s *BlobStore) GetBlobFileInCore(fileID uint32) (*BlobFile, error) {
	blobfileID := (int)(fileID)
	cc, ok := s.blobfiles[blobfileID]
	if !ok {
		return nil, ErrorFileNotFound
	}
	return cc, nil
}

func (s *BlobStore) Snapshot() ([]*proto.File, error) {
	fList, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return nil, err
	}
	var (
		ccID int
	)
	files := make([]*proto.File, 0)
	for _, info := range fList {
		var cc *BlobFile
		if ccID, err = strconv.Atoi(info.Name()); err != nil {
			continue
		}
		if ccID > BlobFileFileCount {
			continue
		}
		if cc, err = s.GetBlobFileInCore(uint32(ccID)); err != nil {
			continue
		}

		crc, lastOid, vcCnt := cc.getCheckSum()
		f := &proto.File{Name: info.Name(), Crc: crc, Modified: info.ModTime().Unix(), MarkDel: false, LastObjID: lastOid, NeedleCnt: vcCnt}
		files = append(files, f)
	}

	return files, nil
}
