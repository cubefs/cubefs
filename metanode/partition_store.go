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
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	mmap "github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
)

const (
	snapshotDir       = "snapshot"
	snapshotDirTmp    = ".snapshot"
	snapshotBackup    = ".snapshot_backup"
	inodeFile         = "inode"
	dentryFile        = "dentry"
	extendFile        = "extend"
	multipartFile     = "multipart"
	applyIDFile       = "apply"
	SnapshotSign      = ".sign"
	metadataFile      = "meta"
	metadataFileTmp   = ".meta"
	inodeDeletedFile  = "inode_deleted"
	dentryDeletedFile = "dentry_deleted"
)

func (mp *metaPartition) loadMetadata() (err error) {
	metaFile := path.Join(mp.config.RootDir, metadataFile)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadMetadata]: OpenFile %s", err.Error())
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.NewErrorf("[loadMetadata]: ReadFile %s, data: %s", err.Error(),
			string(data))
		return
	}
	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.NewErrorf("[loadMetadata]: Unmarshal MetaPartitionConfig %s",
			err.Error())
		return
	}

	if mConf.checkMeta() != nil {
		return
	}
	mp.config.PartitionId = mConf.PartitionId
	mp.config.VolName = mConf.VolName
	mp.config.Start = mConf.Start
	mp.config.End = mConf.End
	mp.config.Peers = mConf.Peers
	mp.config.Learners = mConf.Learners
	mp.config.Cursor = mp.config.Start
	mp.config.StoreMode = mConf.StoreMode
	if mp.config.StoreMode < proto.StoreModeMem || mp.config.StoreMode > proto.StoreModeRocksDb {
		mp.config.StoreMode = proto.StoreModeMem
	}
	mp.config.RocksDBDir = mConf.RocksDBDir
	mp.config.VirtualMPs = mConf.VirtualMPs
	if len(mp.config.VirtualMPs) == 0 {
		//first boot, need convert to virtual mp
		mp.config.VirtualMPs = append(mp.config.VirtualMPs, VirtualMetaPartitionConf{ID: mConf.PartitionId, Start: mConf.Start, End: mConf.End})
	}
	sort.Slice(mp.config.VirtualMPs, func(i, j int) bool {
		return mp.config.VirtualMPs[i].ID < mp.config.VirtualMPs[j].ID
	})
	mp.config.Start = mp.config.VirtualMPs[0].Start
	mp.config.InodeStart = mp.config.VirtualMPs[0].Start
	if mp.config.RocksDBDir == "" {
		// new version but old config; need select one dir
		err = mp.selectRocksDBDir()
	}

	mp.config.CreationType = mConf.CreationType
	mp.config.RocksWalFileSize = mConf.RocksWalFileSize
	mp.config.RocksWalMemSize = mConf.RocksWalMemSize
	mp.config.RocksLogFileSize = mConf.RocksLogFileSize
	mp.config.RocksLogReversedTime = mConf.RocksLogReversedTime
	mp.config.RocksLogReVersedCnt = mConf.RocksLogReVersedCnt
	mp.config.RocksWalTTL = mConf.RocksWalTTL

	log.LogInfof("loadMetadata: load complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	log.LogInfof("loadMetadata: partitionID(%v) creationType(%v) RocksDBWalFileSize(%v) RocksDBWalMemSize(%v) +" +
		"RocksDBLogFileSize(%v) RocksDBReservedCount(%v) RocksDBLogReservedTime(%v) WALTTL(%v)", mp.config.PartitionId,
		mp.config.CreationType, mp.config.RocksWalFileSize, mp.config.RocksWalMemSize, mp.config.RocksLogFileSize,
		mp.config.RocksLogReVersedCnt, mp.config.RocksLogReversedTime, mp.config.RocksWalTTL)
	return
}

func (mp *metaPartition) loadInode(ctx context.Context, rootDir string) (err error) {
	var (
		numInodes uint64
		status    uint8
	)
	defer func() {
		if err == nil {
			log.LogInfof("loadInode: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numInodes)
		}
	}()
	handler, _ := mp.inodeTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, inodeFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	inoBuf := make([]byte, 4)
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadInode] ReadHeader: %s", err.Error())
			return
		}
		length := binary.BigEndian.Uint32(inoBuf)

		// next read body
		if uint32(cap(inoBuf)) >= length {
			inoBuf = inoBuf[:length]
		} else {
			inoBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			err = errors.NewErrorf("[loadInode] ReadBody: %s", err.Error())
			return
		}
		ino := NewInode(0, 0)
		if mp.marshalVersion == MetaPartitionMarshVersion2 {
			if err = ino.UnmarshalV2(ctx, inoBuf); err != nil {
				err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
				return
			}
		} else {
			if err = ino.Unmarshal(ctx, inoBuf); err != nil {
				err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
				return
			}
		}

		if status, err = mp.fsmCreateInode(handler, ino); err != nil {
			err = errors.NewErrorf("[loadInode] fsmCreateInode, inode: %v, err: %v", ino, err)
			return
		}
		if status != proto.OpOk {
			err = errors.NewErrorf("[loadInode] fsmCreateInode, inode: %v, resp code: %d", ino, status)
			return
		}

		mp.checkAndInsertFreeList(ino)
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		numInodes += 1
	}
}

func (mp *metaPartition) loadDeletedInode(ctx context.Context, rootDir string) (err error) {
	var numInodes uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadDeletedInode: load complete: partitonID(%v) volume(%v) numInodes(%v)",
				mp.config.PartitionId, mp.config.VolName, numInodes)
		}
	}()
	handler, _ := mp.inodeDeletedTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.inodeDeletedTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, inodeDeletedFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	inoBuf := make([]byte, 4)
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadDeletedInode] ReadHeader: %s", err.Error())
			return
		}
		length := binary.BigEndian.Uint32(inoBuf)

		// next read body
		if uint32(cap(inoBuf)) >= length {
			inoBuf = inoBuf[:length]
		} else {
			inoBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDeletedInode] ReadBody: %s", err.Error())
			return
		}
		dino := new(DeletedINode)
		err = dino.Unmarshal(ctx, inoBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDeletedInode] Unmarshal: %s", err.Error())
			return
		}
		var resp *fsmOpDeletedInodeResponse
		resp, err = mp.fsmCreateDeletedInode(handler, dino)
		if err != nil {
			err = errors.NewErrorf("[loadDeletedInode] fsmCreateDeletedInode, dinode: %v, err: %v", dino, err)
			return
		}
		if resp.Status != proto.OpOk {
			err = errors.NewErrorf("[loadDeletedInode] fsmCreateDeletedInode, dinode: %v, resp code: %d", dino, resp.Status)
			return
		}
		//no need push to free list, maybe mistake delete inode after add bitmap feature
		//mp.checkExpiredAndInsertFreeList(dino)
		if mp.config.Cursor < dino.Inode.Inode {
			mp.config.Cursor = dino.Inode.Inode
		}
		numInodes += 1
	}
}

func (mp *metaPartition) loadDeletedDentry(rootDir string) (err error) {
	var numDentries uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadDeletedDentry: load complete: partitonID(%v) volume(%v) numDentries(%v)",
				mp.config.PartitionId, mp.config.VolName, numDentries)
		}
	}()
	handler, _ := mp.dentryDeletedTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.dentryDeletedTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, dentryDeletedFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadDeletedDentry] OpenFile: %s", err.Error())
		return
	}

	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	dentryBuf := make([]byte, 4)
	for {
		dentryBuf = dentryBuf[:4]
		// First Read 4byte header length
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadDeletedDentry] ReadHeader: %s", err.Error())
			return
		}

		length := binary.BigEndian.Uint32(dentryBuf)

		// next read body
		if uint32(cap(dentryBuf)) >= length {
			dentryBuf = dentryBuf[:length]
		} else {
			dentryBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDeletedDentry]: ReadBody: %s", err.Error())
			return
		}
		ddentry := new(DeletedDentry)
		err = ddentry.Unmarshal(dentryBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDeletedDentry] Unmarshal: %s", err.Error())
			return
		}
		var resp *fsmOpDeletedDentryResponse
		resp, err = mp.fsmCreateDeletedDentry(handler, ddentry, true)
		if err != nil {
			err = errors.NewErrorf("[loadDeletedDentry] fsmCreateDeletedDentry, dentry: %v, err: %v", ddentry, err)
			return
		}
		if resp.Status != proto.OpOk {
			err = errors.NewErrorf("[loadDeletedDentry] fsmCreateDeletedDentry, dentry: %v, resp code: %d", ddentry, resp.Status)
			return
		}
		numDentries += 1
	}
}

// Load dentry from the dentry snapshot.
func (mp *metaPartition) loadDentry(rootDir string) (err error) {
	var numDentries uint64
	defer func() {
		if err == nil {
			log.LogInfof("loadDentry: load complete: partitonID(%v) volume(%v) numDentries(%v)",
				mp.config.PartitionId, mp.config.VolName, numDentries)
		}
	}()
	handler, _ := mp.dentryTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, dentryFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadDentry] OpenFile: %s", err.Error())
		return
	}

	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	dentryBuf := make([]byte, 4)
	for {
		dentryBuf = dentryBuf[:4]
		// First Read 4byte header length
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadDentry] ReadHeader: %s", err.Error())
			return
		}

		length := binary.BigEndian.Uint32(dentryBuf)

		// next read body
		if uint32(cap(dentryBuf)) >= length {
			dentryBuf = dentryBuf[:length]
		} else {
			dentryBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDentry]: ReadBody: %s", err.Error())
			return
		}
		dentry := &Dentry{}
		if mp.marshalVersion == MetaPartitionMarshVersion2 {
			if err = dentry.UnmarshalV2(dentryBuf); err != nil {
				err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
				return
			}
		} else {
			if err = dentry.Unmarshal(dentryBuf); err != nil {
				err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
				return
			}
		}
		var status uint8
		status, err = mp.fsmCreateDentry(handler, dentry, true)
		if err != nil {
			err = errors.NewErrorf("[loadDentry] createDentry, dentry: %v, err: %v", dentry, err)
			return
		}
		if status != proto.OpOk {
			err = errors.NewErrorf("[loadDentry] createDentry, dentry: %v, resp code: %d", dentry, status)
			return
		}
		numDentries += 1
	}
}

func (mp *metaPartition) loadExtend(rootDir string) error {
	var err error
	handler, _ := mp.extendTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.extendTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, extendFile)
	if _, err = os.Stat(filename); err != nil {
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = fp.Close()
	}()
	var mem mmap.MMap
	if mem, err = mmap.Map(fp, mmap.RDONLY, 0); err != nil {
		return err
	}
	defer func() {
		_ = mem.Unmap()
	}()
	var offset, n int
	// read number of extends
	var numExtends uint64
	numExtends, n = binary.Uvarint(mem)
	offset += n
	for i := uint64(0); i < numExtends; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		var extend *Extend
		if extend, err = NewExtendFromBytes(mem[offset : offset+int(numBytes)]); err != nil {
			return err
		}
		log.LogDebugf("loadExtend: new extend from bytes: partitionID（%v) volume(%v) inode(%v)",
			mp.config.PartitionId, mp.config.VolName, extend.inode)
		_, _ = mp.fsmSetXAttr(handler, extend)
		offset += int(numBytes)
	}
	log.LogInfof("loadExtend: load complete: partitionID(%v) volume(%v) numExtends(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, numExtends, filename)
	return nil
}

func (mp *metaPartition) loadMultipart(rootDir string) error {
	var err error
	handler, _ := mp.multipartTree.CreateBatchWriteHandle()
	defer func() {
		_ = mp.multipartTree.CommitAndReleaseBatchWriteHandle(handler, false)
	}()
	filename := path.Join(rootDir, multipartFile)
	if _, err = os.Stat(filename); err != nil {
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = fp.Close()
	}()
	var mem mmap.MMap
	if mem, err = mmap.Map(fp, mmap.RDONLY, 0); err != nil {
		return err
	}
	defer func() {
		_ = mem.Unmap()
	}()
	var offset, n int
	// read number of extends
	var numMultiparts uint64
	numMultiparts, n = binary.Uvarint(mem)
	offset += n
	for i := uint64(0); i < numMultiparts; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		var multipart *Multipart
		multipart = MultipartFromBytes(mem[offset : offset+int(numBytes)])
		log.LogDebugf("loadMultipart: create multipart from bytes: partitionID（%v) multipartID(%v)", mp.config.PartitionId, multipart.id)
		var status uint8
		status, err = mp.fsmCreateMultipart(handler, multipart)
		if err != nil {
			err = errors.NewErrorf("[loadMultipart] fsmCreateMultipart, multipart: %v, err: %v", multipart, err)
			return err
		}
		if status != proto.OpOk {
			err = errors.NewErrorf("[loadMultipart] fsmCreateMultipart, multipart: %v, resp code: %d", multipart, status)
			return err
		}
		offset += int(numBytes)
	}
	log.LogInfof("loadMultipart: load complete: partitionID(%v) numMultiparts(%v) filename(%v)",
		mp.config.PartitionId, numMultiparts, filename)
	return nil
}

func (mp *metaPartition) loadApplyID(rootDir string) (err error) {
	filename := path.Join(rootDir, applyIDFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadApplyID] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyID]: ApplyID is empty")
		return
	}
	var cursor uint64
	if strings.Contains(string(data), "|") {
		_, err = fmt.Sscanf(string(data), "%d|%d", &mp.applyID, &cursor)
	} else {
		_, err = fmt.Sscanf(string(data), "%d", &mp.applyID)
	}
	if err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}

	if cursor > atomic.LoadUint64(&mp.config.Cursor) {
		atomic.StoreUint64(&mp.config.Cursor, cursor)
	}
	log.LogInfof("loadApplyID: load complete: partitionID(%v) volume(%v) applyID(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.applyID, filename)
	return
}

func (mp *metaPartition) addVirtualMetaPartitionConf(virtualMPInfo VirtualMetaPartitionConf) (err error) {
	mp.confUpdateMutex.Lock()
	defer mp.confUpdateMutex.Unlock()
	if mp.config.virtualMetaPartitionConfExist(virtualMPInfo) {
		return
	}

	mp.config.addVirtualMetaPartitionConf(virtualMPInfo)
	defer func() {
		if err != nil {
			mp.config.delVirtualMetaPartitionConf(virtualMPInfo)
		}
	}()

	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[addVirtualMetaPartitionConf] checkMeta->%s", err.Error())
		return
	}

	if err = mp.config.persist(); err != nil {
		err = errors.NewErrorf("[addVirtualMetaPartitionConf] config persist->%s", err.Error())
		return
	}
	return
}

func (mp *metaPartition) delVirtualMetaPartitionConf(virtualMPInfo VirtualMetaPartitionConf) (err error) {
	mp.confUpdateMutex.Lock()
	defer mp.confUpdateMutex.Unlock()
	if !mp.config.virtualMetaPartitionConfExist(virtualMPInfo) {
		return
	}

	mp.config.delVirtualMetaPartitionConf(virtualMPInfo)
	defer func() {
		if err != nil {
			mp.config.addVirtualMetaPartitionConf(virtualMPInfo)
		}
	}()

	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[delVirtualMetaPartitionConf] checkMeta->%s", err.Error())
		return
	}

	if err = mp.config.persist(); err != nil {
		err = errors.NewErrorf("[delVirtualMetaPartitionConf] config persist->%s", err.Error())
		return
	}
	return
}

func (mp *metaPartition) updateEnd(id, end uint64) (err error) {
	mp.confUpdateMutex.Lock()
	defer mp.confUpdateMutex.Unlock()

	var needUpdateEndVirtualMPConf *VirtualMetaPartitionConf
	for index, virtualConf := range mp.config.VirtualMPs {
		if virtualConf.ID == id {
			needUpdateEndVirtualMPConf = &mp.config.VirtualMPs[index]
		}
	}
	if needUpdateEndVirtualMPConf == nil {
		err = fmt.Errorf("[updateEnd] not found virtual meta partition:%v", id)
		return
	}
	needUpdateEndVirtualMPOldEnd := needUpdateEndVirtualMPConf.End
	oldEnd := atomic.LoadUint64(&mp.config.End)

	atomic.StoreUint64(&mp.config.End, end)
	atomic.StoreUint64(&needUpdateEndVirtualMPConf.End, end)
	defer func() {
		if err != nil {
			atomic.StoreUint64(&mp.config.End, oldEnd)
			atomic.StoreUint64(&needUpdateEndVirtualMPConf.End, needUpdateEndVirtualMPOldEnd)
			return
		}
	}()

	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[updateEnd] checkMeta->%s", err.Error())
		return
	}

	if err = mp.config.persist(); err != nil {
		err = errors.NewErrorf("[updateEnd] config persist->%s", err.Error())
		return
	}
	return
}

func (mp *metaPartition) updateMetaConfByMetaConfSnap(newMetaConf *MetaPartitionConfig) (err error) {
	mp.confUpdateMutex.Lock()
	defer mp.confUpdateMutex.Unlock()
	if newMetaConf == nil {
		return
	}
	oldStart := atomic.LoadUint64(&mp.config.Start)
	oldEnd := atomic.LoadUint64(&mp.config.End)
	oldCursor := atomic.LoadUint64(&mp.config.Cursor)
	oldVirtualMPsConf := mp.config.VirtualMPs
	oldPeers := mp.config.Peers
	oldLearner := mp.config.Learners
	atomic.StoreUint64(&mp.config.Start, newMetaConf.Start)
	atomic.StoreUint64(&mp.config.End, newMetaConf.End)
	atomic.StoreUint64(&mp.config.Cursor, newMetaConf.Cursor)
	mp.config.VirtualMPs = newMetaConf.VirtualMPs
	mp.config.Peers = newMetaConf.Peers
	mp.config.Learners = newMetaConf.Learners
	defer func() {
		if err != nil {
			atomic.StoreUint64(&mp.config.Start, oldStart)
			atomic.StoreUint64(&mp.config.End, oldEnd)
			atomic.StoreUint64(&mp.config.Cursor, oldCursor)
			mp.config.VirtualMPs = oldVirtualMPsConf
			mp.config.Peers = oldPeers
			mp.config.Learners = oldLearner
		}
	}()
	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[updateMetaConfByMetaConfSnap] checkEnd->%s", err.Error())
		return
	}

	if err = mp.config.persist(); err != nil {
		err = errors.NewErrorf("[updateMetaConfByMetaConfSnap] config persist->%s", err.Error())
		return
	}
	return
}

func (mp *metaPartition) persistMetadata() (err error) {
	mp.confUpdateMutex.Lock()
	defer mp.confUpdateMutex.Unlock()
	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[persistMetadata] checkMeta->%s", err.Error())
		return
	}

	if err = mp.config.persist(); err != nil {
		err = errors.NewErrorf("[persistMetadata] config persist->%s", err.Error())
		return
	}
	return
	//// TODO Unhandled errors
	//os.MkdirAll(mp.config.RootDir, 0755)
	//filename := path.Join(mp.config.RootDir, metadataFileTmp)
	//fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	//if err != nil {
	//	return
	//}
	//defer func() {
	//	// TODO Unhandled errors
	//	fp.Sync()
	//	fp.Close()
	//	os.Remove(filename)
	//}()
	//
	//data, err := json.Marshal(mp.config)
	//if err != nil {
	//	return
	//}
	//if _, err = fp.Write(data); err != nil {
	//	return
	//}
	//if err = os.Rename(filename, path.Join(mp.config.RootDir, metadataFile)); err != nil {
	//	return
	//}
	//log.LogInfof("persistMetata: persist complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
	//	mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	//log.LogInfof("persistMetadata: persist complete: partitionID(%v) creationType(%v) RocksDBWalFileSize(%v) +" +
	//	"RocksDBWalMemSize(%v) RocksDBLogFileSize(%v) RocksDBReservedCount(%v) RocksDBLogReservedTime(%v) WALTTL(%v)", mp.config.PartitionId,
	//	mp.config.CreationType, mp.config.RocksWalFileSize, mp.config.RocksWalMemSize, mp.config.RocksLogFileSize,
	//	mp.config.RocksLogReVersedCnt, mp.config.RocksLogReversedTime, mp.config.RocksWalTTL)
	//return
}

func (mp *metaPartition) storeApplyID(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, applyIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d|%d", sm.applyIndex, atomic.LoadUint64(&mp.config.Cursor))); err != nil {
		return
	}
	log.LogInfof("storeApplyID: store complete: partitionID(%v) volume(%v) applyID(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.applyIndex)
	return
}

func (mp *metaPartition) storeInode(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, inodeFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()

	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	if err = sm.snap.Range(InodeType, func(item interface{}) (bool, error) {
		inode := item.(*Inode)
		var data []byte
		if data, err = inode.MarshalV2(); err != nil {
			return false, err
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, err
		}
		// set body
		if _, err = fp.Write(data); err != nil {
			return false, err
		}
		if _, err = sign.Write(data); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		log.LogErrorf("storeInode: store failed:%v", err)
		return
	}
	crc = sign.Sum32()
	log.LogInfof("storeInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(InodeType), crc)
	return
}

func (mp *metaPartition) storeDeletedInode(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, inodeDeletedFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	if err = sm.snap.Range(DelInodeType, func(item interface{}) (bool, error) {
		delInode := item.(*DeletedINode)
		var data []byte
		if data, err = delInode.Marshal(); err != nil {
			return false, err
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, err
		}
		// set body
		if _, err = fp.Write(data); err != nil {
			return false, err
		}
		if _, err = sign.Write(data); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		log.LogErrorf("storeDeletedInode: store failed:%v", err)
		return
	}
	crc = sign.Sum32()
	log.LogInfof("storeDeletedInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(DelInodeType), crc)
	return
}

func (mp *metaPartition) storeDeletedDentry(rootDir string, sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, dentryDeletedFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	if err = sm.snap.Range(DelDentryType, func(item interface{}) (bool, error) {
		delDentry := item.(*DeletedDentry)
		var data []byte
		if data, err = delDentry.Marshal(); err != nil {
			return false, err
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = fp.Write(data); err != nil {
			return false, err
		}
		if _, err = sign.Write(data); err != nil {
			return false,err
		}
		return true, nil
	}); err != nil {
		log.LogErrorf("storeDeletedDentry: store failed:%v", err)
		return
	}
	crc = sign.Sum32()
	log.LogInfof("storeDeletedDentry: store complete: partitoinID(%v) volume(%v) numDentries(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(DelDentryType), crc)
	return
}

func (mp *metaPartition) storeDentry(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, dentryFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	if err = sm.snap.Range(DentryType, func(item interface{}) (bool, error) {
		dentry := item.(*Dentry)
		var data []byte
		if data, err = dentry.MarshalV2(); err != nil {
			return false, err
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false, err
		}
		if _, err = fp.Write(data); err != nil {
			return false, err
		}
		if _, err = sign.Write(data); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		log.LogErrorf("storeDentry: store failed:%v", err)
		return
	}
	crc = sign.Sum32()
	log.LogInfof("storeDentry: store complete: partitoinID(%v) volume(%v) numDentries(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(DentryType), crc)
	return
}

func (mp *metaPartition) storeExtend(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var fp = path.Join(rootDir, extendFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	var writer = bufio.NewWriterSize(f, 4*1024*1024)
	var crc32 = crc32.NewIEEE()
	var varintTmp = make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, sm.snap.Count(ExtendType))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}
	err = sm.snap.Range(ExtendType, func(item interface{}) (bool, error) {
		extend := item.(*Extend)
		var data []byte
		if data, err = extend.Bytes(); err != nil {
			return false, err
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(data)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false, err
		}
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false, err
		}
		// write raw
		if _, err = writer.Write(data); err != nil {
			return false, err
		}
		if _, err = crc32.Write(data); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		log.LogErrorf("storeExtend: store failed:%v", err)
		return
	}

	if err = writer.Flush(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	crc = crc32.Sum32()
	log.LogInfof("storeExtend: store complete: partitoinID(%v) volume(%v) numExtends(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(ExtendType), crc)
	return
}

func (mp *metaPartition) storeMultipart(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var fp = path.Join(rootDir, multipartFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	var writer = bufio.NewWriterSize(f, 4*1024*1024)
	var crc32 = crc32.NewIEEE()
	var varintTmp = make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, sm.snap.Count(MultipartType))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}
	err = sm.snap.Range(MultipartType, func(item interface{}) (bool, error) {
		multipart := item.(*Multipart)
		var data []byte
		if data, err = multipart.Bytes(); err != nil {
			return false, err
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(data)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false, err
		}
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false, err
		}
		// write raw
		if _, err = writer.Write(data); err != nil {
			return false, err
		}
		if _, err = crc32.Write(data); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		log.LogErrorf("storeMultipart: store failed:%v", err)
		return
	}

	if err = writer.Flush(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	crc = crc32.Sum32()
	log.LogInfof("storeMultipart: store complete: partitoinID(%v) volume(%v) numMultiparts(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.snap.Count(MultipartType), crc)
	return
}
