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
	"hash/crc32"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/tracing"

	"io/ioutil"
	"os"
	"path"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

// Apply applies the given operational commands.
func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaItem{}
	var fsmError error
	defer func() {
		if err != nil {
			if err == rocksdbError {
				exporter.WarningRocksdbError(fmt.Sprintf("action[Apply] clusterID[%s] volumeName[%s] partitionID[%v]" +
					" apply failed witch rocksdb error[msg:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
					mp.config.PartitionId, msg))
			}
			log.LogErrorf("Mp[%d] action[Apply] failed,index:%v,msg:%v,resp:%v", mp.config.PartitionId, index, msg, resp)
			return
		}
		if fsmError == rocksdbError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[Apply] clusterID[%s] volumeName[%s] partitionID[%v]" +
				" apply failed witch rocksdb error[msg:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, msg))
		}
		mp.uploadApplyID(index)
	}()
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}

	var ctx = context.Background()
	if tracing.IsEnabled() {
		var tracer = tracing.NewTracer("metaPartition.Apply").SetTag("op", msg.Op)
		defer tracer.Finish()
		ctx = tracer.Context()
	}
	mp.inodeTree.SetApplyID(index)

	switch msg.Op {
	case opFSMCreateInode:
		mp.monitorData[statistics.ActionMetaCreateInode].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		resp, fsmError = mp.fsmCreateInode(ino)
	case opFSMUnlinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmUnlinkInode(ino, msg.Timestamp, msg.TrashEnable)
	case opFSMUnlinkInodeBatch:
		inodes, err := InodeBatchUnmarshal(ctx, msg.V)
		if err != nil {
			return nil, err
		}
		resp, fsmError = mp.fsmUnlinkInodeBatch(inodes, msg.Timestamp, msg.TrashEnable)
	case opFSMExtentTruncate:
		mp.monitorData[statistics.ActionMetaTruncate].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmExtentsTruncate(ino)
	case opFSMCreateLinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmCreateLinkInode(ino)
	case opFSMEvictInode:
		mp.monitorData[statistics.ActionMetaEvictInode].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmEvictInode(ino, msg.Timestamp, msg.TrashEnable)
	case opFSMEvictInodeBatch:
		inodes, err := InodeBatchUnmarshal(ctx, msg.V)
		if err != nil {
			return nil, err
		}
		resp, fsmError = mp.fsmBatchEvictInode(inodes, msg.Timestamp, msg.TrashEnable)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		resp, fsmError = mp.fsmSetAttr(req)
	case opFSMCreateDentry:
		mp.monitorData[statistics.ActionMetaCreateDentry].UpdateData(0)

		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmCreateDentry(den, false)
	case opFSMDeleteDentry:
		mp.monitorData[statistics.ActionMetaDeleteDentry].UpdateData(0)

		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmDeleteDentry(den, msg.Timestamp, msg.From, false, msg.TrashEnable)
	case opFSMDeleteDentryBatch:
		db, err := DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, fsmError = mp.fsmBatchDeleteDentry(db, msg.Timestamp, msg.From, msg.TrashEnable)
	case opFSMUpdateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmUpdateDentry(den, msg.Timestamp, msg.From, msg.TrashEnable)
	case opFSMUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmUpdatePartition(req.End)
	case opFSMExtentsAdd:
		mp.monitorData[statistics.ActionMetaExtentsAdd].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmAppendExtents(ctx, ino)
	case opFSMExtentsInsert:
		mp.monitorData[statistics.ActionMetaExtentsInsert].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmInsertExtents(ctx, ino)
	case opFSMStoreTick:
		msg := &storeMsg{
			command:    opFSMStoreTick,
			applyIndex: index,
			snap:       NewSnapshot(mp),
		}
		mp.storeChan <- msg
	case opFSMInternalDeleteInode:
		fsmError = mp.internalDelete(msg.V)
	case opFSMCursorReset:
		resp, err = mp.internalCursorReset(msg.V)
	case opFSMInternalDeleteInodeBatch:
		fsmError = mp.internalDeleteBatch(ctx, msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmSetXAttr(extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmRemoveXAttr(extend)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, fsmError = mp.fsmCreateMultipart(multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, fsmError = mp.fsmRemoveMultipart(multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, fsmError = mp.fsmAppendMultipart(multipart)
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(msg.V)
		if cursor > mp.config.Cursor {
			mp.config.Cursor = cursor
		}

	case opFSMCleanExpiredDentry:
		var batch DeletedDentryBatch
		batch, err = DeletedDentryBatchUnmarshal(msg.V)
		if err != nil {
			return
		}
		resp, fsmError = mp.fsmCleanExpiredDentry(batch)
	case opFSMCleanExpiredInode:
		var batch FSMDeletedINodeBatch
		batch, err = FSMDeletedINodeBatchUnmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, fsmError = mp.fsmCleanExpiredInode(batch)
	case opFSMRecoverDeletedInode:
		ino := new(FSMDeletedINode)
		err = ino.Unmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, fsmError = mp.fsmRecoverDeletedInode(ino)

	case opFSMBatchRecoverDeletedInode:
		var batch FSMDeletedINodeBatch
		batch, err = FSMDeletedINodeBatchUnmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, fsmError = mp.fsmBatchRecoverDeletedInode(batch)
	case opFSMBatchRecoverDeletedDentry:
		var batch DeletedDentryBatch
		batch, err = DeletedDentryBatchUnmarshal(msg.V)
		if err != nil {
			return
		}
		resp, fsmError = mp.fsmBatchRecoverDeletedDentry(batch)
	case opFSMRecoverDeletedDentry:
		ddentry := new(DeletedDentry)
		err = ddentry.Unmarshal(msg.V)
		if err != nil {
			return
		}
		resp, fsmError = mp.fsmRecoverDeletedDentry(ddentry)
	case opFSMCleanDeletedInode:
		di := NewFSMDeletedINode(0)
		err = di.Unmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, fsmError = mp.fsmCleanDeletedInode(di)
	case opFSMBatchCleanDeletedInode:
		var inos FSMDeletedINodeBatch
		inos, err = FSMDeletedINodeBatchUnmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, fsmError = mp.fsmBatchCleanDeletedInode(inos)
	case opFSMCleanDeletedDentry:
		ddentry := new(DeletedDentry)
		err = ddentry.Unmarshal(msg.V)
		if err != nil {
			return
		}
		resp, fsmError = mp.fsmCleanDeletedDentry(ddentry)
	case opFSMBatchCleanDeletedDentry:
		var batch DeletedDentryBatch
		batch, err = DeletedDentryBatchUnmarshal(msg.V)
		if err != nil {
			return
		}
		resp, fsmError = mp.fsmBatchCleanDeletedDentry(batch)
	case opFSMInternalCleanDeletedInode:
		//todo:fsmError
		err = mp.internalClean(msg.V)
	case opFSMExtentDelSync:
		mp.fsmSyncDelExtents(msg.V)
	}

	return
}

// ApplyMemberChange  apply changes to the raft member.
func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func() {
		if err == nil {
			//todo:lizhenzhen need persist apply id?
			mp.uploadApplyID(index)
		}
	}()
	// change memory status
	var (
		updated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddNode(req, index)
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confRemoveNode(req, index)
	case raftproto.ConfUpdateNode:
		//updated, err = mp.confUpdateNode(req, index)
	case raftproto.ConfAddLearner:
		req := &proto.AddMetaPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confAddLearner(req, index)
	case raftproto.ConfPromoteLearner:
		req := &proto.PromoteMetaPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mp.confPromoteLearner(req, index)
	}
	if err != nil {
		return
	}
	if updated {
		mp.config.sortPeers()
		if err = mp.persistMetadata(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] err[%v].", err)
			return
		}
	}
	return
}

// Snapshot returns the snapshot of the current meta partition.
func (mp *metaPartition) Snapshot() (snap raftproto.Snapshot, err error) {
	snap, err = newMetaItemIterator(mp)
	return
}

func (mp *metaPartition) ResetDbByNewDir(newDbDir string) (err error){
	if err = mp.db.CloseDb(); err != nil {
		log.LogErrorf("Close old db failed:%v", err.Error())
		return
	}

	if err = mp.db.ReleaseRocksDb(); err != nil {
		log.LogErrorf("release db dir failed:%v", err.Error())
		return
	}

	if err = os.Rename(newDbDir, mp.getRocksDbRootDir()); err != nil {
		log.LogErrorf("rename db dir[%s --> %s] failed:%v", newDbDir, mp.getRocksDbRootDir(), err.Error())
		return
	}

	if err = mp.db.ReOpenDb(mp.getRocksDbRootDir()); err != nil {
		log.LogErrorf("reopen db[%s] failed:%v", mp.getRocksDbRootDir(), err.Error())
		return
	}

	partitionId := strconv.FormatUint(mp.config.PartitionId, 10)
	fileInfoList, err := ioutil.ReadDir(path.Join(mp.config.RocksDBDir, partitionPrefix + partitionId))
	if err != nil {
		return nil
	}

	for _, file := range fileInfoList {
		if file.IsDir() && strings.HasPrefix(file.Name(), "db") {
			if file.Name() == "db" {
				continue
			}
			oldName := path.Join(mp.config.RocksDBDir, partitionPrefix + partitionId, file.Name())
			newName := path.Join(mp.config.RocksDBDir, partitionPrefix + partitionId,("expired_" + file.Name()))
			os.Rename(oldName, newName)
		}
	}
	return nil
}

func newRocksdbHandle(newDir string) (db *RocksDbInfo, err error) {
	if _, err = os.Stat(newDir); err == nil {
		os.RemoveAll(newDir)
	}
	os.MkdirAll(newDir, 0x755)
	err = nil

	db = NewRocksDb()
	if err = db.OpenDb(newDir); err != nil {
		log.LogErrorf("open db failed, error(%v)", err)
		return
	}
	return
}

func (mp *metaPartition) resetMetaTree(metaTree *MetaTree) (err error) {
	if mp.HasMemStore() {
		mp.inodeTree = metaTree.InodeTree
		mp.dentryTree = metaTree.DentryTree
		mp.dentryDeletedTree = metaTree.DeletedDentryTree
		mp.inodeDeletedTree = metaTree.DeletedInodeTree
		mp.extendTree = metaTree.ExtendTree
		mp.multipartTree = metaTree.MultipartTree
	}

	if mp.HasRocksDBStore() {
		if err = mp.initRocksDBTree(); err != nil {
			return
		}
	}
	return
}

// ApplySnapshot applies the given snapshots.
func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	var (
		data                   []byte
		index                  int
		appIndexID             uint64
		cursor                 uint64
		wg                     sync.WaitGroup
		inodeBatchCh           = make(chan InodeBatch, 128)
		dentryBatchCh          = make(chan DentryBatch, 128)
		multipartBatchCh       = make(chan MultipartBatch, 128)
		extendBatchCh          = make(chan ExtendBatch, 128)
		snapshotSign           = crc32.NewIEEE()
		snapshotCrcStoreSuffix = "snapshotCrc"
		leaderCrc              uint32
		db                     = NewRocksDb()
	)

	nowStr := strconv.FormatInt(time.Now().Unix(), 10)
	newDbDir := mp.getRocksDbRootDir() + "_" + nowStr
	db, err = newRocksdbHandle(newDbDir)
	if err != nil {
		log.LogErrorf("ApplySnapshot: new rocksdb handle failed, metaPartition id(%d) error(%v)", mp.config.PartitionId, err)
		return
	}

	metaTree := newMetaTree(mp.config.StoreMode, db)
	if metaTree == nil {
		log.LogErrorf("ApplySnapshot: new meta tree tree failed")
		err = errors.NewErrorf("new meta tree failed")
		return
	}

	defer func() {
		if err == ErrSnapShotEOF {
			crc := snapshotSign.Sum32()
			leaderCrcStr := strconv.FormatUint(uint64(leaderCrc), 10)
			crcStoreFile := fmt.Sprintf("%s/%s", mp.config.RootDir, snapshotCrcStoreSuffix)
			var crcBuff = bytes.NewBuffer(make([]byte, 0, 16))
			storeStr := fmt.Sprintf("%v, %v, %v", time.Now().Format("2006-01-02 15:04:05"), leaderCrcStr, crc)
			crcBuff.WriteString(storeStr)
			if err = ioutil.WriteFile(crcStoreFile, crcBuff.Bytes(), 0775); err != nil {
				log.LogWarnf("write to file failed, err is :%v", err)
			}

			if leaderCrc != 0  && leaderCrc != crc {
				log.LogWarnf("ApplySnapshot partitionID(%v) leader crc[%v] and local[%v] is different, snaps", mp.config.PartitionId, leaderCrc, crc)
			}

			if newErr := db.CloseDb(); newErr != nil {
				err = newErr
				log.LogInfof("ApplySnapshot: metaPartition(%v) recover from snap failed; Close new db failed:%s", mp.config.PartitionId, newErr.Error())
				return
			}


			if newErr := mp.ResetDbByNewDir(newDbDir); newErr != nil {
				err = newErr
				log.LogInfof("ApplySnapshot: metaPartition(%v) recover from snap failed; Reset db failed:%s", mp.config.PartitionId, newErr.Error())
				return
			}

			mp.applyID = appIndexID
			if err = mp.resetMetaTree(metaTree); err != nil {
				log.LogErrorf("ApplySnapshot: reset meta tree failed:%v", err)
				return
			}
			if cursor != 0 {
				mp.config.Cursor = cursor
			}
			err = nil
			// store message
			mp.storeChan <- &storeMsg{
				command:    opFSMStoreTick,
				applyIndex: mp.applyID,
				snap:       NewSnapshot(mp),
			}
			mp.extReset <- struct{}{}
			log.LogInfof("ApplySnapshot: finish with EOF: partitionID(%v) applyID(%v),cursor(%v)", mp.config.PartitionId, mp.applyID, mp.config.Cursor)
			return
		}
		if err == rocksdbError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[ApplySnapshot] clusterID[%s] volumeName[%s] partitionID[%v]" +
				" apply snapshot failed witch rocksdb error", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId))
		}
		db.CloseDb()
		log.LogErrorf("ApplySnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
	}()

	//todo
	wg.Add(4)
	go func() {
		defer wg.Done()
		for inodes := range inodeBatchCh {
			for _, ino := range inodes {
				if cursor < ino.Inode {
					cursor = ino.Inode
				}
				if err = metaTree.InodeTree.Create(ino, true); err != nil {
					log.LogErrorf("ApplySnapshot: create inode failed, partitionID(%v) inode(%v) error(%v)",
						mp.config.PartitionId, ino, err)
					return
				}
				log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, ino)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for dentries := range dentryBatchCh {
			for _, dentry := range dentries {
				if err = metaTree.DentryTree.Create(dentry, true); err != nil {
					log.LogErrorf("ApplySnapshot: create dentry failed, partitionID(%v) dentry(%v) error(%v)", mp.config.PartitionId, dentry, err)
					return
				}
				log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for multiparts := range multipartBatchCh {
			for _, multipart := range multiparts {
				if err = metaTree.MultipartTree.Create(multipart, true); err != nil {
					log.LogErrorf("ApplySnapshot: create multipart failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, multipart, err)
					return
				}
				log.LogDebugf("ApplySnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for extends := range extendBatchCh {
			for _, extend := range extends {
				if err = metaTree.ExtendTree.Create(extend, true); err != nil {
					log.LogErrorf("ApplySnapshot: create extentd attributes failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, extend, err)
					return
				}
				log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
					mp.config.PartitionId, extend)
			}
		}
	}()
	//todo: inode deleted tree/ dentry deleted tree

	ctx := context.Background()
	extValue := make([]byte , 1)
	for {
		data, err = iter.Next()
		if err != nil {
			if err == io.EOF{
				err = ErrSnapShotEOF
			}
			return
		}
		if index == 0 {
			appIndexID = binary.BigEndian.Uint64(data)
			//for flush ApplyID to rocksdb
			metaTree.InodeTree.SetApplyID(appIndexID)
			lenData := make([]byte, 4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = snapshotSign.Write(data); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			index++
			continue
		}
		snap := NewMetaItem(0, nil, nil)
		if err = snap.UnmarshalBinary(data); err != nil {
			return
		}
		index++
		switch snap.Op {
		case opFSMCreateInode:
			ino := NewInode(0, 0)

			// TODO Unhandled errors
			if mp.marshalVersion == MetaPartitionMarshVersion2 {
				if err = ino.UnmarshalV2WithKeyAndValue(ctx, snap.K, snap.V); err != nil {
					return
				}
			} else {
				if err = ino.UnmarshalKey(snap.K); err != nil {
					return
				}
				if err = ino.UnmarshalValue(ctx, snap.V); err != nil {
					return
				}
			}
			if cursor < ino.Inode {
				cursor = ino.Inode
			}
			if err = metaTree.InodeTree.Create(ino, true); err != nil {
				log.LogErrorf("ApplySnapshot: create inode failed, partitionID(%v) inode(%v)", mp.config.PartitionId, ino)
				return
			}
			log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, ino)

		case opFSMCreateDentry:
			dentry := &Dentry{}
			if mp.marshalVersion == MetaPartitionMarshVersion2 {
				if err = dentry.UnmarshalV2WithKeyAndValue(snap.K, snap.V); err != nil {
					return
				}
			} else {
				if err = dentry.UnmarshalKey(snap.K); err != nil {
					return
				}
				if err = dentry.UnmarshalValue(snap.V); err != nil {
					return
				}
			}
			if err = metaTree.DentryTree.Create(dentry, true); err != nil {
				log.LogErrorf("ApplySnapshot: create dentry failed, partitionID(%v) dentry(%v) error(%v)", mp.config.PartitionId, dentry, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			if err = metaTree.ExtendTree.Create(extend, true); err != nil {
				log.LogErrorf("ApplySnapshot: create extentd attributes failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, extend, err)
				return
			}
			log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
				mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			var multipart = MultipartFromBytes(snap.V)
			if err = metaTree.MultipartTree.Create(multipart, true); err != nil {
				log.LogErrorf("ApplySnapshot: create multipart failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, multipart, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
		case opExtentFileSnapshot:
			fileName := string(snap.K)
			fileName = path.Join(mp.config.RootDir, fileName)
			if err = ioutil.WriteFile(fileName, snap.V, 0644); err != nil {
				log.LogErrorf("ApplySnapshot: write snap extent delete file fail: partitionID(%v) err(%v)",
					mp.config.PartitionId, err)
			}
			log.LogDebugf("ApplySnapshot: write snap extent delete file: partitionID(%v) filename(%v).",
				mp.config.PartitionId, fileName)
			lenData := make([]byte, 4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = snapshotSign.Write(data); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
		case opFSMBatchCreate:
			var mulItems *MulItems
			mulItems, err = MulItemsUnmarshal(ctx, snap.V)
			if err != nil {
				return
			}
			inodeBatchCh <- mulItems.InodeBatches
			dentryBatchCh <- mulItems.DentryBatches
			multipartBatchCh <- mulItems.MultipartBatches
			extendBatchCh <- mulItems.ExtendBatches
			lenData := make([]byte, 4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = snapshotSign.Write(data); err != nil {
				log.LogWarnf("create CRC for snapshotCheck failed, err is :%v", err)
			}
		case opFSMSnapShotCrc:
			leaderCrc = binary.BigEndian.Uint32(snap.V)

		case opFSMCreateDeletedDentry:
			ddentry := new(DeletedDentry)
			err = ddentry.UnmarshalKey(snap.K)
			if err != nil {
				return
			}
			err = ddentry.UnmarshalValue(snap.V)
			if err != nil {
				return
			}
			err = metaTree.DeletedDentryTree.Create(ddentry, true)
			if err != nil {
				log.LogErrorf("ApplySnapshot: create deleted dentry failed: partitionID(%v) dentry(%v) error(%v)", mp.config.PartitionId, ddentry, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create deleted dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, ddentry)

		case opFSMCreateDeletedInode:
			dino := new(DeletedINode)
			err = dino.UnmarshalKey(snap.K)
			if err != nil {
				return
			}
			err = dino.UnmarshalValue(ctx, snap.V)
			if err != nil {
				return
			}
			if err = metaTree.DeletedInodeTree.Create(dino, true); err != nil {
				log.LogErrorf("ApplySnapshot: create deleted inode failed: partitionID(%v) deleted inode(%v) error(%v)", mp.config.PartitionId, dino, err)
				return
			}
			log.LogDebugf("ApplySnapshot: create deleted inode: partitionID(%v) inode(%v).", mp.config.PartitionId, dino)
		case opSnapSyncExtent:
			if err = db.Put(snap.V, extValue); err != nil {
				log.LogErrorf("Add del extent item to rocks db failed:%s", err.Error())
			}
			log.LogDebugf("ApplySnapshot: write snap delete extent: partitonID(%v) extentkey(%v).",
				mp.config.PartitionId, snap.V)
		default:
			err = fmt.Errorf("unknown op=%d", snap.Op)
			return
		}
	}
}

// HandleFatalEvent handles the fatal errors.
func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.Warning(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

// HandleLeaderChange handles the leader changes.
func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	exporter.Warning(fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader))
	if mp.config.NodeId == leader {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", serverPort), time.Second)
		if err != nil {
			log.LogErrorf(fmt.Sprintf("HandleLeaderChange serverPort not exsit ,error %v", err))
			go mp.raftPartition.TryToLeader(mp.config.PartitionId)
			return
		}
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if mp.config.NodeId != leader {
		mp.storeChan <- &storeMsg{
			command: stopStoreTick,
		}
		return
	}
	mp.storeChan <- &storeMsg{
		command: startStoreTick,
	}
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, err := mp.nextInodeID()
		if err != nil {
			log.LogFatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		go mp.initInode(ino)
	}
}

func (mp *metaPartition) submit(ctx context.Context, op uint32, from string, data []byte) (resp interface{}, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("metaPartition.submit").
		SetTag("op", op).
		SetTag("size", len(data))
	defer tracer.Finish()
	ctx = tracer.Context()

	snap := NewMetaItem(0, nil, nil)
	snap.Op = op
	if data != nil {
		snap.V = data
	}
	snap.From = from
	snap.Timestamp = time.Now().UnixNano() / 1000
	snap.TrashEnable = false

	cmd, err := snap.MarshalJson()
	if err != nil {
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.SubmitWithCtx(ctx, cmd)

	return
}

func (mp *metaPartition) submitTrash(ctx context.Context, op uint32, from string, data []byte) (resp interface{}, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("metaPartition.submitTrash").
		SetTag("op", op).
		SetTag("size", len(data))
	defer tracer.Finish()
	ctx = tracer.Context()

	snap := NewMetaItem(0, nil, nil)
	snap.Op = op
	if data != nil {
		snap.V = data
	}
	snap.From = from
	snap.Timestamp = time.Now().UnixNano() / 1000
	snap.TrashEnable = true
	cmd, err := snap.MarshalJson()
	if err != nil {
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.SubmitWithCtx(ctx, cmd)

	return
}

func (mp *metaPartition) uploadApplyID(applyId uint64) {
	atomic.StoreUint64(&mp.applyID, applyId)

	if mp.HasRocksDBStore() {
		if math.Abs(float64(mp.applyID - mp.inodeTree.GetApplyID())) > 1000 /*todo*/ {
			//persist to rocksdb
			if err := mp.inodeTree.PersistBaseInfo(); err != nil {
				log.LogErrorf("action[uploadApplyID] persist base info failed:%v", err)
			}
		}
	}
}
