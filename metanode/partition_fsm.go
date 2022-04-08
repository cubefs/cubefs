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
				mp.TryToLeader(mp.config.PartitionId)
			}
			log.LogErrorf("Mp[%d] action[Apply] failed,index:%v,msg:%v,resp:%v,err:%v", mp.config.PartitionId, index, msg, resp, err)
			return
		}
		if fsmError == rocksdbError {
			exporter.WarningRocksdbError(fmt.Sprintf("action[Apply] clusterID[%s] volumeName[%s] partitionID[%v]" +
				" apply failed witch rocksdb error[msg:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, msg))
			mp.TryToLeader(mp.config.PartitionId)
			err = fsmError
			return
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
		resp, fsmError = mp.fsmUpdatePartition(req.End)
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
		req := &proto.CursorResetRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			log.LogInfof("mp[%v] reset cursor, json unmarshal failed:%s", mp.config.PartitionId, err.Error())
			return mp.config.Cursor, err
		}
		resp, fsmError = mp.internalCursorReset(req)
	case opFSMInternalDeleteInodeBatch:
		if len(msg.V) == 0 {
			return
		}
		var inodes InodeBatch
		inodes, err = InodeBatchUnmarshal(ctx, msg.V)
		if err != nil {
			log.LogErrorf("mp[%v] inode batch unmarshal failed:%s", mp.config.PartitionId, err.Error())
			return
		}
		fsmError = mp.internalDeleteBatch(inodes)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmSetXAttr(extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, fsmError = mp.fsmRemoveXAttr(extend)
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
		fsmError = mp.internalClean(msg.V)
	case opFSMExtentDelSync:
		mp.fsmSyncDelExtents(msg.V)
	}

	return
}

// ApplyMemberChange  apply changes to the raft member.
func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func() {
		if err == nil {
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

func (mp *metaPartition) GetNodeVersion(nodeID uint64) (metaNodeVersion *MetaNodeVersion, err error) {
	var conn *net.TCPConn
	var recoverPeer *proto.Peer

	metaNodeVersion = &MetaNodeVersion{}
	recoverPeer = nil
	err = nil

	//get peer addr info
	for index, peer := range mp.config.Peers {
		if peer.ID != nodeID {
			continue
		}
		recoverPeer = &mp.config.Peers[index]
		break
	}

	if recoverPeer == nil {
		return nil, fmt.Errorf("can not find node[%v]", nodeID)
	}

	if conn, err = mp.config.ConnPool.GetConnect(recoverPeer.Addr); err != nil {
		err = errors.Trace(err, "get connection failed")
		log.LogErrorf("get connection from %v failed", recoverPeer.Addr)
		return
	}
	p := NewPacketGetMetaNodeVersionInfo(context.Background())
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		err = errors.Trace(err, "write packet to connection failed")
		log.LogErrorf("write packet to connection failed, peer addr: %v", recoverPeer.Addr)
		return
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		log.LogErrorf("readFromConn err: %v", err)
		return
	}
	mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
	if p.ResultCode != proto.OpOk {
		log.LogErrorf("resultCode=[%v] expectRes=[%v]", p.ResultCode, proto.OpOk)
		return nil, fmt.Errorf("get metanode version info error")
	}

	if err = json.Unmarshal(p.Data, metaNodeVersion); err != nil {
		log.LogErrorf("json.Unmarshal err: %v", err)
		return
	}

	return metaNodeVersion, nil
}

// Snapshot returns the snapshot of the current meta partition.
func (mp *metaPartition) Snapshot(recoverNode uint64) (snap raftproto.Snapshot, err error) {
	var version, batchSnapVersion *MetaNodeVersion
	version, err = mp.GetNodeVersion(recoverNode)
	if err != nil {
		log.LogInfof("snapshot: use MetaItemIterator to send an item, get node[%v] version:%v, err:%v", recoverNode, version, err)
		snap, err = newMetaItemIterator(mp)
		return
	}

	batchSnapVersion = NewMetaNodeVersion(RocksDBVersion)
	if version.LessThan(batchSnapVersion) {
		log.LogInfof("node version[%v] do not support batch snap[required:%v],", version, batchSnapVersion)
		snap, err = newMetaItemIterator(mp)
		return
	}

	log.LogInfof("node version[%v] support batch snap: use newMetaItemIteratorV2 to batch send items.", version)
	snap, err = newMetaItemIteratorV2(mp, version)
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
		deletedInodeBatchCh    = make(chan DeletedINodeBatch, 128)
		deletedDentryBatchCh   = make(chan DeletedDentryBatch, 128)
		delExtentsBatchCh      = make(chan DelExtentBatch, 128)
		snapshotSign           = crc32.NewIEEE()
		snapshotCrcStoreSuffix = "snapshotCrc"
		leaderCrc              uint32
		db                     *RocksDbInfo
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
		close(inodeBatchCh)
		close(dentryBatchCh)
		close(multipartBatchCh)
		close(extendBatchCh)
		close(deletedInodeBatchCh)
		close(deletedDentryBatchCh)
		close(delExtentsBatchCh)
		wg.Wait()
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

	wg.Add(7)
	go func() {
		defer wg.Done()
		for inodes := range inodeBatchCh {
			for _, ino := range inodes {
				if cursor < ino.Inode {
					cursor = ino.Inode
				}
				if e := metaTree.InodeTree.Create(ino, true); e != nil {
					log.LogErrorf("ApplySnapshot: create inode failed, partitionID(%v) inode(%v) error(%v)",
						mp.config.PartitionId, ino, e)
					err = e
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
				if e := metaTree.DentryTree.Create(dentry, true); e != nil {
					log.LogErrorf("ApplySnapshot: create dentry failed, partitionID(%v) dentry(%v) error(%v)", mp.config.PartitionId, dentry, e)
					err = e
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
				if e := metaTree.MultipartTree.Create(multipart, true); e != nil {
					log.LogErrorf("ApplySnapshot: create multipart failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, multipart, e)
					err = e
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
				if e := metaTree.ExtendTree.Create(extend, true); e != nil {
					log.LogErrorf("ApplySnapshot: create extentd attributes failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, extend, e)
					err = e
					return
				}
				log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
					mp.config.PartitionId, extend)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for eks := range delExtentsBatchCh {
			for _, ekInfo := range eks {
				db.Put(ekInfo.key, ekInfo.data)
				log.LogDebugf("ApplySnapshot: put del extetns info: partitionID(%v) extent(%v)",
					mp.config.PartitionId, ekInfo.key)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for delInodes := range deletedInodeBatchCh {
			for _, delInode := range delInodes {
				if e := metaTree.DeletedInodeTree.Create(delInode, true); e != nil {
					log.LogErrorf("ApplySnapshot: create deleted inode failed, partitionID(%v) delInode(%v) error(%v)", mp.config.PartitionId, delInode, e)
					err = e
					return
				}
				log.LogDebugf("ApplySnapshot: create deleted inode: partitionID(%v) delInode(%v)", mp.config.PartitionId, delInode)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for delDentries := range deletedDentryBatchCh {
			for _, delDentry := range delDentries {
				if e := metaTree.DeletedDentryTree.Create(delDentry, true); e != nil {
					log.LogErrorf("ApplySnapshot: create deleted dentry failed, partitionID(%v) delDentry(%v) error(%v)", mp.config.PartitionId, delDentry, e)
					err = e
					return
				}
				log.LogDebugf("ApplySnapshot: create deleted dentry: partitionID(%v) delDentry(%v)", mp.config.PartitionId, delDentry)
			}
		}
	}()

	ctx := context.Background()
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
			log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)", mp.config.PartitionId, extend)
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
			log.LogInfof("batch info[inodeCnt:%v dentryCnt:%v multipartCnt:%v extendCnt:%v delInodeCnt:%v delDentryCnt:%v]",
				len(mulItems.InodeBatches), len(mulItems.DentryBatches), len(mulItems.MultipartBatches), len(mulItems.ExtendBatches),
				len(mulItems.DeletedInodeBatches), len(mulItems.DeletedDentryBatches))
			inodeBatchCh <- mulItems.InodeBatches
			dentryBatchCh <- mulItems.DentryBatches
			multipartBatchCh <- mulItems.MultipartBatches
			extendBatchCh <- mulItems.ExtendBatches
			deletedInodeBatchCh <- mulItems.DeletedInodeBatches
			deletedDentryBatchCh <- mulItems.DeletedDentryBatches
			delExtentsBatchCh <- mulItems.DelExtents
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
	//reset last submit
	atomic.StoreInt64(&mp.lastSubmit, 0)
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

	// only record the first time
	atomic.CompareAndSwapInt64(&mp.lastSubmit, 0, time.Now().Unix())

	cmd, err := snap.MarshalJson()
	if err != nil {
		atomic.StoreInt64(&mp.lastSubmit, 0)
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.SubmitWithCtx(ctx, cmd)

	atomic.StoreInt64(&mp.lastSubmit, 0)
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
		if math.Abs(float64(mp.applyID - mp.inodeTree.GetApplyID())) > maximumApplyIdDifference {
			//persist to rocksdb
			if err := mp.inodeTree.PersistBaseInfo(); err != nil {
				log.LogErrorf("action[uploadApplyID] persist base info failed:%v", err)
			}
		}
	}
}
