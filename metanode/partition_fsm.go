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
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/errors"
	"hash/crc32"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"io/ioutil"
	"os"
	"path"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

// Apply applies the given operational commands.
func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	var (
		ctx           = context.Background()
		msg           = new(MetaItem)
		dbWriteHandle interface{}
	)
	if dbWriteHandle, err = mp.inodeTree.CreateBatchWriteHandle(); err != nil {
		log.LogErrorf("action[Apply] create write batch handle failed:%v", err)
		return
	}
	mp.waitPersistCommitCnt++

	defer func() {
		if err != nil {
			log.LogErrorf("Mp[%d] action[Apply] failed,index:%v,msg:%v,resp:%v,err:%v", mp.config.PartitionId, index, msg, resp, err)
			if err == accessDBError {
				return
			}
			exporter.WarningRocksdbError(fmt.Sprintf("action[Apply] clusterID[%s] volumeName[%s] partitionID[%v]"+
				" apply failed, [error:%v], [msg:%v]", mp.manager.metaNode.clusterId, mp.config.VolName,
				mp.config.PartitionId, err, msg))
			mp.tryToGiveUpLeader()
			return
		}
		mp.uploadApplyID(index)
	}()
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}

	mp.inodeTree.SetApplyID(index)

	//commit db
	defer func() {
		if err != nil {
			_ = mp.inodeTree.ReleaseBatchWriteHandle(dbWriteHandle)
			return
		}

		err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, true)
	}()

	switch msg.Op {
	case opFSMCreateInode:
		mp.monitorData[proto.ActionMetaCreateInode].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		mp.inodeTree.SetCursor(ino.Inode)
		resp, err = mp.fsmCreateInode(dbWriteHandle, ino)
	case opFSMUnlinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, err = mp.fsmUnlinkInode(dbWriteHandle, ino.Inode, 1, msg.Timestamp, msg.TrashEnable)
	case opFSMUnlinkInodeBatch:
		var inodes InodeBatch
		inodes, err = InodeBatchUnmarshal(ctx, msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmUnlinkInodeBatch(dbWriteHandle, inodes, msg.Timestamp, msg.TrashEnable)
	case opFSMExtentTruncate:
		mp.monitorData[proto.ActionMetaTruncate].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, err = mp.fsmExtentsTruncate(dbWriteHandle, ino)
	case opFSMCreateLinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, err = mp.fsmCreateLinkInode(dbWriteHandle, ino)
	case opFSMEvictInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, err = mp.fsmEvictInode(dbWriteHandle, ino, msg.Timestamp, msg.TrashEnable)
	case opFSMEvictInodeBatch:
		var inodes InodeBatch
		inodes, err = InodeBatchUnmarshal(ctx, msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmBatchEvictInode(dbWriteHandle, inodes, msg.Timestamp, msg.TrashEnable)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		resp, err = mp.fsmSetAttr(dbWriteHandle, req)
	case opFSMCreateDentry:
		mp.monitorData[proto.ActionMetaCreateDentry].UpdateData(0)

		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmCreateDentry(dbWriteHandle, den, false)
	case opFSMDeleteDentry:
		mp.monitorData[proto.ActionMetaDeleteDentry].UpdateData(0)

		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmDeleteDentry(dbWriteHandle, den, msg.Timestamp, msg.From, false, msg.TrashEnable)
	case opFSMDeleteDentryBatch:
		var batchDen DentryBatch
		batchDen, err = DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp, err = mp.fsmBatchDeleteDentry(dbWriteHandle, batchDen, msg.Timestamp, msg.From, msg.TrashEnable)
	case opFSMUpdateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmUpdateDentry(dbWriteHandle, den, msg.Timestamp, msg.From, msg.TrashEnable)
	case opFSMUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, _ = mp.fsmUpdatePartition(req.VirtualMPID, req.End)
	case opFSMExtentsAdd:
		mp.monitorData[proto.ActionMetaExtentsAdd].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, err = mp.fsmAppendExtents(ctx, dbWriteHandle, ino)
	case opFSMExtentsInsert:
		mp.monitorData[proto.ActionMetaExtentsInsert].UpdateData(0)

		ino := NewInode(0, 0)
		if err = ino.Unmarshal(ctx, msg.V); err != nil {
			return
		}
		resp, err = mp.fsmInsertExtents(ctx, dbWriteHandle, ino)
	case opFSMStoreTick:
		sMsg := &storeMsg{command: resetStoreTick}
		log.LogInfof("MP [%d] store tick wait:%d, water level:%d", mp.config.PartitionId, mp.waitPersistCommitCnt, GetDumpWaterLevel())
		if mp.waitPersistCommitCnt > GetDumpWaterLevel() {
			mp.waitPersistCommitCnt = 0
			sMsg.command = opFSMStoreTick
			sMsg.applyIndex = index
			sMsg.snap = NewSnapshot(mp)
			if sMsg.snap == nil {
				return
			}
		}

		select {
		case mp.storeChan <- sMsg:
		default:
		}
		mp.fsmStoreConfig()

	case opFSMInternalDeleteInode:
		err = mp.internalDelete(dbWriteHandle, msg.V)
	case opFSMCursorReset:
		req := &proto.CursorResetRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			log.LogInfof("mp[%v] reset cursor, json unmarshal failed:%s", mp.config.PartitionId, err.Error())
			return mp.config.Cursor, err
		}
		resp, err = mp.internalCursorReset(req)
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
		err = mp.internalDeleteBatch(dbWriteHandle, inodes)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmSetXAttr(dbWriteHandle, extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		resp, err = mp.fsmRemoveXAttr(dbWriteHandle, extend)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmCreateMultipart(dbWriteHandle, multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmRemoveMultipart(dbWriteHandle, multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp, err = mp.fsmAppendMultipart(dbWriteHandle, multipart)
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(msg.V)
		mp.inodeTree.SetCursor(cursor)
		if cursor <= mp.config.Cursor {
			return
		}

		log.LogDebugf("mp[%v] sync cursor(%v)", mp.config.PartitionId, cursor)
		atomic.StoreUint64(&mp.config.Cursor, cursor)
		if err = mp.inodeTree.PersistBaseInfo(); err != nil {
			log.LogErrorf("mp[%v] persist base info failed:%v", mp.config.PartitionId, err)
			return
		}

	case opFSMCleanExpiredDentry:
		var batch DeletedDentryBatch
		batch, err = DeletedDentryBatchUnmarshal(msg.V)
		if err != nil {
			return
		}
		resp, err = mp.fsmCleanExpiredDentry(dbWriteHandle, batch)
	case opFSMCleanExpiredInode:
		var batch FSMDeletedINodeBatch
		batch, err = FSMDeletedINodeBatchUnmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, err = mp.fsmCleanExpiredInode(dbWriteHandle, batch)
	case opFSMRecoverDeletedInode:
		ino := new(FSMDeletedINode)
		err = ino.Unmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, err = mp.fsmRecoverDeletedInode(dbWriteHandle, ino)
	case opFSMExtentMerge:
		var im *InodeMerge
		im, err = InodeMergeUnmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, err = mp.fsmExtentsMerge(dbWriteHandle, im)
	case opFSMBatchRecoverDeletedInode:
		var batch FSMDeletedINodeBatch
		batch, err = FSMDeletedINodeBatchUnmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, err = mp.fsmBatchRecoverDeletedInode(dbWriteHandle, batch)
	case opFSMBatchRecoverDeletedDentry:
		var batch DeletedDentryBatch
		batch, err = DeletedDentryBatchUnmarshal(msg.V)
		if err != nil {
			return
		}
		resp, err = mp.fsmBatchRecoverDeletedDentry(dbWriteHandle, batch)
	case opFSMRecoverDeletedDentry:
		ddentry := new(DeletedDentry)
		err = ddentry.Unmarshal(msg.V)
		if err != nil {
			return
		}
		resp, err = mp.fsmRecoverDeletedDentry(dbWriteHandle, ddentry)
	case opFSMCleanDeletedInode:
		di := NewFSMDeletedINode(0)
		err = di.Unmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, err = mp.fsmCleanDeletedInode(dbWriteHandle, di)
	case opFSMBatchCleanDeletedInode:
		var inos FSMDeletedINodeBatch
		inos, err = FSMDeletedINodeBatchUnmarshal(msg.V)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		resp, err = mp.fsmBatchCleanDeletedInode(dbWriteHandle, inos)
	case opFSMCleanDeletedDentry:
		ddentry := new(DeletedDentry)
		err = ddentry.Unmarshal(msg.V)
		if err != nil {
			return
		}
		resp, err = mp.fsmCleanDeletedDentry(dbWriteHandle, ddentry)
	case opFSMBatchCleanDeletedDentry:
		var batch DeletedDentryBatch
		batch, err = DeletedDentryBatchUnmarshal(msg.V)
		if err != nil {
			return
		}
		resp, err = mp.fsmBatchCleanDeletedDentry(dbWriteHandle, batch)
	case opFSMInternalCleanDeletedInode:
		err = mp.internalClean(dbWriteHandle, msg.V)
	case opFSMExtentDelSync:
		mp.fsmSyncDelExtents(msg.V)
	case opFSMExtentDelSyncV2:
		mp.fsmSyncDelExtentsV2(msg.V)
	case opFSMMetaRaftAddVirtualMP:
		req := &proto.AddVirtualMetaPartitionRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp = mp.fsmAddVirtualVirtualMetaPartitionPartition(req)
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

func (mp *metaPartition) GetRecoverNodeVersion(nodeID uint64) (metaNodeVersion *MetaNodeVersion, err error) {
	var (
		recoverPeer     *proto.Peer
		nodeVersionInfo *proto.VersionValue
		recoverNodeInfo *proto.MetaNodeInfo
	)

	//get peer addr info
	for index, peer := range mp.config.Peers {
		if peer.ID != nodeID {
			continue
		}
		recoverPeer = &mp.config.Peers[index]
		break
	}

	if recoverPeer == nil {
		err = fmt.Errorf("can not find node[%v]", nodeID)
		return
	}

	if len(strings.Split(recoverPeer.Addr, ":")) < 1 {
		err = fmt.Errorf("error recover node addr:%s", recoverPeer.Addr)
		return
	}

	//get recover node profPort
	recoverNodeInfo, err = masterClient.NodeAPI().GetMetaNode(recoverPeer.Addr)
	if err != nil {
		err = fmt.Errorf("get recover node info failed:%v", err)
		return
	}

	if recoverNodeInfo.ProfPort == "" {
		recoverNodeInfo.ProfPort = mp.manager.metaNode.profPort
	}

	metaHttpClient := meta.NewMetaHttpClient(fmt.Sprintf("%s:%s", strings.Split(recoverPeer.Addr, ":")[0], recoverNodeInfo.ProfPort), false)
	nodeVersionInfo, err = metaHttpClient.GetMetaNodeVersion()
	if err != nil {
		err = fmt.Errorf("get node version failed:%v", err)
		return
	}

	metaNodeVersion = NewMetaNodeVersion(nodeVersionInfo.Version)
	return
}

// Snapshot returns the snapshot of the current meta partition.
func (mp *metaPartition) Snapshot(recoverNode uint64) (snap raftproto.Snapshot, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("mp(%v) gen snapshot failed:%v", mp.config.PartitionId, err)
			log.LogFlush()
		}
	}()
	var (
		version *MetaNodeVersion
		snapV   SnapshotVersion
		ok      bool
	)
	version, err = mp.GetRecoverNodeVersion(recoverNode)
	if err != nil {
		log.LogErrorf("snapshot: mp[%v] get recover node version failed, so send latest snapshot, get node[%v] version:%v, err:%v",
			mp.config.PartitionId, recoverNode, version, err)
		return newBatchMetaItemIterator(mp, LatestSnapV)
	}

	if version.LessThan(NewMetaNodeVersion(RocksDBVersion)) {
		log.LogInfof("Snapshot: mp[%v] get recover node[%v] version[%s], less than rocksdb version, so send base snap",
			mp.config.PartitionId, recoverNode, version.VersionStr())
		return newMetaItemIterator(mp)
	}

	//todo: change version map
	if snapV, ok = MetaBatchSnapshotVersionMap[version.VersionStr()]; !ok {
		log.LogErrorf("Snapshot: mp[%v] get recover node[%v] version[%s] mismatch, so send latest batch snap",
			mp.config.PartitionId, recoverNode, version.VersionStr())
		return newBatchMetaItemIterator(mp, LatestSnapV)
	}
	log.LogInfof("Snapshot: mp[%v] get recover node[%v] version[%s], equal or more than rocksdb version, so send batch snap",
		mp.config.PartitionId, recoverNode, version.VersionStr())
	return newBatchMetaItemIterator(mp, snapV)
}

func (mp *metaPartition) ResetDbByNewDir(newDBDir string) (err error) {
	if err = mp.db.CloseDb(); err != nil {
		log.LogErrorf("Close old db failed:%v", err.Error())
		return
	}

	if err = mp.db.ReleaseRocksDb(); err != nil {
		log.LogErrorf("release db dir failed:%v", err.Error())
		return
	}

	if err = os.Rename(newDBDir, mp.getRocksDbRootDir()); err != nil {
		log.LogErrorf("rename db dir[%s --> %s] failed:%v", newDBDir, mp.getRocksDbRootDir(), err.Error())
		return
	}

	if err = mp.db.ReOpenDb(mp.getRocksDbRootDir(), mp.config.RocksWalFileSize, mp.config.RocksWalMemSize,
		mp.config.RocksLogFileSize, mp.config.RocksLogReversedTime, mp.config.RocksLogReVersedCnt, mp.config.RocksWalTTL); err != nil {
		log.LogErrorf("reopen db[%s] failed:%v", mp.getRocksDbRootDir(), err.Error())
		return
	}

	partitionId := strconv.FormatUint(mp.config.PartitionId, 10)
	fileInfoList, err := ioutil.ReadDir(path.Join(mp.config.RocksDBDir, partitionPrefix+partitionId))
	if err != nil {
		return nil
	}

	for _, file := range fileInfoList {
		if file.IsDir() && strings.HasPrefix(file.Name(), "db_") {
			oldName := path.Join(mp.config.RocksDBDir, partitionPrefix+partitionId, file.Name())
			//last snap shot failed, exist db_timestamp dir, remove it
			//newName := path.Join(mp.config.RocksDBDir, partitionPrefix + partitionId, "expired_" + file.Name())
			os.RemoveAll(oldName)
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
	//apply snapshot, tmp rocks db
	if err = db.OpenDb(newDir, 0, 0, 0, 0, 0, 0); err != nil {
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
func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator, v uint32) (err error) {
	snapV := SnapshotVersion(v)
	switch snapV {
	case BaseSnapshotV:
		log.LogInfof("mp[%v] apply base snapshot", mp.config.PartitionId)
		return mp.ApplyBaseSnapshot(peers, iter)
	case BatchSnapshotV1, BatchSnapshotV2:
		log.LogInfof("mp[%v] apply batch snapshot(snap version:%v)", mp.config.PartitionId, BatchSnapshotV1)
		return mp.ApplyBatchSnapshot(peers, iter, snapV)
	default:
		return fmt.Errorf("unknown snap version:%v", snapV)
	}
}

func (mp *metaPartition) ApplyBaseSnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	var (
		data          []byte
		index         int
		appIndexID    uint64
		cursor        uint64
		db            *RocksDbInfo
		dbWriteHandle interface{}
		count         int
	)

	nowStr := strconv.FormatInt(time.Now().Unix(), 10)
	newDBDir := mp.getRocksDbRootDir() + "_" + nowStr
	db, err = newRocksdbHandle(newDBDir)
	if err != nil {
		log.LogErrorf("ApplyBaseSnapshot: new rocksdb handle failed, metaPartition id(%d) error(%v)", mp.config.PartitionId, err)
		return
	}

	metaTree := newMetaTree(mp.config.StoreMode, db)
	if metaTree == nil {
		log.LogErrorf("ApplyBaseSnapshot: new meta tree for mp[%v] failed", mp.config.PartitionId)
		err = errors.NewErrorf("new meta tree failed")
		return
	}
	defer func() {
		if err != nil {
			if err == rocksDBError {
				exporter.WarningRocksdbError(fmt.Sprintf("action[ApplyBaseSnapshot] clusterID[%s] volumeName[%s] partitionID[%v]"+
					" apply base snapshot failed witch rocksdb error", mp.manager.metaNode.clusterId, mp.config.VolName,
					mp.config.PartitionId))
			}
			log.LogErrorf("ApplyBaseSnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
			_ = db.CloseDb()
			return
		}

		if err = db.CloseDb(); err != nil {
			log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) recover from snap failed; Close new db(dir:%s) failed:%s", mp.config.PartitionId, db.dir, err.Error())
			return
		}

		if err = mp.afterApplySnapshotHandle(newDBDir, appIndexID, cursor, metaTree, nil, nil); err != nil {
			log.LogErrorf("ApplyBaseSnapshot metaPartition(%v) recover from snap failed; after ApplySnapshot handle failed:%s", mp.config.PartitionId, err.Error())
			return
		}
		log.LogInfof("ApplyBaseSnapshot: finish, partitionID(%v) applyID(%v),cursor(%v)", mp.config.PartitionId, mp.applyID, mp.config.Cursor)
		return
	}()

	dbWriteHandle, err = metaTree.InodeTree.CreateBatchWriteHandle()
	if err != nil {
		log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) create batch write handle failed:%v", mp.config.PartitionId, err)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) Recover failed:%v", mp.config.PartitionId, err)
			_ = metaTree.InodeTree.ReleaseBatchWriteHandle(dbWriteHandle)
			return
		}

		if err = metaTree.InodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, true); err != nil {
			log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) commit write handle failed:%v", mp.config.PartitionId, err)
			return
		}
	}()
	ctx := context.Background()
	for {
		data, err = iter.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}
		if index == 0 {
			appIndexID = binary.BigEndian.Uint64(data)
			metaTree.InodeTree.SetApplyID(appIndexID)
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
				metaTree.InodeTree.SetCursor(cursor)
			}
			if _, _, err = metaTree.InodeTree.Create(dbWriteHandle, ino, true); err != nil {
				log.LogErrorf("ApplyBaseSnapshot: create inode failed, partitionID(%v) inode(%v)", mp.config.PartitionId, ino)
				return
			}
			log.LogDebugf("ApplyBaseSnapshot: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, ino)

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
			if _, _, err = metaTree.DentryTree.Create(dbWriteHandle, dentry, true); err != nil {
				log.LogErrorf("ApplyBaseSnapshot: create dentry failed, partitionID(%v) dentry(%v) error(%v)", mp.config.PartitionId, dentry, err)
				return
			}
			log.LogDebugf("ApplyBaseSnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			if _, _, err = metaTree.ExtendTree.Create(dbWriteHandle, extend, true); err != nil {
				log.LogErrorf("ApplyBaseSnapshot: create extentd attributes failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, extend, err)
				return
			}
			log.LogDebugf("ApplyBaseSnapshot: set extend attributes: partitionID(%v) extend(%v)", mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			var multipart = MultipartFromBytes(snap.V)
			if _, _, err = metaTree.MultipartTree.Create(dbWriteHandle, multipart, true); err != nil {
				log.LogErrorf("ApplySnapshot: create multipart failed, partitionID(%v) extend(%v) error(%v)", mp.config.PartitionId, multipart, err)
				return
			}
			log.LogDebugf("ApplyBaseSnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
		case opExtentFileSnapshot:
			fileName := string(snap.K)
			fileName = path.Join(mp.config.RootDir, fileName)
			if err = ioutil.WriteFile(fileName, snap.V, 0644); err != nil {
				log.LogErrorf("ApplySnapshot: write snap extent delete file fail: partitionID(%v) err(%v)",
					mp.config.PartitionId, err)
			}
			log.LogDebugf("ApplyBaseSnapshot: write snap extent delete file: partitionID(%v) filename(%v).",
				mp.config.PartitionId, fileName)
		default:
			err = fmt.Errorf("unknown op=%d", snap.Op)
			return
		}

		if count, err = metaTree.InodeTree.BatchWriteCount(dbWriteHandle); err != nil {
			return
		}

		if count < DefMaxWriteBatchCount {
			continue
		}

		if err = metaTree.InodeTree.CommitAndReleaseBatchWriteHandle(dbWriteHandle, true); err != nil {
			log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) commit write handle failed:%v", mp.config.PartitionId, err)
			dbWriteHandle = nil
			return
		}
		if dbWriteHandle, err = metaTree.InodeTree.CreateBatchWriteHandle(); err != nil {
			log.LogErrorf("ApplyBaseSnapshot: metaPartition(%v) create batch write handle failed:%v", mp.config.PartitionId, err)
			return
		}
	}
}

func (mp *metaPartition) ApplyBatchSnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator, batchSnapVersion SnapshotVersion) (err error) {
	var (
		data                    []byte
		index                   int
		appIndexID              uint64
		cursor                  uint64
		snapshotSign            = crc32.NewIEEE()
		snapshotCrcStoreSuffix  = "snapshotCrc"
		leaderCrc               uint32
		db                      *RocksDbInfo
		metaConf                *MetaPartitionConfig
		virtualMetaPartitions   []*VirtualMetaPartition
		needInitBitMapAllocator bool
	)

	if mp.HasMemStore() {
		needInitBitMapAllocator = true
	}
	virtualMetaPartitions = InitVirtualMetaPartitionByConf(mp.config.VirtualMPs, needInitBitMapAllocator)
	nowStr := strconv.FormatInt(time.Now().Unix(), 10)
	newDBDir := mp.getRocksDbRootDir() + "_" + nowStr
	db, err = newRocksdbHandle(newDBDir)
	if err != nil {
		log.LogErrorf("ApplyBatchSnapshot: new rocksdb handle failed, metaPartition id(%d) error(%v)", mp.config.PartitionId, err)
		return
	}

	metaTree := newMetaTree(mp.config.StoreMode, db)
	if metaTree == nil {
		log.LogErrorf("ApplyBatchSnapshot: new meta tree for mp[%v] failed", mp.config.PartitionId)
		err = errors.NewErrorf("new meta tree failed")
		return
	}

	defer func() {
		if err != nil {
			if err == rocksDBError {
				exporter.WarningRocksdbError(fmt.Sprintf("action[ApplyBatchSnapshot] clusterID[%s] volumeName[%s] partitionID[%v]"+
					" apply batch snapshot failed witch rocksdb error", mp.manager.metaNode.clusterId, mp.config.VolName,
					mp.config.PartitionId))
			}
			_ = db.CloseDb()
			log.LogErrorf("ApplyBatchSnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
			return
		}

		if err = db.CloseDb(); err != nil {
			log.LogInfof("ApplyBatchSnapshot: metaPartition(%v) recover from snap failed; Close new db(dir:%s) failed:%s", mp.config.PartitionId, db.dir, err.Error())
			return
		}

		if err = mp.afterApplySnapshotHandle(newDBDir, appIndexID, cursor, metaTree, metaConf, virtualMetaPartitions); err != nil {
			log.LogErrorf("ApplyBatchSnapshot metaPartition(%v) recover from snap failed; after ApplySnapshot handle failed:%s", mp.config.PartitionId, err.Error())
			return
		}
		log.LogInfof("ApplyBatchSnapshot: apply snapshot finish: partitionID(%v) applyID(%v) cursor(%v)", mp.config.PartitionId, mp.applyID, mp.config.Cursor)
	}()

	ctx := context.Background()
	for {
		data, err = iter.Next()
		if err != nil {
			if err == io.EOF {
				err = nil
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
		case opFSMBatchCreate:
			var mulItems *MulItems
			mulItems, err = MulItemsUnmarshal(ctx, snap.V, batchSnapVersion)
			if err != nil {
				return
			}
			log.LogInfof("batch info[inodeCnt:%v dentryCnt:%v multipartCnt:%v extendCnt:%v delInodeCnt:%v delDentryCnt:%v]",
				len(mulItems.InodeBatches), len(mulItems.DentryBatches), len(mulItems.MultipartBatches), len(mulItems.ExtendBatches),
				len(mulItems.DeletedInodeBatches), len(mulItems.DeletedDentryBatches))

			if err = mp.metaItemBatchCreate(db, metaTree, mulItems, &cursor, virtualMetaPartitions); err != nil {
				log.LogErrorf("ApplyBatchSnapshot: batch create meta item failed, partitionID(%v), error(%v)", mp.config.PartitionId, err)
				return
			}
			lenData := make([]byte, 4)
			binary.BigEndian.PutUint32(lenData, uint32(len(data)))
			if _, err = snapshotSign.Write(lenData); err != nil {
				log.LogWarnf("ApplyBatchSnapshot: create CRC for snapshotCheck failed, err is :%v", err)
			}
			if _, err = snapshotSign.Write(data); err != nil {
				log.LogWarnf("ApplyBatchSnapshot: create CRC for snapshotCheck failed, err is :%v", err)
			}
		case opFSMSnapShotCrc:
			leaderCrc = binary.BigEndian.Uint32(snap.V)
			crc := snapshotSign.Sum32()
			leaderCrcStr := strconv.FormatUint(uint64(leaderCrc), 10)
			crcStoreFile := fmt.Sprintf("%s/%s", mp.config.RootDir, snapshotCrcStoreSuffix)
			var crcBuff = bytes.NewBuffer(make([]byte, 0, 16))
			storeStr := fmt.Sprintf("%v, %v, %v", time.Now().Format("2006-01-02 15:04:05"), leaderCrcStr, crc)
			crcBuff.WriteString(storeStr)
			if err = ioutil.WriteFile(crcStoreFile, crcBuff.Bytes(), 0775); err != nil {
				log.LogWarnf("write to file failed, err is :%v", err)
			}

			if leaderCrc != 0 && leaderCrc != crc {
				log.LogWarnf("ApplyBatchSnapshot partitionID(%v) leader crc[%v] and local[%v] is different, snaps", mp.config.PartitionId, leaderCrc, crc)
				err = fmt.Errorf("crc mismatch")
			}
		case opFSMSyncMetaConf:
			metaConf = new(MetaPartitionConfig)
			if err = metaConf.unmarshalJson(snap.V); err != nil {
				log.LogErrorf("ApplyBatchSnapshot, partitionID(%v) unmarshal meta config failed:%v", mp.config.PartitionId, err)
				return
			}
			log.LogDebugf("ApplyBatchSnapshot, partitionID(%v) metaConf:%s", mp.config.PartitionId, string(snap.V))
			virtualMetaPartitions = InitVirtualMetaPartitionByConf(metaConf.VirtualMPs, needInitBitMapAllocator)
		default:
			err = fmt.Errorf("unknown op=%d", snap.Op)
			return
		}
	}
}

func (mp *metaPartition) metaItemBatchCreate(db *RocksDbInfo, metaTree *MetaTree, mulItems *MulItems, cursor *uint64,
	vMPs VirtualMetaPartitions) (err error) {
	defer func() {
		log.LogDebugf("metaItemBatchCreate: meta item batch create finished, partitionID: %v, error: %v", mp.config.PartitionId, err)
	}()

	var dbHandle interface{}
	if dbHandle, err = metaTree.InodeTree.CreateBatchWriteHandle(); err != nil {
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf("metaItemBatchCreate, create meta item failed:%v", err)
			err = metaTree.InodeTree.ReleaseBatchWriteHandle(dbHandle)
			return
		}
		err = metaTree.InodeTree.CommitAndReleaseBatchWriteHandle(dbHandle, true)
		if err != nil {
			log.LogErrorf("metaItemBatchCreate: commit batch write handle failed, partitionID(%v) error(%v)", mp.config.PartitionId, err)
			return
		}

	}()
	for _, inode := range mulItems.InodeBatches {
		if vMP := vMPs.FindVirtualMetaPartitionByInodeID(inode.Inode); vMP != nil {
			vMP.InodeIDAlloter.SetId(inode.Inode)
		}
		if _, _, err = metaTree.InodeTree.Create(dbHandle, inode, true); err != nil {
			err = fmt.Errorf("create inode failed:%v", err)
			return
		}
		if *cursor < inode.Inode {
			*cursor = inode.Inode
			metaTree.InodeTree.SetCursor(*cursor)
		}
		log.LogDebugf("metaItemBatchCreate: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, inode)
	}

	for _, dentry := range mulItems.DentryBatches {
		if _, _, err = metaTree.DentryTree.Create(dbHandle, dentry, true); err != nil {
			err = fmt.Errorf("create dentry failed:%v", err)
			return
		}
		log.LogDebugf("metaItemBatchCreate: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
	}

	for _, ext := range mulItems.ExtendBatches {
		if _, _, err = metaTree.ExtendTree.Create(dbHandle, ext, true); err != nil {
			err = fmt.Errorf("create extend failed:%v", err)
			return
		}
		log.LogDebugf("metaItemBatchCreate: set extend attributes: partitionID(%v) extend(%v)", mp.config.PartitionId, ext)
	}

	for _, mul := range mulItems.MultipartBatches {
		if _, _, err = metaTree.MultipartTree.Create(dbHandle, mul, true); err != nil {
			err = fmt.Errorf("create multipart failed:%v", err)
			return
		}
		log.LogDebugf("metaItemBatchCreate: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, mul)
	}

	for _, delInode := range mulItems.DeletedInodeBatches {
		if vMP := vMPs.FindVirtualMetaPartitionByInodeID(delInode.Inode.Inode); vMP != nil {
			vMP.InodeIDAlloter.SetId(delInode.Inode.Inode)
		}
		if _, _, err = metaTree.DeletedInodeTree.Create(dbHandle, delInode, true); err != nil {
			err = fmt.Errorf("create deleted inode failed:%v", err)
			return
		}
		log.LogDebugf("metaItemBatchCreate: create deleted inode: partitionID(%v) delInode(%v)", mp.config.PartitionId, delInode)
	}

	for _, delDentry := range mulItems.DeletedDentryBatches {
		if _, _, err = metaTree.DeletedDentryTree.Create(dbHandle, delDentry, true); err != nil {
			err = fmt.Errorf("create deleted dentry failed:%v", err)
			return
		}
		log.LogDebugf("metaItemBatchCreate: create deleted dentry: partitionID(%v) delDentry(%v)", mp.config.PartitionId, delDentry)
	}

	for _, ekInfo := range mulItems.DelExtents {
		_ = db.Put(ekInfo.key, ekInfo.data)
		log.LogDebugf("metaItemBatchCreate: put del extetns info: partitionID(%v) extent(%v)", mp.config.PartitionId, ekInfo.key)
	}
	return
}

func (mp *metaPartition) afterApplySnapshotHandle(newDBDir string, appIndexID, newCursor uint64, newMetaTree *MetaTree,
	metaConf *MetaPartitionConfig, virtualMetaPartitions []*VirtualMetaPartition) (err error) {
	if err = mp.ResetDbByNewDir(newDBDir); err != nil {
		log.LogErrorf("afterApplySnapshotHandle: metaPartition(%v) recover from snap failed; Reset db failed:%s", mp.config.PartitionId, err.Error())
		return
	}

	mp.applyID = appIndexID
	if err = mp.resetMetaTree(newMetaTree); err != nil {
		log.LogErrorf("afterApplySnapshotHandle: metaPartition(%v) recover from snap failed; reset meta tree failed:%s", mp.config.PartitionId, err.Error())
		return
	}

	if err = mp.updateMetaConfByMetaConfSnap(metaConf); err != nil {
		log.LogErrorf("afterApplySnapshotHandle: metaPartition(%v) recover from snap failed; update meta conf failed:%s", mp.config.PartitionId, err.Error())
		return
	}

	mp.updateVirtualMetaPartitions(virtualMetaPartitions)

	if newCursor > mp.config.Cursor {
		atomic.StoreUint64(&mp.config.Cursor, newCursor)
	}
	err = nil
	// store message
	sMsg := &storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID,
		snap:       NewSnapshot(mp),
	}
	if sMsg.snap != nil {
		mp.storeChan <- sMsg
	}
	mp.extReset <- struct{}{}
	return
}

// HandleFatalEvent handles the fatal errors.
func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.WarningCritical(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
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
		id, err := mp.nextInodeID(mp.config.PartitionId)
		if err != nil {
			log.LogFatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		go mp.initInode(ino)
	}
}

func (mp *metaPartition) submit(ctx context.Context, op uint32, from string, data []byte) (resp interface{}, err error) {

	item := NewMetaItem(0, nil, nil)
	item.Op = op
	if data != nil {
		item.V = data
	}
	item.From = from
	item.Timestamp = time.Now().UnixNano() / 1000
	item.TrashEnable = false

	// only record the first time
	atomic.CompareAndSwapInt64(&mp.lastSubmit, 0, time.Now().Unix())

	cmd, err := item.MarshalJson()
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

	item := NewMetaItem(0, nil, nil)
	item.Op = op
	if data != nil {
		item.V = data
	}
	item.From = from
	item.Timestamp = time.Now().UnixNano() / 1000
	item.TrashEnable = true
	cmd, err := item.MarshalJson()
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
		if math.Abs(float64(mp.applyID-mp.inodeTree.GetPersistentApplyID())) > math.Min(float64(mp.raftPartition.RaftConfig().RetainLogs), maximumApplyIdDifference) {
			//persist to rocksdb
			if err := mp.inodeTree.PersistBaseInfo(); err != nil {
				log.LogErrorf("action[uploadApplyID] persist base info failed:%v", err)
			}
		}
	}
}
