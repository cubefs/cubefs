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
	"encoding/json"
	"fmt"
	"io"
	"net"
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
func (mp *MetaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaItem{}
	defer func() {
		if err == nil {
			mp.updateApplyID(index)
		}
	}()

	mp.inodeTree.SetApplyID(index)

	if err = msg.UnmarshalJson(command); err != nil {
		return
	}
	switch msg.Op {
	case opFSMCreateInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		if mp.config.MaxInode < ino.Inode {
			mp.config.MaxInode = ino.Inode
		}
		resp = mp.fsmCreateInode(ino)
	case opFSMUnlinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmUnlinkInode(ino)
	case opFSMUnlinkInodeBatch:
		inodes, err := InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmUnlinkInodeBatch(inodes)
	case opFSMExtentTruncate:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmExtentsTruncate(ino)
	case opFSMCreateLinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmCreateLinkInode(ino)
	case opFSMEvictInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmEvictInode(ino)
	case opFSMEvictInodeBatch:
		inodes, err := InodeBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchEvictInode(inodes)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.fsmSetAttr(req)
	case opFSMCreateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmCreateDentry(den, false)
	case opFSMDeleteDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmDeleteDentry(den)
	case opFSMDeleteDentryBatch:
		db, err := DentryBatchUnmarshal(msg.V)
		if err != nil {
			return nil, err
		}
		resp = mp.fsmBatchDeleteDentry(db)
	case opFSMUpdateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmUpdateDentry(den)
	case opFSMUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.fsmUpdatePartition(req.End)
	case opFSMExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtents(ino)
	case opFSMStoreTick:
		msg := &storeMsg{
			command:    opFSMStoreTick,
			applyIndex: index,
			snapshot:   NewSnapshot(mp),
		}
		mp.storeChan <- msg
	case opFSMInternalDeleteInode:
		err = mp.internalDelete(msg.V)
	case opFSMInternalDeleteInodeBatch:
		err = mp.internalDeleteBatch(msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	case opFSMSetXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(extend)
	case opFSMRemoveXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmRemoveXAttr(extend)
	case opFSMCreateMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmCreateMultipart(multipart)
	case opFSMRemoveMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmRemoveMultipart(multipart)
	case opFSMAppendMultipart:
		var multipart *Multipart
		multipart = MultipartFromBytes(msg.V)
		resp = mp.fsmAppendMultipart(multipart)
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(msg.V)
		if cursor > mp.config.Cursor {
			mp.config.Cursor = cursor
		}
		if cursor > mp.config.MaxInode {
			mp.config.MaxInode = cursor
		}
	}

	return
}

// ApplyMemberChange  apply changes to the raft member.
func (mp *MetaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func() {
		if err == nil {
			mp.updateApplyID(index)
		}
	}()

	req := &proto.MetaPartitionDecommissionRequest{}
	if err = json.Unmarshal(confChange.Context, req); err != nil {
		return
	}

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
func (mp *MetaPartition) Snapshot() (snap raftproto.Snapshot, err error) {
	snap, err = newMetaItemIterator(mp)
	return
}

// ApplySnapshot applies the given snapshots.
func (mp *MetaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	log.LogInfof("apply snapshot begin")

	defer func() {
		if err != nil {
			log.LogErrorf("partition:[%d] apply snapshot has err:[%s]", mp.config.PartitionId, mp.applyID, err.Error())
		}
	}()

	var (
		data          []byte
		index         int
		appIndexID    uint64
		cursor        uint64
		inodeTree     InodeTree
		dentryTree    DentryTree
		extendTree    ExtendTree
		multipartTree MultipartTree
	)

	start := time.Now()

	switch mp.config.StoreType {
	case proto.MetaTypeMemory:
		inodeTree = &InodeBTree{NewBtree()}
		dentryTree = &DentryBTree{NewBtree()}
		extendTree = &ExtendBTree{NewBtree()}
		multipartTree = &MultipartBTree{NewBtree()}
	case proto.MetaTypeRocks:
		log.LogInfof("clean rocksdb data to apply snapshot")
		if err := mp.inodeTree.Clear(); err != nil {
			return err
		}
		if err := mp.dentryTree.Clear(); err != nil {
			return err
		}
		if err := mp.extendTree.Clear(); err != nil {
			return err
		}
		if err := mp.multipartTree.Clear(); err != nil {
			return err
		}
		mp.inodeTree.SetApplyID(0)
	default:
		log.LogWarnf("unsupport store type for %v", mp.config.StoreType)
	}

	defer func() {
		if err == io.EOF {
			mp.applyID = appIndexID
			mp.inodeTree.SetApplyID(appIndexID)
			mp.inodeTree = inodeTree
			mp.dentryTree = dentryTree
			mp.extendTree = extendTree
			mp.multipartTree = multipartTree
			mp.config.Cursor = cursor
			mp.config.MaxInode = cursor
			err = nil
			// store message
			mp.storeChan <- &storeMsg{
				command:    opFSMStoreTick,
				applyIndex: mp.applyID,
				snapshot:   NewSnapshot(mp),
			}
			mp.extReset <- struct{}{}
			log.LogDebugf("ApplySnapshot: finish with EOF: partitionID(%v) applyID(%v)", mp.config.PartitionId, mp.applyID)
			return
		} else if err != nil {
			log.LogErrorf("ApplySnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
		} else {
			log.LogInfof("partition:[%d] ApplySnapshot success use time:[%v]", mp.config.PartitionId, time.Now().Sub(start))
		}

	}()
	for {
		data, err = iter.Next()
		if err != nil {
			return
		}
		if index == 0 {
			appIndexID = binary.BigEndian.Uint64(data)
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
			log.LogIfNotNil(ino.UnmarshalKey(snap.K))
			log.LogIfNotNil(ino.UnmarshalValue(snap.V))
			if cursor < ino.Inode {
				cursor = ino.Inode
			}
			log.LogIfNotNil(inodeTree.Put(ino))
			log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, ino)
		case opFSMCreateDentry:
			dentry := &Dentry{}
			if err = dentry.UnmarshalKey(snap.K); err != nil {
				log.LogErrorf("[ApplySnapshot] opFSMCreateDentry UnmarshalKey key: %v, err: %v", snap.K, err)
				return
			}
			if err = dentry.UnmarshalValue(snap.V); err != nil {
				log.LogErrorf("[ApplySnapshot] opFSMCreateDentry UnmarshalValue val: %v, err: %v", snap.V, err)
				return
			}
			log.LogIfNotNil(dentryTree.Put(dentry))
			log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			log.LogIfNotNil(extendTree.Put(extend))
			log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)", mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			var multipart = MultipartFromBytes(snap.V)
			log.LogIfNotNil(multipartTree.Put(multipart))
			log.LogDebugf("ApplySnapshot: create multipart: partitionID(%v) multipart(%v)", mp.config.PartitionId, multipart)
		case opExtentFileSnapshot:
			fileName := string(snap.K)
			fileName = path.Join(mp.config.RootDir, fileName)
			if err = ioutil.WriteFile(fileName, snap.V, 0644); err != nil {
				log.LogErrorf("ApplySnapshot: write snap extent delete file fail: partitionID(%v) err(%v)",
					mp.config.PartitionId, err)
			}
			log.LogDebugf("ApplySnapshot: write snap extent delete file: partitonID(%v) filename(%v).",
				mp.config.PartitionId, fileName)
		default:
			err = fmt.Errorf("unknown op=%d", snap.Op)
			return
		}
	}
}

// HandleFatalEvent handles the fatal errors.
func (mp *MetaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.Warning(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

// HandleLeaderChange handles the leader changes.
func (mp *MetaPartition) HandleLeaderChange(leader uint64) {
	exporter.Warning(fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader))
	if mp.config.NodeId == leader {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", serverPort), time.Second)
		if err != nil {
			log.LogErrorf(fmt.Sprintf("HandleLeaderChange serverPort not exsit ,error %v", err))
			go mp.raftPartition.TryToLeader(mp.config.PartitionId)
			return
		}
		log.LogDebugf("[metaPartition] HandleLeaderChange close conn %v, nodeId: %v, leader: %v", serverPort, mp.config.NodeId, leader)
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if mp.config.NodeId != leader {
		log.LogDebugf("[metaPartition] pid: %v HandleLeaderChange become unleader nodeId: %v, leader: %v", mp.config.PartitionId, mp.config.NodeId, leader)
		mp.storeChan <- &storeMsg{
			command: stopStoreTick,
		}
		return
	}
	mp.storeChan <- &storeMsg{
		command: startStoreTick,
	}
	log.LogDebugf("[metaPartition] pid: %v HandleLeaderChange become leader conn %v, nodeId: %v, leader: %v", mp.config.PartitionId, serverPort, mp.config.NodeId, leader)
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, err := mp.nextInodeID()
		if err != nil {
			log.LogFatalf("[HandleLeaderChange] init root inode id: %s.", err.Error())
		}
		ino := NewInode(id, proto.Mode(os.ModePerm|os.ModeDir))
		go mp.initInode(ino)
	}
}

// Put puts the given key-value pair (operation key and operation request) into the raft store.
func (mp *MetaPartition) submit(op uint32, data []byte) (resp interface{}, err error) {
	snap := NewMetaItem(0, nil, nil)
	snap.Op = op
	if data != nil {
		snap.V = data
	}
	cmd, err := snap.MarshalJson()
	if err != nil {
		return
	}

	// submit to the raft store
	resp, err = mp.raftPartition.Submit(cmd)
	return
}

func (mp *MetaPartition) updateApplyID(applyId uint64) {
	atomic.StoreUint64(&mp.applyID, applyId)
}

func (mp *MetaPartition) updatePersistedApplyID(applyId uint64) {
	atomic.StoreUint64(&mp.persistedApplyID, applyId)
}
