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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"

	"io/ioutil"
	"os"
	"path"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Apply applies the given operational commands.
func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaItem{}
	defer func() {
		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
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
		resp = mp.fsmDeleteDentry(den, false)
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
	case opFSMExtentsAddWithCheck:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendExtentsWithCheck(ino)
	case opFSMObjExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmAppendObjExtents(ino)
	case opFSMExtentsEmpty:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmExtentsEmpty(ino)
	// case opFSMExtentsDel:
	// 	ino := NewInode(0, 0)
	// 	if err = ino.Unmarshal(msg.V); err != nil {
	// 		return
	// 	}
	// 	resp = mp.fsmDelExtents(ino)
	case opFSMClearInodeCache:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.fsmClearInodeCache(ino)
	case opFSMSentToChan:

		resp = mp.fsmSendToChan(msg.V)

	case opFSMStoreTick:
		inodeTree := mp.inodeTree.GetTree()
		dentryTree := mp.dentryTree.GetTree()
		extendTree := mp.extendTree.GetTree()
		multipartTree := mp.multipartTree.GetTree()
		msg := &storeMsg{
			command:       opFSMStoreTick,
			applyIndex:    index,
			inodeTree:     inodeTree,
			dentryTree:    dentryTree,
			extendTree:    extendTree,
			multipartTree: multipartTree,
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
	case opFSMUpdateXAttr:
		var extend *Extend
		if extend, err = NewExtendFromBytes(msg.V); err != nil {
			return
		}
		err = mp.fsmSetXAttr(extend)
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

// ApplySnapshot applies the given snapshots.
func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) (err error) {
	var (
		data          []byte
		index         int
		appIndexID    uint64
		cursor        uint64
		inodeTree     = NewBtree()
		dentryTree    = NewBtree()
		extendTree    = NewBtree()
		multipartTree = NewBtree()
	)
	defer func() {
		if err == io.EOF {
			mp.applyID = appIndexID
			mp.inodeTree = inodeTree
			mp.dentryTree = dentryTree
			mp.extendTree = extendTree
			mp.multipartTree = multipartTree
			mp.config.Cursor = cursor
			err = nil
			// store message
			mp.storeChan <- &storeMsg{
				command:       opFSMStoreTick,
				applyIndex:    mp.applyID,
				inodeTree:     mp.inodeTree,
				dentryTree:    mp.dentryTree,
				extendTree:    mp.extendTree,
				multipartTree: mp.multipartTree,
			}
			select {
			case mp.extReset <- struct{}{}:
				log.LogDebugf("ApplySnapshot: finish with EOF: partitionID(%v) applyID(%v)", mp.config.PartitionId, mp.applyID)
				return
			case <-mp.stopC:
				log.LogWarnf("ApplySnapshot: revice stop signal, exit now, partition(%d), applyId(%d)", mp.config.PartitionId, mp.applyID)
				return
			}
		}
		log.LogErrorf("ApplySnapshot: stop with error: partitionID(%v) err(%v)", mp.config.PartitionId, err)
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
			ino.UnmarshalKey(snap.K)
			ino.UnmarshalValue(snap.V)
			if cursor < ino.Inode {
				cursor = ino.Inode
			}
			inodeTree.ReplaceOrInsert(ino, true)
			log.LogDebugf("ApplySnapshot: create inode: partitonID(%v) inode(%v).", mp.config.PartitionId, ino)
		case opFSMCreateDentry:
			dentry := &Dentry{}
			if err = dentry.UnmarshalKey(snap.K); err != nil {
				return
			}
			if err = dentry.UnmarshalValue(snap.V); err != nil {
				return
			}
			dentryTree.ReplaceOrInsert(dentry, true)
			log.LogDebugf("ApplySnapshot: create dentry: partitionID(%v) dentry(%v)", mp.config.PartitionId, dentry)
		case opFSMSetXAttr:
			var extend *Extend
			if extend, err = NewExtendFromBytes(snap.V); err != nil {
				return
			}
			extendTree.ReplaceOrInsert(extend, true)
			log.LogDebugf("ApplySnapshot: set extend attributes: partitionID(%v) extend(%v)",
				mp.config.PartitionId, extend)
		case opFSMCreateMultipart:
			var multipart = MultipartFromBytes(snap.V)
			multipartTree.ReplaceOrInsert(multipart, true)
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
func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.Warning(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

// HandleLeaderChange handles the leader changes.
func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	exporter.Warning(fmt.Sprintf("metaPartition(%v) changeLeader to (%v)", mp.config.PartitionId, leader))
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
func (mp *metaPartition) submit(op uint32, data []byte) (resp interface{}, err error) {
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

func (mp *metaPartition) uploadApplyID(applyId uint64) {
	atomic.StoreUint64(&mp.applyID, applyId)
}
