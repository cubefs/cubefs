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

package metanode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	defer func() {
		mp.uploadApplyID(index)
	}()
	msg := &MetaItem{}
	if err = msg.UnmarshalJson(command); err != nil {
		return
	}
	switch msg.Op {
	case opCreateInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		resp = mp.createInode(ino)
	case opDeleteInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.deleteInode(ino)
	case opFSMExtentTruncate:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.extentsTruncate(ino)
	case opFSMCreateLinkInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.createLinkInode(ino)
	case opFSMEvictInode:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.evictInode(ino)
	case opFSMSetAttr:
		req := &SetattrRequest{}
		err = json.Unmarshal(msg.V, req)
		if err != nil {
			return
		}
		err = mp.setAttr(req)
	case opCreateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.createDentry(den)
	case opDeleteDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.deleteDentry(den)
	case opUpdateDentry:
		den := &Dentry{}
		if err = den.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.updateDentry(den)
	case opOpen:
		openReq := &OpenReq{}
		if err = json.Unmarshal(msg.V, openReq); err != nil {
			return
		}
		resp = mp.openFile(openReq)
	case opReleaseOpen:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.releaseOpen(ino)
	case opDeletePartition:
		resp = mp.deletePartition()
	case opUpdatePartition:
		req := &UpdatePartitionReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.updatePartition(req.End)
	case opExtentsAdd:
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(msg.V); err != nil {
			return
		}
		resp = mp.appendExtents(ino)
	case opStoreTick:
		msg := &storeMsg{
			command:    opStoreTick,
			applyIndex: index,
			inodeTree:  mp.getInodeTree(),
			dentryTree: mp.getDentryTree(),
		}
		mp.storeChan <- msg
	case opFSMInternalDeleteInode:
		err = mp.internalDelete(msg.V)
	case opFSMInternalDelExtentFile:
		err = mp.delOldExtentFile(msg.V)
	case opFSMInternalDelExtentCursor:
		err = mp.setExtentDeleteFileCursor(msg.V)
	}
	return
}

func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func() {
		if err == nil {
			mp.uploadApplyID(index)
		}
	}()
	req := &proto.MetaPartitionOfflineRequest{}
	if err = json.Unmarshal(confChange.Context, req); err != nil {
		return
	}
	// Change memory state
	var (
		updated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		updated, err = mp.confAddNode(req, index)
	case raftproto.ConfRemoveNode:
		updated, err = mp.confRemoveNode(req, index)
	case raftproto.ConfUpdateNode:
		updated, err = mp.confUpdateNode(req, index)
	}
	if err != nil {
		return
	}
	if updated {
		mp.config.sortPeers()
		if err = mp.storeMeta(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] err[%v].", err)
			return
		}
	}
	return
}

func (mp *metaPartition) Snapshot() (raftproto.Snapshot, error) {
	applyID := mp.applyID
	ino := mp.getInodeTree()
	dentry := mp.getDentryTree()
	finfos, err := ioutil.ReadDir(mp.config.RootDir)
	if err != nil {
		return nil, err
	}
	var fileList []string
	for _, in := range finfos {
		if in.IsDir() {
			continue
		}
		if strings.HasPrefix(in.Name(), prefixDelExtent) {
			fileList = append(fileList, in.Name())
		}
	}
	snapIter := NewMetaItemIterator(applyID, ino, dentry, mp.config.RootDir,
		fileList)
	return snapIter, nil
}

func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer,
	iter raftproto.SnapIterator) (err error) {
	var (
		data       []byte
		index      int
		appIndexID uint64
		cursor     uint64
		inodeTree  = NewBtree()
		dentryTree = NewBtree()
	)
	defer func() {
		if err == io.EOF {
			mp.applyID = appIndexID
			mp.inodeTree = inodeTree
			mp.dentryTree = dentryTree
			mp.config.Cursor = cursor
			err = nil
			// store message
			mp.storeChan <- &storeMsg{
				command:    opStoreTick,
				applyIndex: mp.applyID,
				inodeTree:  mp.inodeTree,
				dentryTree: mp.dentryTree,
			}
			mp.extReset <- struct{}{}
			log.LogDebugf("[ApplySnapshot] successful.")
			return
		}
		log.LogErrorf("[ApplySnapshot]: %s", err.Error())
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
		switch snap.Op {
		case opCreateInode:
			ino := NewInode(0, 0)
			ino.UnmarshalKey(snap.K)
			ino.UnmarshalValue(snap.V)
			if cursor < ino.Inode {
				cursor = ino.Inode
			}
			inodeTree.ReplaceOrInsert(ino, true)
			log.LogDebugf("action[ApplySnapshot] create inode[%v].", ino)
		case opCreateDentry:
			dentry := &Dentry{}
			dentry.UnmarshalKey(snap.K)
			dentry.UnmarshalValue(snap.V)
			dentryTree.ReplaceOrInsert(dentry, true)
			log.LogDebugf("action[ApplySnapshot] create dentry[%v].", dentry)
		case opSnapExtentFile:
			fileName := string(snap.K)
			fileName = path.Join(mp.config.RootDir, fileName)
			if err = ioutil.WriteFile(fileName, snap.V, 0644); err != nil {
				log.LogErrorf("action[ApplySnapshot] SnapDeleteExtent[%v].",
					err.Error())
			}
			log.LogDebugf("action[ApplySnapshot] SnapDeleteExtent[%v].", fileName)
		default:
			err = fmt.Errorf("unknown op=%d", snap.Op)
			return
		}
	}
}

func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
}

func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	exporter.Alarm(exporterKey, fmt.Sprintf("LeaderChange: partition=%d, "+
		"newLeader=%d", mp.config.PartitionId, leader))
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

func (mp *metaPartition) Put(key, val interface{}) (resp interface{}, err error) {
	snap := NewMetaItem(0, nil, nil)
	snap.Op = key.(uint32)
	if val != nil {
		snap.V = val.([]byte)
	}
	cmd, err := snap.MarshalJson()
	if err != nil {
		return
	}
	//submit raftStore
	resp, err = mp.raftPartition.Submit(cmd)
	return
}

func (mp *metaPartition) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

func (mp *metaPartition) Del(key interface{}) (interface{}, error) {
	return nil, nil
}

func (mp *metaPartition) uploadApplyID(applyId uint64) {
	atomic.StoreUint64(&mp.applyID, applyId)
}
