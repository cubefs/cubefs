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

package master

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"io"
	"strconv"
)

const (
	applied = "applied"
)

type raftLeaderChangeHandler func(leader uint64)

type raftPeerChangeHandler func(confChange *proto.ConfChange) (err error)

type raftCmdApplyHandler func(cmd *RaftCmd) (err error)

type raftApplySnapshotHandler func()

// MetadataFsm represents the finite state machine of a metadata partition
type MetadataFsm struct {
	store               *raftstore.RocksDBStore
	applied             uint64
	leaderChangeHandler raftLeaderChangeHandler
	peerChangeHandler   raftPeerChangeHandler
	applyHandler        raftCmdApplyHandler
	snapshotHandler     raftApplySnapshotHandler
}

func newMetadataFsm(store *raftstore.RocksDBStore) (fsm *MetadataFsm) {
	fsm = new(MetadataFsm)
	fsm.store = store
	return
}

// Corresponding to the LeaderChange interface in Raft library.
func (mf *MetadataFsm) registerLeaderChangeHandler(handler raftLeaderChangeHandler) {
	mf.leaderChangeHandler = handler
}

// Corresponding to the PeerChange interface in Raft library.
func (mf *MetadataFsm) registerPeerChangeHandler(handler raftPeerChangeHandler) {
	mf.peerChangeHandler = handler
}

// Corresponding to the Apply interface in Raft library.
func (mf *MetadataFsm) registerApplyHandler(handler raftCmdApplyHandler) {
	mf.applyHandler = handler
}

// Corresponding to the ApplySnapshot interface in Raft library.
func (mf *MetadataFsm) registerApplySnapshotHandler(handler raftApplySnapshotHandler) {
	mf.snapshotHandler = handler
}

func (mf *MetadataFsm) restore() {
	mf.restoreApplied()
}

func (mf *MetadataFsm) restoreApplied() {

	value, err := mf.Get(applied)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied err:%v", err.Error()))
	}
	byteValues := value.([]byte)
	if len(byteValues) == 0 {
		mf.applied = 0
		return
	}
	applied, err := strconv.ParseUint(string(byteValues), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied,err:%v ", err.Error()))
	}
	mf.applied = applied
}

// Apply implements the interface of raft.StateMachine
func (mf *MetadataFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	cmd := new(RaftCmd)
	if err = cmd.Unmarshal(command); err != nil {
		log.LogErrorf("action[fsmApply],unmarshal data:%v, err:%v", command, err.Error())
		panic(err)
	}
	log.LogInfof("action[fsmApply],cmd.op[%v],cmd.K[%v],cmd.V[%v]", cmd.Op, cmd.K, string(cmd.V))
	cmdMap := make(map[string][]byte)
	cmdMap[cmd.K] = cmd.V
	cmdMap[applied] = []byte(strconv.FormatUint(uint64(index), 10))
	switch cmd.Op {
	case opSyncDeleteDataNode, opSyncDeleteMetaNode, opSyncDeleteVol, opSyncDeleteDataPartition, opSyncDeleteMetaPartition:
		if err = mf.delKeyAndPutIndex(cmd.K, cmdMap); err != nil {
			panic(err)
		}
	default:
		if err = mf.batchPut(cmdMap); err != nil {
			panic(err)
		}
	}
	mf.applied = index
	return
}

// ApplyMemberChange implements the interface of raft.StateMachine
func (mf *MetadataFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	var err error
	if mf.peerChangeHandler != nil {
		err = mf.peerChangeHandler(confChange)
	}
	return nil, err
}

// Snapshot implements the interface of raft.StateMachine
func (mf *MetadataFsm) Snapshot() (proto.Snapshot, error) {
	snapshot := mf.store.RocksDBSnapshot()
	iterator := mf.store.Iterator(snapshot)
	iterator.SeekToFirst()
	return &MetadataSnapshot{
		applied:  mf.applied,
		snapshot: snapshot,
		fsm:      mf,
		iterator: iterator,
	}, nil
}

// ApplySnapshot implements the interface of raft.StateMachine
func (mf *MetadataFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) (err error) {
	log.LogInfof(fmt.Sprintf("action[ApplySnapshot] begin,applied[%v]", mf.applied))
	var data []byte
	for err == nil {
		if data, err = iterator.Next(); err != nil {
			break
		}
		cmd := &RaftCmd{}
		if err = json.Unmarshal(data, cmd); err != nil {
			goto errHandler
		}
		if _, err = mf.store.Put(cmd.K, cmd.V, true); err != nil {
			goto errHandler
		}

		if err = mf.applyHandler(cmd); err != nil {
			goto errHandler
		}
	}
	if err != nil && err != io.EOF {
		goto errHandler
	}
	mf.snapshotHandler()
	log.LogInfof(fmt.Sprintf("action[ApplySnapshot] success,applied[%v]", mf.applied))
	return nil
errHandler:
	log.LogError(fmt.Sprintf("action[ApplySnapshot] failed,err:%v", err.Error()))
	return err
}

// HandleFatalEvent implements the interface of raft.StateMachine
func (mf *MetadataFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

// HandleLeaderChange implements the interface of raft.StateMachine
func (mf *MetadataFsm) HandleLeaderChange(leader uint64) {
	if mf.leaderChangeHandler != nil {
		go mf.leaderChangeHandler(leader)
	}
}

// Put implements the interface of raft.StateMachine
func (mf *MetadataFsm) Put(key, val interface{}) (interface{}, error) {
	return mf.store.Put(key, val, true)
}

func (mf *MetadataFsm) batchPut(cmdMap map[string][]byte) (err error) {
	return mf.store.BatchPut(cmdMap, true)
}

// Get implements the interface of raft.StateMachine
func (mf *MetadataFsm) Get(key interface{}) (interface{}, error) {
	return mf.store.Get(key)
}

// Del implements the interface of raft.StateMachine
func (mf *MetadataFsm) Del(key interface{}) (interface{}, error) {
	return mf.store.Del(key, true)
}

func (mf *MetadataFsm) delKeyAndPutIndex(key string, cmdMap map[string][]byte) (err error) {
	return mf.store.DeleteKeyAndPutIndex(key, cmdMap, true)
}
