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

package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util/log"
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

type raftCmdApplyHandler func(cmd *RaftCmdData) (err error)

type raftApplySnapshotHandler func()

//MetadataFsm 元数据状态机
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

func (mf *MetadataFsm) registerLeaderChangeHandler(handler raftLeaderChangeHandler) {
	mf.leaderChangeHandler = handler
}

func (mf *MetadataFsm) registerPeerChangeHandler(handler raftPeerChangeHandler) {
	mf.peerChangeHandler = handler
}

func (mf *MetadataFsm) registerApplyHandler(handler raftCmdApplyHandler) {
	mf.applyHandler = handler
}

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

//Apply 实现raft.StateMachine接口
func (mf *MetadataFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	cmd := new(RaftCmdData)
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
	if err = mf.applyHandler(cmd); err != nil {
		panic(err)
	}
	mf.applied = index
	return
}

//ApplyMemberChange 实现raft.StateMachine接口
func (mf *MetadataFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	var err error
	if mf.peerChangeHandler != nil {
		err = mf.peerChangeHandler(confChange)
	}
	return nil, err
}

//Snapshot 实现raft.StateMachine接口
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

//ApplySnapshot 实现raft.StateMachine接口
func (mf *MetadataFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) (err error) {
	log.LogInfof(fmt.Sprintf("action[ApplySnapshot] begin,applied[%v]", mf.applied))
	var data []byte
	for err == nil {
		if data, err = iterator.Next(); err != nil {
			break
		}
		cmd := &RaftCmdData{}
		if err = json.Unmarshal(data, cmd); err != nil {
			goto errDeal
		}
		if _, err = mf.store.Put(cmd.K, cmd.V); err != nil {
			goto errDeal
		}

		if err = mf.applyHandler(cmd); err != nil {
			goto errDeal
		}
	}
	if err != nil && err != io.EOF {
		goto errDeal
	}
	mf.snapshotHandler()
	log.LogInfof(fmt.Sprintf("action[ApplySnapshot] success,applied[%v]", mf.applied))
	return nil
errDeal:
	log.LogError(fmt.Sprintf("action[ApplySnapshot] failed,err:%v", err.Error()))
	return err
}

//HandleFatalEvent 实现raft.StateMachine接口
func (mf *MetadataFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

//HandleLeaderChange 实现raft.StateMachine接口
func (mf *MetadataFsm) HandleLeaderChange(leader uint64) {
	if mf.leaderChangeHandler != nil {
		go mf.leaderChangeHandler(leader)
	}
}

//Put 实现raftstore.Store接口
func (mf *MetadataFsm) Put(key, val interface{}) (interface{}, error) {
	return mf.store.Put(key, val)
}

func (mf *MetadataFsm) batchPut(cmdMap map[string][]byte) (err error) {
	return mf.store.BatchPut(cmdMap)
}

//Get 实现raftstore.Store接口
func (mf *MetadataFsm) Get(key interface{}) (interface{}, error) {
	return mf.store.Get(key)
}

//Del 实现raftstore.Store接口
func (mf *MetadataFsm) Del(key interface{}) (interface{}, error) {
	return mf.store.Del(key)
}

func (mf *MetadataFsm) delKeyAndPutIndex(key string, cmdMap map[string][]byte) (err error) {
	return mf.store.DeleteKeyAndPutIndex(key, cmdMap)
}
