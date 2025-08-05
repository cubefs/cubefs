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

package master

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	raftstore "github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	applied = "applied"
)

type raftLeaderChangeHandler func(leader uint64)

type raftPeerChangeHandler func(confChange *proto.ConfChange) (err error)

type raftUserCmdApplyHandler func(opt uint32, key string, cmdMap map[string][]byte) (err error)

type raftApplySnapshotHandler func()

// MetadataFsm represents the finite state machine of a metadata partition
type MetadataFsm struct {
	store               *raftstore.RocksDBStore
	rs                  *raft.RaftServer
	applied             uint64
	retainLogs          uint64
	leaderChangeHandler raftLeaderChangeHandler
	peerChangeHandler   raftPeerChangeHandler
	snapshotHandler     raftApplySnapshotHandler
	UserAppCmdHandler   raftUserCmdApplyHandler
	onSnapshot          bool
	raftLk              sync.Mutex
}

func newMetadataFsm(store *raftstore.RocksDBStore, retainsLog uint64, rs *raft.RaftServer) (fsm *MetadataFsm) {
	fsm = new(MetadataFsm)
	fsm.store = store
	fsm.rs = rs
	fsm.retainLogs = retainsLog
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

// Corresponding to the ApplySnapshot interface in Raft library.
func (mf *MetadataFsm) registerApplySnapshotHandler(handler raftApplySnapshotHandler) {
	mf.snapshotHandler = handler
}

// Corresponding to the ApplyRaftCmd interface in Raft library.
func (mf *MetadataFsm) registerRaftUserCmdApplyHandler(handler raftUserCmdApplyHandler) {
	mf.UserAppCmdHandler = handler
}

func (mf *MetadataFsm) restore() {
	mf.restoreApplied()
}

func (mf *MetadataFsm) restoreApplied() {
	value, err := mf.store.Get(applied)
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
	log.LogInfof("[restoreApplied] apply index(%v)", applied)
}

// Apply implements the interface of raft.StateMachine
func (mf *MetadataFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	mf.raftLk.Lock()
	defer mf.raftLk.Unlock()

	cmd := new(RaftCmd)
	if err = cmd.Unmarshal(command); err != nil {
		log.LogErrorf("action[fsmApply],unmarshal data:%v, err:%v", command, err.Error())
		panic(err)
	}

	log.LogDebugf("action[Apply] apply index(%v), op %d", index, cmd.Op)

	if cmd.Op == opSyncUpdateDataPartition || cmd.Op == opSyncDeleteDataPartition {
		dpv := &dataPartitionValue{}
		if err := json.Unmarshal(cmd.V, dpv); err != nil {
			log.LogErrorf("action[fsmApply],unmarshal  cmd data:%v, err:%v", command, err.Error())
			panic(err)
		}
		log.LogInfof("action[fsmApply] apply index %v op %v, dp (%v), status(%v) hosts(%v) IsRecover(%v)",
			index, cmd.Op, dpv.PartitionID, dpv.DecommissionStatus, dpv.Hosts, dpv.IsRecover)
	}
	cmdMap := make(map[string][]byte)
	deleteSet := make(map[string]util.Null)
	if cmd.Op != opSyncBatchPut {
		cmdMap[cmd.K] = cmd.V
		cmdMap[applied] = []byte(strconv.FormatUint(uint64(index), 10))
	} else {
		nestedCmdMap := make(map[string]*RaftCmd)
		if err = json.Unmarshal(cmd.V, &nestedCmdMap); err != nil {
			log.LogErrorf("action[fsmApply],unmarshal nested cmd data:%v, err:%v", command, err.Error())
			panic(err)
		}
		for cmdK, cmd := range nestedCmdMap {
			switch cmd.Op {
			case opSyncDeleteDataNode, opSyncDeleteMetaNode, opSyncDeleteVol, opSyncDeleteDataPartition, opSyncDeleteMetaPartition,
				opSyncDeleteUserInfo, opSyncDeleteAKUser, opSyncDeleteVolUser, opSyncDeleteQuota, opSyncDeleteLcNode,
				opSyncDeleteLcConf, opSyncDeleteLcTask, opSyncDeleteLcResult, opSyncS3QosDelete, opSyncDeleteDecommissionDisk:
				deleteSet[cmdK] = util.Null{}
			// NOTE: opSyncPutFollowerApiLimiterInfo, opSyncPutApiLimiterInfo need special handle?
			default:
				cmdMap[cmdK] = cmd.V
			}
		}
		cmdMap[applied] = []byte(strconv.FormatUint(uint64(index), 10))
	}

	switch cmd.Op {
	case opSyncDeleteDataNode, opSyncDeleteMetaNode, opSyncDeleteVol, opSyncDeleteDataPartition, opSyncDeleteMetaPartition,
		opSyncDeleteUserInfo, opSyncDeleteAKUser, opSyncDeleteVolUser, opSyncDeleteQuota, opSyncDeleteLcNode,
		opSyncDeleteLcConf, opSyncDeleteLcTask, opSyncDeleteLcResult, opSyncS3QosDelete, opSyncDeleteDecommissionDisk,
		opSyncDeleteFlashNode, opSyncDeleteFlashGroup:
		if err = mf.delKeyAndPutIndex(cmd.K, cmdMap); err != nil {
			panic(err)
		}
	case opSyncPutFollowerApiLimiterInfo, opSyncPutApiLimiterInfo:
		mf.UserAppCmdHandler(cmd.Op, cmd.K, cmdMap)
		//if err = mf.delKeyAndPutIndex(cmd.K, cmdMap); err != nil {
		//	panic(err)api_args_parse.go
		//}
		if err = mf.store.BatchPut(cmdMap, true); err != nil {
			panic(err)
		}
	default:
		// sync put data
		if err = mf.store.BatchDeleteAndPut(deleteSet, cmdMap, true); err != nil {
			panic(err)
		}
	}
	log.LogDebugf("action[Apply],persist index[%v]", string(cmdMap[applied]))
	mf.applied = index

	if mf.applied > 0 && (mf.applied%mf.retainLogs) == 0 {
		log.LogWarnf("action[Apply],truncate raft log,retainLogs[%v],index[%v]", mf.retainLogs, mf.applied)
		mf.rs.Truncate(GroupID, mf.applied)
	}
	return
}

// ApplyMemberChange implements the interface of raft.StateMachine
func (mf *MetadataFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	mf.raftLk.Lock()
	defer mf.raftLk.Unlock()

	var err error
	if mf.peerChangeHandler != nil {
		err = mf.peerChangeHandler(confChange)
	}
	return nil, err
}

// Snapshot implements the interface of raft.StateMachine
func (mf *MetadataFsm) Snapshot() (proto.Snapshot, error) {
	mf.raftLk.Lock()
	defer mf.raftLk.Unlock()

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
	log.LogWarnf("action[ApplySnapshot] reset rocksdb before applying snapshot")
	mf.onSnapshot = true

	defer func() {
		mf.onSnapshot = false
	}()

	if log.EnableDebug() {
		func() {
			snap := mf.store.RocksDBSnapshot()
			defer mf.store.ReleaseSnapshot(snap)
			iter := mf.store.Iterator(snap)
			defer iter.Close()
			cnt := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				cnt++
			}
			log.LogDebugf("[ApplySnapshot] scan %v keys before clear", cnt)
		}()
	}

	mf.store.Clear()

	if log.EnableDebug() {
		func() {
			snap := mf.store.RocksDBSnapshot()
			defer mf.store.ReleaseSnapshot(snap)
			iter := mf.store.Iterator(snap)
			defer iter.Close()
			cnt := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				cnt++
			}
			log.LogDebugf("[ApplySnapshot] scan %v keys after clear", cnt)
		}()
	}

	log.LogWarnf(fmt.Sprintf("action[ApplySnapshot] begin,applied[%v]", mf.applied))
	var data []byte
	var appliedIndex []byte
	for err == nil {
		bgTime := stat.BeginStat()
		if data, err = iterator.Next(); err != nil {
			break
		}
		stat.EndStat("ApplySnapshot-Next", err, bgTime, 1)
		cmd := &RaftCmd{}
		if err = json.Unmarshal(data, cmd); err != nil {
			goto errHandler
		}
		bgTime = stat.BeginStat()
		if cmd.K != applied {
			if _, err = mf.store.Put(cmd.K, cmd.V, false); err != nil {
				goto errHandler
			}
		} else {
			appliedIndex = cmd.V
		}
		stat.EndStat("ApplySnapshot-Put", err, bgTime, 1)
	}
	if err != nil && err != io.EOF {
		goto errHandler
	}

	if err = mf.store.Flush(); err != nil {
		log.LogError(fmt.Sprintf("action[ApplySnapshot] Flush failed,err:%v", err.Error()))
		goto errHandler
	}

	// NOTE: we write applied index at last
	log.LogDebugf("[ApplySnapshot] find applied index(%v)", appliedIndex)
	if appliedIndex != nil {
		if _, err = mf.store.Put(applied, appliedIndex, true); err != nil {
			goto errHandler
		}
	} else {
		log.LogErrorf("[ApplySnapshot] not found applied index in snapshot")
	}

	mf.snapshotHandler()
	log.LogWarnf(fmt.Sprintf("action[ApplySnapshot] success,applied[%v]", mf.applied))
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

func (mf *MetadataFsm) delKeyAndPutIndex(key string, cmdMap map[string][]byte) (err error) {
	return mf.store.DeleteKeyAndPutIndex(key, cmdMap, true)
}

// Stop stops the RaftServer
func (mf *MetadataFsm) Stop() {
	if mf.rs != nil {
		mf.rs.Stop()
	}
}
