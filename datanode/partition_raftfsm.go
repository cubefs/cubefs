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

package datanode

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

/* The functions below implement the interfaces defined in the raft library. */

// Apply puts the data onto the disk.
func (dp *DataPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	var extentID uint64
	msg := &RaftCmdItem{}
	defer func(index uint64) {
		if err != nil {
			key := fmt.Sprintf("%s_datapartition_apply_err", dp.clusterID)
			prefix := fmt.Sprintf("Datapartition(%v)_Extent(%v)", dp.partitionID, extentID)
			err = errors.Trace(err, prefix)
			exporter.NewAlarm(key)
			resp = proto.OpExistErr
			dp.stopRaftC <- extentID
		} else {
			dp.uploadApplyID(index)
			resp = proto.OpOk
		}
	}(index)
	if err = msg.raftCmdUnmarshal(command); err != nil {
		return
	}

	switch msg.Op {
	case opRandomWrite, opRandomSyncWrite:
		extentID, err = dp.ApplyRandomWrite(msg, index)
	default:
		err = fmt.Errorf(fmt.Sprintf("Wrong random operate %v", msg.Op))
		return
	}
	return
}

// ApplyMemberChange supports adding new raft member or deleting an existing raft member.
// It does not support updating an existing member at this point.
func (dp *DataPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func(index uint64) {
		dp.uploadApplyID(index)
	}(index)

	req := &proto.DataPartitionDecommissionRequest{}
	if err = json.Unmarshal(confChange.Context, req); err != nil {
		return
	}

	// Change memory the status
	var (
		isUpdated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		isUpdated, err = dp.addRaftNode(req, index)
	case raftproto.ConfRemoveNode:
		isUpdated, err = dp.removeRaftNode(req, index)
	case raftproto.ConfUpdateNode:
		isUpdated, err = dp.updateRaftNode(req, index)
	}
	if err != nil {
		log.LogErrorf("action[ApplyMemberChange] dp[%v] type[%v] err[%v].", dp.partitionID, confChange.Type, err)
		return
	}
	if isUpdated {
		if err = dp.PersistMetadata(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] dp[%v] PersistMetadata err[%v].", dp.partitionID, err)
			return
		}
	}
	return
}

// Snapshot persists the in-memory data (as a snapshot) to the disk.
// Note that the data in each data partition has already been saved on the disk. Therefore there is no need to take the
// snapshot in this case.
func (dp *DataPartition) Snapshot() (raftproto.Snapshot, error) {
	applyID := dp.appliedID
	snapIterator := NewItemIterator(applyID)
	return snapIterator, nil
}

// ApplySnapshot asks the raft leader for the snapshot data to recover the contents on the local disk.
func (dp *DataPartition) ApplySnapshot(peers []raftproto.Peer, iterator raftproto.SnapIterator) (err error) {
	// Never delete the raft log which hadn't applied, so snapshot no need.
	return
}

// HandleFatalEvent notifies the application when panic happens.
func (dp *DataPartition) HandleFatalEvent(err *raft.FatalError) {
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
}

// HandleLeaderChange notifies the application when the raft leader has changed.
func (dp *DataPartition) HandleLeaderChange(leader uint64) {
	//exporter.Alarm(ModuleName, fmt.Sprintf("LeaderChange: partition=%d, "+
	//	"newLeader=%d", dp.config.PartitionID, leader))
	exporter.NewAlarm(ModuleName)

	if dp.config.NodeID == leader {
		dp.isRaftLeader = true
	}
}

// Put submits the raft log to the raft store.
func (dp *DataPartition) Put(key, val interface{}) (resp interface{}, err error) {
	if dp.raftPartition == nil {
		err = fmt.Errorf("%s key=%v", RaftNotStarted, key)
		return
	}
	item := &RaftCmdItem{
		Op: key.(uint32),
		K:  nil,
		V:  nil,
	}
	if val != nil {
		item.V = val.([]byte)
	}
	cmd, err := item.raftCmdMarshalJSON()
	if err != nil {
		return
	}

	resp, err = dp.raftPartition.Submit(cmd)
	return
}

// Get returns the raft log based on the given key. It is not needed for replicating data partition.
func (dp *DataPartition) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

// Del deletes the raft log based on the given key. It is not needed for replicating data partition.
func (dp *DataPartition) Del(key interface{}) (interface{}, error) {
	return nil, nil
}

func (dp *DataPartition) uploadApplyID(applyID uint64) {
	atomic.StoreUint64(&dp.appliedID, applyID)
}
