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
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/exporter"
	"net"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

/* The functions below implement the interfaces defined in the raft library. */

// Apply puts the data onto the disk.
func (dp *DataPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	resp, err = dp.ApplyRandomWrite(command, index)
	return
}

// ApplyMemberChange supports adding new raft member or deleting an existing raft member.
// It does not support updating an existing member at this point.
func (dp *DataPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func(index uint64) {
		if err == nil {
			dp.uploadApplyID(index)
		} else {
			// only stop myself partition
			if !IsDiskErr(err.Error()) {
				dp.stopRaft()
			}

			err = fmt.Errorf("[ApplyMemberChange] ApplyID(%v) Partition(%v) apply err(%v)]", index, dp.partitionID, err)
			exporter.Warning(err.Error())
		}
	}(index)

	// Change memory the status
	var (
		isUpdated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddDataPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		isUpdated, err = dp.addRaftNode(req, index)
		if isUpdated && err == nil {
			dp.updateReplicas(true)
			if dp.isLeader {
				dp.ExtentStore().MoveAllToBrokenTinyExtentC(storage.TinyExtentCount)
			}
		}
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveDataPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		isUpdated, err = dp.removeRaftNode(req, index)
	case raftproto.ConfUpdateNode:
		log.LogDebugf("[updateRaftNode]: not support.")
	}
	if err != nil {
		log.LogErrorf("action[ApplyMemberChange] dp(%v) type(%v) err(%v).", dp.partitionID, confChange.Type, err)
		return
	}
	if isUpdated {
		dp.DataPartitionCreateType = proto.NormalCreateDataPartition
		if err = dp.PersistMetadata(); err != nil {
			dp.checkIsDiskError(err)
			log.LogErrorf("action[ApplyMemberChange] dp(%v) PersistMetadata err(%v).", dp.partitionID, err)
			return
		}
	}
	return
}

// Snapshot persists the in-memory data (as a snapshot) to the disk.
// Note that the data in each data partition has already been saved on the disk. Therefore there is no need to take the
// snapshot in this case.
func (dp *DataPartition) Snapshot() (raftproto.Snapshot, error) {
	snapIterator := NewItemIterator(dp.raftPartition.AppliedIndex())
	log.LogInfof("SendSnapShot PartitionID(%v) Snapshot lastTruncateID(%v) currentApplyID(%v) firstCommitID(%v)",
		dp.partitionID, dp.lastTruncateID, dp.appliedID, dp.raftPartition.CommittedIndex())
	return snapIterator, nil
}

// ApplySnapshot asks the raft leader for the snapshot data to recover the contents on the local disk.
func (dp *DataPartition) ApplySnapshot(peers []raftproto.Peer, iterator raftproto.SnapIterator) (err error) {
	// Never delete the raft log which hadn't applied, so snapshot no need.
	log.LogInfof("PartitionID(%v) ApplySnapshot to (%v)", dp.partitionID, dp.raftPartition.CommittedIndex())
	return
}

// HandleFatalEvent notifies the application when panic happens.
func (dp *DataPartition) HandleFatalEvent(err *raft.FatalError) {
	log.LogFatalf("action[HandleFatalEvent] err(%v).", err)
}

// HandleLeaderChange notifies the application when the raft leader has changed.
func (dp *DataPartition) HandleLeaderChange(leader uint64) {
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("HandleLeaderChange(%v)  Raft Panic (%v)", dp.partitionID, r)
			panic(mesg)
		}
	}()
	if dp.config.NodeID == leader {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", serverPort), time.Second)
		if err != nil {
			log.LogErrorf(fmt.Sprintf("HandleLeaderChange PartitionID(%v) serverPort not exsit ,error %v", dp.partitionID, err))
			go dp.raftPartition.TryToLeader(dp.partitionID)
			return
		}
		conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
	}
	if dp.config.NodeID == leader {
		dp.isRaftLeader = true
	}
}

// Put submits the raft log to the raft store.
func (dp *DataPartition) Put(key interface{}, val interface{}) (resp interface{}, err error) {
	if dp.raftPartition == nil {
		err = fmt.Errorf("%s key=%v", RaftNotStarted, key)
		return
	}
	resp, err = dp.raftPartition.Submit(val.([]byte))
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
