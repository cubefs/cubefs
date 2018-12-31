// Copyright 2018 The CFS Authors.
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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
	"strings"
)

/* The functions below implement the interfaces defined in the raft library. */

// Apply puts the data onto the disk.
func (dp *DataPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	opItem := &rndWrtOpItem{}
	defer func(index uint64) {
		if err != nil {
			key := fmt.Sprintf("%s_datapartition_apply_err", dp.clusterID)
			prefix := fmt.Sprintf("datapartition_%v_extent_%v", dp.partitionID, opItem.extentID)
			err = errors.Annotatef(err, prefix)
			exporter.Alarm(key, err.Error())
			resp = proto.OpExistErr
			dp.repairC <- opItem.extentID
		} else {
			dp.uploadApplyID(index)
			resp = proto.OpOk
		}
	}(index)
	msg := &RndWrtCmdItem{}
	if err = msg.rndWrtCmdUnmarshal(command); err != nil {
		return
	}

	switch msg.Op {
	case opRandomWrite:
		if opItem, err = rndWrtDataUnmarshal(msg.V); err != nil {
			log.LogErrorf("randomWrite_%v err[%v] unmarshal failed", dp.ID(), err)
			return
		}
		log.LogDebugf("randomWrite_%v_%v_%v_%v apply", dp.ID(), opItem.extentID, opItem.offset, opItem.size)
		for i := 0; i < maxRetryCounts; i++ {
			err = dp.ExtentStore().Write(opItem.extentID, opItem.offset, opItem.size, opItem.data, opItem.crc)
			if err != nil {
				if ignore := dp.checkWriteErrs(err.Error()); ignore {
					log.LogErrorf("randomWrite_%v_%v_%v_%v extent file had deleted. err[%v]", dp.ID(),
						opItem.extentID, opItem.offset, opItem.size, err)
					err = nil
				}
			}
			dp.addDiskErrs(err, WriteFlag)
			if err == nil {
				break
			}
			log.LogErrorf("randomWrite_%v_%v_%v_%v apply err[%v] retry[%v]", dp.ID(), opItem.extentID,
				opItem.offset, opItem.size, err, i)
		}
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

	req := &proto.DataPartitionOfflineRequest{}
	if err = json.Unmarshal(confChange.Context, req); err != nil {
		return
	}

	// Change memory status TODO what does this mean?
	var (
		updated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		updated, err = dp.addRaftNode(req, index)
	case raftproto.ConfRemoveNode:
		updated, err = dp.removeRaftNode(req, index)
	case raftproto.ConfUpdateNode:
		updated, err = dp.updateRaftNode(req, index)
	}
	if err != nil {
		log.LogErrorf("action[ApplyMemberChange] dp[%v] type[%v] err[%v].", dp.partitionID, confChange.Type, err)
		return
	}
	if updated {
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
	applyID := dp.applyID
	snapIterator := NewItemIterator(applyID)
	return snapIterator, nil
}

// ApplySnapshot asks the raft leader for the snapshot data to recover the contents on the local disk.
func (dp *DataPartition) ApplySnapshot(peers []raftproto.Peer, iterator raftproto.SnapIterator) (err error) {
	var (
		data       []byte
		appIndexID uint64
		extents    []*storage.ExtentInfo
		targetAddr string
		firstHost  string
	)
	defer func() {
		if err == io.EOF {
			dp.applyID = appIndexID
			err = nil
			log.LogDebugf("[ApplySnapshot] successful applyID[%v].", dp.applyID)
			return
		}
		if err != nil {
			log.LogErrorf("[ApplySnapshot]: err %s", err.Error())
		}
	}()

	leaderAddr, _ := dp.IsRaftLeader()

	if len(dp.replicas) > 0 {
		replicaAddrParts := strings.Split(dp.replicas[0], ":")
		firstHost = strings.TrimSpace(replicaAddrParts[0])
	}

	if firstHost == LocalIP && leaderAddr != "" {
		targetAddr = leaderAddr
	} else if firstHost != "" {
		targetAddr = dp.replicas[0]
	} else {
		err = fmt.Errorf("[ApplySnapshot]: not expect err firstHost[%s] localIP[%v] raftLeader[%v]",
			firstHost, LocalIP, leaderAddr)
		return
	}

	extents, err = dp.getExtentInfo(targetAddr)
	if err != nil {
		err = errors.Annotatef(err, "[ApplySnapshot] getExtentInfo DataPartition[%v]", dp.partitionID)
		return
	}
	dp.ExtentRepair(extents)

	data, err = iterator.Next()
	appIndexID = binary.BigEndian.Uint64(data)
	dp.applyID = appIndexID
	return
}

// HandleFatalEvent notifies the application when panic happens.
func (dp *DataPartition) HandleFatalEvent(err *raft.FatalError) {
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
}

// HandleLeaderChange notifies the application when the raft leader has changed.
func (dp *DataPartition) HandleLeaderChange(leader uint64) {
	exporter.Alarm(ModuleName, fmt.Sprintf("LeaderChange: partition=%d, "+
		"newLeader=%d", dp.config.PartitionID, leader))

	if dp.config.NodeID == leader {
		dp.isRaftLeader = true
	}
}

// Put submits the raft log to the raft store. TODO check if this description is correct
func (dp *DataPartition) Put(key, val interface{}) (resp interface{}, err error) {
	if dp.raftPartition == nil {
		err = fmt.Errorf("%s key=%v", RaftNotStarted, key)
		return
	}
	item := &RndWrtCmdItem{
		Op: key.(uint32),
		K:  nil,
		V:  nil,
	}
	if val != nil {
		item.V = val.([]byte)
	}
	cmd, err := item.rndWrtCmdMarshalJSON()
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
	atomic.StoreUint64(&dp.applyID, applyID)
}
