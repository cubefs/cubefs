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

package datanode

const (
	IntervalToUpdateReplica       = 600 // interval to update the replica
	IntervalToUpdatePartitionSize = 60  // interval to update the partition size
	NumOfFilesToRecoverInParallel = 10  // number of files to be recovered simultaneously
)

// Network protocol
const (
	NetworkProtocol = "tcp"
)

// Status of load data partition extent header
const (
	FinishLoadDataPartitionExtentHeader = 1
)

// cmd response
const (
	ReadFlag  = 1
	WriteFlag = 2
)

// Error code
const (
	RaftNotStarted = "RaftNotStarted"
)

// Action description
const (
	ActionNotifyFollowerToRepair        = "ActionNotifyFollowerRepair"
	ActionStreamRead                    = "ActionStreamRead"
	ActionCreateExtent                  = "ActionCreateExtent:"
	ActionMarkDelete                    = "ActionMarkDelete:"
	ActionGetAllExtentWatermarks        = "ActionGetAllExtentWatermarks:"
	ActionWrite                         = "ActionWrite:"
	ActionRepair                        = "ActionRepair:"
	ActionDecommissionPartition         = "ActionDecommissionPartition"
	ActionAddDataPartitionRaftMember    = "ActionAddDataPartitionRaftMember"
	ActionRemoveDataPartitionRaftMember = "ActionRemoveDataPartitionRaftMember"
	ActionDataPartitionTryToLeader      = "ActionDataPartitionTryToLeader"

	ActionCreateDataPartition        = "ActionCreateDataPartition"
	ActionLoadDataPartition          = "ActionLoadDataPartition"
	ActionDeleteDataPartition        = "ActionDeleteDataPartition"
	ActionStreamReadTinyDeleteRecord = "ActionStreamReadTinyDeleteRecord"
	ActionSyncTinyDeleteRecord       = "ActionSyncTinyDeleteRecord"
	ActionStreamReadTinyExtentRepair = "ActionStreamReadTinyExtentRepair"
	ActionBatchMarkDelete            = "ActionBatchMarkDelete"
)

// Apply the raft log operation. Currently we only have the random write operation.
const (
	MinTinyExtentsToRepair = 10 // minimum number of tiny extents to repair
)

// Tiny extent has been put back to store
const (
	IsReleased = 1
)

const (
	MinAvaliTinyExtentCnt = 5
)

// Sector size
const (
	DiskSectorSize = 512
)

const (
	RepairRead = true
	StreamRead = false
)

const (
	BufferWrite = false
)

const (
	DecommsionEnter         = 1
	DecommsionWaitAddRes    = 2
	DecommsionWaitAddResFin = 3
	DecommsionRemoveNew     = 4
	DecommsionRemoveOld     = 5
	DecommsionErr           = 6
)

const (
	EmptyResponse                      = 'E'
	TinyExtentRepairReadResponseArgLen = 17
	MaxSyncTinyDeleteBufferSize        = 2400000
	MaxFullSyncTinyDeleteTime          = 3600 * 24
	MinTinyExtentDeleteRecordSyncSize  = 4 * 1024 * 1024
)
