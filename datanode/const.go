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

// The status of the server
const (
	Standby uint32 = iota
	Start
	Running
	Shutdown
	Stopped
)

const (
	IntervalToUpdateReplica       = 600 // interval to update the replica
	IntervalToUpdatePartitionSize = 60 // interval to update the partition size
	NumOfFilesToRecoverInParallel = 7  // number of files to be recovered simultaneously
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
	ActionNotifyFollowerToRepair     = "ActionNotifyFollowerRepair"
	ActionStreamRead                 = "ActionStreamRead"
	ActionGetDataPartitionMetrics    = "ActionGetDataPartitionMetrics"
	ActionCreateExtent               = "ActionCreateExtent:"
	ActionMarkDelete                 = "ActionMarkDelete:"
	ActionGetAllExtentWatermarks     = "ActionGetAllExtentWatermarks:"
	ActionWrite                      = "ActionWrite:"
	ActionRepair                     = "ActionRepair:"
	ActionDecommissionPartition      = "ActionDecommissionPartition"
	ActionCreateDataPartition        = "ActionCreateDataPartition"
	ActionLoadDataPartition          = "ActionLoadDataPartition"
	ActionDeleteDataPartition        = "ActionDeleteDataPartition"
	ActionStreamReadTinyDeleteRecord = "ActionStreamReadTinyDeleteRecord"
	ActionSyncTinyDeleteRecord       = "ActionSyncTinyDeleteRecord"
)

// Apply the raft log operation. Currently we only have the random write operation.
const (
	opRandomWrite uint32 = iota
	opRandomSyncWrite
)

const (
	maxRetryCounts         = 10        // maximum number of retries for random writes
	MinTinyExtentsToRepair = 10        // minimum number of tiny extents to repair
	NumOfRaftLogsToRetain  = 100000    // Count of raft logs per data partition
	MaxUnit64              = 1<<64 - 1 // Unit64 max value
)

// Tiny extent has been put back to store
const (
	IsReleased = 1
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
	NotUpdateSize = false
	UpdateSize    = true
)

const (
	BufferWrite = false
)

const (
	MaxSyncTinyDeleteBufferSize = 2400000
	MaxFullSyncTinyDeleteTime   = 60 * 2
)
