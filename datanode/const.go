// Copyright 2018 The Container File System Authors.
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
	// TODO what is replication host? the replica?
	IntervalToUpdateReplica       = 60 // interval to update the replication host
	IntervalToUpdatePartitionSize = 60 // interval to update the partition size
	NumOfFilesToRecoverInParallel = 7  // number of files to be recovered simultaneously
)

// Network protocol
const (
	NetworkProtocol = "tcp"
)

// Status of loading the extent header
const (
	StartLoadingExtentHeader  = -1
	FinishLoadingExtentHeader = 1
)

// cmd response
const (
	ReadFlag         = 1
	WriteFlag        = 2
	MaxActiveExtents = 20000
)

// Error code
const (
	RaftNotStarted = "RaftNotStarted"
)

// Action description
// TODO why not put the "Action" at the end. This reads weird.
const (
	ActionNotifyFollowerRepair    = "ActionNotifyFollowerRepair"
	ActionStreamRead              = "ActionStreamRead"              //ActionStreamRead is StreamRead Log Prefix
	ActionGetDataPartitionMetrics = "ActionGetDataPartitionMetrics" //ActionGetDataPartitionMetrics is GetDataPartitionMetrics Log Prefix
	ActionCheckReplyAvail         = "ActionCheckReplyAvail"
	ActionCreateExtent            = "ActionCreateExtent:"
	ActionMarkDel                 = "ActionMarkDel:"
	ActionGetAllExtentWaterMarker = "ActionGetAllExtentWaterMarker:"
	ActionWrite                   = "ActionWrite:"
	ActionRead                    = "ActionRead:"
	ActionRepair                  = "ActionRepair:"
	ActionOfflinePartition        = "ActionOfflinePartition"
	ActionCreateDataPartition     = "ActionCreateDataPartition"
	ActionLoadDataPartition       = "ActionLoadDataPartition"
	ActionDeleteDataPartition     = "ActionDeleteDataPartition"
)

// TODO: explain
// Apply raft log opera code
const (
	opRandomWrite uint32 = iota
	// 目前只有一个
)

const (
	maxRetryCounts  = 10     // maximum number of retries for random writes
	// TODO why it is called MinFixTinyExtents while the commment says "Invalid tiny extent count"
	// 每次修复tinyExtent的时候最少修复多少个 （一共只有64个）
	MinFixTinyExtents = 10      // Invalid tiny extent count
	dpRetainRaftLogs  = 100000 // Count of raft logs per data partition
)

// TODO should we call it "IsReleased" or "IsFree"?
// Tiny extent has put back to store
const (
	HasReturnToStore = 1
)

// Sector size
const (
	DiskSectorSize = 512
)

const (
	RepairRead = true
	StreamRead = false
)
