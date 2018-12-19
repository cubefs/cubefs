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

package datanode

// The status of the server
const (
	//Standby is server status
	Standby uint32 = iota
	//Start is server ready start
	Start
	//Running is server has runing
	Running
	//Shutdown is sevrer has ready shutdown
	Shutdown
	//Stopped is server has stop
	Stopped
)

const (
	UpdateReplicationHostsTime = 60  // Timer update replication host
	UpdatePartitionSizeTime    = 60  // Timer update partition size
	SimultaneouslyRecoverFiles = 7   // Files
)

// Net type
const (
	NetType = "tcp"
)

// Status of load data partition extent header
const (
	FinishLoadDataPartitionExtentHeader = 1
	StartLoadDataPartitionExtentHeader  = -1
)

// Pack cmd response
const (
	ReadFlag         = 1
	WriteFlag        = 2
	MaxActiveExtents = 20000
)

// Error code
const (
	RaftIsNotStart = "RaftIsNotStart"
)

// Action description
const (
	LocalProcessAddr              = "LocalProcess"
	ActionStreamRead              = "ActionStreamRead"                  //ActionStreamRead is StreamRead Log Prefix
	ActionGetDataPartitionMetrics = "ActionGetDataPartitionMetrics"     //ActionGetDataPartitionMetrics is GetDataPartitionMetrics Log Prefix
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

// Apply raft log opera code
const (
	opRandomWrite uint32 = iota
)

const (
	maxApplyErrRetry  = 10         // Random write apply retry times
	MinFixTinyExtents = 1          // Invalid tiny extent count
	dpRetainRaftLogs  = 100000     // Count of raft logs per data partition
)

// Tiny extent has put back to store
const (
	HasReturnToStore = 1
)

// Sector size
const (
	DiskSectorSize = 512
)
