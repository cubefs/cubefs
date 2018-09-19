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

const (
	Standby uint32 = iota
	Start
	Running
	Shutdown
	Stopped
)

const (
	RequestChanSize = 10240
)

const (
	ActionSendToNext                                 = "ActionSendToNext"
	LocalProcessAddr                                 = "LocalProcess"
	ActionReceiveFromNext                            = "ActionReceiveFromNext"
	ActionStreamRead                                 = "ActionStreamRead"
	ActionWriteToCli                                 = "ActionWriteToCli"
	ActionGetDataPartitionMetrics                    = "ActionGetDataPartitionMetrics"
	ActionCheckAndAddInfos                           = "ActionCheckAndAddInfos"
	ActionCheckChunkInfo                             = "ActionCheckChunkInfo"
	ActionPostToMaster                               = "ActionPostToMaster"
	ActionFollowerRequireChunkRepairCmd              = "ActionFollowerRequireChunkRepairCmd"
	ActionLeaderToFollowerOpRepairReadPackBuffer     = "ActionLeaderToFollowerOpRepairReadPackBuffer"
	ActionLeaderToFollowerOpRepairReadSendPackBuffer = "ActionLeaderToFollowerOpRepairReadSendPackBuffer"

	ActionGetFollowers    = "ActionGetFollowers"
	ActionCheckReplyAvail = "ActionCheckReplyAvail"
)

//stats
const (
	ReportToMonitorRole = 1
	ReportToSelfRole    = 3
)

const (
	InFlow = iota
	OutFlow
)

const (
	NetType = "tcp"
)

const (
	ObjectIDSize = 8
)

//pack cmd response
const (
	NoFlag           = 0
	ReadFlag         = 1
	WriteFlag        = 2
	MaxActiveExtents = 50000
)

const (
	ConnIsNullErr = "ConnIsNullErr"
)

const (
	LogHeartbeat         = "HB:"
	LogStats             = "Stats:"
	LogLoad              = "Load:"
	LogExit              = "Exit:"
	LogShutdown          = "Shutdown:"
	LogCreatePartition   = "CRV:"
	LogCreateFile        = "CRF:"
	LogDelPartition      = "DELV:"
	LogDelFile           = "DELF:"
	LogMarkDel           = "MDEL:"
	LogPartitionSnapshot = "Snapshot:"
	LogGetWm             = "WM:"
	LogGetAllWm          = "AllWM:"
	LogCompactChunk      = "CompactChunk:"
	LogWrite             = "WR:"
	LogRead              = "RD:"
	LogRepairRead        = "RRD:"
	LogStreamRead        = "SRD:"
	LogRepairNeedles     = "RN:"
	LogRepair            = "Repair:"
	LogChecker           = "Checker:"
	LogTask              = "Master Task:"
	LogGetFlow           = "GetFlowInfo:"
)
