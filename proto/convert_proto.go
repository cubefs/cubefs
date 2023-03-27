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

package proto

type StoreMode				uint8
type VolConvertState		uint8
type ConvertTaskState		uint32
type MPConvertState			uint32

const (
	TaskInit 					ConvertTaskState = iota
	TaskGetVolumeInfo
	TaskSetVolumeState
	TaskGetVolumeMPInfo
	TaskConvertLoop
	TaskFinished
)

const (
	MPGetDetailInfo 			MPConvertState = iota
	MPSelectLearner
	MPAddLearner
	MPWaitLearnerSync
	MPPromteLearner
	MPWaitStable
	MPDelReplica
	MPWaitInterval
	MPStopped
)

const StoreModeDef				StoreMode = 0
const (
	StoreModeMem     			StoreMode = 1<<iota
	StoreModeRocksDb
	StoreModeMax
)

const (
	VolConvertStInit          VolConvertState = iota
	VolConvertStPrePared
	VolConvertStRunning
	VolConvertStStopped
	VolConvertStFinished
)

const (
	ProcessorStopped uint32 = iota
	ProcessorStopping
	ProcessorStarting
	ProcessorRunning
)

type MetaPartitionLayout struct {
	PercentOfMP      uint32 `json:"percent_of_mp"`
	PercentOfReplica uint32 `json:"percent_of_replica"`
}

type ConvertTaskInfo struct {
	ProcessorID    int32               `json:"processor_id"`
	VolName        string              `json:"vol_name"`
	ClusterName    string              `json:"cluster_name"`
	SelectedMP     []uint64            `json:"selected_mp"`
	FinishedMP     []uint64            `json:"finished_mp"`
	IgnoredMP      []uint64            `json:"ignore_mp"`
	RunningMP      uint64              `json:"running_mp"`
	Layout         MetaPartitionLayout `json:"layout"`
	TaskState      ConvertTaskState    `json:"state"`
	MPConvertState MPConvertState      `json:"mp_state"`
	ErrMsg         string              `json:"err_msg"`
}

type ConvertProcessorInfo struct {
	Id    uint32 `json:"id"`
	State uint32 `json:"state"`
	Count uint32 `json:"count"`
}

func (c *ConvertProcessorInfo)StateStr() string {
	switch c.State {
	case ProcessorStopped:
		return "Stopped"
	case ProcessorStopping:
		return "Stopping"
	case ProcessorStarting:
		return "Starting"
	case ProcessorRunning:
		return "Running"
	default:
	}
	return "Unknown"
}

type ConvertClusterInfo struct {
	ClusterName string   `json:"cluster_name"`
	Nodes       []string `json:"nodes"`
}

type ConvertProcessorDetailInfo struct {
	Processor 		*ConvertProcessorInfo `json:"processor"`
	Tasks			[]*ConvertTaskInfo    `json:"tasks"`
}

type ConvertClusterDetailInfo struct {
	Cluster   	*ConvertClusterInfo		`json:"cluster"`
	Tasks		[]*ConvertTaskInfo		`json:"tasks"`
}

type ConvertNodeViewInfo struct {
	Port         string                  `json:"port"`
	Processors   []*ConvertProcessorInfo `json:"processor"`
	Clusters     []*ConvertClusterInfo   `json:"cluster"`
	Tasks        []*ConvertTaskInfo      `json:"task"`
}


type SelectMetaNodeInfo struct {
	PartitionID		uint64			`json:"partition_id"`
	OldNodeAddr     string          `json:"old_node_addr"`
	NewNodeAddr   	string 		 	`json:"new_node_addr"`
	StoreMode		uint8			`json:"store_mode"`
}

func(mode *StoreMode) Str() string {
	switch *mode {
	case StoreModeMem:
		return "Mem"
	case StoreModeRocksDb:
		return "Rocks Db"
	case StoreModeMem | StoreModeRocksDb:
		return "Mem&Rocks"
	default:
	}
	return "Unknown"
}

func(st * VolConvertState) Str() string {
	switch *st {
	case VolConvertStInit:
		return "Init"
	case VolConvertStPrePared:
		return "Prepared"
	case VolConvertStRunning:
		return "Running"
	case VolConvertStStopped:
		return "Stopped"
	case VolConvertStFinished:
		return "Finished"
	default:
	}
	return "Unknown"
}

func (st *ConvertTaskState) Str() string {
	switch *st {
	case TaskInit:
		return "Init"
	case TaskGetVolumeInfo:
		return "GetVolumeInfo"
	case TaskSetVolumeState:
		return "SetVolumeState"
	case TaskGetVolumeMPInfo:
		return "GetVolumeMPInfo"
	case TaskConvertLoop:
		return "ConvertLoop"
	case TaskFinished:
		return "Finished"
	default:
	}
	return "Unknown"
}

func (state *MPConvertState) Str()string {
	switch *state {
	case MPGetDetailInfo:
		return "GetDetailInfo"
	case MPSelectLearner:
		return "SelectLearner"
	case MPAddLearner:
		return "AddLearner"
	case MPWaitLearnerSync:
		return "WaitLearnerSync"
	case MPPromteLearner:
		return "PromteLearner"
	case MPWaitStable:
		return "WaitStable"
	case MPDelReplica:
		return "DelReplica"
	case MPWaitInterval:
		return "WaitInterval"
	case MPStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

func TaskStateStr(state ConvertTaskState) string {
	switch state {
	case TaskInit:
		return "Init"
	case TaskGetVolumeInfo:
		return "GetVolumeInfo"
	case TaskSetVolumeState:
		return "SetVolumeState"
	case TaskGetVolumeMPInfo:
		return "GetVolumeMPInfo"
	case TaskConvertLoop:
		return "ConvertLoop"
	case TaskFinished:
		return "Finished"
	default:
		return "Unknown"
	}
}