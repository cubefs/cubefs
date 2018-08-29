// Copyright 2018 The ChuBao Authors.
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

const (
	ParaNodeAddr          = "addr"
	ParaName              = "name"
	ParaId                = "id"
	ParaCount             = "count"
	ParaReplicas          = "replicas"
	ParaDataPartitionType = "type"
	ParaStart             = "start"
	ParaEnable            = "enable"
	ParaThreshold         = "threshold"
)

const (
	DeleteExcessReplicationErr     = "DeleteExcessReplicationErr "
	AddLackReplicationErr          = "AddLackReplicationErr "
	CheckDataPartitionDiskErrorErr = "CheckDataPartitionDiskErrorErr  "
	GetAvailDataNodeHostsErr       = "GetAvailDataNodeHostsErr "
	GetAvailMetaNodeHostsErr       = "GetAvailMetaNodeHostsErr "
	GetDataReplicaFileCountInfo    = "GetDataReplicaFileCountInfo "
	DataNodeOfflineInfo            = "dataNodeOfflineInfo"
	HandleDataPartitionOfflineErr  = "HandleDataPartitionOffLineErr "
)

const (
	UnderlineSeparator = "_"
)

const (
	DefaultMaxMetaPartitionInodeID      uint64  = 1<<63 - 1
	DefaultMetaPartitionInodeIDStep     uint64  = 1 << 24
	DefaultMetaNodeReservedMem          uint64  = 1 << 32
	RuntimeStackBufSize                         = 4096
	NodesAliveRate                      float32 = 0.5
	MinReadWriteDataPartitions                  = 200
	MinReadWriteDataPartitionsForClient         = 10
	SpaceAvailRate                              = 0.95
)

const (
	OK = iota
	Failed
)

const (
	VolNormal     uint8 = 0
	VolMarkDelete uint8 = 1
)
