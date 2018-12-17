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

package master

const (
	ParaNodeAddr          = "addr"
	ParaDiskPath          = "disk"
	ParaName              = "name"
	ParaId                = "id"
	ParaCount             = "count"
	ParaReplicas          = "replicas"
	ParaStart             = "start"
	ParaEnable            = "enable"
	ParaThreshold         = "threshold"
	ParaRandomWrite       = "randomWrite"
	ParaDataPartitionSize = "size"
	ParaVolCapacity       = "capacity"
)

const (
	DeleteExcessReplicationErr     = "DeleteExcessReplicationErr "
	AddLackReplicationErr          = "AddLackReplicationErr "
	CheckDataPartitionDiskErrorErr = "CheckDataPartitionDiskErrorErr  "
	GetAvailDataNodeHostsErr       = "GetAvailDataNodeHostsErr "
	GetAvailMetaNodeHostsErr       = "GetAvailMetaNodeHostsErr "
	GetDataReplicaFileCountInfo    = "GetDataReplicaFileCountInfo "
	DataNodeOfflineInfo            = "dataNodeOfflineInfo"
	DiskOfflineInfo                = "DiskOfflineInfo"
	HandleDataPartitionOfflineErr  = "HandleDataPartitionOffLineErr "
)

const (
	UnderlineSeparator = "_"
)

const (
	DefaultMaxMetaPartitionInodeID  uint64  = 1<<63 - 1
	DefaultMetaPartitionInodeIDStep uint64  = 1 << 24
	DefaultMetaNodeReservedMem      uint64  = 1 << 32
	RuntimeStackBufSize                     = 4096
	NodesAliveRate                  float32 = 0.5
	SpaceAvailRate                          = 0.90
	DefaultNodeSetCapacity                  = 6
	MinReadWriteDataPartitions              = 10
	CheckMissFileReplicaTime                = 600
	DefaultVolCapacity                      = 200
	LoadDataPartitionPeriod                 = 12 * 60 * 60
	DefaultInitDataPartitions               = 10
	VolExpandDataPartitionStepRatio         = 0.1
	VolMaxExpandDataPartitionCount          = 100
)

const (
	OK     = iota
	Failed
)

const (
	VolNormal     uint8 = 0
	VolMarkDelete uint8 = 1
)

const (
	OpSyncAddMetaNode          uint32 = 0x01
	OpSyncAddDataNode          uint32 = 0x02
	OpSyncAddDataPartition     uint32 = 0x03
	OpSyncAddVol               uint32 = 0x04
	OpSyncAddMetaPartition     uint32 = 0x05
	OpSyncUpdateDataPartition  uint32 = 0x06
	OpSyncUpdateMetaPartition  uint32 = 0x07
	OpSyncDeleteDataNode       uint32 = 0x08
	OpSyncDeleteMetaNode       uint32 = 0x09
	OpSyncAllocDataPartitionID uint32 = 0x0A
	OpSyncAllocMetaPartitionID uint32 = 0x0B
	OpSyncAllocCommonID        uint32 = 0x0C
	OpSyncPutCluster           uint32 = 0x0D
	OpSyncUpdateVol            uint32 = 0x0E
	OpSyncDeleteVol            uint32 = 0x0F
	OpSyncDeleteDataPartition  uint32 = 0x10
	OpSyncDeleteMetaPartition  uint32 = 0x11
	OpSyncAddNodeSet           uint32 = 0x12
	OpSyncUpdateNodeSet        uint32 = 0x13
)

const (
	KeySeparator          = "#"
	MetaNodeAcronym       = "mn"
	DataNodeAcronym       = "dn"
	DataPartitionAcronym  = "dp"
	MetaPartitionAcronym  = "mp"
	VolAcronym            = "vol"
	ClusterAcronym        = "c"
	NodeSetAcronym        = "s"
	MaxDataPartitionIDKey = KeySeparator + "max_dp_id"
	MaxMetaPartitionIDKey = KeySeparator + "max_mp_id"
	MaxCommonIDKey        = KeySeparator + "max_common_id"
	MetaNodePrefix        = KeySeparator + MetaNodeAcronym + KeySeparator
	DataNodePrefix        = KeySeparator + DataNodeAcronym + KeySeparator
	DataPartitionPrefix   = KeySeparator + DataPartitionAcronym + KeySeparator
	VolPrefix             = KeySeparator + VolAcronym + KeySeparator
	MetaPartitionPrefix   = KeySeparator + MetaPartitionAcronym + KeySeparator
	ClusterPrefix         = KeySeparator + ClusterAcronym + KeySeparator
	NodeSetPrefix         = KeySeparator + NodeSetAcronym + KeySeparator
)
