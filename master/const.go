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
	paraNodeAddr          = "addr"
	paraDiskPath          = "disk"
	paraName              = "name"
	paraID                = "id"
	paraCount             = "count"
	paraReplicas          = "replicas"
	paraStart             = "start"
	paraEnable            = "enable"
	paraThreshold         = "threshold"
	paraRandomWrite       = "randomWrite"
	paraDataPartitionSize = "size"
	paraVolCapacity       = "capacity"
)

const (
	deleteExcessReplicationErr     = "deleteExcessReplicationErr "
	addLackReplicationErr          = "addLackReplicationErr "
	checkDataPartitionDiskErrorErr = "checkDataPartitionDiskErrorErr  "
	getAvailDataNodeHostsErr       = "getAvailDataNodeHostsErr "
	getAvailMetaNodeHostsErr       = "getAvailMetaNodeHostsErr "
	getDataReplicaFileCountInfo    = "getDataReplicaFileCountInfo "
	dataNodeOfflineInfo            = "dataNodeOfflineInfo"
	diskOfflineInfo                = "diskOfflineInfo"
	handleDataPartitionOfflineErr  = "HandleDataPartitionOffLineErr "
)

const (
	underlineSeparator = "_"
)

const (
	defaultMaxMetaPartitionInodeID  uint64  = 1<<63 - 1
	defaultMetaPartitionInodeIDStep uint64  = 1 << 24
	defaultMetaNodeReservedMem      uint64  = 1 << 32
	runtimeStackBufSize                     = 4096
	nodesAliveRate                  float32 = 0.5
	spaceAvailRate                          = 0.90
	defaultNodeSetCapacity                  = 6
	minReadWriteDataPartitions              = 10
	checkMissFileReplicaTime                = 600
	defaultVolCapacity                      = 200
	loadDataPartitionPeriod                 = 12 * 60 * 60
	defaultInitDataPartitions               = 10
	volExpandDataPartitionStepRatio         = 0.1
	volMaxExpandDataPartitionCount          = 100
)

const (
	volNormal     uint8 = 0
	volMarkDelete uint8 = 1
)

const (
	opSyncAddMetaNode          uint32 = 0x01
	opSyncAddDataNode          uint32 = 0x02
	opSyncAddDataPartition     uint32 = 0x03
	opSyncAddVol               uint32 = 0x04
	opSyncAddMetaPartition     uint32 = 0x05
	opSyncUpdateDataPartition  uint32 = 0x06
	opSyncUpdateMetaPartition  uint32 = 0x07
	opSyncDeleteDataNode       uint32 = 0x08
	opSyncDeleteMetaNode       uint32 = 0x09
	opSyncAllocDataPartitionID uint32 = 0x0A
	opSyncAllocMetaPartitionID uint32 = 0x0B
	opSyncAllocCommonID        uint32 = 0x0C
	opSyncPutCluster           uint32 = 0x0D
	opSyncUpdateVol            uint32 = 0x0E
	opSyncDeleteVol            uint32 = 0x0F
	opSyncDeleteDataPartition  uint32 = 0x10
	opSyncDeleteMetaPartition  uint32 = 0x11
	opSyncAddNodeSet           uint32 = 0x12
	opSyncUpdateNodeSet        uint32 = 0x13
)

const (
	keySeparator          = "#"
	metaNodeAcronym       = "mn"
	dataNodeAcronym       = "dn"
	dataPartitionAcronym  = "dp"
	metaPartitionAcronym  = "mp"
	volAcronym            = "vol"
	clusterAcronym        = "c"
	nodeSetAcronym        = "s"
	maxDataPartitionIDKey = keySeparator + "max_dp_id"
	maxMetaPartitionIDKey = keySeparator + "max_mp_id"
	maxCommonIDKey        = keySeparator + "max_common_id"
	metaNodePrefix        = keySeparator + metaNodeAcronym + keySeparator
	dataNodePrefix        = keySeparator + dataNodeAcronym + keySeparator
	dataPartitionPrefix   = keySeparator + dataPartitionAcronym + keySeparator
	volPrefix             = keySeparator + volAcronym + keySeparator
	metaPartitionPrefix   = keySeparator + metaPartitionAcronym + keySeparator
	clusterPrefix         = keySeparator + clusterAcronym + keySeparator
	nodeSetPrefix         = keySeparator + nodeSetAcronym + keySeparator
)
