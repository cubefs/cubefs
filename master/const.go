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

package master

// TODO why not put the following into handle_admin.go?
// Keys in the request
const (
	addrKey              = "addr"
	diskPathKey          = "disk"
	nameKey              = "name"
	idKey                = "id"
	countKey             = "count"
	replicasKey          = "replicas"
	startkey             = "start"
	enablekey            = "enable"
	thresholdKey         = "threshold"
	randomWriteKey       = "randomWrite"
	dataPartitionSizeKey = "size"
	volCapacityKey       = "capacity"
)

const (
	deleteIllegalReplicaErr       = "deleteIllegalReplicaErr "
	addMissingReplicaErr          = "addMissingReplicaErr "
	checkDataPartitionDiskErr     = "checkDataPartitionDiskErr  "
	getAvailDataNodeHostsErr      = "getAvailDataNodeHostsErr "
	getAvailMetaNodeHostsErr      = "getAvailMetaNodeHostsErr "
	getDataReplicaFileCountInfo   = "getDataReplicaFileCountInfo "  // TODO rename 副本上文件的个数
	dataNodeOfflineInfo           = "dataNodeOfflineInfo"
	diskOfflineInfo               = "diskOfflineInfo"
	handleDataPartitionOfflineErr = "HandleDataPartitionOffLineErr "
)

const (
	underlineSeparator = "_"
)

const (
	defaultInitMetaPartitionCount           = 3
	defaultMaxMetaPartitionInodeID  uint64  = 1 << 63 - 1
	defaultMetaPartitionInodeIDStep uint64  = 1 << 24
	defaultMetaNodeReservedMem      uint64  = 1 << 32
	runtimeStackBufSize                     = 4096
	nodesAliveRate                  float32 = 0.5 // TODO explain
	spaceAvailRate                          = 0.90 // TODO explain
	defaultNodeSetCapacity                  = 6
	minNumOfRWDataPartitions                = 10
	intervalToCheckMissingReplica           = 600
	defaultVolCapacity                      = 200
	intervalToLoadDataPartition             = 12 * 60 * 60
	defaultInitDataPartitionCnt             = 10
	volExpandDataPartitionStepRatio         = 0.1 // TODO explain  expand -》 扩容
	volMaxExpandDataPartitionCount          = 100 // TODO explain 每次最多创建多少个
)

// TODO We can just use a flag (called "isMarkedToBeDeleted") to indicate if a volume is marked (to be deleted) or not.
// There is no need to employ normal here.
const (
	normal     uint8 = 0
	markDelete uint8 = 1
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
