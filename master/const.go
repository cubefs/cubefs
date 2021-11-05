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

package master

import (
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
)

// Keys in the request
const (
	addrKey                     = "addr"
	destAddrKey                 = "destAddr"
	diskPathKey                 = "disk"
	nameKey                     = "name"
	idKey                       = "id"
	countKey                    = "count"
	startKey                    = "start"
	endKey                      = "end"
	nodeTypeKey                 = "nodeType"
	strictFlagKey               = "strict"
	stateKey                    = "state"
	enableKey                   = "enable"
	thresholdKey                = "threshold"
	dataPartitionSizeKey        = "size"
	metaPartitionCountKey       = "mpCount"
	volCapacityKey              = "capacity"
	dpWritableThresholdKey      = "dpWriteableThreshold"
	volOwnerKey                 = "owner"
	volAuthKey                  = "authKey"
	replicaNumKey               = "replicaNum"
	mpReplicaNumKey             = "mpReplicaNum"
	followerReadKey             = "followerRead"
	forceROWKey                 = "forceROW"
	authenticateKey             = "authenticate"
	akKey                       = "ak"
	keywordsKey                 = "keywords"
	zoneNameKey                 = "zoneName"
	autoRepairKey               = "autoRepair"
	tokenKey                    = "token"
	tokenTypeKey                = "tokenType"
	enableTokenKey              = "enableToken"
	userKey                     = "user"
	nodeHostsKey                = "hosts"
	nodeDeleteBatchCountKey     = "batchCount"
	nodeMarkDeleteRateKey       = "markDeleteRate"
	dataNodeRepairTaskCountKey  = "dataNodeRepairTaskCount"
	dataNodeReqRateKey          = "dataNodeReqRate"
	dataNodeReqOpRateKey        = "dataNodeReqOpRate"
	dataNodeReqVolOpRateKey     = "dataNodeReqVolOpRate"
	dataNodeReqVolPartRateKey   = "dataNodeReqVolPartRate"
	dataNodeReqVolOpPartRateKey = "dataNodeReqVolOpPartRate"
	metaNodeReqRateKey          = "metaNodeReqRate"
	metaNodeReqOpRateKey        = "metaNodeReqOpRate"
	clientReadVolRateKey        = "clientReadVolRate"
	clientWriteVolRateKey       = "clientWriteVolRate"
	clientVolOpRateKey          = "clientVolOpRate"
	nodeDeleteWorkerSleepMs     = "deleteWorkerSleepMs"
	descriptionKey              = "description"
	dpRecoverPoolSizeKey        = "dpRecoverPool"
	mpRecoverPoolSizeKey        = "mpRecoverPool"
	dpSelectorNameKey           = "dpSelectorName"
	dpSelectorParmKey           = "dpSelectorParm"
	bucketPolicyKey             = "bucketPolicy"
	sourceKey                   = "source"
	targetKey                   = "target"
	autoKey                     = "auto"
	volumeKey                   = "volume"
	opcodeKey                   = "opcode"
	volWriteMutexKey            = "volWriteMutex"
	extentMergeInoKey           = "extentMergeIno"
	extentMergeSleepMsKey       = "extentMergeSleepMs"
	fixTinyDeleteRecordKey      = "fixTinyDeleteRecordKey"
	crossRegionHAKey            = "crossRegion"
	regionNameKey               = "regionName"
	regionTypeKey               = "regionType"
	addReplicaTypeKey           = "addReplicaType"
	convertModeKey              = "convertMode"
	partitionTypeKey            = "partitionType"
)

const (
	nodeTypeDataNode = "dataNode"
	nodeTypeMetaNode = "metaNode"
	nodeTypeAll      = "all"
)

const (
	partitionTypeDataPartition = "dataPartition"
	partitionTypeMetaPartition = "metaPartition"
)

const (
	deleteIllegalReplicaErr       = "deleteIllegalReplicaErr "
	addMissingReplicaErr          = "addMissingReplicaErr "
	checkDataPartitionDiskErr     = "checkDataPartitionDiskErr  "
	dataNodeOfflineErr            = "dataNodeOfflineErr "
	diskOfflineErr                = "diskOfflineErr "
	diskAutoOfflineErr            = "diskAutoOfflineErr "
	handleDataPartitionOfflineErr = "handleDataPartitionOffLineErr "
	balanceDataPartitionZoneErr   = "balanceDataPartitionZoneErr "
)

const (
	underlineSeparator = "_"
	commaSeparator     = ","
)

const (
	LRUCacheSize    = 3 << 30
	WriteBufferSize = 4 * util.MB
)

const (
	defaultInitMetaPartitionCount                      = 5
	defaultMaxInitMetaPartitionCount                   = 100
	defaultMaxMetaPartitionInodeID              uint64 = 1<<63 - 1
	defaultMetaPartitionInodeIDStep             uint64 = 1 << 24
	defaultMetaNodeReservedMem                  uint64 = 1 << 30
	runtimeStackBufSize                                = 4096
	spaceAvailableRate                                 = 0.90
	defaultNodeSetCapacity                             = 256
	minNumOfRWDataPartitions                           = 10
	intervalToCheckMissingReplica                      = 600
	intervalToWarnDataPartition                        = 600
	intervalToLoadDataPartition                        = 12 * 60 * 60
	defaultInitDataPartitionCnt                        = 10
	volExpansionRatio                                  = 0.1
	maxNumberOfDataPartitionsForExpansion              = 100
	EmptyCrcValue                               uint32 = 4045511210
	DefaultZoneName                                    = proto.DefaultZoneName
	retrySendSyncTaskInternal                          = 5 * time.Second
	defaultRangeOfCountDifferencesAllowed              = 50
	defaultMinusOfMaxInodeID                           = 1000
	defaultPercentMinusOfInodeCount                    = 0.20
	defaultRecoverPoolSize                             = -1
	maxDataPartitionsRecoverPoolSize                   = 50
	maxMetaPartitionsRecoverPoolSize                   = 30
	defaultMinusOfNodeSetCount                         = 3
	defaultLearnerPromThreshold                        = 90
	minRateLimit                                       = 100
	minPartRateLimit                                   = 1
	diskErrDataPartitionOfflineBatchCount              = 200
	defaultHighUsedRatioDataNodesThreshold             = 0.85
	defaultHighUsedRatioMetaNodesThreshold             = 0.85
	defaultQuorumDataPartitionMasterRegionCount        = 3
	defaultQuorumMetaPartitionMasterRegionCount        = 3
	defaultQuorumMetaPartitionLearnerReplicaNum        = 2
	maxQuorumVolDataPartitionReplicaNum                = 5
	defaultMinDpWriteableThreshold                = 0.5
)

const (
	normal          uint8 = 0
	markDelete      uint8 = 1
	normalZone            = 0
	unavailableZone       = 1
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
	opSyncBatchPut             uint32 = 0x14
	opSyncUpdateDataNode       uint32 = 0x15
	opSyncUpdateMetaNode       uint32 = 0x16
	opSyncAddUserInfo          uint32 = 0x17
	opSyncDeleteUserInfo       uint32 = 0x18
	opSyncUpdateUserInfo       uint32 = 0x19
	opSyncAddAKUser            uint32 = 0x1A
	opSyncDeleteAKUser         uint32 = 0x1B
	opSyncAddVolUser           uint32 = 0x1C
	opSyncDeleteVolUser        uint32 = 0x1D
	opSyncUpdateVolUser        uint32 = 0x1E

	OpSyncAddToken     uint32 = 0x20
	OpSyncDelToken     uint32 = 0x21
	OpSyncUpdateToken  uint32 = 0x22
	OpSyncAddRegion    uint32 = 0x23
	OpSyncUpdateRegion uint32 = 0x24
	OpSyncDelRegion    uint32 = 0x25
)

const (
	keySeparator          = "#"
	idSeparator           = "$" // To seperate ID of server that submits raft changes
	metaNodeAcronym       = "mn"
	dataNodeAcronym       = "dn"
	dataPartitionAcronym  = "dp"
	metaPartitionAcronym  = "mp"
	volAcronym            = "vol"
	regionAcronym         = "region"
	clusterAcronym        = "c"
	nodeSetAcronym        = "s"
	tokenAcronym          = "t"
	maxDataPartitionIDKey = keySeparator + "max_dp_id"
	maxMetaPartitionIDKey = keySeparator + "max_mp_id"
	maxCommonIDKey        = keySeparator + "max_common_id"
	metaNodePrefix        = keySeparator + metaNodeAcronym + keySeparator
	dataNodePrefix        = keySeparator + dataNodeAcronym + keySeparator
	dataPartitionPrefix   = keySeparator + dataPartitionAcronym + keySeparator
	volPrefix             = keySeparator + volAcronym + keySeparator
	regionPrefix          = keySeparator + regionAcronym + keySeparator
	metaPartitionPrefix   = keySeparator + metaPartitionAcronym + keySeparator
	clusterPrefix         = keySeparator + clusterAcronym + keySeparator
	nodeSetPrefix         = keySeparator + nodeSetAcronym + keySeparator

	akAcronym      = "ak"
	userAcronym    = "user"
	volUserAcronym = "voluser"
	akPrefix       = keySeparator + akAcronym + keySeparator
	userPrefix     = keySeparator + userAcronym + keySeparator
	volUserPrefix  = keySeparator + volUserAcronym + keySeparator
	TokenPrefix    = keySeparator + tokenAcronym + keySeparator
)
