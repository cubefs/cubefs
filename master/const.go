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

package master

import (
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

// Keys in the request
const (
	addrKey               = "addr"
	diskPathKey           = "disk"
	nameKey               = "name"
	idKey                 = "id"
	countKey              = "count"
	startKey              = "start"
	enableKey             = "enable"
	thresholdKey          = "threshold"
	dirQuotaKey           = "dirQuota"
	dataPartitionSizeKey  = "size"
	metaPartitionCountKey = "mpCount"
	volCapacityKey        = "capacity"
	volTypeKey            = "volType"
	cacheRuleKey          = "cacheRuleKey"
	emptyCacheRuleKey     = "emptyCacheRule"

	forceDelVolKey          = "forceDelVol"
	ebsBlkSizeKey           = "ebsBlkSize"
	cacheCapacity           = "cacheCap"
	cacheActionKey          = "cacheAction"
	cacheThresholdKey       = "cacheThreshold"
	cacheTTLKey             = "cacheTTL"
	cacheHighWaterKey       = "cacheHighWater"
	cacheLowWaterKey        = "cacheLowWater"
	cacheLRUIntervalKey     = "cacheLRUInterval"
	clientVersion           = "version"
	domainIdKey             = "domainId"
	volOwnerKey             = "owner"
	volAuthKey              = "authKey"
	replicaNumKey           = "replicaNum"
	followerReadKey         = "followerRead"
	authenticateKey         = "authenticate"
	akKey                   = "ak"
	keywordsKey             = "keywords"
	zoneNameKey             = "zoneName"
	crossZoneKey            = "crossZone"
	normalZonesFirstKey     = "normalZonesFirst"
	userKey                 = "user"
	nodeHostsKey            = "hosts"
	nodeDeleteBatchCountKey = "batchCount"
	nodeMarkDeleteRateKey   = "markDeleteRate"
	nodeDeleteWorkerSleepMs = "deleteWorkerSleepMs"
	nodeAutoRepairRateKey   = "autoRepairRate"
	clusterLoadFactorKey    = "loadFactor"
	maxDpCntLimitKey        = "maxDpCntLimit"
	clusterCreateTimeKey    = "clusterCreateTime"
	descriptionKey          = "description"
	dpSelectorNameKey       = "dpSelectorName"
	dpSelectorParmKey       = "dpSelectorParm"
	nodeTypeKey             = "nodeType"
	ratio                   = "ratio"
	rdOnlyKey               = "rdOnly"
	srcAddrKey              = "srcAddr"
	targetAddrKey           = "targetAddr"
	forceKey                = "force"
	raftForceDelKey         = "raftForceDel"
	enablePosixAclKey       = "enablePosixAcl"
	enableTransactionKey    = "enableTransaction"
	QosEnableKey            = "qosEnable"
	DiskEnableKey           = "diskenable"
	IopsWKey                = "iopsWKey"
	IopsRKey                = "iopsRKey"
	FlowWKey                = "flowWKey"
	FlowRKey                = "flowRKey"
	ClientReqPeriod         = "reqPeriod"
	ClientTriggerCnt        = "triggerCnt"
	QosMasterLimit          = "qosLimit"
	decommissionLimit       = "decommissionLimit"
	DiskDisableKey          = "diskDisable"
	Limit                   = "limit"
	TimeOut                 = "timeout"
	CountByMeta             = "countByMeta"
	dpReadOnlyWhenVolFull   = "dpReadOnlyWhenVolFull"
	PeriodicKey             = "periodic"
	IPKey                   = "ip"
	OperateKey              = "op"
	UIDKey                  = "uid"
	CapacityKey             = "capacity"
	configKey               = "config"
)

const (
	deleteIllegalReplicaErr       = "deleteIllegalReplicaErr "
	addMissingReplicaErr          = "addMissingReplicaErr "
	checkDataPartitionDiskErr     = "checkDataPartitionDiskErr  "
	dataNodeOfflineErr            = "dataNodeOfflineErr "
	diskOfflineErr                = "diskOfflineErr "
	handleDataPartitionOfflineErr = "handleDataPartitionOffLineErr "
)

const (
	underlineSeparator = "_"
)

const (
	LRUCacheSize       = 3 << 30
	WriteBufferSize    = 4 * util.MB
	MaxFlowLimit       = 10 * util.TB
	MinFlowLimit       = 100 * util.MB
	MinIoLimit         = 100
	MinMagnify         = 10
	MaxMagnify         = 100
	QosMasterAcceptCnt = 3000
	MinZoneDiskLimit   = 300
	MaxZoneDiskLimit   = 10000
)

const (
	defaultFaultDomainZoneCnt                    = 3
	defaultNormalCrossZoneCnt                    = 3
	defaultInitMetaPartitionCount                = 3
	defaultMaxInitMetaPartitionCount             = 100
	defaultMaxMetaPartitionInodeID        uint64 = 1<<63 - 1
	defaultMetaPartitionInodeIDStep       uint64 = 1 << 22
	defaultMetaNodeReservedMem            uint64 = 1 << 30
	runtimeStackBufSize                          = 4096
	spaceAvailableRate                           = 0.90
	defaultNodeSetCapacity                       = 18
	minNumOfRWDataPartitions                     = 10
	intervalToCheckMissingReplica                = 600
	intervalToWarnDataPartition                  = 600
	intervalToLoadDataPartition                  = 12 * 60 * 60
	defaultInitDataPartitionCnt                  = 10
	volExpansionRatio                            = 0.1
	maxNumberOfDataPartitionsForExpansion        = 100
	EmptyCrcValue                         uint32 = 4045511210
	DefaultZoneName                              = proto.DefaultZoneName
	retrySendSyncTaskInternal                    = 3 * time.Second
	defaultRangeOfCountDifferencesAllowed        = 50
	defaultMinusOfMaxInodeID                     = 1000
	defaultNodeSetGrpBatchCnt                    = 3
	defaultMigrateDpCnt                          = 50
	defaultMigrateMpCnt                          = 15
	defaultMaxReplicaCnt                         = 16
	defaultIopsRLimit                     uint64 = 1 << 35
	defaultIopsWLimit                     uint64 = 1 << 35
	defaultFlowWLimit                     uint64 = 1 << 35
	defaultFlowRLimit                     uint64 = 1 << 35
	defaultLimitTypeCnt                          = 4
	defaultClientTriggerHitCnt                   = 1
	defaultClientReqPeriodSeconds                = 1
)

const (
	normal               uint8 = 0
	markDelete           uint8 = 1
	normalZone                 = 0
	unavailableZone            = 1
	metaNodesUnAvailable       = 2
	dataNodesUnAvailable       = 3
)

const (
	defaultEbsBlkSize = 8 * 1024 * 1024

	defaultCacheThreshold   = 10 * 1024 * 1024
	defaultCacheTtl         = 30
	defaultCacheHighWater   = 80
	defaultCacheLowWater    = 40
	defaultCacheLruInterval = 5
)

const (
	opSyncAddMetaNode               uint32 = 0x01
	opSyncAddDataNode               uint32 = 0x02
	opSyncAddDataPartition          uint32 = 0x03
	opSyncAddVol                    uint32 = 0x04
	opSyncAddMetaPartition          uint32 = 0x05
	opSyncUpdateDataPartition       uint32 = 0x06
	opSyncUpdateMetaPartition       uint32 = 0x07
	opSyncDeleteDataNode            uint32 = 0x08
	opSyncDeleteMetaNode            uint32 = 0x09
	opSyncAllocDataPartitionID      uint32 = 0x0A
	opSyncAllocMetaPartitionID      uint32 = 0x0B
	opSyncAllocCommonID             uint32 = 0x0C
	opSyncPutCluster                uint32 = 0x0D
	opSyncUpdateVol                 uint32 = 0x0E
	opSyncDeleteVol                 uint32 = 0x0F
	opSyncDeleteDataPartition       uint32 = 0x10
	opSyncDeleteMetaPartition       uint32 = 0x11
	opSyncAddNodeSet                uint32 = 0x12
	opSyncUpdateNodeSet             uint32 = 0x13
	opSyncBatchPut                  uint32 = 0x14
	opSyncUpdateDataNode            uint32 = 0x15
	opSyncUpdateMetaNode            uint32 = 0x16
	opSyncAddUserInfo               uint32 = 0x17
	opSyncDeleteUserInfo            uint32 = 0x18
	opSyncUpdateUserInfo            uint32 = 0x19
	opSyncAddAKUser                 uint32 = 0x1A
	opSyncDeleteAKUser              uint32 = 0x1B
	opSyncAddVolUser                uint32 = 0x1C
	opSyncDeleteVolUser             uint32 = 0x1D
	opSyncUpdateVolUser             uint32 = 0x1E
	opSyncNodeSetGrp                uint32 = 0x1F
	opSyncDataPartitionsView        uint32 = 0x20
	opSyncExclueDomain              uint32 = 0x23
	opSyncUpdateZone                uint32 = 0x24
	opSyncAllocClientID             uint32 = 0x25
	opSyncPutApiLimiterInfo         uint32 = 0x26
	opSyncPutFollowerApiLimiterInfo uint32 = 0x27

	opSyncAddDecommissionDisk    uint32 = 0x28
	opSyncDeleteDecommissionDisk uint32 = 0x29
	opSyncUpdateDecommissionDisk uint32 = 0x2A

	DecommissionDiskAcronym = "dd"
	DecommissionDiskPrefix  = keySeparator + DecommissionDiskAcronym + keySeparator

	opSyncAcl uint32 = 0x36
	opSyncUid uint32 = 0x37
)

const (
	keySeparator          = "#"
	idSeparator           = "$" // To seperate ID of server that submits raft changes
	metaNodeAcronym       = "mn"
	dataNodeAcronym       = "dn"
	dataPartitionAcronym  = "dp"
	metaPartitionAcronym  = "mp"
	volAcronym            = "vol"
	clusterAcronym        = "c"
	nodeSetAcronym        = "s"
	nodeSetGrpAcronym     = "g"
	zoneAcronym           = "zone"
	domainAcronym         = "zoneDomain"
	apiLimiterAcronym     = "al"
	maxDataPartitionIDKey = keySeparator + "max_dp_id"
	maxMetaPartitionIDKey = keySeparator + "max_mp_id"
	maxCommonIDKey        = keySeparator + "max_common_id"
	maxClientIDKey        = keySeparator + "client_id"
	metaNodePrefix        = keySeparator + metaNodeAcronym + keySeparator
	dataNodePrefix        = keySeparator + dataNodeAcronym + keySeparator
	dataPartitionPrefix   = keySeparator + dataPartitionAcronym + keySeparator
	volPrefix             = keySeparator + volAcronym + keySeparator
	metaPartitionPrefix   = keySeparator + metaPartitionAcronym + keySeparator
	clusterPrefix         = keySeparator + clusterAcronym + keySeparator
	nodeSetPrefix         = keySeparator + nodeSetAcronym + keySeparator
	nodeSetGrpPrefix      = keySeparator + nodeSetGrpAcronym + keySeparator
	DomainPrefix          = keySeparator + domainAcronym + keySeparator
	zonePrefix            = keySeparator + zoneAcronym + keySeparator
	apiLimiterPrefix      = keySeparator + apiLimiterAcronym + keySeparator
	MultiVerPrefix        = keySeparator + "multiVer" + keySeparator
	AclPrefix             = keySeparator + "acl" + keySeparator
	UidPrefix             = keySeparator + "uid" + keySeparator
	akAcronym             = "ak"
	userAcronym           = "user"
	volUserAcronym        = "voluser"
	volNameAcronym        = "volname"
	akPrefix              = keySeparator + akAcronym + keySeparator
	userPrefix            = keySeparator + userAcronym + keySeparator
	volUserPrefix         = keySeparator + volUserAcronym + keySeparator
	volWarnUsedRatio      = 0.9
	volCachePrefix        = keySeparator + volNameAcronym + keySeparator
)
