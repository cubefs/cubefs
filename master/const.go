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
	"github.com/chubaofs/chubaofs/util/unit"
)

// Keys in the request
const (
	addrKey                      = "addr"
	destAddrKey                  = "destAddr"
	diskPathKey                  = "disk"
	nameKey                      = "name"
	idKey                        = "id"
	countKey                     = "count"
	startKey                     = "start"
	endKey                       = "end"
	nodeTypeKey                  = "nodeType"
	strictFlagKey                = "strict"
	stateKey                     = "state"
	enableKey                    = "enable"
	thresholdKey                 = "threshold"
	dataPartitionSizeKey         = "size"
	metaPartitionCountKey        = "mpCount"
	volCapacityKey               = "capacity"
	dpWritableThresholdKey       = "dpWriteableThreshold"
	volOwnerKey                  = "owner"
	volAuthKey                   = "authKey"
	replicaNumKey                = "replicaNum"
	mpReplicaNumKey              = "mpReplicaNum"
	dpHostDelayIntervalKey       = "hostDelayInterval"
	dpFollReadHostWeightKey      = "follReadHostWeight"
	followerReadKey              = "followerRead"
	nearReadKey                  = "nearRead"
	forceROWKey                  = "forceROW"
	extentExpirationKey          = "ekExpireSec"
	enableWriteCacheKey          = "writeCache"
	authenticateKey              = "authenticate"
	akKey                        = "ak"
	keywordsKey                  = "keywords"
	zoneNameKey                  = "zoneName"
	autoRepairKey                = "autoRepair"
	tokenKey                     = "token"
	tokenTypeKey                 = "tokenType"
	enableTokenKey               = "enableToken"
	userKey                      = "user"
	nodeHostsKey                 = "hosts"
	nodeDeleteBatchCountKey      = "batchCount"
	nodeMarkDeleteRateKey        = "markDeleteRate"
	dataNodeRepairTaskCountKey   = "dataNodeRepairTaskCount"
	dataNodeRepairTaskSSDKey     = "dataNodeRepairTaskSSDZoneCount"
	dataNodeRepairTaskCntZoneKey = "dataNodeRepairTaskZoneCount"
	dataNodeReqRateKey           = "dataNodeReqRate"
	dataNodeReqOpRateKey         = "dataNodeReqOpRate"
	dataNodeReqVolOpRateKey      = "dataNodeReqVolOpRate"
	dataNodeReqVolPartRateKey    = "dataNodeReqVolPartRate"
	dataNodeReqVolOpPartRateKey  = "dataNodeReqVolOpPartRate"
	metaNodeReqRateKey           = "metaNodeReqRate"
	metaNodeReqOpRateKey         = "metaNodeReqOpRate"
	metaNodeReadDirLimitKey      = "metaNodeReadDirLimit"
	clientReadVolRateKey         = "clientReadVolRate"
	clientWriteVolRateKey        = "clientWriteVolRate"
	clientVolOpRateKey           = "clientVolOpRate"
	objectVolActionRateKey       = "objectVolActionRate"
	nodeDeleteWorkerSleepMs      = "deleteWorkerSleepMs"
	dataNodeFlushFDIntervalKey   = "dataNodeFlushFDInterval"
	normalExtentDeleteExpireKey  = "normalExtentDeleteExpire"
	descriptionKey               = "description"
	dpRecoverPoolSizeKey         = "dpRecoverPool"
	mpRecoverPoolSizeKey         = "mpRecoverPool"
	dpSelectorNameKey            = "dpSelectorName"
	dpSelectorParmKey            = "dpSelectorParm"
	bucketPolicyKey              = "bucketPolicy"
	sourceKey                    = "source"
	targetKey                    = "target"
	autoKey                      = "auto"
	volumeKey                    = "volume"
	opcodeKey                    = "opcode"
	actionKey                    = "action"
	volWriteMutexKey             = "volWriteMutex"
	extentMergeInoKey            = "extentMergeIno"
	extentMergeSleepMsKey        = "extentMergeSleepMs"
	fixTinyDeleteRecordKey       = "fixTinyDeleteRecordKey"
	crossRegionHAKey             = "crossRegion"
	regionNameKey                = "regionName"
	regionTypeKey                = "regionType"
	idcNameKey                   = "idcName"
	mediumTypeKey                = "mediumType"
	smartRulesKey                = "smartRules"
	smartKey                     = "smart"
	addReplicaTypeKey            = "addReplicaType"
	convertModeKey               = "convertMode"
	partitionTypeKey             = "partitionType"
	volMinWritableMPNum          = "minWritableMp"
	volMinWritableDPNum          = "minWritableDp"
	trashRemainingDaysKey        = "trashRemainingDays"
	volMetaLayoutKey             = "metaLayout"
	StoreModeKey                 = "storeMode"
	volConvertStKey              = "convertState"
	versionKey                   = "version"
	isManualKey                  = "isManual"
	isRecoverKey                 = "isRecover"
	mediumKey                    = "medium"
	zoneTagKey                   = "zoneTag"
	forceKey                     = "force"
	ecEnableKey                  = "ecEnable"
	ecScrubEnableKey             = "ecScrubEnable"
	ecScrubPeriodKey             = "ecScrubPeriod"
	ecDataNumKey                 = "ecDataNum"
	ecParityNumKey               = "ecParityNum"
	ecSaveTimeKey                = "ecSaveTime"
	ecWaitTimeKey                = "ecWaitTime"
	ecTimeOutKey                 = "ecTimeOut"
	ecRetryWaitKey               = "ecRetryWait"
	ecMaxUnitSizeKey             = "ecMaxUnitSize"
	ecMaxScrubExtentsKey         = "ecMaxScrubExtents"
	maxCodecConcurrentKey        = "maxCodecConcurrent"
	needDelEcKey                 = "delEc"
	ecTestKey                    = "test"
	dataNodeHttpPortKey          = "httpPort"
	compactTagKey                = "compactTag"
	dumpWaterLevelKey            = "metaNodeDumpWaterLevel"
	monitorSummarySecondKey      = "monitorSummarySec"
	monitorReportSecondKey       = "monitorReportSec"
	volTrashCleanIntervalKey     = "trashCleanInterval"
	volBatchDelInodeCntKey       = "batchDelInodeCnt"
	volDelInodeIntervalKey       = "delInodeInterval"
	dataNodeFlushFDParallelismOnDiskKey = "dataNodeFlushFDParallelism"
)

const (
	nodeTypeDataNode = "dataNode"
	nodeTypeMetaNode = "metaNode"
	nodeTypeEcNode   = "ecNode"
	nodeTypeAll      = "all"
)

const (
	mediumAll = "all"
	mediumSSD = "ssd"
	mediumHDD = "hdd"
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
	WriteBufferSize = 4 * unit.MB
)

const (
	defaultInitMetaPartitionCount                      = 3
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
	defaultUnrecoverableDuration                       = 24 * 60 * 60
	defaultCheckRecoverDuration                        = 10 * 60 * 60
	defaultDecommissionDuration                        = 10 * 60
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
	minReadDirLimitNum                                 = 500000
	minNormalExtentDeleteExpire                        = 10 * 60
	diskErrDataPartitionOfflineBatchCount              = 200
	defaultHighUsedRatioDataNodesThreshold             = 0.85
	defaultHighUsedRatioMetaNodesThreshold             = 0.85
	defaultQuorumDataPartitionMasterRegionCount        = 3
	defaultQuorumMetaPartitionMasterRegionCount        = 3
	defaultQuorumMetaPartitionLearnerReplicaNum        = 2
	maxQuorumVolDataPartitionReplicaNum                = 5
	defaultMinDpWriteableThreshold                     = 0.5
	defaultVolMaxWritableMPNum                         = 100
	defaultVolMaxWritableDPNum                         = 6000
	defaultLowCapacityVolMaxWritableDPNum              = 100
	defaultLowCapacityVol                              = 10 * 1024
	maxTrashRemainingDays                              = 30
	defaultMetaNodeVersion                             = "3.0.0"
	minCrossRegionVolMasterRegionZonesCount            = 1
	defaultClientPkgAddr                               = "http://storage.jd.local/dpgimage/cfs_spark/"
	defaultCompactTag                                  = "default"
	defaultEcMigrationSaveTime                         = 100 * 365 * 24 * 60 //min
	defaultMinEcTime                                   = 5                   //min
	defaultEcMigrationTimeOut                          = 30                  //min
	defaultEcMigrationWaitTime                         = 30 * 24 * 60        //min
	defaultEcMigrationRetryWait                        = 60                  //min
	defaultEcMaxUnitSize                               = 32 * 1024 * 1024    //byte
	defaultEcScrubPeriod                               = 30 * 24 * 60        //min
	defaultEcScrubPeriodTime                           = 60                  //second
	defaultEcScrubDiskConcurrentExtents                = 3
	defaultMaxCodecConcurrent                          = 10
	defaultEcScrubEnable                               = false
	defaultEcEnable                                    = false
	defaultEcDataNum                                   = 4
	defaultEcParityNum                                 = 2
	defaultDataNodeHttpPort                            = "17320"
	defaultSSDZoneTaskLimit                            = 20
	defaultFollReadHostWeight                          = 0
	defaultChildFileMaxCount                           = 2000 * 10000    //default 2kw
	defaultDataNodeFlushFDParallelismOnDisk            = 5
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

	OpSyncAddIDC    uint32 = 0x26
	OpSyncUpdateIDC uint32 = 0x27
	OpSyncDelIDC    uint32 = 0x28

	opSyncAddEcNode         uint32 = 0x30
	opSyncDeleteEcNode      uint32 = 0x31
	opSyncUpdateEcNode      uint32 = 0x32
	opSyncAddCodecNode      uint32 = 0x33
	opSyncDeleteCodecNode   uint32 = 0x34
	opSyncAddEcPartition    uint32 = 0x35
	opSyncDelEcPartition    uint32 = 0x36
	opSyncUpdateEcPartition uint32 = 0x37
	opSyncAddMigrateTask    uint32 = 0x38
	opSyncDeleteMigrateTask uint32 = 0x39
	opSyncUpdateMigrateTask uint32 = 0x3A
)

const (
	keySeparator               = "#"
	idSeparator                = "$" // To seperate ID of server that submits raft changes
	metaNodeAcronym            = "mn"
	dataNodeAcronym            = "dn"
	dataPartitionAcronym       = "dp"
	frozenDataPartitionAcronym = "frozen_dp"
	metaPartitionAcronym       = "mp"
	volAcronym                 = "vol"
	regionAcronym              = "region"
	idcAcronym                 = "idc"
	clusterAcronym             = "c"
	nodeSetAcronym             = "s"
	tokenAcronym               = "t"
	maxDataPartitionIDKey      = keySeparator + "max_dp_id"
	maxMetaPartitionIDKey      = keySeparator + "max_mp_id"
	maxCommonIDKey             = keySeparator + "max_common_id"
	metaNodePrefix             = keySeparator + metaNodeAcronym + keySeparator
	dataNodePrefix             = keySeparator + dataNodeAcronym + keySeparator
	dataPartitionPrefix        = keySeparator + dataPartitionAcronym + keySeparator
	volPrefix                  = keySeparator + volAcronym + keySeparator
	regionPrefix               = keySeparator + regionAcronym + keySeparator
	idcPrefix                  = keySeparator + idcAcronym + keySeparator
	metaPartitionPrefix        = keySeparator + metaPartitionAcronym + keySeparator
	clusterPrefix              = keySeparator + clusterAcronym + keySeparator
	nodeSetPrefix              = keySeparator + nodeSetAcronym + keySeparator
	frozenDPPrefix             = keySeparator + frozenDataPartitionAcronym + keySeparator
	codecNodeAcronym           = "ncn"
	ecNodeAcronym              = "nec"
	migrateAcronym             = "dto" //dp transfer ec
	ecPartitionAcronym         = "ecdp"
	codecNodePrefix            = keySeparator + codecNodeAcronym + keySeparator
	ecNodePrefix               = keySeparator + ecNodeAcronym + keySeparator
	migratePrefix              = keySeparator + migrateAcronym + keySeparator
	ecPartitionPrefix          = keySeparator + ecPartitionAcronym + keySeparator

	akAcronym      = "ak"
	userAcronym    = "user"
	volUserAcronym = "voluser"
	akPrefix       = keySeparator + akAcronym + keySeparator
	userPrefix     = keySeparator + userAcronym + keySeparator
	volUserPrefix  = keySeparator + volUserAcronym + keySeparator
	TokenPrefix    = keySeparator + tokenAcronym + keySeparator
)
