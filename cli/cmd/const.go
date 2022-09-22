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

package cmd

const (
	//List of operation name for cli
	CliOpGet               = "get"
	CliOpList              = "list"
	CliOpStatus            = "stat"
	CliOpCreate            = "create"
	CliOpDelete            = "delete"
	CliOpInfo              = "info"
	CliOpAdd               = "add"
	CliOpSet               = "set"
	CliOpDecommission      = "decommission"
	CliOpDecommissionDisk  = "decommissionDisk"
	CliOpDownloadZip       = "load"
	CliOpMetaCompatibility = "meta"
	CliOpFreeze            = "freeze"
	CliOpUnfreeze          = "unfreeze"
	CliOpResetRecover      = "resetRecover"
	CliOpTransfer          = "transfer"
	CliOpSetThreshold      = "threshold"
	CliOpCheck             = "check"
	CliOpCheckCommit       = "check-commit"
	CliOpReset             = "reset"
	CliOpReplicate         = "add-replica"
	CliOpDelReplica        = "del-replica"
	CliOpAddLearner        = "add-learner"
	CliOpPromoteLearner    = "promote-learner"
	CliOpResetCursor       = "reset-cursor"
	CliOpListMpAllInos     = "list-inos"
	CliOpCheckSnapshot     = "check-snapshot"
	CliOpSetClientPkgAddr  = "client-package-addr"

	CliOpSetRocksDBDiskThreshold        = "rocksdb-threshold"
	CliOpSetMemModeRocksDBDiskThreshold = "mem-rocksdb-threshold"

	// monitor op
	CliOpIPTopPartition      = "ip-top-partition"
	CliOpTopPartitionOp      = "partition-op"
	CliOpGetTopVolOp         = "vol-op"
	CliOpGetTopVol           = "top-vol"
	CliOpClusterTopIP        = "cluster-top-ip"
	CliOpClusterTopVol       = "cluster-top-vol"
	CliOpClusterTopPartition = "cluster-top-partition"
	CliOpClusterTopOp        = "cluster-top-op"
	CliOpTopIPByOp           = "op-top-ip"
	CliOpTopVolByOp          = "op-top-vol"
	CliOpTopPartitionByOp    = "op-top-partition"

	//Shorthand format of operation name
	CliOpDecommissionShortHand = "dec"

	//resource name
	CliResourceDataNode      = "datanode [COMMAND]"
	CliResourceMetaNode      = "metanode"
	CliResourceDataPartition = "datapartition"
	CliResourceMetaPartition = "metapartition"
	CliResourceTopology      = "topology"
	CliResourceRaftNode      = "raftnode"
	CliResourceDisk          = "disk"
	CliResourceConfig        = "config"

	//Flags
	CliFlagName                 = "name"
	CliFlagOnwer                = "user"
	CliFlagDataPartitionSize    = "dp-size"
	CliFlagDataPartitionCount   = "dp-count"
	CliFlagMetaPartitionCount   = "mp-count"
	CliFlagReplicas             = "replicas"
	CliFlagMpReplicas           = "mp-replicas"
	CliFlagEnable               = "enable"
	CliFlagEnableFollowerRead   = "follower-read"
	CliFlagEnableNearRead       = "near-read"
	CliFlagEnableForceROW       = "force-row"
	CliFlagEnableWriteCache     = "write-cache"
	CliFlagExtentCacheExpireSec = "ek-expire-second"
	CliFlagEnableCrossRegionHA  = "cross-region"
	CliFlagAutoRepair           = "auto-repair"
	CliFlagOSSBucketPolicy      = "bucket-policy"
	CliFlagVolWriteMutexEnable  = "vol-write-mutex"
	CliFlagAuthenticate         = "authenticate"
	CliFlagEnableToken          = "enable-token"
	CliFlagEnableAutoFill       = "auto-fill"
	CliFlagCapacity             = "capacity"
	CliFlagThreshold            = "threshold"
	CliFlagAutoPromote          = "auto-promote"
	CliFlagAddress              = "addr"
	CliFlagDiskPath             = "path"
	CliFlagAuthKey              = "authkey"
	CliFlagINodeStartID         = "inode-start"
	CliFlagId                   = "id"
	CliFlagZoneName             = "zonename"
	CliFlagRegionType           = "region-type"
	CliFlagAddReplicaType       = "add-replica-type"
	CliFlagTrashDays            = "trash-days"
	CliFlagCluster              = "cluster"
	CliFlagVolName              = "vol"
	CliFlagNodesAddrs           = "nodes"
	CliFlagMetaLayout           = "meta-layout"
	CliFlagStoreMode            = "store-mode"
	CliFlagIsSmart              = "smart"
	CliSmartRulesMode           = "smart-rules"
	CliExtentDelByRocksKey      = "extentDelByRocks"
	CliFlagRaft                 = "raft"
	CliFlagCompactTag           = "compact"
	CliFlagFollReadDelayInterval = "host-delay-interval"
	CliFlagFollReadHostWeight    = "set-host-weight"

	CliOpRollBack              = "rollback"
	CliOpGetCanEcDel           = "get-can-del"
	CliOpGetCanEcMigrate       = "get-can-migrate"
	CliOpGetTaskStatus         = "get-task-status"
	CliOpMigrateEc             = "migrate-ec"
	CliOpDelAleadyEcDp         = "deldp-ec"
	CliOpEcSet                 = "ec-set"
	CliOpCheckEcData           = "check-stripe-consistency"
	CliOpGetEcExtentHosts      = "getEcExtentHosts"
	CliOpGetEcTinyDelInfo      = "getEcTinyDelInfo"
	CliOpCheckConsistency      = "check-consistency"
	CliOpStopMigratingEc       = "stop-migrating"
	CliFlagEcEnable            = "ecEnable"
	CliFlagEcScrubEnable       = "ecScrubEnable"
	CliFlagEcDataNum           = "ecDataNum"
	CliFlagEcParityNum         = "ecParityNum"
	CliFlagEcSaveTime          = "ecSaveTime"
	CliFlagEcWaitTime          = "ecWaitTime"
	CliFlagEcTimeOut           = "ecTimeOut"
	CliFlagEcRetryWait         = "ecRetryWait"
	CliFlagEcMaxUnitSize       = "ecMaxUnitSize"
	CliFlagEcScrubPeriod       = "ecScrubPeriod"
	CliFlagEcMaxScrubExtents   = "ecDiskConcurrentExtents"
	CliFlagMaxCodecConcurrent  = "maxCodecConcurrent"
	CliResourceEcNode          = "ecnode"
	CliResourceCodecnodeNode   = "codecnode"

	//CliFlagSetDataPartitionCount	= "count" use dp-count instead

	//Shorthand format of resource name
	ResourceDataNodeShortHand      = "dn"
	ResourceMetaNodeShortHand      = "mn"
	ResourceDataPartitionShortHand = "dp"
	ResourceMetaPartitionShortHand = "mp"
)

const (
	UsedSizeNotEqualErr         = "used size not equal"
	RaftNoLeader                = "no raft leader"
	ReplicaNotConsistent        = "replica number not consistent"
	PartitionNotHealthyInMaster = "partition not healthy in master"
)

// for parse raft log
const (
	opFSMCreateInode uint32 = iota
	opFSMUnlinkInode
	opFSMCreateDentry
	opFSMDeleteDentry
	opFSMDeletePartition
	opFSMUpdatePartition
	opFSMDecommissionPartition
	opFSMExtentsAdd
	opFSMStoreTick
	startStoreTick
	stopStoreTick
	opFSMUpdateDentry
	opFSMExtentTruncate
	opFSMCreateLinkInode
	opFSMEvictInode
	opFSMInternalDeleteInode
	opFSMSetAttr
	opFSMInternalDelExtentFile
	opFSMInternalDelExtentCursor
	opExtentFileSnapshot
	opFSMSetXAttr
	opFSMRemoveXAttr
	opFSMCreateMultipart
	opFSMRemoveMultipart
	opFSMAppendMultipart
	opFSMSyncCursor

	//supplement action
	opFSMInternalDeleteInodeBatch
	opFSMDeleteDentryBatch
	opFSMUnlinkInodeBatch
	opFSMEvictInodeBatch

	opFSMCursorReset

	opFSMExtentsInsert
)

var EcStatusMap = map[uint8]string{
	0: "NotMigrate",
	1: "Migrating",
	2: "FinishEc",
	3: "RollBack",
	4: "OnlyEcExist",
	5: "MigrateFailed",
}

var tinyDelStatusMap = map[uint32]string{
	0: "baseDeleteMark",
	1: "deleteMark",
	2: "deleting",
	3: "deleted",
}
