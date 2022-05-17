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

package proto

import (
	"fmt"
)

// api
const (
	// All
	VersionPath = "/version"

	// Admin APIs
	AdminGetCluster                = "/admin/getCluster"
	AdminGetDataPartition          = "/dataPartition/get"
	AdminLoadDataPartition         = "/dataPartition/load"
	AdminCreateDataPartition       = "/dataPartition/create"
	AdminFreezeDataPartition       = "/dataPartition/freeze"
	AdminUnfreezeDataPartition     = "/dataPartition/unfreeze"
	AdminDecommissionDataPartition = "/dataPartition/decommission"
	AdminDiagnoseDataPartition     = "/dataPartition/diagnose"
	AdminResetDataPartition        = "/dataPartition/reset"
	AdminTransferDataPartition     = "/dataPartition/transfer"
	AdminManualResetDataPartition  = "/dataPartition/manualReset"
	AdminDataPartitionUpdate       = "/dataPartition/update"
	AdminDataPartitionSetIsRecover = "/dataPartition/setIsRecover"
	AdminCanDelDataPartitions      = "/dataPartition/candel"
	AdminCanMigrateDataPartitions  = "/dataPartition/canmigrate"
	AdminDelDpAlreadyEc            = "/dataPartition/deldpalreadyec"
	AdminDpMigrateEc               = "/dataPartition/ecmigreate"
	AdminDpStopMigrating           = "/dataPartition/stopMigrating"
	AdminDNStopMigrating           = "/dataNode/stopMigrating"
	AdminResetCorruptDataNode      = "/dataNode/reset"
	AdminDeleteDataReplica         = "/dataReplica/delete"
	AdminAddDataReplica            = "/dataReplica/add"
	AdminAddDataReplicaLearner     = "/dataLearner/add"
	AdminPromoteDataReplicaLearner = "/dataLearner/promote"
	AdminDeleteVol                 = "/vol/delete"
	AdminUpdateVol                 = "/vol/update"
	AdminUpdateVolEcInfo           = "/vol/updateEcInfo"
	AdminSetVolConvertSt           = "/vol/setConvertSate"
	AdminVolBatchUpdateDps         = "/vol/batchUpdateDataPartitions"
	AdminCreateVol                 = "/admin/createVol"
	AdminGetVol                    = "/admin/getVol"
	AdminClusterFreeze             = "/cluster/freeze"
	AdminClusterStat               = "/cluster/stat"
	AdminGetIP                     = "/admin/getIp"
	AdminGetLimitInfo              = "/admin/getLimitInfo"
	AdminCreateMetaPartition       = "/metaPartition/create"
	AdminSetMetaNodeThreshold      = "/threshold/set"
	AdminClusterEcSet              = "/cluster/ecSet"
	AdminClusterGetScrub           = "/scrub/get"
	AdminListVols                  = "/vol/list"
	AdminSetNodeInfo               = "/admin/setNodeInfo"
	AdminGetNodeInfo               = "/admin/getNodeInfo"
	AdminSetNodeState              = "/admin/setNodeState"
	AdminMergeNodeSet              = "/admin/mergeNodeSet"
	AdminClusterAutoMergeNodeSet   = "/cluster/autoMergeNodeSet"
	AdminApplyVolMutex             = "/vol/writeMutex/apply"
	AdminReleaseVolMutex           = "/vol/writeMutex/release"
	AdminGetVolMutex               = "/vol/writeMutex/get"
	AdminSetVolConvertMode         = "/vol/setConvertMode"
	AdminSetVolMinRWPartition      = "/vol/setMinRWPartition"
	AdminEnableTrash               = "/admin/trash"
	AdminStatTrash                 = "/admin/trash/stat"
	AdminSetClientPkgAddr          = "/clientPkgAddr/set"
	AdminGetClientPkgAddr          = "/clientPkgAddr/get"

	AdminSmartVolList = "/admin/smartVol/list"
	AdminSetMNRocksDBDiskThreshold = "/rocksdbDiskThreshold/set"

	AdminCompactVolList = "/admin/compactVol/list"
	AdminCompactVolSet  = "/admin/compactVol/set"

	//graphql master api
	AdminClusterAPI = "/api/cluster"
	AdminUserAPI    = "/api/user"
	AdminVolumeAPI  = "/api/volume"

	//graphql coonsole api
	ConsoleIQL        = "/iql"
	ConsoleLoginAPI   = "/login"
	ConsoleMonitorAPI = "/cfs_monitor"
	ConsoleFile       = "/file"
	ConsoleFileDown   = "/file/down"
	ConsoleFileUpload = "/file/upload"

	// Client APIs
	ClientDataPartitions             = "/client/partitions"
	ClientVol                        = "/client/vol"
	ClientMetaPartition              = "/metaPartition/get"
	ClientVolStat                    = "/client/volStat"
	ClientMetaPartitions             = "/client/metaPartitions"
	ClientMetaPartitionSnapshotCheck = "/getSnapshotCrc"

	ClientDataPartitionsDbBack = "/client/dataPartitions"

	//raft node APIs
	AddRaftNode    = "/raftNode/add"
	RemoveRaftNode = "/raftNode/remove"

	// Node APIs
	AddDataNode                    = "/dataNode/add"
	DecommissionDataNode           = "/dataNode/decommission"
	DecommissionDisk               = "/disk/decommission"
	GetDataNode                    = "/dataNode/get"
	AddMetaNode                    = "/metaNode/add"
	DecommissionMetaNode           = "/metaNode/decommission"
	GetMetaNode                    = "/metaNode/get"
	AdminUpdateMetaNode            = "/metaNode/update"
	AdminLoadMetaPartition         = "/metaPartition/load"
	AdminDiagnoseMetaPartition     = "/metaPartition/diagnose"
	AdminResetMetaPartition        = "/metaPartition/reset"
	AdminManualResetMetaPartition  = "/metaPartition/manualReset"
	AdminResetCorruptMetaNode      = "/metaNode/reset"
	AdminDecommissionMetaPartition = "/metaPartition/decommission"
	AdminAddMetaReplica            = "/metaReplica/add"
	AdminDeleteMetaReplica         = "/metaReplica/delete"
	AdminSelectMetaReplicaNode     = "/metaReplica/selectNode"
	AdminAddMetaReplicaLearner     = "/metaLearner/add"
	AdminPromoteMetaReplicaLearner = "/metaLearner/promote"
	AdminMetaPartitionSetIsRecover = "/metaPartition/setIsRecover"

	// Operation response
	GetMetaNodeTaskResponse   = "/metaNode/response"          // Method: 'POST', ContentType: 'application/json'
	GetDataNodeTaskResponse   = "/dataNode/response"          // Method: 'POST', ContentType: 'application/json'
	DataNodeValidateCRCReport = "/dataNode/validateCRCReport" // Method: 'POST', ContentType: 'application/json'

	GetCodecNodeTaskResponse = "/codecNode/response" // Method: 'POST', ContentType: 'application/json'
	GetEcNodeTaskResponse    = "/ecNode/response"    // Method: 'POST', ContentType: 'application/json'

	GetTopologyView = "/topo/get"
	UpdateZone      = "/zone/update"
	GetAllZones     = "/zone/list"
	SetZoneRegion   = "/zone/setRegion"
	UpdateRegion    = "/region/update"
	GetRegionView   = "/region/get"
	RegionList      = "/region/list"
	CreateRegion    = "/region/create"

	SetZoneIDC = "/zone/setIDC"
	GetIDCView = "/idc/get"
	IDCList    = "/idc/list"
	CreateIDC  = "/idc/create"
	DeleteDC   = "/idc/delete"

	//token
	TokenGetURI    = "/token/get"
	TokenAddURI    = "/token/add"
	TokenDelURI    = "/token/delete"
	TokenUpdateURI = "/token/update"

	// Header keys
	SkipOwnerValidation = "Skip-Owner-Validation"
	ForceDelete         = "Force-Delete"

	// APIs for user management
	UserCreate          = "/user/create"
	UserDelete          = "/user/delete"
	UserUpdate          = "/user/update"
	UserUpdatePolicy    = "/user/updatePolicy"
	UserRemovePolicy    = "/user/removePolicy"
	UserDeleteVolPolicy = "/user/deleteVolPolicy"
	UserGetInfo         = "/user/info"
	UserGetAKInfo       = "/user/akInfo"
	UserTransferVol     = "/user/transferVol"
	UserList            = "/user/list"
	UsersOfVol          = "/vol/users"

	//CodecNode API
	GetAllCodecNodes      = "/codecNode/getAllNodes"
	GetCodecNode          = "/codecNode/get"
	AddCodecNode          = "/codecNode/add"
	DecommissionCodecNode = "/codecNode/decommission"

	//ecnode
	AddEcNode          = "/ecNode/add"
	GetEcNode          = "/ecNode/get"
	DecommissionEcNode = "/ecNode/decommission"
	DecommissionEcDisk = "/ecNode/diskDecommission"

	//EcDataPartition API
	AdminGetEcPartition          = "/ecPartition/get"
	AdminDecommissionEcPartition = "/ecPartition/decommission"
	AdminDiagnoseEcPartition     = "/ecPartition/diagnose"
	AdminEcPartitionRollBack     = "/ecPartition/rollback"
	AdminGetAllTaskStatus        = "/ecPartition/gettaskstatus"
	AdminDeleteEcReplica         = "/ecReplica/delete"
	AdminAddEcReplica            = "/ecReplica/add"
	ClientEcPartitions           = "/client/ecPartitions"

	//graphql api for header
	HeadAuthorized  = "Authorization"
	ParamAuthorized = "_authorization"
	UserKey         = "_user_key"
	UserInfoKey     = "_user_info_key"
)

const TimeFormat = "2006-01-02 15:04:05"

const (
	ReadOnlyToken  = 1
	ReadWriteToken = 2
)

const DbBackMaster = "dbbak.jd.local"

var IsDbBack bool = false

type BucketAccessPolicy uint8

func (p BucketAccessPolicy) String() string {
	switch p {
	case OSSBucketPolicyPrivate:
		return "private"
	case OSSBucketPolicyPublicRead:
		return "public-read"
	default:
	}
	return "unknown"
}

const (
	OSSBucketPolicyPrivate BucketAccessPolicy = iota
	OSSBucketPolicyPublicRead
)

type CrossRegionHAType uint8

func (p CrossRegionHAType) String() string {
	switch p {
	case DefaultCrossRegionHAType:
		return "default"
	case CrossRegionHATypeQuorum:
		return "quorum"
	default:
	}
	return "unknown"
}

const (
	DefaultCrossRegionHAType CrossRegionHAType = iota // 默认类型，表示主被复制中所有复制组成员必须全部成功才可判定为成功
	CrossRegionHATypeQuorum                           // 表示主备复制中采用Quorum机制，复制组成员成功数量达到Quorum数值要求即可判定为成功
)

type RegionType uint8

func (r RegionType) String() string {
	switch r {
	case MasterRegion:
		return "master-region"
	case SlaveRegion:
		return "slave-region"
	default:
	}
	return "unknown"
}

const (
	_ RegionType = iota
	MasterRegion
	SlaveRegion
)

type MediumType uint8

const (
	MediumInit    MediumType = 0
	MediumSSD     MediumType = 1
	MediumHDD     MediumType = 2
	MediumEC      MediumType = 3
	MediumSSDName            = "ssd"
	MediumHDDName            = "hdd"
	MediumECName             = "ec"
)

func StrToMediumType(str string) (mType MediumType, err error) {
	switch str {
	case MediumHDDName:
		mType = MediumHDD
	case MediumSSDName:
		mType = MediumSSD
	case MediumECName:
		mType = MediumEC
	default:
		err = fmt.Errorf("invalid medium type: %v", str)
	}
	return
}

func (m MediumType) String() string {
	switch m {
	case MediumHDD:
		return MediumHDDName
	case MediumSSD:
		return MediumSSDName
	case MediumEC:
		return MediumECName
	default:
		return "unknown"
	}
}

func (m MediumType) Check() bool {
	return m == MediumSSD || m == MediumHDD || m == MediumEC
}

type CompactTag uint8

const (
	CompactDefault     CompactTag = 0
	CompactOpen        CompactTag = 1
	CompactClose       CompactTag = 3
	CompactDefaultName            = "default"
	CompactOpenName               = "Enabled"
	CompactCloseName              = "Disabled"
)

const (
	CompatTagClosedTimeDuration = 10 * 60
	ForceRowClosedTimeDuration  = 5 * 60
)

func StrToCompactTag(str string) (cTag CompactTag, err error) {
	switch str {
	case CompactDefaultName:
		cTag = CompactDefault
	case CompactOpenName:
		cTag = CompactOpen
	case CompactCloseName:
		cTag = CompactClose
	default:
		err = fmt.Errorf("invalid compact tag: %v", str)
	}
	return
}

func (ct CompactTag) String() string {
	switch ct {
	case CompactDefault:
		return CompactDefaultName
	case CompactOpen:
		return CompactOpenName
	case CompactClose:
		return CompactCloseName
	default:
		return "unknown"
	}
}

func (ct CompactTag) Bool() bool {
	switch ct {
	case CompactOpen:
		return true
	default:
		return false
	}
}

type AddReplicaType uint8

func (a AddReplicaType) String() string {
	switch a {
	case DefaultAddReplicaType:
		return "default"
	case AutoChooseAddrForQuorumVol:
		return "auto-choose-addr-for-quorum-vol"
	default:
	}
	return "unknown"
}

const (
	DefaultAddReplicaType AddReplicaType = iota
	AutoChooseAddrForQuorumVol
)

type Token struct {
	TokenType int8
	Value     string
	VolName   string
}

// HTTPReply uniform response structure
type HTTPReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// RegisterMetaNodeResp defines the response to register a meta node.
type RegisterMetaNodeResp struct {
	ID uint64
}

// ClusterInfo defines the cluster infomation.
type ClusterInfo struct {
	Cluster string
	Ip      string

	// MUST keep for old version client
	ClientReadLimitRate  uint64
	ClientWriteLimitRate uint64
}

type LimitInfo struct {
	Cluster                     string
	MetaNodeDeleteBatchCount    uint64
	MetaNodeDeleteWorkerSleepMs uint64
	MetaNodeReadDirLimitNum     uint64

	MetaNodeReqRateLimit             uint64
	MetaNodeReqOpRateLimitMap        map[uint8]uint64
	DataNodeReqZoneRateLimitMap      map[string]uint64
	DataNodeReqZoneOpRateLimitMap    map[string]map[uint8]uint64
	DataNodeReqZoneVolOpRateLimitMap map[string]map[string]map[uint8]uint64
	DataNodeReqVolPartRateLimitMap   map[string]uint64
	DataNodeReqVolOpPartRateLimitMap map[string]map[uint8]uint64
	DataNodeDeleteLimitRate          uint64
	ClientReadVolRateLimitMap        map[string]uint64
	ClientWriteVolRateLimitMap       map[string]uint64
	ClientReadRateLimit              uint64
	ClientWriteRateLimit             uint64
	ClientVolOpRateLimit             map[uint8]int64 // less than 0: no limit; equal 0: disable op

	DataNodeFixTinyDeleteRecordLimitOnDisk uint64
	DataNodeRepairTaskLimitOnDisk          uint64
	DataNodeRepairClusterTaskLimitOnDisk   uint64
	DataNodeRepairSSDZoneTaskLimitOnDisk   uint64
	DataNodeRepairTaskCountZoneLimit       map[string]uint64

	ExtentMergeIno              map[string][]uint64
	ExtentMergeSleepMs          uint64
	MetaNodeDumpWaterLevel      uint64
	DataNodeFlushFDInterval     uint32

	MonitorSummarySec		uint64
	MonitorReportSec		uint64
}

// CreateDataPartitionRequest defines the request to create a data partition.
type CreateDataPartitionRequest struct {
	PartitionType string
	PartitionId   uint64
	PartitionSize int
	VolumeId      string
	IsRandomWrite bool
	Members       []Peer
	Learners      []Learner
	Hosts         []string
	CreateType    int
	VolumeHAType  CrossRegionHAType
}

// CreateDataPartitionResponse defines the response to the request of creating a data partition.
type CreateDataPartitionResponse struct {
	PartitionId uint64
	Status      uint8
	Result      string
}

// DeleteDataPartitionRequest defines the request to delete a data partition.
type DeleteDataPartitionRequest struct {
	DataPartitionType string
	PartitionId       uint64
	PartitionSize     int
}

// DeleteDataPartitionResponse defines the response to the request of deleting a data partition.
type DeleteDataPartitionResponse struct {
	Status      uint8
	Result      string
	PartitionId uint64
}

// DataPartitionDecommissionRequest defines the request of decommissioning a data partition.
type DataPartitionDecommissionRequest struct {
	PartitionId uint64
	RemovePeer  Peer
	AddPeer     Peer
}

// AddDataPartitionRaftMemberRequest defines the request of add raftMember a data partition.
type AddDataPartitionRaftMemberRequest struct {
	PartitionId uint64
	AddPeer     Peer
}

// PromoteDataPartitionRaftLearnerRequest defines the request of promote learner raftMember a data partition.
type PromoteDataPartitionRaftLearnerRequest struct {
	PartitionId    uint64  `json:"pid"`
	PromoteLearner Learner `json:"learner"`
}

// RemoveDataPartitionRaftMemberRequest defines the request of add raftMember a data partition.
type RemoveDataPartitionRaftMemberRequest struct {
	PartitionId     uint64
	RemovePeer      Peer
	ReserveResource bool
	RaftOnly        bool
}

// AddDataPartitionRaftLearnerRequest defines the request of add raftLearner a data partition.
type AddDataPartitionRaftLearnerRequest struct {
	PartitionId uint64  `json:"pid"`
	AddLearner  Learner `json:"learner"`
}

// ResetDataPartitionRaftMemberRequest defines the request of reset raftMembers of a data partition.
type ResetDataPartitionRaftMemberRequest struct {
	PartitionId uint64
	NewPeers    []Peer
}

// AddMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type AddMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	AddPeer     Peer
}

// AddMetaPartitionRaftLearnerRequest defines the request of add raftLearner a meta partition.
type AddMetaPartitionRaftLearnerRequest struct {
	PartitionId uint64  `json:"pid"`
	AddLearner  Learner `json:"learner"`
}

// AddMetaPartitionRaftLearnerRequest defines the request of add raftLearner a meta partition.
type PromoteMetaPartitionRaftLearnerRequest struct {
	PartitionId    uint64  `json:"pid"`
	PromoteLearner Learner `json:"learner"`
}

// RemoveMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type RemoveMetaPartitionRaftMemberRequest struct {
	PartitionId     uint64
	RemovePeer      Peer
	ReserveResource bool
	RaftOnly        bool
}

// ResetMetaPartitionRaftMemberRequest defines the request of reset raftMembers of a meta partition.
type ResetMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	NewPeers    []Peer
}

// LoadDataPartitionRequest defines the request of loading a data partition.
type LoadDataPartitionRequest struct {
	PartitionId uint64
}

// LoadDataPartitionResponse defines the response to the request of loading a data partition.
type LoadDataPartitionResponse struct {
	PartitionId       uint64
	Used              uint64
	PartitionSnapshot []*File
	Status            uint8
	PartitionStatus   int
	Result            string
	VolName           string
}

type SyncDataPartitionReplicasRequest struct {
	PartitionId      uint64
	PersistenceHosts []string
}

// File defines the file struct.
type File struct {
	Name     string
	Crc      uint32
	Size     uint32
	Modified int64
}

// LoadMetaPartitionMetricRequest defines the request of loading the meta partition metrics.
type LoadMetaPartitionMetricRequest struct {
	PartitionID uint64
	Start       uint64
	End         uint64
}

// LoadMetaPartitionMetricResponse defines the response to the request of loading the meta partition metrics.
type LoadMetaPartitionMetricResponse struct {
	Start    uint64
	End      uint64
	MaxInode uint64
	Status   uint8
	Result   string
}

// HeartBeatRequest define the heartbeat request.
type HeartBeatRequest struct {
	CurrTime   int64
	MasterAddr string
}

// PartitionReport defines the partition report.
type PartitionReport struct {
	VolName         string
	PartitionID     uint64
	PartitionStatus int
	Total           uint64
	Used            uint64
	DiskPath        string
	IsLeader        bool
	ExtentCount     int
	NeedCompare     bool
	IsLearner       bool
	LastUpdateTime  int64
	IsRecover       bool // 表示当前恢复状态, true表示正在恢复, false表示恢复完成
}

// DataNodeHeartbeatResponse defines the response to the data node heartbeat.
type DataNodeHeartbeatResponse struct {
	Total               uint64
	Used                uint64
	Available           uint64
	TotalPartitionSize  uint64 // volCnt * volsize
	RemainingCapacity   uint64 // remaining capacity to create partition
	CreatedPartitionCnt uint32
	MaxCapacity         uint64 // maximum capacity to create partition
	HttpPort            string
	ZoneName            string
	PartitionReports    []*PartitionReport
	Status              uint8
	Result              string
	BadDisks            []string
	DiskInfos           map[string]*DiskInfo
	Version             string
}

type DiskInfo struct {
	Total         uint64
	Used          uint64
	ReservedSpace uint64
	Status        int
	Path          string
	UsageRatio    float64
}

// MetaPartitionReport defines the meta partition report.
type MetaPartitionReport struct {
	PartitionID     uint64
	Start           uint64
	End             uint64
	Status          int
	MaxInodeID      uint64
	IsLeader        bool
	VolName         string
	InodeCnt        uint64
	DentryCnt       uint64
	IsLearner       bool
	ExistMaxInodeID uint64
	StoreMode       StoreMode
	ApplyId         uint64
}

// MetaNodeHeartbeatResponse defines the response to the meta node heartbeat request.
type MetaNodeHeartbeatResponse struct {
	ZoneName             string
	Total                uint64
	Used                 uint64
	MetaPartitionReports []*MetaPartitionReport
	Status               uint8
	ProfPort             string
	Result               string
	RocksDBDiskInfo      []*MetaNodeDiskInfo
	Version              string
}

// DeleteFileRequest defines the request to delete a file.
type DeleteFileRequest struct {
	VolId uint64
	Name  string
}

// DeleteFileResponse defines the response to the request of deleting a file.
type DeleteFileResponse struct {
	Status uint8
	Result string
	VolId  uint64
	Name   string
}

// DeleteMetaPartitionRequest defines the request of deleting a meta partition.
type DeleteMetaPartitionRequest struct {
	PartitionID uint64
}

// DeleteMetaPartitionResponse defines the response to the request of deleting a meta partition.
type DeleteMetaPartitionResponse struct {
	PartitionID uint64
	Status      uint8
	Result      string
}

// UpdateMetaPartitionRequest defines the request to update a meta partition.
type UpdateMetaPartitionRequest struct {
	PartitionID uint64
	VolName     string
	Start       uint64
	End         uint64
}

// UpdateMetaPartitionResponse defines the response to the request of updating the meta partition.
type UpdateMetaPartitionResponse struct {
	PartitionID uint64
	VolName     string
	End         uint64
	Status      uint8
	Result      string
}

// MetaPartitionDecommissionRequest defines the request of decommissioning a meta partition.
type MetaPartitionDecommissionRequest struct {
	PartitionID uint64
	VolName     string
	RemovePeer  Peer
	AddPeer     Peer
}

// MetaPartitionDecommissionResponse defines the response to the request of decommissioning a meta partition.
type MetaPartitionDecommissionResponse struct {
	PartitionID uint64
	VolName     string
	Status      uint8
	Result      string
}

// MetaPartitionLoadRequest defines the request to load meta partition.
type MetaPartitionLoadRequest struct {
	PartitionID uint64
}

// MetaPartitionLoadResponse defines the response to the request of loading meta partition.
type MetaPartitionLoadResponse struct {
	PartitionID uint64
	DoCompare   bool
	ApplyID     uint64
	MaxInode    uint64
	DentryCount uint64
	InodeCount  uint64
	Addr        string
}

// DataPartitionResponse defines the response from a data node to the master that is related to a data partition.
type DataPartitionResponse struct {
	PartitionID uint64
	Status      int8
	ReplicaNum  uint8
	Hosts       []string
	LeaderAddr  string
	Epoch       uint64
	IsRecover   bool
	IsFrozen    bool
	CreateTime  int64
	MediumType  string
	Total       uint64
	Used        uint64
	EcMigrateStatus uint8
	EcHosts         []string
	EcDataNum       uint8
	EcMaxUnitSize   uint64
}

// DataPartitionsView defines the view of a data partition
type DataPartitionsView struct {
	DataPartitions []*DataPartitionResponse
}

func NewDataPartitionsView() (dataPartitionsView *DataPartitionsView) {
	dataPartitionsView = new(DataPartitionsView)
	dataPartitionsView.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

// EcPartitionResponse defines the response from a ec node to the master that is related to a ec partition.
type EcPartitionResponse struct {
	PartitionID    uint64
	Status         int8
	ReplicaNum     uint8
	Hosts          []string
	LeaderAddr     string
	DataUnitsNum   uint8
	ParityUnitsNum uint8
}

// EcPartitionsView defines the view of a ec partition
type EcPartitionsView struct {
	EcPartitions []*EcPartitionResponse
}

func NewEcPartitionsView() (ecPartitionsView *EcPartitionsView) {
	ecPartitionsView = new(EcPartitionsView)
	ecPartitionsView.EcPartitions = make([]*EcPartitionResponse, 0)
	return
}

type MigrateTaskView struct {
	RetryTimes      uint8
	VolName         string
	Status          string
	PartitionID     uint64
	CurrentExtentID uint64
	ModifyTime      int64
}

// MetaPartitionView defines the view of a meta partition
type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	MaxInodeID  uint64
	InodeCount  uint64
	DentryCount uint64
	MaxExistIno uint64
	IsRecover   bool
	Members     []string
	Learners    []string
	LeaderAddr  string
	Status      int8
	StoreMode   StoreMode
	MemCount    uint8
	RocksCount  uint8
}

type OSSSecure struct {
	AccessKey string
	SecretKey string
}

// VolView defines the view of a volume
type VolView struct {
	Name              string
	Owner             string
	Status            uint8
	FollowerRead      bool
	ForceROW          bool
	EnableWriteCache  bool
	CrossRegionHAType CrossRegionHAType
	MetaPartitions    []*MetaPartitionView
	DataPartitions    []*DataPartitionResponse
	EcPartitions      []*EcPartitionResponse
	OSSSecure         *OSSSecure
	OSSBucketPolicy   BucketAccessPolicy
	CreateTime        int64
	ConnConfig        *ConnConfig // todo
	IsSmart           bool
	SmartEnableTime int64
	SmartRules        []string
}

func (v *VolView) SetSmartRules(rules []string) {
	v.SmartRules = rules
}

func (v *VolView) SetSmartEnableTime (sec int64) {
	v.SmartEnableTime = sec
}

func (v *VolView) SetOwner(owner string) {
	v.Owner = owner
}

func (v *VolView) SetOSSSecure(accessKey, secretKey string) {
	v.OSSSecure = &OSSSecure{AccessKey: accessKey, SecretKey: secretKey}
}

func (v *VolView) SetOSSBucketPolicy(ossBucketPolicy BucketAccessPolicy) {
	v.OSSBucketPolicy = ossBucketPolicy
}

func NewVolView(name string, status uint8, followerRead, isSmart bool, createTime int64) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.FollowerRead = followerRead
	view.IsSmart = isSmart
	view.CreateTime = createTime
	view.Status = status
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.DataPartitions = make([]*DataPartitionResponse, 0)
	view.EcPartitions = make([]*EcPartitionResponse, 0)
	return
}

func NewMetaPartitionView(partitionID, start, end uint64, status int8) (mpView *MetaPartitionView) {
	mpView = new(MetaPartitionView)
	mpView.PartitionID = partitionID
	mpView.Start = start
	mpView.End = end
	mpView.Status = status
	mpView.Members = make([]string, 0)
	mpView.Learners = make([]string, 0)
	return
}

// SimpleVolView defines the simple view of a volume
type SimpleVolView struct {
	ID                   uint64
	Name                 string
	Owner                string
	ZoneName             string
	DpReplicaNum         uint8
	MpReplicaNum         uint8
	DpLearnerNum         uint8
	MpLearnerNum         uint8
	InodeCount           uint64
	DentryCount          uint64
	MaxMetaPartitionID   uint64
	Status               uint8
	Capacity             uint64 // GB
	DpWriteableThreshold float64
	RwDpCnt              int
	MpCnt                int
	DpCnt                int
	FollowerRead         bool
	NearRead             bool
	NeedToLowerReplica   bool
	Authenticate         bool
	VolWriteMutexEnable  bool
	CrossZone            bool
	AutoRepair           bool
	CreateTime           string
	EnableToken          bool
	ForceROW             bool
	EnableWriteCache	 bool
	CrossRegionHAType    CrossRegionHAType
	Tokens               map[string]*Token `graphql:"-"`
	Description          string
	DpSelectorName       string
	DpSelectorParm       string
	Quorum               int
	OSSBucketPolicy      BucketAccessPolicy
	DPConvertMode        ConvertMode
	MPConvertMode        ConvertMode
	MasterRegionZone     string
	SlaveRegionZone      string
	ConnConfig			 *ConnConfig	// todo
	ExtentCacheExpireSec int64
	DpMetricsReportConfig	*DpMetricsReportConfig	// todo
	RwMpCnt               int
	MinWritableMPNum      int
	MinWritableDPNum      int
	TrashRemainingDays    uint32
	DefaultStoreMode      StoreMode
	ConvertState          VolConvertState
	MpLayout              MetaPartitionLayout
	TotalSize            uint64
	UsedSize             uint64
	UsedRatio            float64
	FileAvgSize          float64
	CreateStatus         VolCreateStatus
	IsSmart               bool
	SmartEnableTime string
	SmartRules            []string
	CompactTag            string
	CompactTagModifyTime  int64
	EcEnable              bool
	EcDataNum             uint8
	EcParityNum           uint8
	EcWaitTime            int64
	EcSaveTime            int64
	EcTimeOut             int64
	EcRetryWait           int64
	EcMaxUnitSize         uint64
}

// MasterAPIAccessResp defines the response for getting meta partition
type MasterAPIAccessResp struct {
	APIResp APIAccessResp `json:"api_resp"`
	Data    []byte        `json:"data"`
}

type VolInfo struct {
	Name               string
	Owner              string
	CreateTime         int64
	Status             uint8
	TotalSize          uint64
	UsedSize           uint64
	TrashRemainingDays uint32
	IsSmart            bool
	SmartRules         []string
	ForceROW           bool
	CompactTag         uint8
}

func NewVolInfo(name, owner string, createTime int64, status uint8, totalSize, usedSize uint64, remainingDays uint32, isSmart bool, rules []string, forceRow bool, compactTag uint8) *VolInfo {
	return &VolInfo{
		Name:               name,
		Owner:              owner,
		CreateTime:         createTime,
		Status:             status,
		TotalSize:          totalSize,
		UsedSize:           usedSize,
		TrashRemainingDays: remainingDays,
		IsSmart:            isSmart,
		SmartRules:         rules,
		ForceROW:           forceRow,
		CompactTag:         compactTag,
	}
}

// RateLimitInfo defines the rate limit infomation
type RateLimitInfo struct {
	ZoneName                   string
	Volume                     string
	Opcode                     int8
	MetaNodeReqRate            int64
	MetaNodeReqOpRate          int64
	DataNodeRepairTaskCount    int64
	DataNodeRepairTaskSSDZone  int64
	DataNodeReqRate            int64
	DataNodeReqOpRate          int64
	DataNodeReqVolOpRate       int64
	DataNodeReqVolPartRate     int64
	DataNodeReqVolOpPartRate   int64
	ClientReadVolRate          int64
	ClientWriteVolRate         int64
	ExtentMergeIno             string
	ExtentMergeSleepMs         int64
	ClientReadRate             int64
	ClientWriteRate            int64
	ClientVolOpRate            int64
	DnFixTinyDeleteRecordLimit int64
	DataNodeRepairTaskZoneCount  int64
	MetaNodeDumpWaterLevel     int64
	MonitorSummarySecond	   uint64
	MonitorReportSecond		   uint64
}

type ConvertMode uint8

func (c ConvertMode) String() string {
	switch c {
	case DefaultConvertMode:
		return "default"
	case IncreaseReplicaNum:
		return "increase_replica_num"
	default:
	}
	return "unknown"
}

const (
	DefaultConvertMode ConvertMode = iota
	IncreaseReplicaNum
)

type VolCreateStatus uint8

const (
	DefaultVolCreateStatus VolCreateStatus = iota
	VolInCreation
)

type ConnConfig struct {
	IdleTimeoutSec   int64
	ConnectTimeoutNs int64
	WriteTimeoutNs   int64
	ReadTimeoutNs    int64
}

func (config *ConnConfig) String() string {
	if config == nil {
		return ""
	}
	return fmt.Sprintf("IdleTimeout(%v)s ConnectTimeout(%v)ns WriteTimeout(%v)ns ReadTimeout(%v)ns",
		config.IdleTimeoutSec, config.ConnectTimeoutNs, config.WriteTimeoutNs, config.ReadTimeoutNs)
}

type TrashStatus struct {
	Enable bool
}

type DpMetricsReportConfig struct {
	EnableReport      bool
	ReportIntervalSec int64
	FetchIntervalSec  int64
}

func (config *DpMetricsReportConfig) String() string {
	if config == nil {
		return ""
	}
	return fmt.Sprintf("EnableReport(%v) ReportIntervalSec(%v) FetchIntervalSec(%v)",
		config.EnableReport, config.ReportIntervalSec, config.FetchIntervalSec)
}

// EcPartitionReport defines the partition report.
type EcPartitionReport struct {
	VolName         string
	PartitionID     uint64
	PartitionStatus int
	Total           uint64
	Used            uint64
	DiskPath        string
	IsLeader        bool
	ExtentCount     int
	NeedCompare     bool
	IsRecover       bool
	NodeIndex       uint32
}

// EcNodeHeartbeatResponse defines the response to the ec node heartbeat.
type EcNodeHeartbeatResponse struct {
	Total               uint64
	Used                uint64
	Available           uint64
	TotalPartitionSize  uint64 // volCnt * volsize
	CreatedPartitionCnt uint32
	MaxCapacity         uint64 // maximum capacity of disk to create partition
	PartitionReports    []*EcPartitionReport
	HttpPort            string
	CellName            string
	Status              uint8
	Result              string
	BadDisks            []string
	Version             string
}

// CreateEcPartitionRequest defines the request to create a ec partition.
type CreateEcPartitionRequest struct {
	PartitionID   uint64
	PartitionSize uint64
	VolumeID      string
	DataNodeNum   uint32
	ParityNodeNum uint32
	NodeIndex     uint32
	Hosts         []string
	EcMaxUnitSize uint64
	CheckSum      bool
}

// DeleteEcPartitionRequest defines the request to delete a ec partition.
type DeleteEcPartitionRequest struct {
	DataPartitionType string
	PartitionId       uint64
}

// DeleteEcPartitionRequest defines the request to delete a ec partition.
type RepairEcExtentRequest struct {
	DataPartitionType string
	PartitionId       uint64
	ExtentId          uint64
}

// ChangeEcPartitionMembersRequest defines the request to change members of a ec partition.
type ChangeEcPartitionMembersRequest struct {
	DataPartitionType string
	PartitionId       uint64
	Hosts             []string
}

type UpdateEcVolInfoRequest struct {
	ScrubMaxEcDpNum  uint8
	ScrubMaxVolDpNum uint8
	ScrubBeginHour   uint8
	ScrubEndHour     uint8
	ScrubPeriod      uint8
	ScrubNeed        bool
	ScrubStartTime   int64
	VolName          string
}

type UpdateEcScrubInfoRequest struct {
	ScrubEnable     bool
	MaxScrubExtents uint8
	ScrubPeriod     uint32
	StartScrubTime  int64
}

// DeleteEcDataPartitionResponse defines the response to the request of deleting a data partition.
type DeleteEcDataPartitionResponse struct {
	Status      uint8
	Result      string
	PartitionId uint64
}

type CodecNodeClientView struct {
	Addr     string
	IsActive bool
	Version  string
}

// CodecNodeHeartbeatResponse defines the response to the codec node heartbeat.
type CodecNodeHeartbeatResponse struct {
	Status uint8
	Result string
	Version string
}

type EcNodeChangeMemberResponse struct {
	Status      uint8
	Result      string
	VolName     string
	PartitionId uint64
}

type CodecNodeMigrationResponse struct {
	Status          uint8
	Result          string
	PartitionId     uint64
	CurrentExtentID uint64
}

type MetaNodeDiskInfo struct {
	Path               string
	Total              uint64
	Used               uint64
	UsageRatio         float64
	Status             int8
	MPCount            int
}
