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

// api
const (
	// All
	VersionPath = "/version"

	// Admin APIs
	AdminGetCluster                = "/admin/getCluster"
	AdminGetDataPartition          = "/dataPartition/get"
	AdminLoadDataPartition         = "/dataPartition/load"
	AdminCreateDataPartition       = "/dataPartition/create"
	AdminDecommissionDataPartition = "/dataPartition/decommission"
	AdminDiagnoseDataPartition     = "/dataPartition/diagnose"
	AdminResetDataPartition        = "/dataPartition/reset"
	AdminManualResetDataPartition  = "/dataPartition/manualReset"
	AdminResetCorruptDataNode      = "/dataNode/reset"
	AdminDeleteDataReplica         = "/dataReplica/delete"
	AdminAddDataReplica            = "/dataReplica/add"
	AdminAddDataReplicaLearner     = "/dataLearner/add"
	AdminPromoteDataReplicaLearner = "/dataLearner/promote"
	AdminDeleteVol                 = "/vol/delete"
	AdminUpdateVol                 = "/vol/update"
	AdminCreateVol                 = "/admin/createVol"
	AdminGetVol                    = "/admin/getVol"
	AdminClusterFreeze             = "/cluster/freeze"
	AdminClusterStat               = "/cluster/stat"
	AdminGetIP                     = "/admin/getIp"
	AdminGetLimitInfo              = "/admin/getLimitInfo"
	AdminCreateMetaPartition       = "/metaPartition/create"
	AdminSetMetaNodeThreshold      = "/threshold/set"
	AdminListVols                  = "/vol/list"
	AdminSetNodeInfo               = "/admin/setNodeInfo"
	AdminGetNodeInfo               = "/admin/getNodeInfo"
	AdminSetNodeState              = "/admin/setNodeState"
	AdminMergeNodeSet              = "/admin/mergeNodeSet"
	AdminClusterAutoMergeNodeSet   = "/cluster/autoMergeNodeSet"
	AdminApplyVolMutex             = "/vol/writeMutex/apply"
	AdminReleaseVolMutex           = "/vol/writeMutex/release"
	AdminGetVolMutex               = "/vol/writeMutex/get"

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
	ClientDataPartitions = "/client/partitions"
	ClientVol            = "/client/vol"
	ClientMetaPartition  = "/metaPartition/get"
	ClientVolStat        = "/client/volStat"
	ClientMetaPartitions = "/client/metaPartitions"

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
	AdminAddMetaReplicaLearner     = "/metaLearner/add"
	AdminPromoteMetaReplicaLearner = "/metaLearner/promote"

	// Operation response
	GetMetaNodeTaskResponse   = "/metaNode/response"          // Method: 'POST', ContentType: 'application/json'
	GetDataNodeTaskResponse   = "/dataNode/response"          // Method: 'POST', ContentType: 'application/json'
	DataNodeValidateCRCReport = "/dataNode/validateCRCReport" // Method: 'POST', ContentType: 'application/json'

	GetTopologyView = "/topo/get"
	UpdateZone      = "/zone/update"
	GetAllZones     = "/zone/list"

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

	ExtentMergeIno     map[string][]uint64
	ExtentMergeSleepMs uint64
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
	ZoneName            string
	PartitionReports    []*PartitionReport
	Status              uint8
	Result              string
	BadDisks            []string
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
}

// MetaNodeHeartbeatResponse defines the response to the meta node heartbeat request.
type MetaNodeHeartbeatResponse struct {
	ZoneName             string
	Total                uint64
	Used                 uint64
	MetaPartitionReports []*MetaPartitionReport
	Status               uint8
	Result               string
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
	LeaderAddr  string
	Status      int8
}

type OSSSecure struct {
	AccessKey string
	SecretKey string
}

// VolView defines the view of a volume
type VolView struct {
	Name            string
	Owner           string
	Status          uint8
	FollowerRead    bool
	MetaPartitions  []*MetaPartitionView
	DataPartitions  []*DataPartitionResponse
	OSSSecure       *OSSSecure
	OSSBucketPolicy BucketAccessPolicy
	CreateTime      int64
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

func NewVolView(name string, status uint8, followerRead bool, createTime int64) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.FollowerRead = followerRead
	view.CreateTime = createTime
	view.Status = status
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

func NewMetaPartitionView(partitionID, start, end uint64, status int8) (mpView *MetaPartitionView) {
	mpView = new(MetaPartitionView)
	mpView.PartitionID = partitionID
	mpView.Start = start
	mpView.End = end
	mpView.Status = status
	mpView.Members = make([]string, 0)
	return
}

// SimpleVolView defines the simple view of a volume
type SimpleVolView struct {
	ID                  uint64
	Name                string
	Owner               string
	ZoneName            string
	DpReplicaNum        uint8
	MpReplicaNum        uint8
	InodeCount          uint64
	DentryCount         uint64
	MaxMetaPartitionID  uint64
	Status              uint8
	Capacity            uint64 // GB
	RwDpCnt             int
	MpCnt               int
	DpCnt               int
	FollowerRead        bool
	NeedToLowerReplica  bool
	Authenticate        bool
	VolWriteMutexEnable bool
	CrossZone           bool
	AutoRepair          bool
	CreateTime          string
	EnableToken         bool
	Tokens              map[string]*Token `graphql:"-"`
	Description         string
	DpSelectorName      string
	DpSelectorParm      string
	OSSBucketPolicy     BucketAccessPolicy
}

// MasterAPIAccessResp defines the response for getting meta partition
type MasterAPIAccessResp struct {
	APIResp APIAccessResp `json:"api_resp"`
	Data    []byte        `json:"data"`
}

type VolInfo struct {
	Name       string
	Owner      string
	CreateTime int64
	Status     uint8
	TotalSize  uint64
	UsedSize   uint64
}

func NewVolInfo(name, owner string, createTime int64, status uint8, totalSize, usedSize uint64) *VolInfo {
	return &VolInfo{
		Name:       name,
		Owner:      owner,
		CreateTime: createTime,
		Status:     status,
		TotalSize:  totalSize,
		UsedSize:   usedSize,
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
}
