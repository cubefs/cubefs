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

package proto

import "github.com/cubefs/cubefs/util"

// api
const (
	// Admin APIs
	AdminGetMasterApiList                     = "/admin/getMasterApiList"
	AdminSetApiQpsLimit                       = "/admin/setApiQpsLimit"
	AdminGetApiQpsLimit                       = "/admin/getApiQpsLimit"
	AdminRemoveApiQpsLimit                    = "/admin/rmApiQpsLimit"
	AdminGetCluster                           = "/admin/getCluster"
	AdminSetClusterInfo                       = "/admin/setClusterInfo"
	AdminGetDataPartition                     = "/dataPartition/get"
	AdminLoadDataPartition                    = "/dataPartition/load"
	AdminCreateDataPartition                  = "/dataPartition/create"
	AdminCreatePreLoadDataPartition           = "/dataPartition/createPreLoad"
	AdminDecommissionDataPartition            = "/dataPartition/decommission"
	AdminDiagnoseDataPartition                = "/dataPartition/diagnose"
	AdminResetDataPartitionDecommissionStatus = "/dataPartition/resetDecommissionStatus"
	AdminQueryDataPartitionDecommissionStatus = "/dataPartition/queryDecommissionStatus"
	AdminDeleteDataReplica                    = "/dataReplica/delete"
	AdminAddDataReplica                       = "/dataReplica/add"
	AdminDeleteVol                            = "/vol/delete"
	AdminUpdateVol                            = "/vol/update"
	AdminVolShrink                            = "/vol/shrink"
	AdminVolExpand                            = "/vol/expand"
	AdminCreateVol                            = "/admin/createVol"
	AdminGetVol                               = "/admin/getVol"
	AdminClusterFreeze                        = "/cluster/freeze"
	AdminClusterStat                          = "/cluster/stat"
	AdminSetCheckDataReplicasEnable           = "/cluster/setCheckDataReplicasEnable"
	AdminGetIP                                = "/admin/getIp"
	AdminCreateMetaPartition                  = "/metaPartition/create"
	AdminSetMetaNodeThreshold                 = "/threshold/set"
	AdminListVols                             = "/vol/list"
	AdminSetNodeInfo                          = "/admin/setNodeInfo"
	AdminGetNodeInfo                          = "/admin/getNodeInfo"
	AdminGetAllNodeSetGrpInfo                 = "/admin/getDomainInfo"
	AdminGetNodeSetGrpInfo                    = "/admin/getDomainNodeSetGrpInfo"
	AdminGetIsDomainOn                        = "/admin/getIsDomainOn"
	AdminUpdateNodeSetCapcity                 = "/admin/updateNodeSetCapcity"
	AdminUpdateNodeSetId                      = "/admin/updateNodeSetId"
	AdminUpdateDomainDataUseRatio             = "/admin/updateDomainDataRatio"
	AdminUpdateZoneExcludeRatio               = "/admin/updateZoneExcludeRatio"
	AdminSetNodeRdOnly                        = "/admin/setNodeRdOnly"
	AdminSetDpRdOnly                          = "/admin/setDpRdOnly"
	AdminDataPartitionChangeLeader            = "/dataPartition/changeleader"
	AdminChangeMasterLeader                   = "/master/changeleader"
	AdminOpFollowerPartitionsRead             = "/master/opFollowerPartitionRead"
	AdminUpdateDecommissionLimit              = "/admin/updateDecommissionLimit"
	AdminQueryDecommissionLimit               = "/admin/queryDecommissionLimit"
	AdminQueryDecommissionToken               = "/admin/queryDecommissionToken"
	AdminSetFileStats                         = "/admin/setFileStatsEnable"
	AdminGetFileStats                         = "/admin/getFileStatsEnable"
	AdminGetClusterValue                      = "/admin/getClusterValue"
	AdminSetClusterUuidEnable                 = "/admin/setClusterUuidEnable"
	AdminGetClusterUuid                       = "/admin/getClusterUuid"
	AdminGenerateClusterUuid                  = "/admin/generateClusterUuid"
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

	// qos api
	QosGetStatus           = "/qos/getStatus"
	QosGetClientsLimitInfo = "/qos/getClientsInfo"
	QosGetZoneLimitInfo    = "/qos/getZoneLimit" // include disk enable
	QosUpdate              = "/qos/update"       // include disk enable
	QosUpdateMagnify       = "/qos/updateMagnify"
	QosUpdateClientParam   = "/qos/updateClientParam"
	QosUpdateZoneLimit     = "/qos/updateZoneLimit" // include disk enable
	QosUpload              = "/admin/qosUpload"
	QosUpdateMasterLimit   = "/qos/masterLimit"

	//raft node APIs
	AddRaftNode    = "/raftNode/add"
	RemoveRaftNode = "/raftNode/remove"
	RaftStatus     = "/get/raftStatus"

	// node APIs
	AddDataNode                        = "/dataNode/add"
	DecommissionDataNode               = "/dataNode/decommission"
	QueryDataNodeDecoProgress          = "/dataNode/queryDecommissionProgress"
	QueryDataNodeDecoFailedDps         = "/dataNode/queryDecommissionFailedDps"
	MigrateDataNode                    = "/dataNode/migrate"
	CancelDecommissionDataNode         = "/dataNode/cancelDecommission"
	DecommissionDisk                   = "/disk/decommission"
	RecommissionDisk                   = "/disk/recommission"
	QueryDiskDecoProgress              = "/disk/queryDecommissionProgress"
	MarkDecoDiskFixed                  = "/disk/MarkDecommissionDiskFixed"
	CancelDecommissionDisk             = "/disk/cancelDecommission"
	QueryDecommissionDiskDecoFailedDps = "/disk/queryDecommissionFailedDps"
	GetDataNode                        = "/dataNode/get"
	AddMetaNode                        = "/metaNode/add"
	DecommissionMetaNode               = "/metaNode/decommission"
	MigrateMetaNode                    = "/metaNode/migrate"
	GetMetaNode                        = "/metaNode/get"
	AdminUpdateMetaNode                = "/metaNode/update"
	AdminUpdateDataNode                = "/dataNode/update"
	AdminGetInvalidNodes               = "/invalid/nodes"
	AdminLoadMetaPartition             = "/metaPartition/load"
	AdminDiagnoseMetaPartition         = "/metaPartition/diagnose"
	AdminDecommissionMetaPartition     = "/metaPartition/decommission"
	AdminChangeMetaPartitionLeader     = "/metaPartition/changeleader"
	AdminAddMetaReplica                = "/metaReplica/add"
	AdminDeleteMetaReplica             = "/metaReplica/delete"

	// Operation response
	GetMetaNodeTaskResponse = "/metaNode/response" // Method: 'POST', ContentType: 'application/json'
	GetDataNodeTaskResponse = "/dataNode/response" // Method: 'POST', ContentType: 'application/json'

	GetTopologyView = "/topo/get"
	UpdateZone      = "/zone/update"
	GetAllZones     = "/zone/list"

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

var GApiInfo map[string]string = map[string]string{
	"admingetmasterapilist":           AdminGetMasterApiList,
	"adminsetapiqpslimit":             AdminSetApiQpsLimit,
	"admingetcluster":                 AdminGetCluster,
	"adminsetclusterinfo":             AdminSetClusterInfo,
	"admingetdatapartition":           AdminGetDataPartition,
	"adminloaddatapartition":          AdminLoadDataPartition,
	"admincreatedatapartition":        AdminCreateDataPartition,
	"admincreatepreloaddatapartition": AdminCreatePreLoadDataPartition,
	"admindecommissiondatapartition":  AdminDecommissionDataPartition,
	"admindiagnosedatapartition":      AdminDiagnoseDataPartition,
	"admindeletedatareplica":          AdminDeleteDataReplica,
	"adminadddatareplica":             AdminAddDataReplica,
	"admindeletevol":                  AdminDeleteVol,
	"adminupdatevol":                  AdminUpdateVol,
	"adminvolshrink":                  AdminVolShrink,
	"adminvolexpand":                  AdminVolExpand,
	"admincreatevol":                  AdminCreateVol,
	"admingetvol":                     AdminGetVol,
	"adminclusterfreeze":              AdminClusterFreeze,
	"adminclusterstat":                AdminClusterStat,
	"admingetip":                      AdminGetIP,
	"admincreatemetapartition":        AdminCreateMetaPartition,
	"adminsetmetanodethreshold":       AdminSetMetaNodeThreshold,
	"adminlistvols":                   AdminListVols,
	"adminsetnodeinfo":                AdminSetNodeInfo,
	"admingetnodeinfo":                AdminGetNodeInfo,
	"admingetallnodesetgrpinfo":       AdminGetAllNodeSetGrpInfo,
	"admingetnodesetgrpinfo":          AdminGetNodeSetGrpInfo,
	"admingetisdomainon":              AdminGetIsDomainOn,
	"adminupdatenodesetcapcity":       AdminUpdateNodeSetCapcity,
	"adminupdatenodesetid":            AdminUpdateNodeSetId,
	"adminupdatedomaindatauseratio":   AdminUpdateDomainDataUseRatio,
	"adminupdatezoneexcluderatio":     AdminUpdateZoneExcludeRatio,
	"adminsetnoderdonly":              AdminSetNodeRdOnly,
	"adminsetdprdonly":                AdminSetDpRdOnly,
	"admindatapartitionchangeleader":  AdminDataPartitionChangeLeader,
	//"adminclusterapi":                 AdminClusterAPI,
	//"adminuserapi":                    AdminUserAPI,
	//"adminvolumeapi":                  AdminVolumeAPI,
	//"consoleiql":                      ConsoleIQL,
	//"consoleloginapi":                 ConsoleLoginAPI,
	//"consolemonitorapi":               ConsoleMonitorAPI,
	//"consolefile":                     ConsoleFile,
	//"consolefiledown":                 ConsoleFileDown,
	//"consolefileupload":               ConsoleFileUpload,
	"clientdatapartitions":   ClientDataPartitions,
	"clientvol":              ClientVol,
	"clientmetapartition":    ClientMetaPartition,
	"clientvolstat":          ClientVolStat,
	"clientmetapartitions":   ClientMetaPartitions,
	"qosgetstatus":           QosGetStatus,
	"qosgetclientslimitinfo": QosGetClientsLimitInfo,
	"qosgetzonelimitinfo":    QosGetZoneLimitInfo,
	"qosupdate":              QosUpdate,
	//"qosupdatemagnify":               QosUpdateMagnify,
	"qosupdateclientparam":           QosUpdateClientParam,
	"qosupdatezonelimit":             QosUpdateZoneLimit,
	"qosupload":                      QosUpload,
	"qosupdatemasterlimit":           QosUpdateMasterLimit,
	"addraftnode":                    AddRaftNode,
	"removeraftnode":                 RemoveRaftNode,
	"raftstatus":                     RaftStatus,
	"adddatanode":                    AddDataNode,
	"decommissiondatanode":           DecommissionDataNode,
	"migratedatanode":                MigrateDataNode,
	"canceldecommissiondatanode":     CancelDecommissionDataNode,
	"decommissiondisk":               DecommissionDisk,
	"getdatanode":                    GetDataNode,
	"addmetanode":                    AddMetaNode,
	"decommissionmetanode":           DecommissionMetaNode,
	"migratemetanode":                MigrateMetaNode,
	"getmetanode":                    GetMetaNode,
	"adminupdatemetanode":            AdminUpdateMetaNode,
	"adminupdatedatanode":            AdminUpdateDataNode,
	"admingetinvalidnodes":           AdminGetInvalidNodes,
	"adminloadmetapartition":         AdminLoadMetaPartition,
	"admindiagnosemetapartition":     AdminDiagnoseMetaPartition,
	"admindecommissionmetapartition": AdminDecommissionMetaPartition,
	"adminchangemetapartitionleader": AdminChangeMetaPartitionLeader,
	"adminaddmetareplica":            AdminAddMetaReplica,
	"admindeletemetareplica":         AdminDeleteMetaReplica,
	"getmetanodetaskresponse":        GetMetaNodeTaskResponse,
	"getdatanodetaskresponse":        GetDataNodeTaskResponse,
	"gettopologyview":                GetTopologyView,
	"updatezone":                     UpdateZone,
	"getallzones":                    GetAllZones,
	"usercreate":                     UserCreate,
	"userdelete":                     UserDelete,
	"userupdate":                     UserUpdate,
	"userupdatepolicy":               UserUpdatePolicy,
	"userremovepolicy":               UserRemovePolicy,
	"userdeletevolpolicy":            UserDeleteVolPolicy,
	"usergetinfo":                    UserGetInfo,
	"usergetakinfo":                  UserGetAKInfo,
	"usertransfervol":                UserTransferVol,
	"userlist":                       UserList,
	"usersofvol":                     UsersOfVol,
}

//const TimeFormat = "2006-01-02 15:04:05"

const (
	TimeFormat                 = "2006-01-02 15:04:05"
	DefaultDirChildrenNumLimit = 20000000
	MinDirChildrenNumLimit     = 1000000
)

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
	Cluster                     string
	Ip                          string
	MetaNodeDeleteBatchCount    uint64
	MetaNodeDeleteWorkerSleepMs uint64
	DataNodeDeleteLimitRate     uint64
	DataNodeAutoRepairLimitRate uint64
	DirChildrenNumLimit         uint32
	EbsAddr                     string
	ServicePath                 string
	ClusterUuid                 string
	ClusterUuidEnable           bool
}

// CreateDataPartitionRequest defines the request to create a data partition.
type CreateDataPartitionRequest struct {
	PartitionTyp        int
	PartitionId         uint64
	PartitionSize       int
	ReplicaNum          int
	VolumeId            string
	IsRandomWrite       bool
	Members             []Peer
	Hosts               []string
	CreateType          int
	LeaderSize          int
	DecommissionedDisks []string
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

// RemoveDataPartitionRaftMemberRequest defines the request of add raftMember a data partition.
type RemoveDataPartitionRaftMemberRequest struct {
	PartitionId uint64
	RemovePeer  Peer
	Force       bool
}

// AddMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type AddMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	AddPeer     Peer
}

// RemoveMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type RemoveMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	RemovePeer  Peer
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

type QosToDataNode struct {
	EnableDiskQos     bool
	QosIopsReadLimit  uint64
	QosIopsWriteLimit uint64
	QosFlowReadLimit  uint64
	QosFlowWriteLimit uint64
}

// HeartBeatRequest define the heartbeat request.
type HeartBeatRequest struct {
	CurrTime   int64
	MasterAddr string
	FLReadVols []string
	QosToDataNode
	FileStatsEnable bool
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
}

type DataNodeQosResponse struct {
	IopsRLimit uint64
	IopsWLimit uint64
	FlowRlimit uint64
	FlowWlimit uint64
	Status     uint8
	Result     string
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
	StartTime           int64
	ZoneName            string
	PartitionReports    []*PartitionReport
	Status              uint8
	Result              string
	BadDisks            []string
}

// MetaPartitionReport defines the meta partition report.
type MetaPartitionReport struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Status      int
	Size        uint64
	MaxInodeID  uint64
	IsLeader    bool
	VolName     string
	InodeCnt    uint64
	DentryCnt   uint64
	FreeListLen uint64
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
	PartitionType int
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	Hosts         []string
	LeaderAddr    string
	Epoch         uint64
	IsRecover     bool
	PartitionTTL  int64
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
	FreeListLen uint64
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
	Name           string
	Owner          string
	Status         uint8
	FollowerRead   bool
	MetaPartitions []*MetaPartitionView
	DataPartitions []*DataPartitionResponse
	DomainOn       bool
	OSSSecure      *OSSSecure
	CreateTime     int64
	CacheTTL       int
	VolType        int
}

func (v *VolView) SetOwner(owner string) {
	v.Owner = owner
}

func (v *VolView) SetOSSSecure(accessKey, secretKey string) {
	v.OSSSecure = &OSSSecure{AccessKey: accessKey, SecretKey: secretKey}
}

func NewVolView(name string, status uint8, followerRead bool, createTime int64, cacheTTL int, volType int) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.FollowerRead = followerRead
	view.CreateTime = createTime
	view.Status = status
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.DataPartitions = make([]*DataPartitionResponse, 0)
	view.CacheTTL = cacheTTL
	view.VolType = volType
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

const (
	QosStateNormal   uint8 = 0x01
	QosStateHitLimit uint8 = 0x02

	MinIopsLimit uint64 = 3
	MinFLowLimit uint64 = 128 * util.KB
)

const (
	IopsReadType  uint32 = 0x01
	IopsWriteType uint32 = 0x02
	FlowReadType  uint32 = 0x03
	FlowWriteType uint32 = 0x04
)

const (
	QosDefaultBurst                   = 16000000
	QosDefaultClientCnt        uint32 = 100
	QosDefaultDiskMaxFLowLimit int    = 0x7FFFFFFF
	QosDefaultDiskMaxIoLimit   int    = 100000
)

func QosTypeString(factorType uint32) string {
	switch factorType {
	case IopsReadType:
		return "IopsRead"
	case IopsWriteType:
		return "IopsWrite"
	case FlowReadType:
		return "FlowRead"
	case FlowWriteType:
		return "FlowWrite"
	}
	return "unkown"
}

type ClientLimitInfo struct {
	UsedLimit  uint64
	UsedBuffer uint64
	Used       uint64
	Need       uint64
}

type ClientReportLimitInfo struct {
	ID        uint64
	FactorMap map[uint32]*ClientLimitInfo
	Host      string
	Status    uint8
	reserved  string
}

func NewClientReportLimitInfo() *ClientReportLimitInfo {
	return &ClientReportLimitInfo{
		FactorMap: make(map[uint32]*ClientLimitInfo, 0),
	}
}

type LimitRsp2Client struct {
	ID            uint64
	Enable        bool
	ReqPeriod     uint32
	HitTriggerCnt uint8
	FactorMap     map[uint32]*ClientLimitInfo
	Magnify       map[uint32]uint32
	reserved      string
}

func NewLimitRsp2Client() *LimitRsp2Client {
	limit := &LimitRsp2Client{
		FactorMap: make(map[uint32]*ClientLimitInfo, 0),
		Magnify:   make(map[uint32]uint32, 0),
	}
	return limit
}

// SimpleVolView defines the simple view of a volume
type SimpleVolView struct {
	ID                    uint64
	Name                  string
	Owner                 string
	ZoneName              string
	DpReplicaNum          uint8
	MpReplicaNum          uint8
	InodeCount            uint64
	DentryCount           uint64
	MaxMetaPartitionID    uint64
	Status                uint8
	Capacity              uint64 // GB
	RwDpCnt               int
	MpCnt                 int
	DpCnt                 int
	FollowerRead          bool
	NeedToLowerReplica    bool
	Authenticate          bool
	CrossZone             bool
	DefaultPriority       bool
	DomainOn              bool
	CreateTime            string
	EnableToken           bool
	EnablePosixAcl        bool
	Description           string
	DpSelectorName        string
	DpSelectorParm        string
	DefaultZonePrior      bool
	DpReadOnlyWhenVolFull bool

	VolType          int
	ObjBlockSize     int
	CacheCapacity    uint64
	CacheAction      int
	CacheThreshold   int
	CacheHighWater   int
	CacheLowWater    int
	CacheLruInterval int
	CacheTtl         int
	CacheRule        string
	PreloadCapacity  uint64
}

type NodeSetInfo struct {
	ID           uint64
	ZoneName     string
	Capacity     int
	DataUseRatio float64
	MetaUseRatio float64
	MetaUsed     uint64
	MetaTotal    uint64
	MetaNodes    []*MetaNodeInfo
	DataUsed     uint64
	DataTotal    uint64
	DataNodes    []*DataNodeInfo
}
type SimpleNodeSetGrpInfo struct {
	ID          uint64
	Status      uint8
	NodeSetInfo []NodeSetInfo
}

type SimpleNodeSetGrpInfoList struct {
	DomainId             uint64
	Status               uint8
	SimpleNodeSetGrpInfo []*SimpleNodeSetGrpInfo
}

type DomainNodeSetGrpInfoList struct {
	DomainOn              bool
	DataRatioLimit        float64
	ZoneExcludeRatioLimit float64
	NeedDomain            bool
	ExcludeZones          []string
	DomainNodeSetGrpInfo  []*SimpleNodeSetGrpInfoList
}

// MasterAPIAccessResp defines the response for getting meta partition
type MasterAPIAccessResp struct {
	APIResp APIAccessResp `json:"api_resp"`
	Data    []byte        `json:"data"`
}

type VolInfo struct {
	Name                  string
	Owner                 string
	CreateTime            int64
	Status                uint8
	TotalSize             uint64
	UsedSize              uint64
	DpReadOnlyWhenVolFull bool
}

func NewVolInfo(name, owner string, createTime int64, status uint8, totalSize, usedSize uint64, dpReadOnlyWhenVolFull bool) *VolInfo {
	return &VolInfo{
		Name:                  name,
		Owner:                 owner,
		CreateTime:            createTime,
		Status:                status,
		TotalSize:             totalSize,
		UsedSize:              usedSize,
		DpReadOnlyWhenVolFull: dpReadOnlyWhenVolFull,
	}
}

// ZoneView define the view of zone
type ZoneView struct {
	Name    string
	Status  string
	NodeSet map[uint64]*NodeSetView
}

type NodeSetView struct {
	DataNodeLen int
	MetaNodeLen int
	MetaNodes   []NodeView
	DataNodes   []NodeView
}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	Zones []*ZoneView
}

const (
	PartitionTypeNormal  = 0
	PartitionTypeCache   = 1
	PartitionTypePreLoad = 2
)

func GetDpType(volType int, isPreload bool) int {

	if volType == VolumeTypeHot {
		return PartitionTypeNormal
	}

	if isPreload {
		return PartitionTypePreLoad
	}

	return PartitionTypeCache
}

func IsCacheDp(typ int) bool {
	return typ == PartitionTypeCache
}

func IsNormalDp(typ int) bool {
	return typ == PartitionTypeNormal
}

func IsPreLoadDp(typ int) bool {
	return typ == PartitionTypePreLoad
}

const (
	VolumeTypeHot  = 0
	VolumeTypeCold = 1
)

func IsCold(typ int) bool {
	return typ == VolumeTypeCold
}

func IsHot(typ int) bool {
	return typ == VolumeTypeHot
}

const (
	NoCache = 0
	RCache  = 1
	RWCache = 2
)

const (
	LFClient = 1 // low frequency client
)
