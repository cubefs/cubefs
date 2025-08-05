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

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

type ContextUserKey string

// api
const (
	// Admin APIs
	AdminGetMasterApiList                     = "/admin/getMasterApiList"
	AdminSetApiQpsLimit                       = "/admin/setApiQpsLimit"
	AdminGetApiQpsLimit                       = "/admin/getApiQpsLimit"
	AdminRemoveApiQpsLimit                    = "/admin/rmApiQpsLimit"
	AdminGetCluster                           = "/admin/getCluster"
	AdminSetClusterInfo                       = "/admin/setClusterInfo"
	AdminGetMonitorPushAddr                   = "/admin/getMonitorPushAddr"
	AdminGetClusterDataNodes                  = "/admin/cluster/getAllDataNodes"
	AdminGetClusterMetaNodes                  = "/admin/cluster/getAllMetaNodes"
	AdminGetDataPartition                     = "/dataPartition/get"
	AdminLoadDataPartition                    = "/dataPartition/load"
	AdminCreateDataPartition                  = "/dataPartition/create"
	AdminCreatePreLoadDataPartition           = "/dataPartition/createPreLoad"
	AdminDecommissionDataPartition            = "/dataPartition/decommission"
	AdminDiagnoseDataPartition                = "/dataPartition/diagnose"
	AdminResetDataPartitionDecommissionStatus = "/dataPartition/resetDecommissionStatus"
	AdminQueryDataPartitionDecommissionStatus = "/dataPartition/queryDecommissionStatus"
	AdminCheckReplicaMeta                     = "/dataPartition/checkReplicaMeta"
	AdminRecoverReplicaMeta                   = "/dataPartition/recoverReplicaMeta"
	AdminRecoverBackupDataReplica             = "/dataPartition/recoverBackupDataReplica"
	AdminDeleteDataReplica                    = "/dataReplica/delete"
	AdminAddDataReplica                       = "/dataReplica/add"
	AdminDeleteVol                            = "/vol/delete"
	AdminUpdateVol                            = "/vol/update"
	AdminVolShrink                            = "/vol/shrink"
	AdminVolExpand                            = "/vol/expand"
	AdminVolForbidden                         = "/vol/forbidden"
	AdminVolEnableAuditLog                    = "/vol/auditlog"
	AdminVolSetDpRepairBlockSize              = "/vol/setDpRepairBlockSize"
	AdminCreateVol                            = "/admin/createVol"
	AdminGetVol                               = "/admin/getVol"
	AdminClusterFreeze                        = "/cluster/freeze"
	AdminClusterForbidMpDecommission          = "/cluster/forbidMetaPartitionDecommission"
	AdminClusterStat                          = "/cluster/stat"
	AdminSetCheckDataReplicasEnable           = "/cluster/setCheckDataReplicasEnable"
	AdminGetIP                                = "/admin/getIp"
	AdminCreateMetaPartition                  = "/metaPartition/create"
	AdminSetMetaNodeThreshold                 = "/threshold/set"
	AdminSetMasterVolDeletionDelayTime        = "/volDeletionDelayTime/set"
	AdminListVols                             = "/vol/list"
	AdminSetNodeInfo                          = "/admin/setNodeInfo"
	AdminGetNodeInfo                          = "/admin/getNodeInfo"
	AdminGetAllNodeSetGrpInfo                 = "/admin/getDomainInfo"
	AdminGetNodeSetGrpInfo                    = "/admin/getDomainNodeSetGrpInfo"
	AdminGetIsDomainOn                        = "/admin/getIsDomainOn"
	AdminUpdateNodeSetCapcity                 = "/admin/updateNodeSetCapcity"
	AdminUpdateNodeSetId                      = "/admin/updateNodeSetId"
	AdminUpdateNodeSetNodeSelector            = "/admin/updateNodeSetNodeSelector"
	AdminUpdateDomainDataUseRatio             = "/admin/updateDomainDataRatio"
	AdminUpdateZoneExcludeRatio               = "/admin/updateZoneExcludeRatio"
	AdminSetNodeRdOnly                        = "/admin/setNodeRdOnly"
	AdminSetDpRdOnly                          = "/admin/setDpRdOnly"
	AdminSetConfig                            = "/admin/setConfig"
	AdminGetConfig                            = "/admin/getConfig"
	AdminDataPartitionChangeLeader            = "/dataPartition/changeleader"
	AdminChangeMasterLeader                   = "/master/changeleader"
	AdminOpFollowerPartitionsRead             = "/master/opFollowerPartitionRead"
	AdminUpdateDecommissionLimit              = "/admin/updateDecommissionLimit"
	AdminQueryDecommissionLimit               = "/admin/queryDecommissionLimit"
	AdminQueryDecommissionFailedDisk          = "/admin/queryDecommissionFailedDisk"
	AdminAbortDecommissionDisk                = "/admin/abortDecommissionDisk"
	AdminResetDataPartitionRestoreStatus      = "/admin/resetDataPartitionRestoreStatus"
	AdminGetOpLog                             = "/admin/getOpLog"

	// #nosec G101
	AdminQueryDecommissionToken = "/admin/queryDecommissionToken"
	AdminSetFileStats           = "/admin/setFileStatsEnable"
	AdminGetFileStats           = "/admin/getFileStatsEnable"
	AdminGetClusterValue        = "/admin/getClusterValue"
	AdminSetClusterUuidEnable   = "/admin/setClusterUuidEnable"
	AdminGetClusterUuid         = "/admin/getClusterUuid"
	AdminGenerateClusterUuid    = "/admin/generateClusterUuid"
	AdminSetDpDiscard           = "/admin/setDpDiscard"
	AdminGetDiscardDp           = "/admin/getDiscardDp"

	AdminSetConLcNodeNum  = "/admin/setConLcNodeNum"
	AdminGetAllLcNodeInfo = "/admin/getAllLcNodeInfo"

	AdminLcNode = "/admin/lcnode"

	AdminUpdateDecommissionDiskLimit = "/admin/updateDecommissionDiskLimit"
	AdminEnableAutoDecommissionDisk  = "/admin/enableAutoDecommissionDisk"
	AdminQueryAutoDecommissionDisk   = "/admin/queryAutoDecommissionDisk"
	// graphql master api
	AdminClusterAPI               = "/api/cluster"
	AdminUserAPI                  = "/api/user"
	AdminVolumeAPI                = "/api/volume"
	AdminSetDiskBrokenThreshold   = "/admin/setDiskBrokenThreshold"
	AdminQueryDiskBrokenThreshold = "/admin/queryDiskBrokenThreshold"

	AdminGetUpgradeCompatibleSettings = "/admin/getUpgradeCompatibleSettings"

	// graphql coonsole api
	ConsoleIQL        = "/iql"
	ConsoleLoginAPI   = "/login"
	ConsoleMonitorAPI = "/cfs_monitor"
	ConsoleFile       = "/file"
	ConsoleFileDown   = "/file/down"
	ConsoleFileUpload = "/file/upload"

	// Client APIs
	ClientDataPartitions     = "/client/partitions"
	ClientDiskDataPartitions = "/client/disk/partitions"
	ClientVol                = "/client/vol"
	ClientMetaPartition      = "/metaPartition/get"
	ClientVolStat            = "/client/volStat"
	ClientMetaPartitions     = "/client/metaPartitions"
	GetAllClients            = "/getAllClients"

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

	// acl api
	AdminACL = "/admin/aclOp"
	// uid api
	AdminUid = "/admin/uidOp"

	// raft node APIs
	AddRaftNode    = "/raftNode/add"
	RemoveRaftNode = "/raftNode/remove"
	RaftStatus     = "/get/raftStatus"

	// node APIs

	AddDataNode                        = "/dataNode/add"
	DecommissionDataNode               = "/dataNode/decommission"
	QueryDataNodeDecoProgress          = "/dataNode/queryDecommissionProgress"
	QueryDataNodeDecoFailedDps         = "/dataNode/queryDecommissionFailedDps"
	MigrateDataNode                    = "/dataNode/migrate"
	PauseDecommissionDataNode          = "/dataNode/pauseDecommission"
	CancelDecommissionDataNode         = "/dataNode/cancelDecommission"
	ResetDecommissionDataNodeStatus    = "/dataNode/resetDecommissionStatus"
	DecommissionDisk                   = "/disk/decommission"
	RecommissionDisk                   = "/disk/recommission"
	QueryDiskDecoProgress              = "/disk/queryDecommissionProgress"
	DeleteDecommissionDiskRecord       = "/disk/deleteDecommissionDiskRecord"
	PauseDecommissionDisk              = "/disk/pauseDecommission"
	CancelDecommissionDisk             = "/disk/cancelDecommission"
	ResetDecommissionDiskStatus        = "/disk/resetDecommissionStatus"
	QueryDecommissionDiskDecoFailedDps = "/disk/queryDecommissionFailedDps"
	QueryBadDisks                      = "/disk/queryBadDisks"
	QueryDisks                         = "/disk/queryDisks"
	QueryDiskDetail                    = "/disk/detail"
	RestoreStoppedAutoDecommissionDisk = "/disk/restoreStoppedAutoDecommissionDisk"
	QueryAllDecommissionDisk           = "/disk/queryAllDecommissionDisk"
	RecoverBadDisk                     = "/disk/recoverBadDisk"
	QueryBadDiskRecoverProgress        = "/disk/queryBadDiskRecoverProgress"
	DeleteBackupDirectories            = "/disk/deleteBackupDirectories"
	QueryBackupDirectories             = "/disk/queryBackupDirectories"
	GetDataNode                        = "/dataNode/get"
	SetDpCntLimit                      = "/dataNode/setDpCntLimit"
	AddMetaNode                        = "/metaNode/add"
	SetMpCntLimit                      = "/metaNode/setMpCntLimit"
	DecommissionMetaNode               = "/metaNode/decommission"
	MigrateMetaNode                    = "/metaNode/migrate"
	MigrateMetaPartition               = "/metaNode/migratePartition"
	MigrateResult                      = "/metaNode/migrateResult"
	GetMetaNode                        = "/metaNode/get"
	AdminUpdateMetaNode                = "/metaNode/update"
	CreateBalanceTask                  = "/metaNode/createBalanceTask"
	GetBalanceTask                     = "/metaNode/getBalanceTask"
	RunBalanceTask                     = "/metaNode/runBalanceTask"
	StopBalanceTask                    = "/metaNode/stopBalanceTask"
	DeleteBalanceTask                  = "/metaNode/deleteBalanceTask"
	AdminUpdateDataNode                = "/dataNode/update"
	AdminGetInvalidNodes               = "/invalid/nodes"
	AdminLoadMetaPartition             = "/metaPartition/load"
	AdminDiagnoseMetaPartition         = "/metaPartition/diagnose"
	AdminDecommissionMetaPartition     = "/metaPartition/decommission"
	AdminChangeMetaPartitionLeader     = "/metaPartition/changeleader"
	AdminBalanceMetaPartitionLeader    = "/metaPartition/balanceLeader"
	AdminMetaPartitionEmptyStatus      = "/metaPartition/emptyStatus"
	AdminMetaPartitionFreezeEmpty      = "/metaPartition/freezeEmpty"
	AdminMetaPartitionCleanEmpty       = "/metaPartition/cleanEmpty"
	AdminMetaPartitionRemoveBackup     = "/metaPartition/removeBackup"
	AdminMetaPartitionGetCleanTask     = "/metaPartition/getCleanTask"
	AdminAddMetaReplica                = "/metaReplica/add"
	AdminDeleteMetaReplica             = "/metaReplica/delete"
	AdminPutDataPartitions             = "/dataPartitions/set"

	// admin multi version snapshot
	AdminCreateVersion     = "/multiVer/create"
	AdminDelVersion        = "/multiVer/del"
	AdminGetVersionInfo    = "/multiVer/get"
	AdminGetAllVersionInfo = "/multiVer/getAll"
	AdminGetVolVer         = "/vol/getVer"
	AdminSetVerStrategy    = "/vol/SetVerStrategy"

	// S3 lifecycle configuration APIS
	SetBucketLifecycle    = "/s3/setLifecycle"
	GetBucketLifecycle    = "/s3/getLifecycle"
	DeleteBucketLifecycle = "/s3/deleteLifecycle"

	AddLcNode = "/lcNode/add"

	QueryDisableDisk = "/dataNode/queryDisableDisk"
	// Operation response
	GetMetaNodeTaskResponse = "/metaNode/response" // Method: 'POST', ContentType: 'application/json'
	GetDataNodeTaskResponse = "/dataNode/response" // Method: 'POST', ContentType: 'application/json'
	GetLcNodeTaskResponse   = "/lcNode/response"   // Method: 'POST', ContentType: 'application/json'

	GetTopologyView = "/topo/get"
	UpdateZone      = "/zone/update"
	GetAllZones     = "/zone/list"
	GetAllNodeSets  = "/nodeSet/list"
	GetNodeSet      = "/nodeSet/get"
	UpdateNodeSet   = "/nodeSet/update"

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
	// graphql api for header
	HeadAuthorized  = "Authorization"
	ParamAuthorized = "_authorization"
	UserKey         = ContextUserKey("_user_key")
	UserInfoKey     = ContextUserKey("_user_info_key")
	// quota
	QuotaCreate = "/quota/create"
	QuotaUpdate = "/quota/update"
	QuotaDelete = "/quota/delete"
	QuotaList   = "/quota/list"
	QuotaGet    = "/quota/get"
	// QuotaBatchModifyPath = "/quota/batchModifyPath"
	QuotaListAll = "/quota/listAll"
	// trash
	AdminSetTrashInterval              = "/vol/setTrashInterval"
	AdminSetVolAccessTimeValidInterval = "/vol/setAccessTimeValidInterval"

	// s3 qos api
	S3QoSSet                     = "/s3/qos/set"
	S3QoSGet                     = "/s3/qos/get"
	S3QoSDelete                  = "/s3/qos/delete"
	AdminEnablePersistAccessTime = "/vol/enablePersistAccessTime"

	AdminVolAddAllowedStorageClass = "/vol/addAllowedStorageClass"
	// FlashNode API
	FlashNodeAdd    = "/flashNode/add"
	FlashNodeSet    = "/flashNode/set"
	FlashNodeRemove = "/flashNode/remove"
	FlashNodeGet    = "/flashNode/get"
	FlashNodeList   = "/flashNode/list"

	// FlashGroup API
	AdminFlashGroupTurn       = "/flashGroup/turn"
	AdminFlashGroupCreate     = "/flashGroup/create"
	AdminFlashGroupSet        = "/flashGroup/set"
	AdminFlashGroupRemove     = "/flashGroup/remove"
	AdminFlashGroupNodeAdd    = "/flashGroup/addFlashNode"
	AdminFlashGroupNodeRemove = "/flashGroup/removeFlashNode"
	AdminFlashGroupGet        = "/flashGroup/get"
	AdminFlashGroupList       = "/flashGroup/list"
	ClientFlashGroups         = "/client/flashGroups"
)

var GApiInfo map[string]string = map[string]string{
	"admingetmasterapilist":              AdminGetMasterApiList,
	"adminsetapiqpslimit":                AdminSetApiQpsLimit,
	"admingetcluster":                    AdminGetCluster,
	"adminsetclusterinfo":                AdminSetClusterInfo,
	"admingetdatapartition":              AdminGetDataPartition,
	"adminloaddatapartition":             AdminLoadDataPartition,
	"admincreatedatapartition":           AdminCreateDataPartition,
	"admincreatepreloaddatapartition":    AdminCreatePreLoadDataPartition,
	"admindecommissiondatapartition":     AdminDecommissionDataPartition,
	"admindiagnosedatapartition":         AdminDiagnoseDataPartition,
	"admindeletedatareplica":             AdminDeleteDataReplica,
	"adminadddatareplica":                AdminAddDataReplica,
	"admindeletevol":                     AdminDeleteVol,
	"adminupdatevol":                     AdminUpdateVol,
	"adminvolshrink":                     AdminVolShrink,
	"adminvolexpand":                     AdminVolExpand,
	"adminvoladdallowedstorageclass":     AdminVolAddAllowedStorageClass,
	"admincreatevol":                     AdminCreateVol,
	"admingetvol":                        AdminGetVol,
	"adminclusterfreeze":                 AdminClusterFreeze,
	"adminclusterforbidmpdecommission":   AdminClusterForbidMpDecommission,
	"adminclusterstat":                   AdminClusterStat,
	"admingetip":                         AdminGetIP,
	"admincreatemetapartition":           AdminCreateMetaPartition,
	"adminsetmetanodethreshold":          AdminSetMetaNodeThreshold,
	"adminsetmastervoldeletiondelaytime": AdminSetMasterVolDeletionDelayTime,
	"adminlistvols":                      AdminListVols,
	"adminsetnodeinfo":                   AdminSetNodeInfo,
	"admingetnodeinfo":                   AdminGetNodeInfo,
	"admingetallnodesetgrpinfo":          AdminGetAllNodeSetGrpInfo,
	"admingetnodesetgrpinfo":             AdminGetNodeSetGrpInfo,
	"admingetisdomainon":                 AdminGetIsDomainOn,
	"adminupdatenodesetcapcity":          AdminUpdateNodeSetCapcity,
	"adminupdatenodesetid":               AdminUpdateNodeSetId,
	"adminupdatedomaindatauseratio":      AdminUpdateDomainDataUseRatio,
	"adminupdatezoneexcluderatio":        AdminUpdateZoneExcludeRatio,
	"adminsetnoderdonly":                 AdminSetNodeRdOnly,
	"adminsetdprdonly":                   AdminSetDpRdOnly,
	"admindatapartitionchangeleader":     AdminDataPartitionChangeLeader,
	"adminsetdpdiscard":                  AdminSetDpDiscard,
	"admingetdiscarddp":                  AdminGetDiscardDp,
	"admingetoplog":                      AdminGetOpLog,

	// "adminclusterapi":                 AdminClusterAPI,
	// "adminuserapi":                    AdminUserAPI,
	// "adminvolumeapi":                  AdminVolumeAPI,
	// "consoleiql":                      ConsoleIQL,
	// "consoleloginapi":                 ConsoleLoginAPI,
	// "consolemonitorapi":               ConsoleMonitorAPI,
	// "consolefile":                     ConsoleFile,
	// "consolefiledown":                 ConsoleFileDown,
	// "consolefileupload":               ConsoleFileUpload,
	"clientdatapartitions":   ClientDataPartitions,
	"clientvol":              ClientVol,
	"clientmetapartition":    ClientMetaPartition,
	"clientvolstat":          ClientVolStat,
	"clientmetapartitions":   ClientMetaPartitions,
	"qosgetstatus":           QosGetStatus,
	"qosgetclientslimitinfo": QosGetClientsLimitInfo,
	"qosgetzonelimitinfo":    QosGetZoneLimitInfo,
	"qosupdate":              QosUpdate,
	// "qosupdatemagnify":               QosUpdateMagnify,
	"qosupdateclientparam":            QosUpdateClientParam,
	"qosupdatezonelimit":              QosUpdateZoneLimit,
	"qosupload":                       QosUpload,
	"qosupdatemasterlimit":            QosUpdateMasterLimit,
	"addraftnode":                     AddRaftNode,
	"removeraftnode":                  RemoveRaftNode,
	"raftstatus":                      RaftStatus,
	"adddatanode":                     AddDataNode,
	"decommissiondatanode":            DecommissionDataNode,
	"migratedatanode":                 MigrateDataNode,
	"canceldecommissiondatanode":      CancelDecommissionDataNode,
	"decommissiondisk":                DecommissionDisk,
	"getdatanode":                     GetDataNode,
	"addmetanode":                     AddMetaNode,
	"decommissionmetanode":            DecommissionMetaNode,
	"migratemetanode":                 MigrateMetaNode,
	"getmetanode":                     GetMetaNode,
	"adminupdatemetanode":             AdminUpdateMetaNode,
	"adminupdatedatanode":             AdminUpdateDataNode,
	"admingetinvalidnodes":            AdminGetInvalidNodes,
	"adminloadmetapartition":          AdminLoadMetaPartition,
	"admindiagnosemetapartition":      AdminDiagnoseMetaPartition,
	"admindecommissionmetapartition":  AdminDecommissionMetaPartition,
	"adminchangemetapartitionleader":  AdminChangeMetaPartitionLeader,
	"adminbalancemetapartitionleader": AdminBalanceMetaPartitionLeader,
	"adminaddmetareplica":             AdminAddMetaReplica,
	"admindeletemetareplica":          AdminDeleteMetaReplica,
	"getmetanodetaskresponse":         GetMetaNodeTaskResponse,
	"getdatanodetaskresponse":         GetDataNodeTaskResponse,
	"gettopologyview":                 GetTopologyView,
	"updatezone":                      UpdateZone,
	"getallzones":                     GetAllZones,
	"usercreate":                      UserCreate,
	"userdelete":                      UserDelete,
	"userupdate":                      UserUpdate,
	"userupdatepolicy":                UserUpdatePolicy,
	"userremovepolicy":                UserRemovePolicy,
	"userdeletevolpolicy":             UserDeleteVolPolicy,
	"usergetinfo":                     UserGetInfo,
	"usergetakinfo":                   UserGetAKInfo,
	"usertransfervol":                 UserTransferVol,
	"userlist":                        UserList,
	"usersofvol":                      UsersOfVol,
}

const (
	MetaFollowerReadKey    = "metaFollowerRead"
	MaximallyReadKey       = "maximallyRead"
	LeaderRetryTimeoutKey  = "leaderRetryTimeout"
	VolEnableDirectRead    = "directRead"
	HostKey                = "host"
	ClientVerKey           = "clientVer"
	RoleKey                = "role"
	BcacheOnlyForNotSSDKey = "enableBcacheNotSSD"
	EnableRemoteCache      = "enableRemoteCache"
)

// const TimeFormat = "2006-01-02 15:04:05"

const (
	TimeFormat                 = "2006-01-02 15:04:05"
	DefaultDirChildrenNumLimit = 20000000
	MinDirChildrenNumLimit     = 1000000
)

const (
	CfgHttpPoolSize     = "httpPoolSize"
	defaultHttpPoolSize = 128
)

type HttpCfg struct {
	PoolSize int
}

func GetHttpTransporter(cfg *HttpCfg) *http.Transport {
	if cfg.PoolSize < defaultHttpPoolSize {
		cfg.PoolSize = defaultHttpPoolSize
	}

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.PoolSize,
		MaxIdleConnsPerHost:   cfg.PoolSize / 2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// HTTPReply uniform response structure
type HTTPReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

type HTTPReplyRaw struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func (raw *HTTPReplyRaw) Unmarshal(body []byte) error {
	r := new(HTTPReplyRaw)
	if err := json.Unmarshal(body, r); err != nil {
		return fmt.Errorf("httpreply unmarshal [%s]", err.Error())
	}
	*raw = *r
	return nil
}

func (raw *HTTPReplyRaw) Success() error {
	if code := raw.Code; code != ErrCodeSuccess {
		err := ParseErrorCode(code)
		return fmt.Errorf("httpreply code[%d] err[%s] msg[%s]", code, err.Error(), raw.Msg)
	}
	return nil
}

func (raw *HTTPReplyRaw) Bytes() []byte {
	return raw.Data
}

func (raw *HTTPReplyRaw) String() string {
	return string(raw.Bytes())
}

func (raw *HTTPReplyRaw) Int64() (int64, error) {
	return strconv.ParseInt(string(raw.Data), 10, 64)
}

func (raw *HTTPReplyRaw) Uint64() (uint64, error) {
	return strconv.ParseUint(string(raw.Data), 10, 64)
}

func (raw *HTTPReplyRaw) Result(result interface{}) error {
	return json.Unmarshal(raw.Data, result)
}

func UnmarshalHTTPReply(body []byte, result interface{}) error {
	raw := new(HTTPReplyRaw)
	if err := raw.Unmarshal(body); err != nil {
		return err
	}
	if err := raw.Success(); err != nil {
		return err
	}

	if result == nil {
		return nil
	}
	switch v := result.(type) {
	case *string:
		*v = raw.String()
	case *int64:
		val, err := raw.Int64()
		if err != nil {
			return err
		}
		*v = val
	case *uint64:
		val, err := raw.Uint64()
		if err != nil {
			return err
		}
		*v = val
	default:
		return raw.Result(result)
	}
	return nil
}

// RegisterMetaNodeResp defines the response to register a meta node.
type RegisterMetaNodeResp struct {
	ID uint64
}

type AclIpInfo struct {
	Ip    string
	CTime int64
}

type AclRsp struct {
	Info    string
	OK      bool
	List    []*AclIpInfo
	Reserve string
}

type UidSpaceRsp struct {
	Info        string
	OK          bool
	UidSpaceArr []*UidSpaceInfo
	Reserve     string
}

type VolumeVerStrategy struct {
	KeepVerCnt  int
	Periodic    int
	Enable      bool
	ForceUpdate bool
	UTime       time.Time
}

func (v *VolumeVerStrategy) GetPeriodic() int {
	return v.Periodic
}

func (v *VolumeVerStrategy) GetPeriodicSecond() int {
	// return v.Periodic*24*3600
	return v.Periodic * 3600
}

func (v *VolumeVerStrategy) TimeUp(curTime time.Time) bool {
	return v.UTime.Add(time.Second * time.Duration(v.GetPeriodicSecond())).Before(curTime)
}

type VolumeVerInfo struct {
	Name             string
	VerSeq           uint64
	VerSeqPrepare    uint64
	VerPrepareStatus uint8
	Enabled          bool
}

// ClusterInfo defines the cluster infomation.
type ClusterInfo struct {
	Cluster                            string
	Ip                                 string
	MetaNodeDeleteBatchCount           uint64
	MetaNodeDeleteWorkerSleepMs        uint64
	DataNodeDeleteLimitRate            uint64
	DataNodeAutoRepairLimitRate        uint64
	DpMaxRepairErrCnt                  uint64
	DirChildrenNumLimit                uint32
	EbsAddr                            string
	ServicePath                        string
	ClusterUuid                        string
	ClusterUuidEnable                  bool
	ClusterEnableSnapshot              bool
	RaftPartitionCanUsingDifferentPort bool
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
	IsMultiVer          bool
	VerSeq              uint64
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
	Force             bool
	DecommissionType  uint32
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
	AutoRemove  bool
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

type StopDataPartitionRepairRequest struct {
	PartitionId uint64
	Stop        bool
}

// DeleteDataPartitionResponse defines the response to the request of deleting a data partition.
type StopDataPartitionRepairResponse struct {
	Status      uint8
	Result      string
	PartitionId uint64
}

type RecoverDataReplicaMetaRequest struct {
	PartitionId uint64
	Peers       []Peer
	Hosts       []string
}

// File defines the file struct.
type File struct {
	Name     string
	Crc      uint32
	Size     uint32
	Modified int64
	ApplyID  uint64
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

type UidLimitToMetaNode struct {
	UidLimitInfo []*UidSpaceInfo
}

type QosToDataNode struct {
	EnableDiskQos     bool
	QosIopsReadLimit  uint64
	QosIopsWriteLimit uint64
	QosFlowReadLimit  uint64
	QosFlowWriteLimit uint64
}

// MultiVersionOpRequest defines the request of
type MultiVersionOpRequest struct {
	VolumeID   string
	VerSeq     uint64
	Op         uint8
	Addr       string
	VolVerList []*VolVersionInfo
}

// MultiVersionOpResponse defines the response to the request of l.
type MultiVersionOpResponse struct {
	VolumeID string
	Addr     string
	Op       uint8
	VerSeq   uint64
	Status   uint8
	Result   string
}

type QuotaHeartBeatInfos struct {
	QuotaHbInfos []*QuotaHeartBeatInfo
}

type TxInfo struct {
	Volume     string
	Mask       TxOpMask
	OpLimitVal int
}

type TxInfos struct {
	TxInfo []*TxInfo
}

type FlashNodeHeartBeatInfos struct {
	FlashNodeHandleReadTimeout   int
	FlashNodeReadDataNodeTimeout int
}

// HeartBeatRequest define the heartbeat request.
type HeartBeatRequest struct {
	CurrTime   int64
	MasterAddr string
	FLReadVols []string
	QosToDataNode
	FileStatsEnable                           bool
	RaftPartitionCanUsingDifferentPortEnabled bool
	UidLimitToMetaNode
	QuotaHeartBeatInfos
	TxInfos
	ForbiddenVols        []string
	DisableAuditVols     []string
	DecommissionDisks    []string // NOTE: for datanode
	VolDpRepairBlockSize map[string]uint64
	DpBackupTimeout      string

	NotifyForbidWriteOpOfProtoVer0 bool     // whether forbid by node granularity, will notify to nodes
	VolsForbidWriteOpOfProtoVer0   []string // whether forbid by volume granularity, will notify to partitions of volume in nodes
	DirectReadVols                 []string
	FlashNodeHeartBeatInfos
}

// DataPartitionReport defines the partition report.
type DataPartitionReport struct {
	VolName                    string
	PartitionID                uint64
	PartitionStatus            int
	Total                      uint64
	Used                       uint64
	DiskPath                   string
	IsLeader                   bool
	ExtentCount                int
	NeedCompare                bool
	DecommissionRepairProgress float64
	LocalPeers                 []Peer
	TriggerDiskError           bool
	ForbidWriteOpOfProtoVer0   bool
}

type DataNodeQosResponse struct {
	IopsRLimit uint64
	IopsWLimit uint64
	FlowRlimit uint64
	FlowWlimit uint64
	Status     uint8
	Result     string
}

type BadDiskStat struct {
	DiskPath             string
	TotalPartitionCnt    int
	DiskErrPartitionList []uint64
}

type DiskStat struct {
	Status   int
	DiskPath string

	Total     uint64
	Used      uint64
	Available uint64
	IOUtil    float64

	TotalPartitionCnt int

	DiskErrPartitionList []uint64
}

// DataNodeHeartbeatResponse defines the response to the data node heartbeat.
type DataNodeHeartbeatResponse struct {
	Total                            uint64
	Used                             uint64
	Available                        uint64
	TotalPartitionSize               uint64 // volCnt * volsize
	RemainingCapacity                uint64 // remaining capacity to create partition
	CreatedPartitionCnt              uint32
	MaxCapacity                      uint64 // maximum capacity to create partition
	StartTime                        int64
	ZoneName                         string
	PartitionReports                 []*DataPartitionReport
	Status                           uint8
	Result                           string
	AllDisks                         []string
	DiskStats                        []DiskStat
	BadDisks                         []string           // Keep this old field for compatibility
	BadDiskStats                     []BadDiskStat      // key: disk path
	CpuUtil                          float64            `json:"cpuUtil"`
	IoUtils                          map[string]float64 `json:"ioUtil"`
	BackupDataPartitions             []BackupDataPartitionInfo
	DiskOpLogs                       []OpLog `json:"DiskOpLog"`
	DpOpLogs                         []OpLog `json:"DpOpLog"`
	ReceivedForbidWriteOpOfProtoVer0 bool
}

type OpLog struct {
	Name  string
	Op    string
	Count int32
}

// MetaPartitionReport defines the meta partition report.
type MetaPartitionReport struct {
	PartitionID               uint64
	Start                     uint64
	End                       uint64
	Status                    int
	Size                      uint64
	MaxInodeID                uint64
	IsLeader                  bool
	VolName                   string
	InodeCnt                  uint64
	DentryCnt                 uint64
	TxCnt                     uint64
	TxRbInoCnt                uint64
	TxRbDenCnt                uint64
	FreeListLen               uint64
	ForbidWriteOpOfProtoVer0  bool
	UidInfo                   []*UidReportSpaceInfo
	QuotaReportInfos          []*QuotaReportInfo
	StatByStorageClass        []*StatOfStorageClass
	StatByMigrateStorageClass []*StatOfStorageClass
	LocalPeers                []Peer
}

// MetaNodeHeartbeatResponse defines the response to the meta node heartbeat request.
type MetaNodeHeartbeatResponse struct {
	ZoneName                         string
	Total                            uint64
	Used                             uint64
	MetaPartitionReports             []*MetaPartitionReport
	Status                           uint8
	Result                           string
	CpuUtil                          float64 `json:"cpuUtil"`
	ReceivedForbidWriteOpOfProtoVer0 bool
}

// LcNodeHeartbeatResponse defines the response to the lc node heartbeat.
type LcNodeHeartbeatResponse struct {
	Status                uint8
	Result                string
	LcTaskCountLimit      int
	LcScanningTasks       map[string]*LcNodeRuleTaskResponse
	SnapshotScanningTasks map[string]*SnapshotVerDelTaskResponse
}

type FlashNodeDiskCacheStat struct {
	DataPath  string
	Medium    string
	Total     int64
	MaxAlloc  int64
	HasAlloc  int64
	FreeSpace int64
	HitRate   float64
	Evicts    int
	ReadRps   int
	KeyNum    int
	Status    int
}

// FlashNodeHeartbeatResponse defines the response to the flash node heartbeat.
type FlashNodeHeartbeatResponse struct {
	Status   uint8
	Result   string
	Version  string
	ZoneName string
	Stat     []*FlashNodeDiskCacheStat
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
	CommittedID uint64
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
	IsDiscard     bool
	MediaType     uint32
}

// DataPartitionsView defines the view of a data partition
type DataPartitionsView struct {
	DataPartitions []*DataPartitionResponse
	VolReadOnly    bool // if true, refresh dps even rw count less than 1
	StatByClass    []*StatOfStorageClass
}

type DiskDataPartitionsView struct {
	DataPartitions []*DataPartitionReport
}

func NewDataPartitionsView() (dataPartitionsView *DataPartitionsView) {
	dataPartitionsView = new(DataPartitionsView)
	dataPartitionsView.DataPartitions = make([]*DataPartitionResponse, 0)
	dataPartitionsView.VolReadOnly = false
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
	TxCnt       uint64
	TxRbInoCnt  uint64
	TxRbDenCnt  uint64
	IsRecover   bool
	Members     []string
	LeaderAddr  string
	Status      int8
	IsFreeze    bool
}

type DataNodeDisksRequest struct{}

type DataNodeDisksResponse struct{}

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
	DeleteLockTime int64
	CacheTTL       int
	VolType        int
}

func (v *VolView) SetOwner(owner string) {
	v.Owner = owner
}

func (v *VolView) SetOSSSecure(accessKey, secretKey string) {
	v.OSSSecure = &OSSSecure{AccessKey: accessKey, SecretKey: secretKey}
}

func NewVolView(name string, status uint8, followerRead bool, createTime int64, cacheTTL int, volType int, deleteLockTime int64) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.FollowerRead = followerRead
	view.CreateTime = createTime
	view.DeleteLockTime = deleteLockTime
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
	QosDefaultBurst                   = 1600000
	QosDefaultClientCnt        uint32 = 100
	QosDefaultDiskMaxFLowLimit int    = 0x7FFFFFFF
	QosDefaultDiskMaxIoLimit   int    = 100000
)

const DefaultDataPartitionBackupTimeOut = time.Hour * 24 * 7

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
	default:
		return "unkown"
	}
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
	Version   *VersionInfo
	_         string // reserved
}

func NewClientReportLimitInfo() *ClientReportLimitInfo {
	log.LogDebugf("NewClientReportLimitInfo %v", GetVersion("client"))
	return &ClientReportLimitInfo{
		FactorMap: make(map[uint32]*ClientLimitInfo),
	}
}

type LimitRsp2Client struct {
	ID            uint64
	Enable        bool
	ReqPeriod     uint32
	HitTriggerCnt uint8
	FactorMap     map[uint32]*ClientLimitInfo
	Magnify       map[uint32]uint32
	Version       *VersionInfo
	_             string // reserved
}

func NewLimitRsp2Client() *LimitRsp2Client {
	limit := &LimitRsp2Client{
		FactorMap: make(map[uint32]*ClientLimitInfo),
		Magnify:   make(map[uint32]uint32),
	}
	return limit
}

type UidSimpleInfo struct {
	UID     uint32
	Limited bool
}

// SimpleVolView defines the simple view of a volume
type SimpleVolView struct {
	ID                      uint64
	Name                    string
	Owner                   string
	ZoneName                string
	DpReplicaNum            uint8
	MpReplicaNum            uint8
	InodeCount              uint64
	DentryCount             uint64
	MaxMetaPartitionID      uint64
	MaxDataPartitionID      uint64
	Status                  uint8
	Capacity                uint64 // GB
	RwDpCnt                 int
	MpCnt                   int
	DpCnt                   int
	FollowerRead            bool
	MetaFollowerRead        bool
	DirectRead              bool
	MaximallyRead           bool
	NeedToLowerReplica      bool
	Authenticate            bool
	CrossZone               bool
	DefaultPriority         bool
	DomainOn                bool
	CreateTime              string
	DeleteLockTime          int64
	LeaderRetryTimeOut      int64
	EnableToken             bool
	EnablePosixAcl          bool
	EnableQuota             bool
	EnableTransactionV1     string
	EnableTransaction       string
	TxTimeout               int64
	TxConflictRetryNum      int64
	TxConflictRetryInterval int64
	TxOpLimit               int
	Description             string
	DpSelectorName          string
	DpSelectorParm          string
	DefaultZonePrior        bool
	DpReadOnlyWhenVolFull   bool
	LeaderRetryTimeout      int64

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
	Uids             []UidSimpleInfo
	TrashInterval    int64

	// multi version snapshot
	LatestVer               uint64
	Forbidden               bool
	DisableAuditLog         bool
	DeleteExecTime          time.Time
	DpRepairBlockSize       uint64
	EnableAutoDpMetaRepair  bool
	AccessTimeInterval      int64
	EnablePersistAccessTime bool

	// hybrid cloud
	VolStorageClass          uint32
	AllowedStorageClass      []uint32
	CacheDpStorageClass      uint32
	ForbidWriteOpOfProtoVer0 bool
	QuotaOfStorageClass      []*StatOfStorageClass

	RemoteCacheEnable         bool
	RemoteCachePath           string
	RemoteCacheAutoPrepare    bool
	RemoteCacheTTL            int64
	RemoteCacheReadTimeoutSec int64
	RemoteCacheMaxFileSizeGB  int64
	RemoteCacheOnlyForNotSSD  bool
	RemoteCacheFollowerRead   bool

	RemoteCacheRemoveDupReq bool // TODO: using it in metanode, origin was named EnableRemoveDupReq
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
	Name                string
	Status              string
	DataNodesetSelector string
	MetaNodesetSelector string
	NodeSet             map[uint64]*NodeSetView
	DataMediaType       string
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
	Dp   = "dpop"
	Disk = "diskop"
	Node = "datanodeop"
	Vol  = "volop"
)

// OpLogView defines the view of all opLogs
type OpLogView struct {
	DpOpLogs      []OpLog
	DiskOpLogs    []OpLog
	ClusterOpLogs []OpLog
	VolOpLogs     []OpLog
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
	VolumeTypeInvalid = -1
	VolumeTypeHot     = 0
	VolumeTypeCold    = 1
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

const (
	InitialDecommission uint32 = iota
	ManualDecommission
	AutoDecommission
	QueryDecommission // used for querying decommission progress for ManualDecommission and AutoDecommission
	AutoAddReplica
	ManualAddReplica
)

type BackupDataPartitionInfo struct {
	Addr        string
	Disk        string
	PartitionID uint64
}

type RecoverBackupDataReplicaRequest struct {
	PartitionId uint64
	Disk        string
}

type RecoverBadDiskRequest struct {
	DiskPath string
}

type DeleteBackupDirectoriesRequest struct {
	DiskPath string
}

type DataPartitionDiskInfo struct {
	PartitionId uint64
	Disk        string
}

// data node hardware media type
const (
	MediaType_Unspecified uint32 = 0
	MediaType_SSD         uint32 = 1
	MediaType_HDD         uint32 = 2
)

var mediaTypeStringMap = map[uint32]string{
	MediaType_Unspecified: "Unspecified",
	MediaType_SSD:         "SSD",
	MediaType_HDD:         "HDD",
}

func MediaTypeString(mediaType uint32) (value string) {
	value, ok := mediaTypeStringMap[mediaType]
	if !ok {
		value = fmt.Sprintf("InvalidValue(%v)", mediaType)
	}
	return
}

func IsValidMediaType(mediaType uint32) bool {
	if mediaType >= MediaType_SSD && mediaType <= MediaType_HDD {
		return true
	}

	return false
}

type StorageClass uint32

const (
	StorageClass_Unspecified uint32 = 0
	StorageClass_Replica_SSD uint32 = 1
	StorageClass_Replica_HDD uint32 = 2
	StorageClass_BlobStore   uint32 = 3
)

var storageClassStringMap = map[uint32]string{
	StorageClass_Unspecified: "Unspecified",
	StorageClass_Replica_SSD: "ReplicaSSD",
	StorageClass_Replica_HDD: "ReplicaHDD",
	StorageClass_BlobStore:   "BlobStore",
}

func StorageClassString(storageClass uint32) (value string) {
	value, ok := storageClassStringMap[storageClass]
	if !ok {
		value = fmt.Sprintf("InvalidValue(%v)", storageClass)
	}
	return
}

func IsValidStorageClass(storageClass uint32) bool {
	if storageClass >= StorageClass_Replica_SSD && storageClass <= StorageClass_BlobStore {
		return true
	}

	return false
}

func IsStorageClassReplica(storageClass uint32) bool {
	return storageClass == StorageClass_Replica_SSD || storageClass == StorageClass_Replica_HDD
}

// IsStorageClassBlobStore : encapsulate in a function in case there are more storage classes of blobstore in the future
func IsStorageClassBlobStore(storageClass uint32) bool {
	return storageClass == StorageClass_BlobStore
}

func IsVolSupportStorageClass(allowedStorageClass []uint32, storeClass uint32) bool {
	for _, storageClass := range allowedStorageClass {
		if storageClass == storeClass {
			return true
		}
	}
	return false
}

func GetVolTypeByStorageClass(storageClass uint32) (err error, volType int) {
	if IsStorageClassReplica(storageClass) {
		volType = VolumeTypeHot
	} else if storageClass == StorageClass_BlobStore {
		volType = VolumeTypeCold
	} else {
		err = fmt.Errorf("got invalid volType by storageClass(%v)", storageClass)
		volType = VolumeTypeInvalid
	}

	return
}

func GetMediaTypeByStorageClass(storageClass uint32) (mediaType uint32) {
	switch storageClass {
	case StorageClass_Replica_SSD:
		mediaType = MediaType_SSD
	case StorageClass_Replica_HDD:
		mediaType = MediaType_HDD
	default:
		mediaType = MediaType_Unspecified
	}

	return
}

func GetStorageClassByMediaType(mediaType uint32) (storageClass uint32) {
	switch mediaType {
	case MediaType_SSD:
		storageClass = StorageClass_Replica_SSD
	case MediaType_HDD:
		storageClass = StorageClass_Replica_HDD
	default:
		storageClass = StorageClass_Unspecified
	}

	return
}

const (
	ForbiddenMigrationRenewalPeriod = 1 * time.Hour
	ForbiddenMigrationRenewalSeonds = 3600
)

// const ForbiddenMigrationRenewalPeriod = 10 * time.Second // for debug

type VolEmptyMpStats struct {
	Name           string               `json:"name"`
	Total          int                  `json:"total"`
	EmptyCount     int                  `json:"emptyCount"`
	MetaPartitions []*MetaPartitionView `json:"metaPartitions"`
}

// FreezeMetaPartitionRequest defines the request of freezing a meta partition.
type FreezeMetaPartitionRequest struct {
	PartitionID uint64
	Freeze      bool
}

type BackupMetaPartitionRequest struct {
	PartitionID uint64
}

type IsRaftStatusOKRequest struct {
	PartitionID uint64
	Ready       bool
	ReplicaNum  int
}
