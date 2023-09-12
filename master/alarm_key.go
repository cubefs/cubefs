package master

import "fmt"

var (
	gAlarmKeyMap map[string]string
)

const (

	//vol
	alarmKeyCreateVolFailed = "create_vol_failed"
	alarmKeyUpdateVolFailed = "update_vol_failed"
	alarmKeyRenameVolFailed = "rename_vol_failed"

	//dp
	alarmKeyDpHasNotRecover      = "dp_has_not_recover"
	alarmKeyDpConvertFailed      = "dp_convert_failed"
	alarmKeyDpReplicaSize        = "check_replica_size"
	alarmKeyDpValidateCrc        = "validate_crc"
	alarmKeyDpDecommissionFailed = "dp_decommission_failed"
	alarmKeyDpCheckStatus        = "dp_check_status"
	alarmKeyDpReset              = "dp_reset"
	alarmKeyDpMissReplica        = "dp_miss_replica"
	alarmKeyDpIllegalReplica     = "dp_illegal_replica"
	alarmKeyDpReplicaNum         = "dp_replica_num"
	alarmKeyDpCreate             = "dp_create"

	//ecdp
	alarmKeyEcdpHasNotRecover      = "ecdp_has_not_recover"
	alarmKeyEcdpMissingRepica      = "ecdp_missing_replica"
	alarmKeyEcdpIllegalRepica      = "ecdp_Illegal_replica"
	alarmKeyEcdpCheckStatus        = "ecdp_check_status"
	alarmKeyEcdpCreateFailed       = "ecdp_create_failed"
	alarmKeyEcdpDecommissionFailed = "ecdp_decommission_failed"
	alarmKeyEcdpReplicaNum         = "ecdp_replica_num"

	//mp
	alarmKeyMpLoadFailed         = "mp_load_failed"
	alarmKeyMpHasNotRecover      = "mp_has_not_recover"
	alarmKeyMpReset              = "mp_reset"
	alarmKeyMpSplit              = "mp_split"
	alarmKeyMpCheckStatus        = "mp_check_status"
	alarmKeyMpMissReplica        = "mp_miss_replica"
	alarmKeyMpValidateCrc        = "mp_validate_crc"
	alarmKeyMpReplicaNum         = "mp_replica_num"
	alarmKeyMpIllegalReplica     = "mp_illegal_replica"
	alarmKeyMpCreate             = "mp_create"
	alarmKeyMpDecommissionFailed = "mp_decommission_failed"

	//disk
	alarmKeyDecommissionDisk = "decommission_disk"

	//node
	alarmKeyAdjustNodeSet      = "adjustNodeSet"
	alarmKeyTaskManagerHasStop = "task_manager_has_stop"
	alarmKeyNodeHeartbeat      = "node_heartbeat"
	alarmKeyNodeRegisterFailed = "node_register_failed"
	alarmKeyDecommissionNode   = "decommission_node"

	//cluster
	alarmKeyLoadClusterMetadata = "load_cluster_metadata"
	alarmKeyCheckClusterSpace   = "check_cluster_space"
	alarmKeyLeaderChanged       = "leader_changed"
	alarmKeyPeerChanged         = "peer_changed"
	alarmKeyChooseTargetHost    = "choose_target_host"
	alarmKeyUpdateViewCache     = "update_view_cache"
	alarmKeyAdminTaskException  = "admin_task_exception"
)

func (c *Cluster) initAlarmKey() {
	gAlarmKeyMap = make(map[string]string, 0)

	//vol
	gAlarmKeyMap[alarmKeyCreateVolFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyCreateVolFailed)
	gAlarmKeyMap[alarmKeyUpdateVolFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyUpdateVolFailed)
	gAlarmKeyMap[alarmKeyRenameVolFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyRenameVolFailed)

	//dp
	gAlarmKeyMap[alarmKeyDpHasNotRecover] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpHasNotRecover)
	gAlarmKeyMap[alarmKeyDpConvertFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpConvertFailed)
	gAlarmKeyMap[alarmKeyDpReplicaSize] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpReplicaSize)
	gAlarmKeyMap[alarmKeyDpValidateCrc] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpValidateCrc)
	gAlarmKeyMap[alarmKeyDpDecommissionFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpDecommissionFailed)
	gAlarmKeyMap[alarmKeyDpCheckStatus] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpCheckStatus)
	gAlarmKeyMap[alarmKeyDpReset] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpReset)
	gAlarmKeyMap[alarmKeyDpMissReplica] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpMissReplica)
	gAlarmKeyMap[alarmKeyDpReplicaNum] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpReplicaNum)
	gAlarmKeyMap[alarmKeyDpIllegalReplica] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpIllegalReplica)
	gAlarmKeyMap[alarmKeyDpCreate] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDpCreate)

	//ecdp
	gAlarmKeyMap[alarmKeyEcdpHasNotRecover] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyEcdpHasNotRecover)
	gAlarmKeyMap[alarmKeyEcdpMissingRepica] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyEcdpMissingRepica)
	gAlarmKeyMap[alarmKeyEcdpIllegalRepica] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyEcdpIllegalRepica)
	gAlarmKeyMap[alarmKeyEcdpCheckStatus] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyEcdpCheckStatus)
	gAlarmKeyMap[alarmKeyEcdpCreateFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyEcdpCreateFailed)
	gAlarmKeyMap[alarmKeyEcdpDecommissionFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyEcdpDecommissionFailed)

	//mp
	gAlarmKeyMap[alarmKeyMpLoadFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpLoadFailed)
	gAlarmKeyMap[alarmKeyMpHasNotRecover] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpHasNotRecover)
	gAlarmKeyMap[alarmKeyMpReset] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpReset)
	gAlarmKeyMap[alarmKeyMpSplit] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpSplit)
	gAlarmKeyMap[alarmKeyMpCheckStatus] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpCheckStatus)
	gAlarmKeyMap[alarmKeyMpMissReplica] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpMissReplica)
	gAlarmKeyMap[alarmKeyMpValidateCrc] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpValidateCrc)
	gAlarmKeyMap[alarmKeyMpReplicaNum] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpReplicaNum)
	gAlarmKeyMap[alarmKeyMpIllegalReplica] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpIllegalReplica)
	gAlarmKeyMap[alarmKeyMpCreate] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpCreate)
	gAlarmKeyMap[alarmKeyMpDecommissionFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyMpDecommissionFailed)

	//disk
	gAlarmKeyMap[alarmKeyDecommissionDisk] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDecommissionDisk)

	//node
	gAlarmKeyMap[alarmKeyAdjustNodeSet] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyAdjustNodeSet)
	gAlarmKeyMap[alarmKeyTaskManagerHasStop] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyTaskManagerHasStop)
	gAlarmKeyMap[alarmKeyNodeHeartbeat] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyNodeHeartbeat)
	gAlarmKeyMap[alarmKeyNodeRegisterFailed] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyNodeRegisterFailed)
	gAlarmKeyMap[alarmKeyDecommissionNode] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyDecommissionNode)

	//cluster
	gAlarmKeyMap[alarmKeyLoadClusterMetadata] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyLoadClusterMetadata)
	gAlarmKeyMap[alarmKeyCheckClusterSpace] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyCheckClusterSpace)
	gAlarmKeyMap[alarmKeyLeaderChanged] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyLeaderChanged)
	gAlarmKeyMap[alarmKeyPeerChanged] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyPeerChanged)
	gAlarmKeyMap[alarmKeyChooseTargetHost] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyChooseTargetHost)
	gAlarmKeyMap[alarmKeyUpdateViewCache] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyUpdateViewCache)
	gAlarmKeyMap[alarmKeyAdminTaskException] = fmt.Sprintf("%v_%v_%v", c.Name, ModuleName, alarmKeyAdminTaskException)
}
