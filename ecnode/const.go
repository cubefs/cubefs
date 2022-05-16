package ecnode

import "github.com/chubaofs/chubaofs/proto"

// Action description
const (
	ActionHeartbeat                     = "ActionHeartbeat:"
	ActionDeleteEcPartition             = "ActionDeleteEcPartition:"
	ActionCreateEcPartition             = "ActionCreateEcPartition:"
	ActionCreateExtent                  = "ActionCreateExtent:"
	ActionMarkDelete                    = "ActionMarkDelete:"
	ActionRead                          = "ActionRead:"
	ActionWrite                         = "ActionWrite:"
	ActionStreamRead                    = "ActionStreamRead:"
	ActionReplicasToRepair              = "ActionReplicasToRepair:"
	ActionChangeMember                  = "ActionChangeMember:"
	ActionGetAllExtentWatermarks        = "ActionGetAllExtentWatermarks:"
	ActionUpdateEcPartition             = "ActionUpdateEcPartition:"
	ActionTinyExtentRepairRead          = "ActionTinyExtentRepairRead:"
	ActionRepairTinyDelInfo             = "ActionRepairTinyDelInfo:"
	ActionStreamReadTinyDelRecord       = "ActionStreamReadTinyDelRecord:"
	ActionStreamReadOriginTinyDelRecord = "ActionStreamReadOriginTinyDelRecord:"
	ActionGetTinyDeletingInfo           = "ActionGetTinyDeletingInfo:"
	ActionRecordTinyDelInfo             = "ActionRecordTinyDelInfo:"
	ActionPersistTinyExtentDelete       = "ActionPersistTinyExtentDelete"
)

const (
	CacheCapacityPerPartition = 256
	DiskMaxFDLimit            = 20000
	MaxTinyDelHandle          = 8
	DiskForceEvictFDRatio     = 0.25
)

const (
	IsInitState = uint64(iota)
	IsStartUpdateParityState
	IsUpdateParityDoneState
)

const (
	NeedUpdateParityData = uint8(iota)
	NeedDelayHandle
	noNeedUpdateParityData
)

const (
	EcNodeLatestVersion = proto.BaseVersion
)
