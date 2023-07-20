package api

const (
	PathStat               = "/stat"
	PathLeadershipTransfer = "/leadership/transfer"
	PathMemberRemove       = "/member/remove"

	PathVolumeGet           = "/volume/get"
	PathVolumeList          = "/volume/list"
	PathV2VolumeList        = "/v2/volume/list"
	PathVolumeAllocatedList = "/volume/allocated/list"

	PathDiskList         = "/disk/list"
	PathDiskInfo         = "/disk/info"
	PathDiskDroppingList = "/disk/droppinglist"
	PathDiskAccess       = "/disk/access"
	PathDiskSet          = "/disk/set"
	PathDiskDrop         = "/disk/drop"
	PathDiskProbe        = "/disk/probe"

	PathServiceList = "/service/list"
	PathServiceGet  = "/service/get"

	PathConfigList   = "/config/list"
	PathConfigGet    = "/config/get"
	PathConfigSet    = "/config/set"
	PathConfigReload = "/config/reload"
)
