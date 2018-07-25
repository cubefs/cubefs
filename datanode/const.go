package datanode

const (
	Standby uint32 = iota
	Start
	Running
	Shutdown
	Stopped
)

const (
	RequestChanSize = 10240
)

const (
	ActionSendToNext                                 = "ActionSendToNext"
	LocalProcessAddr                                 = "LocalProcess"
	ActionReceiveFromNext                            = "ActionReceiveFromNext"
	ActionStreamRead                                 = "ActionStreamRead"
	ActionWriteToCli                                 = "ActionWriteToCli"
	ActionCheckAndAddInfos                           = "ActionCheckAndAddInfos"
	ActionCheckChunkInfo                             = "ActionCheckChunkInfo"
	ActionPostToMaster                               = "ActionPostToMaster"
	ActionLeaderToFollowerOpCRepairReadPackResponse  = "ActionLeaderToFollowerOpCRepairReadPackResponse"
	ActionLeaderToFollowerOpRepairReadPackBuffer     = "ActionLeaderToFollowerOpRepairReadPackBuffer"
	ActionLeaderToFollowerOpRepairReadSendPackBuffer = "ActionLeaderToFollowerOpRepairReadSendPackBuffer"

	ActionGetFollowers    = "ActionGetFollowers"
	ActionCheckReplyAvail = "ActionCheckReplyAvail"
)

//stats
const (
	ReportToMonitorRole = 1
	ReportToSelfRole    = 3
)

const (
	InFlow = iota
	OutFlow
)

const (
	NetType = "tcp"
)

const (
	ObjectIDSize = 8
)

//pack cmd response
const (
	NoFlag           = 0
	ReadFlag         = 1
	WriteFlag        = 2
	MaxActiveExtents = 50000
)

const (
	ConnIsNullErr = "ConnIsNullErr"
)

const (
	LogHeartbeat         = "HB:"
	LogStats             = "Stats:"
	LogLoad              = "Load:"
	LogExit              = "Exit:"
	LogShutdown          = "Shutdown:"
	LogCreatePartition   = "CRV:"
	LogCreateFile        = "CRF:"
	LogDelPartition      = "DELV:"
	LogDelFile           = "DELF:"
	LogMarkDel           = "MDEL:"
	LogPartitionSnapshot = "Snapshot:"
	LogGetWm             = "WM:"
	LogGetAllWm          = "AllWM:"
	LogCompactChunk      = "CompactChunk:"
	LogWrite             = "WR:"
	LogRead              = "RD:"
	LogRepairRead        = "RRD:"
	LogStreamRead        = "SRD:"
	LogRepairNeedles     = "RN:"
	LogRepair            = "Repair:"
	LogChecker           = "Checker:"
	LogTask              = "Master Task:"
	LogGetFlow           = "GetFlowInfo:"
)
