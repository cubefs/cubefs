package repl

const (
	RequestChanSize = 102400
)

const (
	ForceCloseConnect = true
	NoCloseConnect    = false
)

const (
	ActionSendToFollowers         = "ActionSendToFollowers"
	LocalProcessAddr              = "LocalProcess"
	ActionReceiveFromFollower     = "ActionReceiveFromFollower"
	ActionStreamRead              = "ActionStreamRead"
	ActionWriteToCli              = "ActionWriteToCli"
	ActionGetDataPartitionMetrics = "ActionGetDataPartitionMetrics"
	ActionCheckAndAddInfos        = "ActionCheckAndAddInfos"
	ActionCheckReplyAvail         = "ActionCheckReplyAvail"

	ActionPreparePkg = "ActionPreparePkg"
)

const (
	ConnIsNullErr  = "ConnIsNullErr"
	RaftIsNotStart = "RaftIsNotStart"
)
