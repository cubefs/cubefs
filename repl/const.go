package repl

const (
	RequestChanSize = 102400
)

const (
	ForceCloseConnect = true
	NoCloseConnect    = false
)

const (
	ActionSendToFollowers     = "ActionSendToFollowers"
	LocalProcessAddr          = "LocalProcess"
	ActionReceiveFromFollower = "ActionReceiveFromFollower"
	ActionWriteToClient       = "ActionWriteToClient"
	ActionCheckAndAddInfos    = "ActionCheckAndAddInfos"
	ActionCheckReplyAvail     = "ActionCheckReplyAvail"

	ActionPreparePkg = "ActionPreparePkg"
)

const (
	ConnIsNullErr = "ConnIsNullErr"
)
