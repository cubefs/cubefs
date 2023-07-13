package convert

type ConvertStatus int

const (
	ConvertInit ConvertStatus = iota
	ConvertRunning
	ConvertClosing
)
type ConvertVolumeInfo struct {
	Name         string
	Status       ConvertStatus
	RunningMPCnt int
	RunningMps   map[uint64]struct{}
	FinishedMps  []uint64
	Finished     bool
}