package proto

type DataPartitions struct {
	Partitions     []*DataPartition `json:"partitions"`
	PartitionCount int              `json:"partitionCount"`
}
type DataPartition struct {
	ID       uint64   `json:"id"`
	Size     int      `json:"size"`
	Used     int      `json:"used"`
	Status   int      `json:"status"`
	Path     string   `json:"path"`
	Replicas []string `json:"replicas"`
}

type DNDataPartitionInfo struct {
	VolName              string            `json:"volName"`
	ID                   uint64            `json:"id"`
	Size                 int               `json:"size"`
	Used                 int               `json:"used"`
	Status               int               `json:"status"`
	Path                 string            `json:"path"`
	Files                []ExtentInfoBlock `json:"extents"`
	FileCount            int               `json:"fileCount"`
	Replicas             []string          `json:"replicas"`
	TinyDeleteRecordSize int64             `json:"tinyDeleteRecordSize"`
	RaftStatus           *Status           `json:"raftStatus"`
	Peers                []Peer            `json:"peers"`
	Learners             []Learner         `json:"learners"`
}

type TinyExtentHole struct {
	Offset     uint64
	Size       uint64
	PreAllSize uint64
}

type DNTinyExtentInfo struct {
	Holes           []*TinyExtentHole `json:"holes"`
	ExtentAvaliSize uint64            `json:"extentAvaliSize"`
	BlocksNum       uint64            `json:"blockNum"`
}
