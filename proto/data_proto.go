package proto

const (
	FileID = iota
	Size
	Crc
	ModifyTime
)

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

type BlockCrc struct {
	BlockNo int
	Crc     uint32
}

type ExtentInfoBlock [4]uint64

const (
	ExtentInfoFileID = iota
	ExtentInfoSize
	ExtentInfoCrc
	ExtentInfoModifyTime
)

type DNDataPartitionInfoOldVersion struct {
	VolName              string            `json:"volName"`
	ID                   uint64            `json:"id"`
	Size                 int               `json:"size"`
	Used                 int               `json:"used"`
	Status               int               `json:"status"`
	Path                 string            `json:"path"`
	Files                []ExtentInfo `json:"extents"`
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

type DataNodeDiskReport struct {
	Disks []*DataNodeDiskInfo `json:"disks"`
	Zone  string        `json:"zone"`
}

type DataNodeDiskInfo struct {
	Path        string `json:"path"`
	Total       uint64 `json:"total"`
	Used        uint64 `json:"used"`
	Available   uint64 `json:"available"`
	Unallocated uint64 `json:"unallocated"`
	Allocated   uint64 `json:"allocated"`
	Status      int    `json:"status"`
	RestSize    uint64 `json:"restSize"`
	Partitions  int    `json:"partitions"`

	// Limits
	RepairTaskLimit              uint64 `json:"repair_task_limit"`
	ExecutingRepairTask          uint64 `json:"executing_repair_task"`
	FixTinyDeleteRecordLimit     uint64 `json:"fix_tiny_delete_record_limit"`
	ExecutingFixTinyDeleteRecord uint64 `json:"executing_fix_tiny_delete_record"`
}