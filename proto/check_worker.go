package proto

type ExtentInfoWithInode struct {
	Inode uint64
	EK    ExtentKey
}

type NormalEKCheckResult struct {
	VolName string
	ExtInfo ExtentInfoWithInode
}

type NormalEKCheckFailed struct {
	VolName    string
	FailedInfo string
}

type NormalEKAllocateConflict struct {
	VolName         string
	DataPartitionID uint64
	ExtentID        uint64
	OwnerInodes     []uint64
}

type NormalEKOwnerInodeSearchFailedResult struct {
	VolName         string
	DataPartitionID uint64
	ExtentID        uint64
}

type InodeEKCountRecord struct {
	VolName     string
	PartitionID uint64
	InodeID     uint64
	EKCount     uint32
}