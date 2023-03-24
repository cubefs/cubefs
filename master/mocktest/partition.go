package mocktest

import (
	"github.com/cubefs/cubefs/proto"
)

type MockDataPartition struct {
	PartitionID      uint64
	PersistenceHosts []string
	total            int
	used             uint64
	VolName          string
}

type MockMetaPartition struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Status      int8
	Cursor      uint64
	VolName     string
	Members     []proto.Peer
	Replicas    []*MockMetaReplica
}

// MockMetaReplica defines the replica of a meta partition
type MockMetaReplica struct {
	Addr        string
	start       uint64 // lower bound of the inode id
	end         uint64 // upper bound of the inode id
	dataSize    uint64
	nodeID      uint64
	MaxInodeID  uint64
	InodeCount  uint64
	DentryCount uint64
	ReportTime  int64
	Status      int8 // unavailable, readOnly, readWrite
	IsLeader    bool
}

func (mm *MockMetaPartition) isLeaderMetaNode(addr string) bool {
	for _, mr := range mm.Replicas {
		if mr.Addr == addr {
			return mr.IsLeader
		}
	}

	return false
}
