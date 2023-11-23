package mocktest

import (
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
)

type MockDataPartition struct {
	PartitionID      uint64
	PersistenceHosts []string
	total            int
	used             uint64
	VolName          string
	Forbidden        int32
}

func (md *MockDataPartition) IsForbidden() bool {
	return atomic.LoadInt32(&md.Forbidden) != 0
}

func (md *MockDataPartition) SetForbidden(status bool) {
	val := 0
	if status {
		val = 1
	}
	atomic.StoreInt32(&md.Forbidden, int32(val))
}

type MockMetaPartition struct {
	PartitionID    uint64
	Start          uint64
	End            uint64
	Status         int8
	Cursor         uint64
	VolName        string
	Members        []proto.Peer
	Replicas       []*MockMetaReplica
	Forbidden      int32
	EnableAuditLog int32
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

func (mm *MockMetaPartition) IsEnableAuditLog() bool {
	return atomic.LoadInt32(&mm.EnableAuditLog) != 0
}

func (mm *MockMetaPartition) SetEnableAuditLog(status bool) {
	val := 0
	if status {
		val = 1
	}
	atomic.StoreInt32(&mm.EnableAuditLog, int32(val))
}

func (mm *MockMetaPartition) IsForbidden() bool {
	return atomic.LoadInt32(&mm.Forbidden) != 0
}

func (mm *MockMetaPartition) SetForbidden(status bool) {
	val := 0
	if status {
		val = 1
	}
	atomic.StoreInt32(&mm.Forbidden, int32(val))
}
