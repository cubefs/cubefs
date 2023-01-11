package mocktest

import "github.com/chubaofs/chubaofs/proto"

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
	Learners    []proto.Learner
	VirtualMPs  []proto.VirtualMetaPartition
}
