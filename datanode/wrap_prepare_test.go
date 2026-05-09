package datanode

import (
	"testing"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestCheckPartitionReturnsErrNoSpaceWhenDPFullForWrite(t *testing.T) {
	dn := &DataNode{}
	sm := NewSpaceManager(dn)
	disk := &Disk{
		Status:          proto.ReadWrite,
		RejectWrite:     false,
		Total:           1 << 30,
		Used:            0,
		ReservedSpace:   0,
		DiskRdonlySpace: 0,
		dataNode:        dn,
	}
	dp := &DataPartition{
		partitionID:   555,
		partitionSize: 1024,
		used:          1024,
		disk:          disk,
	}
	sm.AttachPartition(dp)
	dn.space = sm

	p := repl.NewPacket()
	p.PartitionID = 555
	p.Opcode = proto.OpWrite

	err := dn.checkPartition(p)
	require.ErrorIs(t, err, storage.ErrNoSpace)
	require.NotNil(t, p.Object)
}

func TestCheckPartitionAllowsReadWhenDPFull(t *testing.T) {
	dn := &DataNode{}
	sm := NewSpaceManager(dn)
	disk := &Disk{
		Status:          proto.ReadWrite,
		RejectWrite:     false,
		Total:           1 << 30,
		Used:            0,
		ReservedSpace:   0,
		DiskRdonlySpace: 0,
		dataNode:        dn,
	}
	dp := &DataPartition{
		partitionID:   556,
		partitionSize: 1024,
		used:          1024,
		disk:          disk,
	}
	sm.AttachPartition(dp)
	dn.space = sm

	p := repl.NewPacket()
	p.PartitionID = 556
	p.Opcode = proto.OpRead

	err := dn.checkPartition(p)
	require.NoError(t, err)
}
