package datanode

import (
	"testing"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestCheckPacketAndPrepareRejectsTinyWriteDuringRepair(t *testing.T) {
	dp := &DataPartition{
		config:      &dataPartitionCfg{},
		isRepairing: true,
	}
	packet := &repl.Packet{
		Packet: proto.Packet{
			PartitionID:        1,
			ExtentType:         proto.TinyExtentType,
			Opcode:             proto.OpWrite,
			RemainingFollowers: 1,
		},
		Object: dp,
	}

	err := (&DataNode{}).checkPacketAndPrepare(packet)

	require.ErrorIs(t, err, storage.ErrDpDecommissionRepair)
}
