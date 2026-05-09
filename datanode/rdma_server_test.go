//go:build linux && rdma

package datanode

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
)

// TestIsReadOp_Classification ensures every read-side opcode that the
// TCP path treats as streaming is also recognised by the RDMA dispatch
// as needing the single-shot handleReadSlot path. Missing one would let
// it slip through to OperatePacket → handleStreamReadPacket →
// rdmaFakeConn.Write panic.
func TestIsReadOp_Classification(t *testing.T) {
	readOps := []uint8{
		proto.OpStreamRead,
		proto.OpRead,
		proto.OpStreamFollowerRead,
		proto.OpExtentRepairRead,
		proto.OpBackupRead,
	}
	for _, op := range readOps {
		if !isReadOp(op) {
			t.Errorf("isReadOp(0x%x) = false, want true", op)
		}
	}

	// Non-read opcodes must NOT be classified as reads — otherwise they
	// would be misrouted to handleReadSlot and never reach OperatePacket.
	nonReadOps := []uint8{
		proto.OpWrite,
		proto.OpSyncWrite,
		proto.OpRandomWrite,
		proto.OpCreateExtent,
		proto.OpMarkDelete,
		proto.OpReadTinyDeleteRecord, // record read, not extent stream read
	}
	for _, op := range nonReadOps {
		if isReadOp(op) {
			t.Errorf("isReadOp(0x%x) = true, want false", op)
		}
	}
}
