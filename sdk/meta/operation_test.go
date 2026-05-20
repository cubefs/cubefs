package meta

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/stretchr/testify/require"
)

// Covers sdk/meta/api.go UpdateExtentKeyAfterMigration and sdk/meta/operation.go request construction.
func TestUpdateExtentKeyAfterMigrationRequestFields(t *testing.T) {
	reqCh := make(chan *proto.UpdateExtentKeyAfterMigrationRequest, 1)

	addr, cleanup := startMockMetaPacketListener(t, func(conn net.Conn) error {
		pkt := proto.NewPacket()
		if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			return err
		}

		var req proto.UpdateExtentKeyAfterMigrationRequest
		if err := json.Unmarshal(pkt.Data, &req); err != nil {
			return err
		}
		reqCh <- &req

		resp := proto.NewPacketReqID()
		resp.ReqID = pkt.ReqID
		resp.Opcode = pkt.Opcode
		resp.PartitionID = pkt.PartitionID
		resp.ResultCode = proto.OpOk
		return resp.WriteToConn(conn)
	})
	t.Cleanup(cleanup)

	mw := &MetaWrapper{
		volname:           "test-vol",
		metaSendTimeout:   30,
		conns:             util.NewConnectPool(),
		partitions:        make(map[uint64]*MetaPartition),
		ranges:            btree.New(32),
		EnableTransaction: 0,
	}
	t.Cleanup(func() { mw.conns.Close() })

	mp := &MetaPartition{
		PartitionID: 11,
		Start:       1,
		End:         1 << 20,
		LeaderAddr:  addr,
		Members:     []string{addr},
	}
	mw.addPartition(mp)

	const inode = uint64(1024)

	err := mw.UpdateExtentKeyAfterMigration(
		inode,
		proto.StorageClass_Replica_HDD,
		nil,
		proto.DefaultHDDPoolId,
		100,
		5,
		"/migrate/file",
	)
	require.NoError(t, err)

	select {
	case req := <-reqCh:
		require.Equal(t, inode, req.Inode)
		require.Equal(t, uint64(100), req.LeaseExpire)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting migration request")
	}
}

// Covers operation.go lookup passing parentID into sendToMetaPartition for dirty-inode near-read suppression.
func TestUpdateExtentKeyAfterMigrationPartitionNotFound(t *testing.T) {
	mw := &MetaWrapper{
		volname:         "test-vol",
		metaSendTimeout: 30,
		conns:           util.NewConnectPool(),
		partitions:      make(map[uint64]*MetaPartition),
		ranges:          btree.New(32),
	}
	t.Cleanup(func() { mw.conns.Close() })

	err := mw.UpdateExtentKeyAfterMigration(42, proto.StorageClass_Replica_HDD, nil,
		proto.DefaultHDDPoolId, 1, 0, "/missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found mp")
}

func TestLookupSkipsNearReadWhenParentInodeDirty(t *testing.T) {
	chLeader := make(chan *proto.Packet, 1)
	addr, cleanup := startMockMetaPacketListener(t, mockLookupOKHandler(chLeader))
	t.Cleanup(cleanup)

	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })

	const parentID = uint64(441229642)
	mw.dirtyInodes.mark(parentID)

	mp := &MetaPartition{
		PartitionID: 6006,
		LeaderAddr:  addr,
		Members:     []string{addr},
	}

	status, ino, mode, err := mw.lookup(mp, parentID, "entry", 0, false)
	require.NoError(t, err)
	require.Equal(t, statusOK, status)
	require.Equal(t, uint64(4242), ino)
	require.Equal(t, uint32(0o644), mode)

	select {
	case got := <-chLeader:
		require.Equal(t, uint32(0), got.ArgLen, "lookup with dirty parent should not use near-read flag")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lookup request on leader")
	}
}
