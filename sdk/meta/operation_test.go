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

// Covers sdk/meta/api.go UpdateExtentKeyAfterMigration (generation arg) and sdk/meta/operation.go request construction (Generation field).
func TestUpdateExtentKeyAfterMigrationCarriesGenerationToRequest(t *testing.T) {
	proto.InitBufferPool(int64(32768))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	addr := ln.Addr().String()
	reqCh := make(chan *proto.UpdateExtentKeyAfterMigrationRequest, 1)
	errCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		pkt := proto.NewPacket()
		if err = pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			errCh <- err
			return
		}

		var req proto.UpdateExtentKeyAfterMigrationRequest
		if err = json.Unmarshal(pkt.Data, &req); err != nil {
			errCh <- err
			return
		}
		reqCh <- &req

		resp := proto.NewPacketReqID()
		resp.ReqID = pkt.ReqID
		resp.Opcode = pkt.Opcode
		resp.PartitionID = pkt.PartitionID
		resp.ResultCode = proto.OpOk
		if err = resp.WriteToConn(conn); err != nil {
			errCh <- err
		}
	}()

	mw := &MetaWrapper{
		volname:           "test-vol",
		metaSendTimeout:   1,
		conns:             util.NewConnectPool(),
		partitions:        make(map[uint64]*MetaPartition),
		ranges:            btree.New(32),
		EnableTransaction: 0,
	}
	defer mw.conns.Close()

	mp := &MetaPartition{
		PartitionID: 11,
		Start:       1,
		End:         1 << 20,
		LeaderAddr:  addr,
		Members:     []string{addr},
	}
	mw.addPartition(mp)

	const (
		inode      = uint64(1024)
		generation = uint64(12345)
	)

	err = mw.UpdateExtentKeyAfterMigration(
		inode,
		proto.StorageClass_Replica_HDD,
		nil,
		proto.DefaultHDDPoolId,
		100,
		5,
		"/migrate/file",
		generation,
	)
	require.NoError(t, err)

	select {
	case req := <-reqCh:
		require.Equal(t, generation, req.Generation)
		require.Equal(t, inode, req.Inode)
		require.Equal(t, uint64(100), req.LeaseExpire)
	case err = <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting migration request")
	}

	select {
	case err = <-errCh:
		require.NoError(t, err)
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting mock meta server exit")
	}
}
