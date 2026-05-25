package meta

import (
	"encoding/json"
	"net"
	"os"
	"sync"
	"syscall"
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

func TestDentryWriteMarksParentDirtyAfterRequestCompletes(t *testing.T) {
	type result struct {
		status int
		inode  uint64
		err    error
	}

	tests := []struct {
		name string
		call func(mw *MetaWrapper, mp *MetaPartition, parentID uint64) result
	}{
		{
			name: "dcreate",
			call: func(mw *MetaWrapper, mp *MetaPartition, parentID uint64) result {
				status, err := mw.dcreate(mp, parentID, "entry", parentID+1, 0o644, "/entry", false)
				return result{status: status, err: err}
			},
		},
		{
			name: "dupdate",
			call: func(mw *MetaWrapper, mp *MetaPartition, parentID uint64) result {
				status, inode, err := mw.dupdate(mp, parentID, "entry", parentID+2, "/entry", false)
				return result{status: status, inode: inode, err: err}
			},
		},
		{
			name: "ddelete",
			call: func(mw *MetaWrapper, mp *MetaPartition, parentID uint64) result {
				status, inode, _, err := mw.ddelete(mp, parentID, "entry", 0, 0, "/entry", false)
				return result{status: status, inode: inode, err: err}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestReceived := make(chan struct{})
			releaseResponse := make(chan struct{})
			addr, cleanup := startMockMetaPacketListener(t, mockBlockedDentryWriteHandler(t, requestReceived, releaseResponse))
			t.Cleanup(cleanup)

			mw := newConnTestMetaWrapper()
			t.Cleanup(func() { mw.conns.Close() })

			mp := &MetaPartition{
				PartitionID: 88,
				Start:       1,
				End:         1 << 20,
				LeaderAddr:  addr,
				Members:     []string{addr},
			}
			const parentID = uint64(100)

			resultCh := make(chan result, 1)
			go func() {
				resultCh <- tt.call(mw, mp, parentID)
			}()

			select {
			case <-requestReceived:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for dentry request")
			}

			require.False(t, mw.dirtyInodes.isDirty(parentID), "parent should not be marked while the write request is still in flight")

			close(releaseResponse)

			select {
			case got := <-resultCh:
				require.NoError(t, got.err)
				require.Equal(t, statusOK, got.status)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for dentry write to finish")
			}

			require.True(t, mw.dirtyInodes.isDirty(parentID), "parent should be marked after the write function returns")
		})
	}
}

func TestDeleteLlEXMarksParentDirtyAfterFunctionReturns(t *testing.T) {
	const (
		parentID   = uint64(100)
		childInode = uint64(200)
	)
	dirMode := uint32(os.ModeDir | 0o755)

	requestReceived := make(chan struct{})
	releaseResponse := make(chan struct{})
	addr, cleanup := startMockMetaPacketListener(t, mockBlockedLookupThenIgetHandler(t, requestReceived, releaseResponse, childInode, dirMode, 3))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	mw.FollowerRead = true
	mw.NearRead = true

	resultCh := make(chan error, 1)
	go func() {
		_, err := mw.Delete_ll_EX(parentID, "subdir", true, 0, "/parent/subdir", false)
		resultCh <- err
	}()

	select {
	case <-requestReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lookup request")
	}

	require.False(t, mw.dirtyInodes.isDirty(parentID), "parent should not be marked before Delete_ll_EX returns")

	close(releaseResponse)

	select {
	case err := <-resultCh:
		require.Equal(t, syscall.ENOTEMPTY, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Delete_ll_EX to finish")
	}

	require.True(t, mw.dirtyInodes.isDirty(parentID), "parent should be marked by the deferred dirty mark")
}

func mockBlockedDentryWriteHandler(t *testing.T, requestReceived chan<- struct{}, releaseResponse <-chan struct{}) func(net.Conn) error {
	t.Helper()
	var signalOnce sync.Once
	return func(conn net.Conn) error {
		pkt := proto.NewPacket()
		if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			return err
		}
		signalOnce.Do(func() {
			close(requestReceived)
		})
		<-releaseResponse

		resp := proto.NewPacketReqID()
		resp.ReqID = pkt.ReqID
		resp.Opcode = pkt.Opcode
		resp.PartitionID = pkt.PartitionID
		resp.ResultCode = proto.OpOk

		var body []byte
		var err error
		switch pkt.Opcode {
		case proto.OpMetaCreateDentry, proto.OpMetaAsyncCreateDentry:
		case proto.OpMetaUpdateDentry, proto.OpMetaAsyncUpdateDentry:
			body, err = json.Marshal(&proto.UpdateDentryResponse{Inode: 101})
		case proto.OpMetaDeleteDentry, proto.OpMetaAsyncDeleteDentry:
			body, err = json.Marshal(&proto.DeleteDentryResponse{Inode: 102})
		default:
			return nil
		}
		if err != nil {
			return err
		}
		if body != nil {
			resp.Data = body
			resp.Size = uint32(len(body))
		}
		return resp.WriteToConn(conn)
	}
}

func mockBlockedLookupThenIgetHandler(t *testing.T, requestReceived chan<- struct{}, releaseResponse <-chan struct{},
	lookupInode uint64, lookupMode uint32, igetNlink uint32,
) func(net.Conn) error {
	t.Helper()
	var signalOnce sync.Once
	return func(conn net.Conn) error {
		for step := 0; step < 2; step++ {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
			}
			if step == 0 {
				signalOnce.Do(func() {
					close(requestReceived)
				})
				<-releaseResponse
			}

			resp := proto.NewPacketReqID()
			resp.ReqID = pkt.ReqID
			resp.Opcode = pkt.Opcode
			resp.PartitionID = pkt.PartitionID
			resp.ResultCode = proto.OpOk

			var body []byte
			var err error
			switch pkt.Opcode {
			case proto.OpMetaLookup:
				body, err = json.Marshal(&proto.LookupResponse{Inode: lookupInode, Mode: lookupMode})
			case proto.OpMetaInodeGet:
				body, err = json.Marshal(&proto.InodeGetResponse{
					Info: &proto.InodeInfo{
						Inode: lookupInode,
						Mode:  lookupMode,
						Nlink: igetNlink,
					},
				})
			default:
				return nil
			}
			if err != nil {
				return err
			}
			resp.Data = body
			resp.Size = uint32(len(body))
			if err = resp.WriteToConn(conn); err != nil {
				return err
			}
		}
		return nil
	}
}
