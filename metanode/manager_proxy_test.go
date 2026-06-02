// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations.

package metanode

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/timeutil"
)

func init() {
	// Packet.WriteToConn / ReadFromConnWithVer use proto.Buffers; without InitBufferPool, Buffers is nil and writes panic.
	// respondToClient recovers the panic, so tests would see an empty client read and obscure failures.
	proto.InitBufferPool(32768)
}

// mockRaftPartitionForServeProxy implements raftstore.Partition for serveProxy tests only.
type mockRaftPartitionForServeProxy struct {
	leaderID  uint64
	term      uint64
	restoring bool
}

func (m *mockRaftPartitionForServeProxy) Submit(cmd []byte) (interface{}, error) {
	return nil, nil
}

func (m *mockRaftPartitionForServeProxy) ChangeMember(
	changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte,
) (interface{}, error) {
	return nil, nil
}

func (m *mockRaftPartitionForServeProxy) Stop() error   { return nil }
func (m *mockRaftPartitionForServeProxy) Delete() error { return nil }

func (m *mockRaftPartitionForServeProxy) Status() *raftstore.PartitionStatus {
	return nil
}

func (m *mockRaftPartitionForServeProxy) IsRestoring() bool { return m.restoring }

func (m *mockRaftPartitionForServeProxy) LeaderTerm() (leaderID, term uint64) {
	return m.leaderID, m.term
}

func (m *mockRaftPartitionForServeProxy) IsRaftLeader() bool { return false }

func (m *mockRaftPartitionForServeProxy) AppliedIndex() uint64   { return 0 }
func (m *mockRaftPartitionForServeProxy) CommittedIndex() uint64 { return 0 }

func (m *mockRaftPartitionForServeProxy) Truncate(index uint64) {}

func (m *mockRaftPartitionForServeProxy) TryToLeader(nodeID uint64) error { return nil }

func (m *mockRaftPartitionForServeProxy) IsOfflinePeer() bool { return false }

func (m *mockRaftPartitionForServeProxy) CloseAndBackup() error { return nil }

func (m *mockRaftPartitionForServeProxy) Closed() bool { return false }

func newTestMetadataManager() *metadataManager {
	return &metadataManager{
		connPool: util.NewConnectPool(),
	}
}

func newTestMetaPartitionForProxy(partitionID, nodeID, leaderPeerID uint64, leaderAddr string) *metaPartition {
	peers := []proto.Peer{{ID: leaderPeerID, Addr: leaderAddr}}
	if leaderAddr == "" {
		peers = nil
	}
	return &metaPartition{
		config: &MetaPartitionConfig{
			PartitionId: partitionID,
			NodeId:      nodeID,
			Peers:       peers,
		},
		raftPartition: &mockRaftPartitionForServeProxy{},
	}
}

func readAllAvailable(c net.Conn, timeout time.Duration) []byte {
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	var buf []byte
	tmp := make([]byte, 4096)
	for {
		n, err := c.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}
	return buf
}

// TestServerProxy_ForbiddenOp verifies forbidden meta partition writes are rejected locally.
func TestServerProxy_ForbiddenOp(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(101, 1, 1, "127.0.0.1:1")
	mp.config.Forbidden = true

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := baseReadMetaPacket()
		p.Opcode = proto.OpMetaCreateInode
		ok := m.serveProxy(srv, mp, p)
		require.False(t, ok)
		require.Equal(t, proto.OpForbidErr, p.ResultCode)
		_ = srv.Close()
	}()

	out := readAllAvailable(client, 2*time.Second)
	wg.Wait()
	require.NotEmpty(t, out, "client should receive error response")
}

// TestServerProxy_LocalLeaderEarlyReturn verifies no client response when this node is raft leader.
func TestServerProxy_LocalLeaderEarlyReturn(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(102, 1, 1, "127.0.0.1:9")
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 1, term: 1}

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := baseReadMetaPacket()
		ok := m.serveProxy(srv, mp, p)
		require.True(t, ok)
		_ = srv.Close()
	}()

	_ = client.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 64)
	n, err := client.Read(buf)
	require.Error(t, err, "expected timeout or close without response data")
	require.Equal(t, 0, n)
	wg.Wait()
}

// TestServerProxy_FollowerReadNoLeader verifies read may be served locally when there is no leader address.
func TestServerProxy_FollowerReadNoLeader(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(103, 2, 0, "")
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 0, term: 0}

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := baseReadMetaPacket()
		p.Arg = []byte{proto.FollowerReadFlag}
		p.ArgLen = 1
		ok := m.serveProxy(srv, mp, p)
		require.True(t, ok)
		_ = srv.Close()
	}()

	_ = client.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 64)
	n, err := client.Read(buf)
	require.Error(t, err)
	require.Equal(t, 0, n)
	wg.Wait()
}

// TestServerProxy_NearReadOk verifies near-read (learner / local read hint) is allowed when lease is fresh and not restoring.
// serveProxy returns before dialing the raft leader.
func TestServerProxy_NearReadOk(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(108, 2, 1, "127.0.0.1:9")
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 1, term: 1}
	mp.leaseApplyTime = timeutil.GetCurrentTimeUnix()

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := nearReadMetaPacket()
		p.PartitionID = 108
		ok := m.serveProxy(srv, mp, p)
		require.True(t, ok)
		_ = srv.Close()
	}()

	_ = client.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 64)
	n, err := client.Read(buf)
	require.Error(t, err)
	require.Equal(t, 0, n)
	wg.Wait()
}

// TestServerProxy_NearReadDisabledWhenRestoring verifies near-read is rejected when the partition is restoring.
func TestServerProxy_NearReadDisabledWhenRestoring(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(109, 2, 0, "")
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 0, term: 0, restoring: true}
	mp.leaseApplyTime = timeutil.GetCurrentTimeUnix()

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := nearReadMetaPacket()
		p.PartitionID = 109
		ok := m.serveProxy(srv, mp, p)
		require.False(t, ok)
		require.Equal(t, proto.OpAgain, p.ResultCode)
		_ = srv.Close()
	}()

	out := readAllAvailable(client, 2*time.Second)
	wg.Wait()
	require.NotEmpty(t, out)
}

// TestServerProxy_NearReadDisabledLeaseStale verifies near-read is rejected when applied lease is older than FollowerReadLeaseTime.
func TestServerProxy_NearReadDisabledLeaseStale(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(110, 2, 0, "")
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 0, term: 0}
	// Must exceed default lease (3600s); equality still allows near-read (check uses > not >=).
	mp.leaseApplyTime = timeutil.GetCurrentTimeUnix() - int64(proto.DefaultFollowerReadLeaseTimeSec+1)

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := nearReadMetaPacket()
		p.PartitionID = 110
		ok := m.serveProxy(srv, mp, p)
		require.False(t, ok)
		require.Equal(t, proto.OpAgain, p.ResultCode)
		_ = srv.Close()
	}()

	out := readAllAvailable(client, 2*time.Second)
	wg.Wait()
	require.NotEmpty(t, out)
}

// TestServerProxy_NearReadIgnoredForNonReadMetaOp verifies the near-read arg does not apply to non read-meta opcodes.
func TestServerProxy_NearReadIgnoredForNonReadMetaOp(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(111, 2, 0, "")
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 0, term: 0}

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := &Packet{}
		p.Magic = proto.ProtoMagic
		p.Opcode = proto.OpMetaCreateInode
		p.ReqID = 9100
		p.PartitionID = 111
		p.Arg = []byte{proto.NearReadFlag}
		p.ArgLen = 1
		ok := m.serveProxy(srv, mp, p)
		require.False(t, ok)
		require.Equal(t, proto.OpAgain, p.ResultCode)
		_ = srv.Close()
	}()

	out := readAllAvailable(client, 2*time.Second)
	wg.Wait()
	require.NotEmpty(t, out)
}

// TestServerProxy_NoLeaderNonRead verifies try-again when there is no leader and follower read does not apply.
func TestServerProxy_NoLeaderNonRead(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(104, 2, 0, "")
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 0, term: 0}

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := &Packet{}
		p.Magic = proto.ProtoMagic
		p.Opcode = proto.OpMetaCreateInode
		p.ReqID = 9001
		p.PartitionID = 104
		p.ArgLen = 0
		ok := m.serveProxy(srv, mp, p)
		require.False(t, ok)
		require.Equal(t, proto.OpAgain, p.ResultCode)
		_ = srv.Close()
	}()

	out := readAllAvailable(client, 2*time.Second)
	wg.Wait()
	require.NotEmpty(t, out)
}

// TestServerProxy_GetConnectFails verifies OpErr when dialing the leader fails.
func TestServerProxy_GetConnectFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := ln.Addr().String()
	require.NoError(t, ln.Close())

	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(105, 2, 1, deadAddr)
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 1, term: 1}

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := baseReadMetaPacket()
		p.PartitionID = 105
		ok := m.serveProxy(srv, mp, p)
		require.False(t, ok)
		require.Equal(t, proto.OpErr, p.ResultCode)
		_ = srv.Close()
	}()

	out := readAllAvailable(client, 5*time.Second)
	wg.Wait()
	require.NotEmpty(t, out)
}

// TestServerProxy_ProxyToLeaderOk verifies forwarding to leader and reading the reply.
func TestServerProxy_ProxyToLeaderOk(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	var wgAccept sync.WaitGroup
	wgAccept.Add(1)
	go func() {
		defer wgAccept.Done()
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		req := &proto.Packet{}
		if err := req.ReadFromConnWithVer(c, proto.NoReadDeadlineTime); err != nil {
			return
		}
		req.ResultCode = proto.OpOk
		_ = req.WriteToConn(c)
	}()

	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(106, 2, 1, ln.Addr().String())
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 1, term: 1}

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := baseReadMetaPacket()
		p.PartitionID = 106
		ok := m.serveProxy(srv, mp, p)
		require.False(t, ok)
		require.Equal(t, proto.OpOk, p.ResultCode)
		_ = srv.Close()
	}()

	out := readAllAvailable(client, 5*time.Second)
	wg.Wait()
	wgAccept.Wait()
	require.NotEmpty(t, out)
}

// TestServerProxy_FollowerReadAfterLeaderReadFails verifies returning true without responding on leader read failure.
func TestServerProxy_FollowerReadAfterLeaderReadFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		req := &proto.Packet{}
		_ = req.ReadFromConnWithVer(c, proto.NoReadDeadlineTime)
		_ = c.Close()
	}()

	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(107, 2, 1, ln.Addr().String())
	mp.raftPartition = &mockRaftPartitionForServeProxy{leaderID: 1, term: 1}

	client, srv := net.Pipe()
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := baseReadMetaPacket()
		p.PartitionID = 107
		p.Arg = []byte{proto.FollowerReadFlag}
		p.ArgLen = 1
		ok := m.serveProxy(srv, mp, p)
		require.True(t, ok)
		_ = srv.Close()
	}()

	_ = client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 64)
	n, rerr := client.Read(buf)
	require.Error(t, rerr)
	require.Equal(t, 0, n)
	wg.Wait()
}

func baseReadMetaPacket() *Packet {
	p := &Packet{}
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpMetaInodeGet
	p.ReqID = 4242
	p.PartitionID = 1
	p.ArgLen = 0
	return p
}

func nearReadMetaPacket() *Packet {
	p := baseReadMetaPacket()
	p.Arg = []byte{proto.NearReadFlag}
	p.ArgLen = 1
	return p
}

// TestServerProxy_IsForbiddenOp_LookupWhenForbidden documents OpMetaLookup is blocked on forbidden partitions.
func TestServerProxy_IsForbiddenOp_LookupWhenForbidden(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(200, 1, 1, "")
	mp.config.Forbidden = true
	require.True(t, m.IsForbiddenOp(mp, proto.OpMetaLookup))
}

// TestServerProxy_IsForbiddenOp_ReadMetaAllowedWhenForbidden verifies read ops not in the deny list stay allowed.
func TestServerProxy_IsForbiddenOp_ReadMetaAllowedWhenForbidden(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(201, 1, 1, "")
	mp.config.Forbidden = true
	require.False(t, m.IsForbiddenOp(mp, proto.OpMetaInodeGet))
}

// TestServerProxy_IsForbiddenOp_NotForbiddenPartition verifies the deny list is ignored when the partition is not forbidden.
func TestServerProxy_IsForbiddenOp_NotForbiddenPartition(t *testing.T) {
	m := newTestMetadataManager()
	mp := newTestMetaPartitionForProxy(202, 1, 1, "")
	mp.config.Forbidden = false
	require.False(t, m.IsForbiddenOp(mp, proto.OpMetaCreateInode))
}
