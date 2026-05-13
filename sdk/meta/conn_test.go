// Copyright 2025 The CubeFS Authors.
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
// permissions and limitations under the License.

package meta

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func newConnTestMetaWrapper() *MetaWrapper {
	return &MetaWrapper{
		volname:            "conn-test-vol",
		metaSendTimeout:    30,
		leaderRetryTimeout: 20,
		conns:              util.NewConnectPool(),
		partitions:         make(map[uint64]*MetaPartition),
		dirtyInodes:        newTestDirtyInodeCacheNoBg(DirtyInodeTTL, MaxDirtyInodeCache),
		FollowerRead:       true,
		NearRead:           true,
		defaultMetaRegion:  "",
		RegionNearRead:     false,
		InnerReq:           false,
	}
}

func TestSendToMetaPartitionLeader_NearReadSetsNearReadFlag(t *testing.T) {
	chNear := make(chan *proto.Packet, 1)
	addrNear, cleanupNear := startMockMetaPacketListener(t, mockLookupOKHandler(chNear))
	t.Cleanup(cleanupNear)

	// Leader is unused for first hop when near-read picks the lowest-latency host first.
	addrLeader := "127.0.0.1:65530"

	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })

	mw.HostLatency.Store(addrNear, time.Millisecond)
	mw.HostLatency.Store(addrLeader, 100*time.Millisecond)

	mp := &MetaPartition{
		PartitionID: 1001,
		LeaderAddr:  addrLeader,
		Members:     []string{addrLeader, addrNear},
		Region:      "",
	}

	req := &proto.LookupRequest{VolName: mw.volname, PartitionID: mp.PartitionID, ParentID: 7, Name: "n", VerSeq: 0}
	pkt := proto.NewPacketReqID()
	pkt.Opcode = proto.OpMetaLookup
	pkt.PartitionID = mp.PartitionID
	require.NoError(t, pkt.MarshalData(req))

	_, err := mw.sendToMetaPartitionLeader(mp, pkt, 20000)
	require.NoError(t, err)

	select {
	case got := <-chNear:
		require.Equal(t, uint32(1), got.ArgLen, "near-read path should set NearReadFlag in Arg")
		require.Len(t, got.Arg, 1)
		require.Equal(t, byte(proto.NearReadFlag), got.Arg[0])
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for near replica to receive request")
	}
}

func TestSendToMetaPartitionLeader_SkipNearReadWhenDirtyInode(t *testing.T) {
	chLeader := make(chan *proto.Packet, 1)
	addrLeader, cleanupLeader := startMockMetaPacketListener(t, mockLookupOKHandler(chLeader))
	t.Cleanup(cleanupLeader)

	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })

	mw.HostLatency.Store(addrLeader, 100*time.Millisecond)

	const parentIno = uint64(441229642)

	mp := &MetaPartition{
		PartitionID: 3935,
		LeaderAddr:  addrLeader,
		Members:     []string{addrLeader},
		Region:      "",
	}

	mw.dirtyInodes.mark(parentIno)

	req := &proto.LookupRequest{VolName: mw.volname, PartitionID: mp.PartitionID, ParentID: parentIno, Name: "f", VerSeq: 0}
	pkt := proto.NewPacketReqID()
	pkt.Opcode = proto.OpMetaLookup
	pkt.PartitionID = mp.PartitionID
	require.NoError(t, pkt.MarshalData(req))

	_, err := mw.sendToMetaPartitionLeader(mp, pkt, 20000, parentIno)
	require.NoError(t, err)

	select {
	case got := <-chLeader:
		require.Equal(t, uint32(0), got.ArgLen, "dirty parent: should not set NearReadFlag on Arg")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for leader to receive request")
	}
}

func TestSendToMetaPartitionLeader_SkipNearReadWhenAnyInodeInBatchDirty(t *testing.T) {
	chLeader := make(chan *proto.Packet, 1)
	addrLeader, cleanupLeader := startMockMetaPacketListener(t, mockLookupOKHandler(chLeader))
	t.Cleanup(cleanupLeader)

	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })

	const dirtyIno = uint64(99)
	mw.dirtyInodes.mark(dirtyIno)

	mp := &MetaPartition{
		PartitionID: 4004,
		LeaderAddr:  addrLeader,
		Members:     []string{addrLeader},
	}

	req := &proto.LookupRequest{VolName: mw.volname, PartitionID: mp.PartitionID, ParentID: 1, Name: "f", VerSeq: 0}
	pkt := proto.NewPacketReqID()
	pkt.Opcode = proto.OpMetaLookup
	pkt.PartitionID = mp.PartitionID
	require.NoError(t, pkt.MarshalData(req))

	// First dirtyIno is clean; second is dirty — near-read must still be skipped.
	_, err := mw.sendToMetaPartitionLeader(mp, pkt, 20000, 1, dirtyIno)
	require.NoError(t, err)

	select {
	case got := <-chLeader:
		require.Equal(t, uint32(0), got.ArgLen)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for leader to receive request")
	}
}

func TestSendToMetaPartitionLeader_NearReadDisabledUsesLeader(t *testing.T) {
	chLeader := make(chan *proto.Packet, 1)
	addrLeader, cleanupLeader := startMockMetaPacketListener(t, mockLookupOKHandler(chLeader))
	t.Cleanup(cleanupLeader)

	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })
	mw.NearRead = false

	mp := &MetaPartition{
		PartitionID: 5005,
		LeaderAddr:  addrLeader,
		Members:     []string{addrLeader},
	}

	req := &proto.LookupRequest{VolName: mw.volname, PartitionID: mp.PartitionID, ParentID: 1, Name: "f", VerSeq: 0}
	pkt := proto.NewPacketReqID()
	pkt.Opcode = proto.OpMetaLookup
	pkt.PartitionID = mp.PartitionID
	require.NoError(t, pkt.MarshalData(req))

	_, err := mw.sendToMetaPartitionLeader(mp, pkt, 20000)
	require.NoError(t, err)

	select {
	case got := <-chLeader:
		require.Equal(t, uint32(0), got.ArgLen, "near-read off: request should go to leader without NearReadFlag")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for leader to receive request")
	}
}

func TestSendToMetaPartitionLeader_InnerReqSkipsNearReadHostPick(t *testing.T) {
	chLeader := make(chan *proto.Packet, 1)
	addrLeader, cleanupLeader := startMockMetaPacketListener(t, mockLookupOKHandler(chLeader))
	t.Cleanup(cleanupLeader)

	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })
	mw.InnerReq = true

	mw.HostLatency.Store(addrLeader, 100*time.Millisecond)

	mp := &MetaPartition{
		PartitionID: 2002,
		LeaderAddr:  addrLeader,
		Members:     []string{addrLeader},
	}

	req := &proto.LookupRequest{VolName: mw.volname, PartitionID: mp.PartitionID, ParentID: 1, Name: "x", VerSeq: 0}
	pkt := proto.NewPacketReqID()
	pkt.Opcode = proto.OpMetaLookup
	pkt.PartitionID = mp.PartitionID
	require.NoError(t, pkt.MarshalData(req))

	_, err := mw.sendToMetaPartitionLeader(mp, pkt, 20000)
	require.NoError(t, err)

	select {
	case got := <-chLeader:
		require.Equal(t, uint32(0), got.ArgLen, "InnerReq: read meta should not use near-read flag")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for leader")
	}
}

func TestSendToMetaPartitionLeader_DeltaWhenSendTimeLimitAboveMin(t *testing.T) {
	addr, cleanup := startMockMetaPacketListener(t, mockLookupOKHandler(nil))
	t.Cleanup(cleanup)

	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })

	mp := &MetaPartition{
		PartitionID: 3003,
		LeaderAddr:  addr,
		Members:     []string{addr},
	}

	req := &proto.LookupRequest{VolName: mw.volname, PartitionID: mp.PartitionID, ParentID: 1, Name: "y", VerSeq: 0}
	pkt := proto.NewPacketReqID()
	pkt.Opcode = proto.OpMetaLookup
	pkt.PartitionID = mp.PartitionID
	require.NoError(t, pkt.MarshalData(req))

	// Exercise delta branch: sendTimeLimit > MinRetryTime*1000 (20s in ms)
	_, err := mw.sendToMetaPartitionLeader(mp, pkt, 25000)
	require.NoError(t, err)
}
