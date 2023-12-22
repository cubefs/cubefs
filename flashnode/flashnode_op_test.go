// Copyright 2023 The CubeFS Authors.
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

package flashnode

import (
	"net"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

const (
	_volume          = "volume"
	_inode    uint64 = 1
	_offset   uint64 = 1024
	_version  uint32 = 0
	_ttl             = 60
	blockSize        = 1024
)

func newTCPConn(t *testing.T) *net.TCPConn {
	tcpAddr, err := net.ResolveTCPAddr("tcp", flashServer.localAddr)
	require.NoError(t, err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	require.NoError(t, err)
	return conn
}

func testTCP(t *testing.T) {
	t.Run("Heartbeat", testTCPHeartbeat)
	t.Run("CachePrepare", testTCPCachePrepare)
	t.Run("CacheRead", testTCPCacheRead)
}

func testTCPHeartbeat(t *testing.T) {
	conn := newTCPConn(t)
	defer conn.Close()

	p := proto.NewPacketReqID()
	require.NoError(t, p.WriteToConn(conn))
	p.Opcode = proto.OpFlashNodeHeartbeat
	require.NoError(t, p.WriteToConn(conn))

	r := proto.NewPacket()
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)

	require.NoError(t, p.WriteToConn(conn))
	conn.Close()
	require.Error(t, r.ReadFromConn(conn, 3))
}

func testTCPCachePrepare(t *testing.T) {
	conn := newTCPConn(t)
	defer conn.Close()
	p := proto.NewPacketReqID()
	r := proto.NewPacket()

	p.Opcode = proto.OpFlashNodeCachePrepare
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // CachePrepareRequest is nil

	p.Size = 1
	p.Data = []byte{'{'}
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // CachePrepareRequest invalid

	prepare := new(proto.CachePrepareRequest)
	prepare.CacheRequest = &proto.CacheRequest{
		Volume:          _volume,
		Inode:           _inode,
		FixedFileOffset: _offset,
		TTL:             _ttl,
	}
	p.MarshalDataPb(prepare)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // No DataSource

	prepare.CacheRequest.Sources = []*proto.DataSource{{
		FileOffset:   0,
		PartitionID:  1,
		ExtentID:     1,
		ExtentOffset: 0,
		Size_:        blockSize,
		Hosts: []string{
			extentListener.Addr().String(), // _randErr
			extentListener.Addr().String(), // _randErr
			extentListener.Addr().String(),
		},
	}}
	prepare.FlashNodes = []string{flashServer.localAddr, httpServer.Addr}
	p.MarshalDataPb(prepare)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)

	time.Sleep(time.Second)
}

func testTCPCacheRead(t *testing.T) {
	conn := newTCPConn(t)
	defer conn.Close()
	p := proto.NewPacketReqID()
	r := proto.NewPacket()

	p.Opcode = proto.OpFlashNodeCacheRead
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // Read limited

	flashServer.readLimiter.SetBurst(100)
	flashServer.readLimiter.SetLimit(10)
	time.Sleep(time.Second)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // CacheReadRequest is nil

	p.Size = 1
	p.Data = []byte{'{'}
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // CacheReadRequest invalid

	prepare := new(proto.CacheReadRequest)
	prepare.CacheRequest = &proto.CacheRequest{
		Volume:          _volume,
		Inode:           _inode,
		FixedFileOffset: _offset,
		TTL:             _ttl,
		Version:         10, // error version
	}
	p.MarshalDataPb(prepare)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode)

	prepare.Size_ = blockSize
	prepare.CacheRequest.Version = _version
	p.MarshalDataPb(prepare)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)
	require.Equal(t, uint32(blockSize), r.Size)
}
