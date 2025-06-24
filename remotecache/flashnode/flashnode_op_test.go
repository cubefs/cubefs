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
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

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
	t.Run("ManualScan", testTCPManualScan)
	t.Run("CachePutBlock", testTCPCachePutBlock)
	t.Run("CacheDelete", testTCPCacheDelete)
	t.Run("CacheReadObject", testTCPObjectCacheRead)
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

	flashServer.readLimiter.SetBurst(200)
	flashServer.readLimiter.SetLimit(20)
	time.Sleep(time.Second)
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
	require.Equal(t, proto.OpOk, r.ResultCode) // No DataSource

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
	flashServer.readLimiter.SetBurst(0)
	flashServer.readLimiter.SetLimit(0)
	time.Sleep(time.Second)
	p.Opcode = proto.OpFlashNodeCacheRead
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // Read limited

	flashServer.readLimiter.SetBurst(200)
	flashServer.readLimiter.SetLimit(20)
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

func testTCPCachePutBlock(t *testing.T) {
	conn := newTCPConn(t)
	defer conn.Close()
	p := proto.NewPacketReqID()
	r := proto.NewPacket()
	p.Opcode = proto.OpFlashNodeCachePutBlock
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // lack parameter
	key := "testTCPCacheWrite_key"
	req := &proto.PutBlockHead{
		UniKey:   key,
		BlockLen: 1,
		TTL:      0,
	}
	_ = p.MarshalDataPb(req)
	p.Opcode = proto.OpFlashNodeCachePutBlock
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)
	buf := make([]byte, proto.CACHE_BLOCK_PACKET_SIZE+4)
	buf[0] = '{'
	binary.BigEndian.PutUint32(buf[proto.CACHE_BLOCK_PACKET_SIZE:], crc32.ChecksumIEEE(buf[:proto.CACHE_BLOCK_PACKET_SIZE]))
	_, err := conn.Write(buf[:proto.CACHE_BLOCK_PACKET_SIZE+4])
	require.NoError(t, err)
}

func testTCPCacheDelete(t *testing.T) {
	conn := newTCPConn(t)
	defer conn.Close()
	p := proto.NewPacketReqID()
	r := proto.NewPacket()
	p.Opcode = proto.OpFlashNodeCacheDelete
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode)

	p.Data = ([]byte)("testTCPCacheWrite_key")
	p.Size = uint32(len(p.Data))
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)
}

func testTCPManualScan(t *testing.T) {
	conn := newTCPConn(t)
	p := proto.NewPacketReqID()

	p.Opcode = proto.OpFlashNodeScan
	require.NoError(t, p.WriteToConn(conn))
	r := proto.NewPacket()
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)
	time.Sleep(time.Second)
	_ = conn.Close()

	conn = newTCPConn(t)
	p.Size = 1
	p.Data = []byte{'{'}
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)
	_ = conn.Close()
	task := &proto.FlashManualTask{
		Id:      uuid.New().String(),
		VolName: "manual_test",
		ManualTaskConfig: proto.ManualTaskConfig{
			Prefix: "/",
		},
		Action: proto.FlashManualWarmupAction,
		Status: int(proto.Flash_Task_Running),
	}
	req := &proto.FlashNodeManualTaskRequest{
		MasterAddr: masterAddr,
		FnNodeAddr: flashServer.localAddr,
		Task:       task,
	}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	body, err := json.Marshal(adminTask)
	require.NoError(t, err)
	p.Size = uint32(len(body))
	p.Data = body
	conn = newTCPConn(t)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpOk, r.ResultCode)
	_ = conn.Close()
}

func testShouldCache(t *testing.T) {
	var wg sync.WaitGroup
	key := "testCacheKey"
	goroutines := 10
	var countA, countB int64
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := flashServer.shouldCache(key)
			switch {
			case errors.Is(err, proto.ErrorNotExistShouldCache):
				atomic.AddInt64(&countA, 1)
			case errors.Is(err, proto.ErrorNotExistShouldNotCache):
				atomic.AddInt64(&countB, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	flashServer.missCache.Delete(key)
	t.Logf("error not exist should cache count: %d", countA)
	t.Logf("error not exist should not cache count: %d", countB)
	require.Equal(t, countA, int64(1))
	require.Equal(t, countB, int64(9))
}

func testTCPObjectCacheRead(t *testing.T) {
	conn := newTCPConn(t)
	defer conn.Close()
	p := proto.NewPacketReqID()
	r := proto.NewPacket()
	flashServer.readLimiter.SetBurst(0)
	flashServer.readLimiter.SetLimit(0)
	time.Sleep(time.Second)
	p.Opcode = proto.OpFlashNodeCacheReadObject
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // Read limited

	flashServer.readLimiter.SetBurst(200)
	flashServer.readLimiter.SetLimit(20)
	time.Sleep(time.Second)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // CacheReadRequestBase is nil

	p.Size = 1
	p.Data = []byte{'{'}
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode) // CacheReadRequestBase invalid

	req := new(proto.CacheReadRequestBase)
	req.Key = "1234567"
	req.Slot = 12345678
	req.Offset = 0
	req.Size_ = 1024
	req.TTL = _ttl
	p.MarshalDataPb(req)
	require.NoError(t, p.WriteToConn(conn))
	require.NoError(t, r.ReadFromConn(conn, 3))
	require.Equal(t, proto.OpErr, r.ResultCode)
}
