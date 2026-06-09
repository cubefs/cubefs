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
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

var connTestProtoOnce sync.Once

func connTestInitProto() {
	connTestProtoOnce.Do(func() {
		proto.InitBufferPool(int64(32768))
	})
}

// startMockMetaPacketListener listens on 127.0.0.1:0, blocks until the accept
// goroutine is running, then returns the address. cleanup closes the listener
// and waits for the accept loop to exit. Each accepted connection is handled in
// its own goroutine so client connect-pool retries do not deadlock the server.
func startMockMetaPacketListener(t *testing.T, handler func(conn net.Conn) error) (addr string, cleanup func()) {
	t.Helper()
	connTestInitProto()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr = ln.Addr().String()
	ready := make(chan struct{})
	done := make(chan struct{})

	go func() {
		close(ready)
		for {
			conn, err := ln.Accept()
			if err != nil {
				break
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = handler(c)
			}(conn)
		}
		close(done)
	}()

	<-ready

	cleanup = func() {
		_ = ln.Close()
		<-done
	}
	return addr, cleanup
}

func mockLookupOKHandler(firstReq chan<- *proto.Packet) func(net.Conn) error {
	return func(conn net.Conn) error {
		pkt := proto.NewPacket()
		if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			return err
		}
		if firstReq != nil {
			select {
			case firstReq <- pkt:
			default:
			}
		}
		resp := proto.NewPacketReqID()
		resp.ReqID = pkt.ReqID
		resp.Opcode = pkt.Opcode
		resp.PartitionID = pkt.PartitionID
		resp.ResultCode = proto.OpOk
		body, err := json.Marshal(&proto.LookupResponse{Inode: 4242, Mode: 0o644})
		if err != nil {
			return err
		}
		resp.Data = body
		resp.Size = uint32(len(body))
		return resp.WriteToConn(conn)
	}
}

// mockLookupThenIgetHandler serves OpMetaLookup then OpMetaInodeGet on one connection.
func mockLookupThenIgetHandler(lookupInode uint64, lookupMode uint32, igetNlink uint32) func(net.Conn) error {
	return func(conn net.Conn) error {
		for step := 0; step < 2; step++ {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
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
				return fmt.Errorf("unexpected opcode %v", pkt.Opcode)
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

func mockExtentsListOKHandler(firstReq chan<- *proto.Packet) func(net.Conn) error {
	return mockExtentsListResultHandler(proto.OpOk, firstReq)
}

func mockExtentsListResultHandler(resultCode uint8, firstReq chan<- *proto.Packet) func(net.Conn) error {
	return func(conn net.Conn) error {
		pkt := proto.NewPacket()
		if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			return err
		}
		if firstReq != nil {
			select {
			case firstReq <- pkt:
			default:
			}
		}
		switch pkt.Opcode {
		case proto.OpMetaExtentsList, proto.OpMetaAsyncExtentsList:
		default:
			return fmt.Errorf("unexpected opcode %v", pkt.Opcode)
		}

		resp := proto.NewPacketReqID()
		resp.ReqID = pkt.ReqID
		resp.Opcode = pkt.Opcode
		resp.PartitionID = pkt.PartitionID
		resp.ResultCode = resultCode
		if resultCode == proto.OpOk {
			body, err := json.Marshal(&proto.GetExtentsResponse{
				Generation: 7,
				Size:       4096,
			})
			if err != nil {
				return err
			}
			resp.Data = body
			resp.Size = uint32(len(body))
		}
		return resp.WriteToConn(conn)
	}
}

// mockBatchIgetThenExtentsListHandler serves OpMetaBatchInodeGet then extents list on one connection.
// mockInodeGetAndExtentsHandler serves OpMetaInodeGet and extents list on one connection (parallel InodeGetExt_ll).
func mockInodeGetAndExtentsHandler(inode uint64, extentsOpCh chan<- uint8) func(net.Conn) error {
	return func(conn net.Conn) error {
		for {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
			}

			resp := proto.NewPacketReqID()
			resp.ReqID = pkt.ReqID
			resp.Opcode = pkt.Opcode
			resp.PartitionID = pkt.PartitionID
			resp.ResultCode = proto.OpOk

			var body []byte
			var err error
			switch pkt.Opcode {
			case proto.OpMetaInodeGet:
				body, err = json.Marshal(&proto.InodeGetResponse{
					Info: &proto.InodeInfo{
						Inode: inode,
						Mode:  0o100644,
						Nlink: 1,
					},
				})
			case proto.OpMetaExtentsList, proto.OpMetaAsyncExtentsList:
				if extentsOpCh != nil {
					extentsOpCh <- pkt.Opcode
				}
				body, err = json.Marshal(&proto.GetExtentsResponse{Generation: 11, Size: 8192})
			default:
				return fmt.Errorf("unexpected opcode %v", pkt.Opcode)
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
	}
}

func mockBatchIgetThenExtentsListHandler(regularInode uint64, extentsOpCh chan<- uint8) func(net.Conn) error {
	return mockBatchIgetThenExtentsResultHandler(regularInode, proto.OpOk, extentsOpCh)
}

func mockBatchIgetThenExtentsResultHandler(regularInode uint64, extentsResultCode uint8, extentsOpCh chan<- uint8) func(net.Conn) error {
	return func(conn net.Conn) error {
		for step := 0; step < 2; step++ {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
			}

			resp := proto.NewPacketReqID()
			resp.ReqID = pkt.ReqID
			resp.Opcode = pkt.Opcode
			resp.PartitionID = pkt.PartitionID

			var body []byte
			var err error
			switch pkt.Opcode {
			case proto.OpMetaBatchInodeGet:
				resp.ResultCode = proto.OpOk
				body, err = json.Marshal(&proto.BatchInodeGetResponse{
					Infos: []*proto.InodeInfo{{
						Inode: regularInode,
						Mode:  0o100644,
					}},
				})
			case proto.OpMetaExtentsList, proto.OpMetaAsyncExtentsList:
				if extentsOpCh != nil {
					extentsOpCh <- pkt.Opcode
				}
				resp.ResultCode = extentsResultCode
				if extentsResultCode == proto.OpOk {
					body, err = json.Marshal(&proto.GetExtentsResponse{Generation: 3, Size: 512})
				}
			default:
				return fmt.Errorf("unexpected opcode %v", pkt.Opcode)
			}
			if err != nil {
				return err
			}
			if body != nil {
				resp.Data = body
				resp.Size = uint32(len(body))
			}
			if err = resp.WriteToConn(conn); err != nil {
				return err
			}
		}
		return nil
	}
}
