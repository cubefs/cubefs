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
