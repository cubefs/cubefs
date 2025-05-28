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
	"hash/crc32"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
)

var _randErr uint32

func initExtentListener() {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	extentListener = listener
	go func() {
		for {
			conn, err := listener.Accept()
			select {
			case <-closeCh:
				return
			default:
			}
			if err == nil {
				go serveExtentConn(conn)
			}
		}
	}()
}

func serveExtentConn(conn net.Conn) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	for {
		c.SetReadDeadline(time.Now().Add(time.Second * _tcpServerTimeoutSec))
		select {
		case <-closeCh:
			return
		default:
		}

		p := proto.NewPacketReqID()
		if err := p.ReadFromConn(c, proto.NoReadDeadlineTime); err != nil {
			return
		}
		if p.Opcode != proto.OpStreamRead && p.Opcode != proto.OpStreamFollowerRead {
			log.Println("error code", p.GetOpMsg())
			return
		}

		r := newReplyPacket(p.ReqID, p.PartitionID, p.ExtentID)
		r.ResultCode = proto.OpOk
		r.Size = p.Size
		r.Data = make([]byte, r.Size)
		switch atomic.AddUint32(&_randErr, 1) {
		case 1:
		case 2:
			r.ResultCode = proto.OpErr
		case 3:
			r.PartitionID = 2333
		default:
			r.CRC = crc32.ChecksumIEEE(r.Data[:r.Size])
		}
		if err := r.WriteToConn(conn); err != nil {
			return
		}
	}
}
