// Copyright 2018 The Chubao Authors.
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

package metanode

import (
	"net"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ForceClosedConnect = true
	NoClosedConnect    = false
)

// The proxy is used during the leader change. When a leader of a partition changes, the proxy forwards the request to
// the new leader.
func (m *metadataManager) serveProxy(conn net.Conn, mp MetaPartition,
	p *Packet) (ok bool) {
	var (
		mConn      *net.TCPConn
		leaderAddr string
		err        error
		reqID      = p.ReqID
		reqOp      = p.Opcode
	)
	if leaderAddr, ok = mp.IsLeader(); ok {
		return
	}
	if leaderAddr == "" {
		err = ErrNoLeader
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		goto end
	}

	mConn, err = m.connPool.GetConnect(leaderAddr)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}

	// send to master connection
	if err = p.WriteToConn(mConn); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}

	// read connection from the master
	if err = p.ReadFromConn(mConn, proto.NoReadDeadlineTime); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceClosedConnect)
		goto end
	}
	if reqID != p.ReqID || reqOp != p.Opcode {
		log.LogErrorf("serveProxy: send and received packet mismatch: req(%v_%v) resp(%v_%v)",
			reqID, reqOp, p.ReqID, p.Opcode)
	}
	m.connPool.PutConnect(mConn, NoClosedConnect)
end:
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("[serveProxy]: req: %d - %v, %s", p.GetReqID(),
			p.GetOpMsg(), err.Error())
	}
	log.LogDebugf("[serveProxy] req: %d - %v, resp: %v", p.GetReqID(), p.GetOpMsg(),
		p.GetResultMsg())
	return
}
