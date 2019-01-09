// Copyright 2018 The Containerfs Authors.
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

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
)

const (
	ForceCloseConnect = true
	NoCloseConnect    = false
)

func (m *metaManager) serveProxy(conn net.Conn, mp MetaPartition,
	p *Packet) (ok bool) {
	var (
		mConn      *net.TCPConn
		leaderAddr string
		err        error
	)
	if leaderAddr, ok = mp.IsLeader(); ok {
		return
	}
	if leaderAddr == "" {
		err = ErrNonLeader
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		goto end
	}
	// GetConnect Master Conn
	mConn, err = m.connPool.GetConnect(leaderAddr)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceCloseConnect)
		goto end
	}
	// Send Master Conn
	if err = p.WriteToConn(mConn); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceCloseConnect)
		goto end
	}
	// Read conn from master
	if err = p.ReadFromConn(mConn, proto.NoReadDeadlineTime); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.connPool.PutConnect(mConn, ForceCloseConnect)
		goto end
	}
	m.connPool.PutConnect(mConn, NoCloseConnect)
end:
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("[serveProxy]: %s", err.Error())
	}
	log.LogDebugf("[serveProxy] request:%v, response: %v", p.GetOpMsg(),
		p.GetResultMsg())
	return
}
