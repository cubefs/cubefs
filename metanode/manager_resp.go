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
// permissions and limitations under the License.

package metanode

import (
	"net"
	"runtime/debug"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"

)

// Reply operation results to the master.
func (m *metadataManager) respondToMaster(task *proto.AdminTask) (err error) {
	// handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	if err = masterClient.NodeAPI().ResponseMetaNodeTask(task); err != nil {
		err = errors.Trace(err, "try respondToMaster failed")
		log.LogError(err.Error())
	}
	return
}

// Reply data through tcp connection to the client.
func (m *metadataManager) respondToClient(conn net.Conn, p *Packet) (err error) {
	// Handle panic
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("respondToClient: panic occurred: %v\n%v", r, string(debug.Stack()))
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()

	// process data and send reply though specified tcp connection.
	err = p.WriteToConn(conn, proto.WriteDeadlineTime)
	if err != nil {
		log.LogErrorf("response to client[%s], "+
			"request[%s], response packet[%s]",
			err.Error(), p.GetOpMsg(), p.GetResultMsg())
	}
	return
}

func (m *metadataManager) responseAckOKToMaster(conn net.Conn, p *Packet) {
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
}
