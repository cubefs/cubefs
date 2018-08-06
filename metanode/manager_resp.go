// Copyright 2018 The ChuBao Authors.
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
	"encoding/json"
	"net"

	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
)

const (
	masterResponsePath = "/metaNode/response" // Method: 'POST',
	// ContentType: 'application/json'
)

// ReplyToMaster reply operation result to master by sending http request.
func (m *metaManager) respondToMaster(data interface{}) (err error) {
	// Handle panic
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
	// Process data and send reply though http specified remote address.
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	postToMaster("POST", masterResponsePath, jsonBytes)
	return
}

// ReplyToClient send reply data though tcp connection to client.
func (m *metaManager) respondToClient(conn net.Conn, p *Packet) (err error) {
	// Handle panic
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
	// Process data and send reply though specified tcp connection.
	err = p.WriteToConn(conn)
	if err != nil {
		log.LogErrorf("response to client[%s], "+
			"request[%s], response packet[%s]",
			err.Error(), p.GetOpMsg(), p.GetResultMesg())
	}
	return
}

func (m *metaManager) responseAckOKToMaster(conn net.Conn, p *Packet) {
	go func() {
		p.PackOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
}
