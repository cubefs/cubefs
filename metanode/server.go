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
	"context"
	"io"
	"net"

	"github.com/chubaofs/chubaofs/util/log"
)

// StartTcpService binds and listens to the specified port.
func (m *MetaNode) startServer() (err error) {
	// initialize and start the server.
	m.httpStopC = make(chan uint8)
	ln, err := net.Listen("tcp", ":"+m.listen)
	if err != nil {
		return
	}
	go func(stopC chan uint8) {
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			select {
			case <-stopC:
				return
			default:
			}
			if err != nil {
				continue
			}
			go m.serveConn(conn, stopC)
		}
	}(m.httpStopC)
	log.LogInfof("start server over...")
	return
}

func (m *MetaNode) stopServer() {
	if m.httpStopC != nil {
		defer func() {
			if r := recover(); r != nil {
				log.LogErrorf("action[StopTcpServer],err:%v", r)
			}
		}()
		close(m.httpStopC)
	}
}

const (
	MetaNodeServerTimeOut = 60*5
)
// Read data from the specified tcp connection until the connection is closed by the remote or the tcp service is down.
func (m *MetaNode) serveConn(conn net.Conn, stopC chan uint8) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	_ = c.SetKeepAlive(true) // Ignore error
	_ = c.SetNoDelay(true)   // Ignore error
	remoteAddr := conn.RemoteAddr().String()
	for {
		select {
		case <-stopC:
			return
		default:
		}
		p := NewPacket(context.Background())
		if err := p.ReadFromConn(conn, MetaNodeServerTimeOut); err != nil {
			if err != io.EOF {
				log.LogError("serve MetaNode: ", err.Error())
			}
			return
		}
		if err := m.handlePacket(conn, p, remoteAddr); err != nil {
			log.LogErrorf("serve handlePacket fail: %v", err)
		}
	}
}

func (m *MetaNode) handlePacket(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	// Handle request
	err = m.metadataManager.HandleMetadataOperation(conn, p, remoteAddr)
	return
}
