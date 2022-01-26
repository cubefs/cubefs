// Copyright 2018 The Cubefs Authors.
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
	"github.com/cubefs/cubefs/util"
	"io"
	"net"
	"smux"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
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

// Read data from the specified tcp connection until the connection is closed by the remote or the tcp service is down.
func (m *MetaNode) serveConn(conn net.Conn, stopC chan uint8) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	remoteAddr := conn.RemoteAddr().String()
	for {
		select {
		case <-stopC:
			return
		default:
		}
		p := &Packet{}
		if err := p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
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

func (m *MetaNode) handlePacket(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	// Handle request
	err = m.metadataManager.HandleMetadataOperation(conn, p, remoteAddr)
	return
}

func (m *MetaNode) startSmuxServer() (err error) {
	// initialize and start the server.
	m.smuxStopC = make(chan uint8)
	addr := util.ShiftAddrPort(":"+m.listen, smuxPortShift)
	ln, err := net.Listen("tcp", addr)
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
			go m.serveSmuxConn(conn, stopC)
		}
	}(m.smuxStopC)
	log.LogInfof("start Smux Server over...")
	return
}

func (m *MetaNode) stopSmuxServer() {
	if smuxPool != nil {
		smuxPool.Close()
		log.LogDebugf("action[stopSmuxServer] stop smux conn pool")
	}

	if m.smuxStopC != nil {
		defer func() {
			if r := recover(); r != nil {
				log.LogErrorf("action[stopSmuxServer],err:%v", r)
			}
		}()
		close(m.smuxStopC)
	}
}

func (m *MetaNode) serveSmuxConn(conn net.Conn, stopC chan uint8) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	remoteAddr := conn.RemoteAddr().String()

	var sess *smux.Session
	var err error
	sess, err = smux.Server(conn, smuxPoolCfg.Config)
	if err != nil {
		log.LogErrorf("action[serveSmuxConn] failed to serve smux connection, err(%v)", err)
		return
	}
	defer sess.Close()

	for {
		select {
		case <-stopC:
			return
		default:
		}

		stream, err := sess.AcceptStream()
		if err != nil {
			if util.FilterSmuxAcceptError(err) != nil {
				log.LogErrorf("action[startSmuxService] failed to accept, err: %s", err)
			} else {
				log.LogInfof("action[startSmuxService] accept done, err: %s", err)
			}
			break
		}
		go m.serveSmuxStream(stream, remoteAddr, stopC)
	}
}

func (m *MetaNode) serveSmuxStream(stream *smux.Stream, remoteAddr string, stopC chan uint8) {
	for {
		select {
		case <-stopC:
			return
		default:
		}

		p := &Packet{}
		if err := p.ReadFromConn(stream, proto.NoReadDeadlineTime); err != nil {
			if err != io.EOF {
				log.LogError("serve MetaNode: ", err.Error())
			}
			return
		}
		if err := m.handlePacket(stream, p, remoteAddr); err != nil {
			log.LogErrorf("serve handlePacket fail: %v", err)
		}
	}
}
