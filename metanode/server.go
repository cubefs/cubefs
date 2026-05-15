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
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/cubefs/cubefs/depends/xtaci/smux"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

// serverConfig holds common server configuration
type serverConfig struct {
	addr    string
	stopC   chan uint8
	handler func(net.Conn, chan uint8)
}

// buildServerAddr constructs server address based on configuration
func (m *MetaNode) buildServerAddr() string {
	if m.bindIp {
		return fmt.Sprintf("%s:%s", m.localAddr, m.listen)
	}
	return fmt.Sprintf(":%s", m.listen)
}

// startGenericServer starts a generic TCP server with given configuration
func (m *MetaNode) startGenericServer(config serverConfig) error {
	ln, err := net.Listen("tcp", config.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", config.addr, err)
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
				log.LogWarnf("failed to accept connection: %v", err)
				continue
			}
			go config.handler(conn, stopC)
		}
	}(config.stopC)

	return nil
}

// StartTcpService binds and listens to the specified port.
func (m *MetaNode) startServer() (err error) {
	// Initialize and start the server
	m.httpStopC = make(chan uint8)

	config := serverConfig{
		addr:    m.buildServerAddr(),
		stopC:   m.httpStopC,
		handler: m.serveConn,
	}

	if err = m.startGenericServer(config); err != nil {
		return err
	}

	log.LogInfof("start server over...")
	return nil
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

// configureTCPConn configures TCP connection with optimal settings
func (m *MetaNode) configureTCPConn(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetNoDelay(true)
	}
}

// handleConnectionError handles connection errors with appropriate logging
func (m *MetaNode) handleConnectionError(err error, context string) {
	if err == nil {
		return
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, io.EOF.Error()) {
		log.LogErrorf("%s: connection error: %v", context, err)
	}
}

// handlePacketError handles packet processing errors with appropriate logging
func (m *MetaNode) handlePacketError(err error, p *Packet) {
	if err == nil {
		return
	}

	errMsg := err.Error()
	// Check for specific error conditions that should be logged as warnings
	if strings.Contains(errMsg, "over quota") ||
		strings.Contains(errMsg, "inode ID out of range") ||
		strings.Contains(errMsg, "unknown meta partition") ||
		p.ResultCode == proto.OpNotExistErr ||
		strings.Contains(errMsg, "rate limited") {
		log.LogWarnf("serve handlePacket fail: %v", err)
	} else {
		log.LogErrorf("serve handlePacket fail: %v", err)
	}
}

// Read data from the specified tcp connection until the connection is closed by the remote or the tcp service is down.
func (m *MetaNode) serveConn(conn net.Conn, stopC chan uint8) {
	defer func() {
		conn.Close()
		m.RemoveConnection()
	}()

	m.AddConnection()
	m.configureTCPConn(conn)
	remoteAddr := conn.RemoteAddr().String()

	for {
		select {
		case <-stopC:
			return
		default:
		}

		p := &Packet{}
		if err := p.ReadFromConnWithVer(conn, proto.NoReadDeadlineTime); err != nil {
			m.handleConnectionError(err, "serve MetaNode")
			return
		}

		if err := m.handlePacket(conn, p, remoteAddr); err != nil {
			if p.ResultCode == proto.OpWriteOpOfProtoVerForbidden {
				return
			}
			m.handlePacketError(err, p)
		}
	}
}

func (m *MetaNode) handlePacket(conn net.Conn, p *Packet, remoteAddr string) error {
	// Handle request
	return m.metadataManager.HandleMetadataOperation(conn, p, remoteAddr)
}

func (m *MetaNode) startSmuxServer() (err error) {
	// Initialize and start the server
	m.smuxStopC = make(chan uint8)

	ipPort := m.buildServerAddr()
	addr := util.ShiftAddrPort(ipPort, smuxPortShift)

	config := serverConfig{
		addr:    addr,
		stopC:   m.smuxStopC,
		handler: m.serveSmuxConn,
	}

	if err = m.startGenericServer(config); err != nil {
		return err
	}

	log.LogInfof("start Smux Server over...")
	return nil
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
	defer func() {
		conn.Close()
		m.RemoveConnection()
	}()

	m.AddConnection()
	m.configureTCPConn(conn)
	remoteAddr := conn.RemoteAddr().String()

	sess, err := smux.Server(conn, smuxPoolCfg.Config)
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
		if err := p.ReadFromConnWithVer(stream, proto.NoReadDeadlineTime); err != nil {
			m.handleConnectionError(err, "serve MetaNode")
			return
		}

		if err := m.handlePacket(stream, p, remoteAddr); err != nil {
			log.LogErrorf("serve handlePacket fail: %v", err)
		}
	}
}
