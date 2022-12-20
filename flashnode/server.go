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

package flashnode

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// NewServer creates a new flash node instance.
func NewServer() *FlashNode {
	return &FlashNode{}
}

// StartTcpService binds and listens to the specified port.
func (f *FlashNode) startTcpServer() (err error) {
	// initialize and start the server.
	f.stopTcpServerC = make(chan uint8)
	f.netListener, err = net.Listen("tcp", ":"+f.listen)
	if err != nil {
		return
	}
	go func(stopC chan uint8) {
		defer f.netListener.Close()
		var latestAlarm time.Time
		for {
			conn, err1 := f.netListener.Accept()
			select {
			case <-stopC:
				log.LogWarnf("http server stopped")
				return
			default:
			}
			if err1 != nil {
				log.LogErrorf("action[startTcpServer] failed to accept, err:%s", err.Error())
				// Alarm only once at 1 minute
				if time.Now().Sub(latestAlarm) > time.Minute {
					warnMsg := fmt.Sprintf("SERVER ACCEPT CONNECTION FAILED!\n"+
						"Failed on accept connection from %v and will retry after 10s.\n"+
						"Error message: %s", f.netListener.Addr(), err.Error())
					exporter.Warning(warnMsg)
					latestAlarm = time.Now()
				}
				time.Sleep(time.Second * 10)
				continue
			}
			go f.serveConn(conn, stopC)
		}
	}(f.stopTcpServerC)
	log.LogInfof("start server over...")
	return
}

func (f *FlashNode) stopServer() {
	if f.stopTcpServerC != nil {
		close(f.stopTcpServerC)
	}
	f.netListener.Close()
}

// serveConn Read data from the specified tcp connection until the connection is closed by the remote or the tcp service is down.
func (f *FlashNode) serveConn(conn net.Conn, stopC chan uint8) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	_ = c.SetKeepAlive(true) // Ignore error
	_ = c.SetNoDelay(true)   // Ignore error
	remoteAddr := conn.RemoteAddr().String()
	connReader := bufio.NewReader(c)
	for {
		select {
		case <-stopC:
			return
		default:
		}
		p := NewPacket(context.Background())
		_ = c.SetReadDeadline(time.Now().Add(time.Second * ServerTimeOut))
		if err := p.ReadFromReader(connReader); err != nil {
			if err != io.EOF {
				log.LogError("serve FlashNode: ", err.Error())
			}
			return
		}
		if err := f.preHandle(p); err != nil {
			logContent := fmt.Sprintf("preHandle %v.", p.LogMessage(p.GetOpMsg(), remoteAddr, p.StartT, err))
			log.LogErrorf(logContent)
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
			respondToClient(conn, p)
			return
		}
		if err := f.handlePacket(conn, p, remoteAddr); err != nil {
			log.LogErrorf("serve handlePacket fail: %v", err)
		}
	}
}
