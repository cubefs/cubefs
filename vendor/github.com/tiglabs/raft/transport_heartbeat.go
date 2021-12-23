// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"net"
	"sync"

	//"fmt"
	//"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util"
)

type heartbeatTransport struct {
	config     *TransportConfig
	raftServer *RaftServer
	listener   net.Listener
	mu         sync.RWMutex
	senders    map[uint64]*transportSender
	stopc      chan struct{}
}

func newHeartbeatTransport(raftServer *RaftServer, config *TransportConfig) (*heartbeatTransport, error) {
	var (
		listener net.Listener
		err      error
	)

	if listener, err = net.Listen("tcp", config.HeartbeatAddr); err != nil {
		return nil, err
	}
	t := &heartbeatTransport{
		config:     config,
		raftServer: raftServer,
		listener:   listener,
		senders:    make(map[uint64]*transportSender),
		stopc:      make(chan struct{}),
	}
	return t, nil
}

func (t *heartbeatTransport) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	select {
	case <-t.stopc:
		return
	default:
		close(t.stopc)
		t.listener.Close()
		for _, s := range t.senders {
			s.stop()
		}
	}
}

func (t *heartbeatTransport) start() {
	util.RunWorkerUtilStop(func() {
		for {
			select {
			case <-t.stopc:
				return
			default:
				conn, err := t.listener.Accept()
				if err != nil {
					continue
				}
				t.handleConn(util.NewConnTimeout(conn))
			}
		}
	}, t.stopc)
}

func (t *heartbeatTransport) handleConn(conn *util.ConnTimeout) {
	util.RunWorker(func() {
		defer conn.Close()

		bufRd := util.NewBufferReader(conn, 16*KB)
		for {
			select {
			case <-t.stopc:
				return
			default:
				if msg, err := reciveMessage(bufRd); err != nil {
					logger.Error("[heartbeatTransport] recive message from conn error", err.Error())
					return
				} else {
					//logger.Debug(fmt.Sprintf("Recive %v from (%v)", msg.ToString(), conn.RemoteAddr()))
					t.raftServer.reciveMessage(msg)
				}
			}
		}
	})
}

func (t *heartbeatTransport) send(msg *proto.Message) {
	s := t.getSender(msg.To)
	s.send(msg)
}

func (t *heartbeatTransport) getSender(nodeId uint64) *transportSender {
	t.mu.RLock()
	sender, ok := t.senders[nodeId]
	t.mu.RUnlock()
	if ok {
		return sender
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if sender, ok = t.senders[nodeId]; !ok {
		sender = newTransportSender(nodeId, 1, 64, HeartBeat, t.config.Resolver)
		t.senders[nodeId] = sender
	}
	return sender
}
