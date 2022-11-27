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
	"runtime"
	"sync"
	"time"

	//"fmt"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
)

type unreachableReporter func(uint64)

type transportSender struct {
	nodeID      uint64
	concurrency uint64
	senderType  SocketType
	resolver    SocketResolver
	inputc      []chan *proto.Message
	send        func(msg *proto.Message)
	mu          sync.Mutex
	stopc       chan struct{}
}

func newTransportSender(nodeID, concurrency uint64, buffSize int, senderType SocketType, resolver SocketResolver) *transportSender {
	sender := &transportSender{
		nodeID:      nodeID,
		concurrency: concurrency,
		senderType:  senderType,
		resolver:    resolver,
		inputc:      make([]chan *proto.Message, concurrency),
		stopc:       make(chan struct{}),
	}
	for i := uint64(0); i < concurrency; i++ {
		sender.inputc[i] = make(chan *proto.Message, buffSize)
		sender.loopSend(sender.inputc[i])
	}

	if (concurrency & (concurrency - 1)) == 0 {
		sender.send = func(msg *proto.Message) {
			idx := 0
			if concurrency > 1 {
				idx = int(msg.ID&concurrency - 1)
			}
			sender.inputc[idx] <- msg
		}
	} else {
		sender.send = func(msg *proto.Message) {
			idx := 0
			if concurrency > 1 {
				idx = int(msg.ID % concurrency)
			}
			sender.inputc[idx] <- msg
		}
	}
	return sender
}

func (s *transportSender) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.stopc:
		return
	default:
		close(s.stopc)
	}
}

func (s *transportSender) loopSend(recvc chan *proto.Message) {
	util.RunWorkerUtilStop(func() {
		conn := getConn(s.nodeID, s.senderType, s.resolver, 0, 2*time.Second)
		bufWr := util.NewBufferWriter(conn, 16*KB)

		defer func() {
			if conn != nil {
				conn.Close()
			}
		}()

		loopCount := 0
		var err error
		for {
			loopCount = loopCount + 1
			if loopCount > 8 {
				loopCount = 0
				runtime.Gosched()
			}

			select {
			case <-s.stopc:
				return

			case msg := <-recvc:
				if conn == nil {
					conn = getConn(s.nodeID, s.senderType, s.resolver, 0, 2*time.Second)
					if conn == nil {
						proto.ReturnMessage(msg)
						// reset chan
						for {
							select {
							case msg := <-recvc:
								proto.ReturnMessage(msg)
								continue
							default:
							}
							break
						}
						time.Sleep(50 * time.Millisecond)
						continue
					}
					bufWr.Reset(conn)
				}
				err = msg.Encode(bufWr)
				proto.ReturnMessage(msg)
				if err != nil {
					goto flush
				}
				// group send message
				flag := false
				for i := 0; i < 16; i++ {
					select {
					case msg := <-recvc:
						err = msg.Encode(bufWr)
						//logger.Debug(fmt.Sprintf("SendMesg %v to (%v) ", msg.ToString(), conn.RemoteAddr()))
						proto.ReturnMessage(msg)
						if err != nil {
							goto flush
						}
					default:
						flag = true
					}
					if flag {
						break
					}
				}
			}

		flush:
			// flush write
			if err == nil {
				err = bufWr.Flush()
			}
			if err != nil {
				logger.Error("[Transport]send message[%s] to %v[%s] error:[%v].", s.senderType, s.nodeID, conn.RemoteAddr(), err)
				conn.Close()
				conn = nil
			}
		}
	}, s.stopc)
}

func getConn(nodeID uint64, socketType SocketType, resolver SocketResolver, rdTime, wrTime time.Duration) (conn *util.ConnTimeout) {
	var (
		addr string
		err  error
	)
	if addr, err = resolver.NodeAddress(nodeID, socketType); err == nil {
		if conn, err = util.DialTimeout(addr, 2*time.Second); err == nil {
			conn.SetReadTimeout(rdTime)
			conn.SetWriteTimeout(wrTime)
		}
	}

	if err != nil {
		conn = nil
		if logger.IsEnableDebug() {
			logger.Debug("[Transport] get connection[%s] to %v[%s] failed,error is: %s", socketType, nodeID, addr, err)
		}
	}
	return
}
