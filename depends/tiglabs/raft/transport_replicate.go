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
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/rdma"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
)

type replicateTransport struct {
	config      *TransportConfig
	raftServer  *RaftServer
	listener    net.Listener
	curSnapshot int32
	mu          sync.RWMutex
	senders     map[uint64]*transportSender
	stopc       chan struct{}
}

func newReplicateTransport(raftServer *RaftServer, config *TransportConfig) (*replicateTransport, error) {
	var (
		listener net.Listener
		err      error
	)

	if IsRdma {
		replicateRdmaAddrSplits := strings.Split(config.ReplicateAddr, ":")
		if len(replicateRdmaAddrSplits) != 2 {
			err = errors.New("illegal replica rdma address")
			return nil, err
		}
		ip, port := replicateRdmaAddrSplits[0], replicateRdmaAddrSplits[1]
		if ip == "" {
			ip = "127.0.0.1"
		}
		if listener, err = rdma.NewRdmaServer(ip, port); err != nil {
			return nil, err
		}
	} else {
		if listener, err = net.Listen("tcp", config.ReplicateAddr); err != nil {
			return nil, err
		}
	}
	t := &replicateTransport{
		config:     config,
		raftServer: raftServer,
		listener:   listener,
		senders:    make(map[uint64]*transportSender),
		stopc:      make(chan struct{}),
	}
	return t, nil
}

func (t *replicateTransport) stop() {
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

func (t *replicateTransport) send(m *proto.Message) {
	s := t.getSender(m.To)
	s.send(m)
}

func (t *replicateTransport) getSender(nodeId uint64) *transportSender {
	t.mu.RLock()
	sender, ok := t.senders[nodeId]
	t.mu.RUnlock()
	if ok {
		return sender
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if sender, ok = t.senders[nodeId]; !ok {
		sender = newTransportSender(nodeId, uint64(t.config.MaxReplConcurrency), t.config.SendBufferSize, Replicate, t.config.Resolver)
		t.senders[nodeId] = sender
	}
	return sender
}

func (t *replicateTransport) sendSnapshot(m *proto.Message, rs *snapshotStatus) {
	var (
		conn *util.ConnTimeout
		err  error
	)
	defer func() {
		atomic.AddInt32(&t.curSnapshot, -1)
		rs.respond(err)
		if conn != nil {
			conn.Close()
		}
		if err != nil {
			logger.Error("[Transport] %v send snapshot to %v failed error is: %v.", m.ID, m.To, err)
		} else if logger.IsEnableWarn() {
			logger.Warn("[Transport] %v send snapshot to %v successful.", m.ID, m.To)
		}

	}()

	if atomic.AddInt32(&t.curSnapshot, 1) > int32(t.config.MaxSnapConcurrency) {
		err = fmt.Errorf("snapshot concurrency exceed the limit %v, now %d", t.config.MaxSnapConcurrency, t.curSnapshot)
		return
	}
	if conn = getConn(m.To, Replicate, t.config.Resolver, 10*time.Minute, 1*time.Minute); conn == nil {
		err = fmt.Errorf("can't get connection to %v.", m.To)
		return
	}

	// send snapshot header message
	bufWr := util.NewBufferWriter(conn, 1*MB)
	if err = m.Encode(bufWr); err != nil {
		return
	}
	if err = bufWr.Flush(); err != nil {
		return
	}

	// send snapshot data
	var (
		data      []byte
		loopCount = 0
		sizeBuf   = make([]byte, 4)
	)
	for err == nil {
		loopCount = loopCount + 1
		if loopCount > 16 {
			loopCount = 0
			runtime.Gosched()
		}

		select {
		case <-rs.stopCh:
			err = fmt.Errorf("raft has shutdown.")

		default:
			data, err = m.Snapshot.Next()
			if len(data) > 0 {
				// write block size
				binary.BigEndian.PutUint32(sizeBuf, uint32(len(data)))
				if _, err = bufWr.Write(sizeBuf); err == nil {
					_, err = bufWr.Write(data)
				}
			}
		}
	}

	// write end flag and flush
	if err != nil && err != io.EOF {
		return
	}
	binary.BigEndian.PutUint32(sizeBuf, 0)
	if _, err = bufWr.Write(sizeBuf); err != nil {
		return
	}
	if err = bufWr.Flush(); err != nil {
		return
	}

	// wait response
	err = nil
	resp := make([]byte, 1)
	io.ReadFull(conn, resp)
	if resp[0] != 1 {
		err = fmt.Errorf("follower response failed.")
	}
}

/*
func (t *replicateTransport) sendSnapshotByRdma(m *proto.Message, rs *snapshotStatus) {
	var (
		conn *util.ConnTimeout
		err  error
	)
	defer func() {
		atomic.AddInt32(&t.curSnapshot, -1)
		rs.respond(err)
		if conn != nil {
			conn.Close()
		}
		if err != nil {
			logger.Error("[Transport] %v send snapshot to %v failed error is: %v.", m.ID, m.To, err)
		} else if logger.IsEnableWarn() {
			logger.Warn("[Transport] %v send snapshot to %v successful.", m.ID, m.To)
		}

	}()

	if atomic.AddInt32(&t.curSnapshot, 1) > int32(t.config.MaxSnapConcurrency) {
		err = fmt.Errorf("snapshot concurrency exceed the limit %v, now %d", t.config.MaxSnapConcurrency, t.curSnapshot)
		return
	}

	if conn = getRdmaConn(m.To, Replicate, t.config.Resolver, 10*time.Minute, 1*time.Minute); conn == nil {
		err = fmt.Errorf("can't get rdma connection to %v.", m.To)
	}
	// send snapshot header message
	rdmaBufWr := util.NewRdmaBufferWriter(conn)
	if err = m.EncodeByRdma(rdmaBufWr); err != nil {
		return
	}
	if err = rdmaBufWr.Flush(); err != nil {
		return
	}

	// send snapshot data
	var (
		data      []byte
		buf       []byte
		buffSize  int
		loopCount = 0
	)
	for err == nil {
		loopCount = loopCount + 1
		if loopCount > 16 {
			loopCount = 0
			runtime.Gosched()
		}

		select {
		case <-rs.stopCh:
			err = fmt.Errorf("raft has shutdown.")

		default:
			data, err = m.Snapshot.Next()
			if len(data) > 0 {
				// write block size
				buffSize = 4 + len(data)
				for i := 0; i < 100; i++ {
					if i%10 == 0 {
						runtime.Gosched()
					}
					if buf, err = rdmaBufWr.GetDataBuffer(uint32(buffSize)); err != nil {
						continue
					}
					//if buf, err = conn.GetRdmaConn().GetConnTxDataBuffer(uint32(buffSize)); err != nil {
					//	continue
					//}

					break
				}
				if err != nil {
					continue
				}

				binary.BigEndian.PutUint32(buf, uint32(len(data)))
				copy(buf[4:], data)
				_, err = rdmaBufWr.Write(buf)
				//_, err = conn.GetRdmaConn().WriteBuffer(buf, buffSize)
				//conn.GetRdmaConn().ReleaseConnTxDataBuffer(buf)
			}
		}
	}

	// write end flag and flush
	if err != nil && err != io.EOF {
		return
	}

	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			runtime.Gosched()
		}
		if buf, err = rdmaBufWr.GetDataBuffer(4); err != nil {
			continue
		}
		//if buf, err = conn.GetRdmaConn().GetConnTxDataBuffer(4); err != nil {
		//	continue
		//}

		break
	}
	if err != nil {
		return
	}

	binary.BigEndian.PutUint32(buf, 0)
	_, err = rdmaBufWr.Write(buf)
	//_, err = conn.GetRdmaConn().WriteBuffer(buf, 4)
	//conn.GetRdmaConn().ReleaseConnTxDataBuffer(buf)
	if err != nil {
		return
	}

	if rdmaBufWr.Flush(); err != nil {
		return
	}
	//if conn.GetRdmaConn().WriteFlush(); err != nil {
	//	return
	//}

	// wait response
	err = nil
	var resp []byte
	//resp := make([]byte, 1)

	if resp, err = conn.GetRdmaConn().GetRecvMsgBuffer(); err == nil {
		if resp[0] != 1 {
			err = fmt.Errorf("follower response failed.")
		}
	}
}
*/

func (t *replicateTransport) start() {
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

func (t *replicateTransport) handleConn(conn *util.ConnTimeout) {
	util.RunWorker(func() {
		defer conn.Close()

		loopCount := 0
		if conn.IsRdma() {
			for {
				loopCount = loopCount + 1
				if loopCount > 16 {
					loopCount = 0
					runtime.Gosched()
				}

				select {
				case <-t.stopc:
					return
				default:
					if msg, err := reciveMessageByRdma(conn); err != nil {
						logger.Error(fmt.Sprintf("[replicateTransport] recive message from rdma conn error, %s", err.Error()))
						return
					} else {
						//logger.Debug(fmt.Sprintf("Recive %v from (%v)", msg.ToString(), conn.RemoteAddr()))
						if msg.Type == proto.ReqMsgSnapShot {
							//if err := t.handleSnapshot(msg, conn, bufRd); err != nil {
							//	return
							//}
							err = errors.NewErrorf("rdma mode does not support processing snapshot")
							logger.Error(fmt.Sprintf("[replicateTransport] recive message from rdma conn error, %s", err.Error()))
							return
						} else {
							logger.Debug("Recive %v size %v from (%v %v) ", msg.ToString(), msg.Size(), conn.IsRdma(), conn.RemoteAddr())
							t.raftServer.reciveMessage(msg)
						}
					}
				}
			}
		} else {
			bufRd := util.NewBufferReader(conn, 16*KB)
			for {
				loopCount = loopCount + 1
				if loopCount > 16 {
					loopCount = 0
					runtime.Gosched()
				}

				select {
				case <-t.stopc:
					return
				default:
					if msg, err := reciveMessage(bufRd); err != nil {
						return
					} else {
						//logger.Debug(fmt.Sprintf("Recive %v from (%v)", msg.ToString(), conn.RemoteAddr()))
						if msg.Type == proto.ReqMsgSnapShot {
							if err := t.handleSnapshot(msg, conn, bufRd); err != nil {
								return
							}
						} else {
							t.raftServer.reciveMessage(msg)
						}
					}
				}
			}
		}
	})
}

var snap_ack = []byte{1}

func (t *replicateTransport) handleSnapshot(m *proto.Message, conn *util.ConnTimeout, bufRd *util.BufferReader) error {
	conn.SetReadTimeout(time.Minute)
	conn.SetWriteTimeout(15 * time.Second)
	bufRd.Grow(1 * MB)
	req := newSnapshotRequest(m, bufRd)
	t.raftServer.reciveSnapshot(req)

	// wait snapshot result
	if err := req.response(); err != nil {
		logger.Error("[Transport] handle snapshot request from %v error: %v.", m.From, err)
		return err
	}

	_, err := conn.Write(snap_ack)
	return err
}
