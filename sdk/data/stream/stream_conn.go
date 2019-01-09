// Copyright 2018 The Container File System Authors.
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

package stream

import (
	"fmt"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

var (
	NotLeaderError = errors.New("NotLeaderError")
)

const (
	StreamSendMaxRetry      = 200
	StreamSendSleepInterval = 100 * time.Millisecond
)

type GetReplyFunc func(conn *net.TCPConn) (err error, again bool)

type StreamConn struct {
	partition uint64
	currAddr  string
	hosts     []string
}

var (
	StreamConnPool = util.NewConnectPool()
)

func NewStreamConn(dp *wrapper.DataPartition) *StreamConn {
	return &StreamConn{
		partition: dp.PartitionID,
		currAddr:  dp.LeaderAddr,
		hosts:     dp.Hosts,
	}
}

func (sc *StreamConn) String() string {
	return fmt.Sprintf("Partition(%v) CurrentAddr(%v) Hosts(%v)", sc.partition, sc.currAddr, sc.hosts)
}

func (sc *StreamConn) Send(req *Packet, getReply GetReplyFunc) (err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		err = sc.sendToPartition(req, getReply)
		if err == nil {
			return
		}
		log.LogWarnf("StreamConn Send: err(%v)", err)
		time.Sleep(StreamSendSleepInterval)
	}
	return errors.New(fmt.Sprintf("StreamConn Send: retried %v times and still failed, sc(%v) reqPacket(%v)", StreamSendMaxRetry, sc, req))
}

func (sc *StreamConn) sendToPartition(req *Packet, getReply GetReplyFunc) (err error) {
	conn, err := StreamConnPool.GetConnect(sc.currAddr)
	if err == nil {
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.PutConnect(conn, false)
			return
		}
		log.LogWarnf("sendToPartition: curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
		StreamConnPool.PutConnect(conn, true)
		if err != NotLeaderError {
			return
		}
	}

	for _, addr := range sc.hosts {
		log.LogWarnf("sendToPartition: try addr(%v) reqPacket(%v)", addr, req)
		conn, err = StreamConnPool.GetConnect(addr)
		if err != nil {
			log.LogWarnf("sendToPartition: failed to get connection to addr(%v) reqPacket(%v) err(%v)", addr, req, err)
			continue
		}
		sc.currAddr = addr
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.PutConnect(conn, false)
			return
		}
		StreamConnPool.PutConnect(conn, true)
		if err != NotLeaderError {
			return
		}
	}
	return errors.New(fmt.Sprintf("sendToPatition Failed: sc(%v) reqPacket(%v)", sc, req))
}

func (sc *StreamConn) sendToConn(conn *net.TCPConn, req *Packet, getReply GetReplyFunc) (err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		log.LogDebugf("sendToConn: send to addr(%v), reqPacket(%v)", sc.currAddr, req)
		err = req.WriteToConn(conn)
		if err != nil {
			msg := fmt.Sprintf("sendToConn: failed to write to addr(%v) err(%v)", sc.currAddr, err)
			log.LogWarn(msg)
			break
		}

		var again bool
		err, again = getReply(conn)
		if !again {
			if err != nil {
				log.LogWarnf("sendToConn: getReply error and RETURN, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
			}
			break
		}

		log.LogWarnf("sendToConn: getReply error and will RETRY, sc(%v) err(%v)", sc, err)
		time.Sleep(StreamSendSleepInterval)
	}

	log.LogDebugf("sendToConn exit: send to addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
	return
}
