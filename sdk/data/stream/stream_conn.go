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

package stream

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/sdk/data/wrapper"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

var (
	TryOtherAddrError = errors.New("TryOtherAddrError")
)

const (
	StreamSendMaxRetry      = 200
	StreamSendSleepInterval = 100 * time.Millisecond
)

type GetReplyFunc func(conn *net.TCPConn) (err error, again bool)

// StreamConn defines the struct of the stream connection.
type StreamConn struct {
	dp       *wrapper.DataPartition
	currAddr string
}

var (
	StreamConnPool = util.NewConnectPool()
)

// NewStreamConn returns a new stream connection.
func NewStreamConn(dp *wrapper.DataPartition, follower bool) *StreamConn {
	if !follower {
		return &StreamConn{
			dp:       dp,
			currAddr: dp.LeaderAddr,
		}
	}

	epoch := atomic.AddUint64(&dp.Epoch, 1)
	choice := len(dp.Hosts)
	currAddr := dp.LeaderAddr
	if choice > 0 {
		index := int(epoch) % choice
		currAddr = dp.Hosts[index]
	}

	return &StreamConn{
		dp:       dp,
		currAddr: currAddr,
	}
}

// String returns the string format of the stream connection.
func (sc *StreamConn) String() string {
	return fmt.Sprintf("Partition(%v) CurrentAddr(%v) Hosts(%v)", sc.dp.PartitionID, sc.currAddr, sc.dp.Hosts)
}

// Send send the given packet over the network through the stream connection until success
// or the maximum number of retries is reached.
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
		if err != TryOtherAddrError {
			return
		}
	}

	for _, addr := range sc.dp.Hosts {
		log.LogWarnf("sendToPartition: try addr(%v) reqPacket(%v)", addr, req)
		conn, err = StreamConnPool.GetConnect(addr)
		if err != nil {
			log.LogWarnf("sendToPartition: failed to get connection to addr(%v) reqPacket(%v) err(%v)", addr, req, err)
			continue
		}
		sc.currAddr = addr
		sc.dp.LeaderAddr = addr
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.PutConnect(conn, false)
			return
		}
		StreamConnPool.PutConnect(conn, true)
		if err != TryOtherAddrError {
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
