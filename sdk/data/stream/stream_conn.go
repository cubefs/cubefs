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

package stream

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

var (
	TryOtherAddrError   = errors.New("TryOtherAddrError")
	DpDiscardError      = errors.New("DpDiscardError")
	LimitedIoError      = errors.New("LimitedIoError")
	ExtentNotFoundError = errors.New("ExtentNotFoundError")
)

const (
	StreamSendMaxRetry      = 200
	StreamSendSleepInterval = 100 * time.Millisecond
	StreamSendMaxTimeout    = 10 * time.Minute
	RetryFactor             = 12 / 10
)

type GetReplyFunc func(conn *net.TCPConn) (err error, again bool)

// StreamConn defines the struct of the stream connection.
type StreamConn struct {
	dp       *wrapper.DataPartition
	currAddr string

	maxRetryTimeout time.Duration
}

var (
	StreamConnPool      = util.NewConnectPool()
	StreamWriteConnPool = util.NewConnectPool()
)

// NewStreamConn returns a new stream connection.
func NewStreamConn(dp *wrapper.DataPartition, follower bool, timeout time.Duration) (sc *StreamConn) {
	defer func() {
		if sc != nil {
			sc.maxRetryTimeout = timeout
		}
	}()

	if !follower {
		sc = &StreamConn{
			dp:       dp,
			currAddr: dp.LeaderAddr,
		}
		return
	}

	defer func() {
		if sc.currAddr == "" {
			/*
			 * If followerRead is enabled, and there is no preferred choice,
			 * currAddr can be arbitrarily selected from the hosts.
			 */
			for _, h := range dp.Hosts {
				if h != "" {
					sc.currAddr = h
					break
				}
			}
		}
	}()

	if dp.ClientWrapper.NearRead() {
		sc = &StreamConn{
			dp:       dp,
			currAddr: getNearestHost(dp),
		}
		return
	}

	epoch := atomic.AddUint64(&dp.Epoch, 1)
	hosts := sortByStatus(dp, false)
	choice := len(hosts)
	currAddr := dp.LeaderAddr
	if choice > 0 {
		index := int(epoch) % choice
		currAddr = hosts[index]
	}

	sc = &StreamConn{
		dp:       dp,
		currAddr: currAddr,
	}
	return
}

// String returns the string format of the stream connection.
func (sc *StreamConn) String() string {
	return fmt.Sprintf("Partition(%v) CurrentAddr(%v) Hosts(%v)", sc.dp.PartitionID, sc.currAddr, sc.dp.Hosts)
}

func (sc *StreamConn) getRetryTimeOut() time.Duration {
	if sc.maxRetryTimeout <= 0 {
		return StreamSendMaxTimeout
	}
	return sc.maxRetryTimeout
}

// Send send the given packet over the network through the stream connection until success
// or the maximum number of retries is reached.
func (sc *StreamConn) Send(retry *bool, req *Packet, getReply GetReplyFunc) (err error) {
	start := time.Now()
	retryInterval := StreamSendSleepInterval
	req.ExtentType |= proto.PacketProtocolVersionFlag

	for i := 0; i < StreamSendMaxRetry; i++ {
		err = sc.sendToDataPartition(req, retry, getReply)
		if err == nil || err == proto.ErrCodeVersionOp || !*retry || err == TryOtherAddrError || strings.Contains(err.Error(), "OpForbidErr") || err == ExtentNotFoundError {
			return
		}

		if time.Since(start) > sc.getRetryTimeOut() {
			log.LogWarnf("StreamConn Send: retry still failed after %d ms, req %d", sc.getRetryTimeOut().Milliseconds(), req.ReqID)
			return errors.NewErrorf("retry failed, err %s", err.Error())
		}

		if req.IsRandomWrite() {
			retryInterval = retryInterval*RetryFactor + time.Duration(rand.Int63n(int64(retryInterval)))
		}

		log.LogWarnf("StreamConn Send: err(%v), req %d, interval %d ms, cost %d ms",
			err, req.ReqID, retryInterval.Milliseconds(), time.Since(start).Milliseconds())
		time.Sleep(retryInterval)
	}
	return errors.NewErrorf("StreamConn Send: retried %v times and still failed, sc(%v) reqPacket(%v), err %s", StreamSendMaxRetry, sc, req, err.Error())
}

func (sc *StreamConn) sendToDataPartition(req *Packet, retry *bool, getReply GetReplyFunc) (err error) {
	conn, err := StreamConnPool.GetConnect(sc.currAddr)
	if err == nil {
		log.LogDebugf("req opcode %v, conn %v", req.Opcode, conn)
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.PutConnectV2(conn, false, sc.currAddr)
			return
		}
		log.LogWarnf("sendToDataPartition: send to curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
		StreamConnPool.PutConnectEx(conn, err)
		if err != TryOtherAddrError || !*retry {
			return
		}
	} else {
		log.LogWarnf("sendToDataPartition: get connection to curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
	}

	hosts := sortByStatus(sc.dp, true)

	for _, addr := range hosts {
		log.LogWarnf("sendToDataPartition: try addr(%v) reqPacket(%v)", addr, req)
		conn, err = StreamConnPool.GetConnect(addr)
		if err != nil {
			log.LogWarnf("sendToDataPartition: failed to get connection to addr(%v) reqPacket(%v) err(%v)", addr, req, err)
			continue
		}
		sc.currAddr = addr
		sc.dp.LeaderAddr = addr
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.PutConnectV2(conn, false, addr)
			return
		}
		StreamConnPool.PutConnect(conn, true)
		if err != TryOtherAddrError {
			return
		}
		log.LogWarnf("sendToDataPartition: try addr(%v) failed! reqPacket(%v) err(%v)", addr, req, err)
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
		// NOTE: if we meet a try again error
		if err == LimitedIoError {
			log.LogWarnf("sendToConn: found ulimit io error, dp %d, req %d, err %s", req.PartitionID, req.ReqID, err.Error())
			i -= 1
		}

		log.LogWarnf("sendToConn: getReply error and will RETRY, sc(%v) err(%v)", sc, err)
		time.Sleep(StreamSendSleepInterval)
	}

	log.LogDebugf("sendToConn exit: send to addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
	return
}

// sortByStatus will return hosts list sort by host status for DataPartition.
// If param selectAll is true, hosts with status(true) is in front and hosts with status(false) is in behind.
// If param selectAll is false, only return hosts with status(true).
func sortByStatus(dp *wrapper.DataPartition, selectAll bool) (hosts []string) {
	var failedHosts []string
	hostsStatus := dp.ClientWrapper.HostsStatus
	var dpHosts []string
	if dp.ClientWrapper.FollowerRead() && dp.ClientWrapper.NearRead() {
		dpHosts = dp.NearHosts
		if len(dpHosts) == 0 {
			dpHosts = dp.Hosts
		}
	} else {
		dpHosts = dp.Hosts
	}

	hosts = make([]string, 0, len(dpHosts))

	for _, addr := range dpHosts {
		status, ok := hostsStatus[addr]
		if ok {
			if status {
				hosts = append(hosts, addr)
			} else {
				failedHosts = append(failedHosts, addr)
			}
		} else {
			failedHosts = append(failedHosts, addr)
			log.LogWarnf("sortByStatus: can not find host[%v] in HostsStatus, dp[%d]", addr, dp.PartitionID)
		}
	}

	if selectAll {
		hosts = append(hosts, failedHosts...)
	}

	return
}

func getNearestHost(dp *wrapper.DataPartition) string {
	hostsStatus := dp.ClientWrapper.HostsStatus
	for _, addr := range dp.NearHosts {
		status, ok := hostsStatus[addr]
		if ok {
			if !status {
				continue
			}
		}
		return addr
	}
	return dp.LeaderAddr
}

func NewStreamConnByHost(host string) *StreamConn {
	return &StreamConn{
		currAddr: host,
	}
}
