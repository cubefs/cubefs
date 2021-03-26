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

package meta

import (
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/util/errors"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	SendRetryLimit    = 100
	SendRetryInterval = 100 * time.Millisecond
	SendTimeLimit     = 20 * time.Second
)

type MetaConn struct {
	conn *net.TCPConn
	id   uint64 //PartitionID
	addr string //MetaNode addr
}

// Connection managements
//

func (mc *MetaConn) String() string {
	return fmt.Sprintf("partitionID(%v) addr(%v)", mc.id, mc.addr)
}

func (mw *MetaWrapper) getConn(partitionID uint64, addr string) (*MetaConn, error) {
	conn, err := mw.conns.GetConnect(addr)
	if err != nil {
		log.LogWarnf("GetConnect conn: addr(%v) err(%v)", addr, err)
		return nil, err
	}
	mc := &MetaConn{conn: conn, id: partitionID, addr: addr}
	return mc, nil
}

func (mw *MetaWrapper) putConn(mc *MetaConn, err error) {
	mw.conns.PutConnect(mc.conn, err != nil)
}

func (mw *MetaWrapper) sendToMetaPartition(mp *MetaPartition, req *proto.Packet) (*proto.Packet, error) {
	var (
		resp  *proto.Packet
		err   error
		mc    *MetaConn
		start time.Time
	)
	errs := make(map[int]error, len(mp.Members))
	mp.Members = sortMembers(mp.LeaderAddr, mp.Members)
	start = time.Now()

	for i := 0; i < SendRetryLimit; i++ {
		for j, addr := range mp.Members {
			mc, err = mw.getConn(mp.PartitionID, addr)
			errs[j] = err
			if err != nil {
				continue
			}
			resp, err = mc.send(req)
			mw.putConn(mc, err)
			if err == nil && !resp.ShouldRetry() {
				goto out
			}
			if err == nil {
				errs[j] = errors.New(fmt.Sprintf("request should retry[%v]", resp.GetResultMsg()))
			} else {
				errs[j] = err
			}
			log.LogWarnf("sendToMetaPartition: retry failed req(%v) mp(%v) mc(%v) errs(%v) resp(%v)", req, mp, mc, errs, resp)
		}
		if time.Since(start) > SendTimeLimit {
			log.LogWarnf("sendToMetaPartition: retry timeout req(%v) mp(%v) time(%v)", req, mp, time.Since(start))
			break
		}
		log.LogWarnf("sendToMetaPartition: req(%v) mp(%v) retry in (%v)", req, mp, SendRetryInterval)
		time.Sleep(SendRetryInterval)
	}
	// compare applied ID of replicas and choose the max one
	if req.IsReadMetaPkt() {
		targetHosts, isErr := mw.GetMaxAppliedIDHosts(mp)
		if !isErr && len(targetHosts) > 0 {
			req.ArgLen = 1
			req.Arg = make([]byte, req.ArgLen)
			req.Arg[0] = proto.FollowerReadFlag
			for _, host := range targetHosts {
				resp, err = mw.sendToSpecifiedMpAddr(mp, host, req)
				if err == nil {
					goto out
				}
			}
		}
	}

out:
	if err != nil || resp == nil {
		return nil, errors.New(fmt.Sprintf("sendToMetaPartition failed: req(%v) mp(%v) errs(%v) resp(%v)", req, mp, errs, resp))
	}
	log.LogDebugf("sendToMetaPartition successful: req(%v) mc(%v) resp(%v)", req, mc, resp)
	return resp, nil
}

func (mw *MetaWrapper) sendToSpecifiedMpAddr(mp *MetaPartition, addr string, req *proto.Packet) (*proto.Packet, error) {
	var (
		resp  *proto.Packet
		err   error
		mc    *MetaConn
		start time.Time
	)
	if addr == "" {
		err = errors.New(fmt.Sprintf("sendToSpecifiedMpAddr failed: addr empty, req(%v) mp(%v)", req, mp))
		return nil, err
	}

	log.LogDebugf("sendToSpecifiedMpAddr: pid(%v), addr(%v), packet(%v), args(%v)", mp.PartitionID, addr, req, string(req.Arg))
	start = time.Now()
	for i := 0; i < SendRetryLimit; i++ {
		if time.Since(start) > SendTimeLimit {
			log.LogWarnf("sendToSpecifiedMpAddr: retry timeout req(%v) mp(%v) time(%v)", req, mp, time.Since(start))
			break
		}
		mc, err = mw.getConn(mp.PartitionID, addr)
		if err != nil {
			log.LogWarnf("sendToSpecifiedMpAddr: failed to connect, req(%v) mp(%v) mc(%v) err(%v) resp(%v)", req, mp, mc, err, resp)
			continue
		}
		resp, err = mc.send(req)
		mw.putConn(mc, err)
		if err == nil {
			log.LogDebugf("sendToSpecifiedMpAddr successful: req(%v) mc(%v) resp(%v)", req, mc, resp)
			return resp, nil
		}
		log.LogWarnf("sendToSpecifiedMpAddr: failed req(%v) mp(%v) mc(%v) err(%v) resp(%v)", req, mp, mc, err, resp)
		time.Sleep(SendRetryInterval)
	}
	return nil, err
}

func (mc *MetaConn) send(req *proto.Packet) (resp *proto.Packet, err error) {
	err = req.WriteToConn(mc.conn)
	if err != nil {
		return nil, errors.Trace(err, "Failed to write to conn, req(%v)", req)
	}
	resp = proto.NewPacket()
	err = resp.ReadFromConn(mc.conn, proto.ReadDeadlineTime)
	if err != nil {
		return nil, errors.Trace(err, "Failed to read from conn, req(%v)", req)
	}
	// Check if the ID and OpCode of the response are consistent with the request.
	if resp.ReqID != req.ReqID || resp.Opcode != req.Opcode {
		log.LogErrorf("send: the response packet mismatch with request: conn(%v to %v) req(%v) resp(%v)",
			mc.conn.LocalAddr(), mc.conn.RemoteAddr(), req, resp)
		return nil, syscall.EBADMSG
	}
	return resp, nil
}

func sortMembers(leader string, members []string) []string {
	if leader == "" {
		return members
	}
	for i, addr := range members {
		if addr == leader {
			members[i], members[0] = members[0], members[i]
			break
		}
	}
	return members
}
