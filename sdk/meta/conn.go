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
	if err != nil {
		mw.conns.PutConnect(mc.conn, true)
	} else {
		mw.conns.PutConnect(mc.conn, false)
	}
}

func (mw *MetaWrapper) sendToMetaPartition(mp *MetaPartition, req *proto.Packet) (*proto.Packet, error) {
	var (
		resp  *proto.Packet
		err   error
		addr  string
		mc    *MetaConn
		start time.Time
	)

	addr = mp.LeaderAddr
	if addr == "" {
		err = errors.New(fmt.Sprintf("sendToMetaPartition failed: leader addr empty, req(%v) mp(%v)", req, mp))
		goto retry
	}
	mc, err = mw.getConn(mp.PartitionID, addr)
	if err != nil {
		goto retry
	}
	resp, err = mc.send(req)
	mw.putConn(mc, err)
	if err == nil && !resp.ShouldRetry() {
		goto out
	}
	log.LogWarnf("sendToMetaPartition: leader failed req(%v) mp(%v) mc(%v) err(%v) resp(%v)", req, mp, mc, err, resp)

retry:
	start = time.Now()
	for i := 0; i < SendRetryLimit; i++ {
		for _, addr = range mp.Members {
			mc, err = mw.getConn(mp.PartitionID, addr)
			if err != nil {
				continue
			}
			resp, err = mc.send(req)
			mw.putConn(mc, err)
			if err == nil && !resp.ShouldRetry() {
				goto out
			}
			log.LogWarnf("sendToMetaPartition: retry failed req(%v) mp(%v) mc(%v) err(%v) resp(%v)", req, mp, mc, err, resp)
		}
		if time.Since(start) > SendTimeLimit {
			log.LogWarnf("sendToMetaPartition: retry timeout req(%v) mp(%v) time(%v)", req, mp, time.Since(start))
			break
		}
		log.LogWarnf("sendToMetaPartition: req(%v) mp(%v) retry in (%v)", req, mp, SendRetryInterval)
		time.Sleep(SendRetryInterval)
	}

out:
	if err != nil || resp == nil {
		return nil, errors.New(fmt.Sprintf("sendToMetaPartition failed: req(%v) mp(%v) err(%v) resp(%v)", req, mp, err, resp))
	}
	log.LogDebugf("sendToMetaPartition successful: req(%v) mc(%v) resp(%v)", req, mc, resp)
	return resp, nil
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
	return resp, nil
}
