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

package meta

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	SendRetryLimit    = 200 // times
	SendRetryInterval = 100 // ms
	MaxRetryTime      = 10 * 60
	MinRetryTime      = 20 // s
)

type MetaConn struct {
	conn *net.TCPConn
	id   uint64 // PartitionID
	addr string // MetaNode addr
}

// Connection managements
//

func (mc *MetaConn) String() string {
	return fmt.Sprintf("partitionID(%v) addr(%v)", mc.id, mc.addr)
}

func (mw *MetaWrapper) getConn(partitionID uint64, addr string) (*MetaConn, error) {
	conn, err := mw.conns.GetConnect(addr)
	if err != nil {
		return nil, err
	}
	mc := &MetaConn{conn: conn, id: partitionID, addr: addr}
	return mc, nil
}

func (mw *MetaWrapper) putConn(mc *MetaConn, err error) {
	mw.conns.PutConnectEx(mc.conn, err)
}

func (mw *MetaWrapper) sendToMetaPartitionLeader(mp *MetaPartition, req *proto.Packet, sendTimeLimit int) (*proto.Packet, error) {
	var (
		resp    *proto.Packet
		err     error
		addr    string
		mc      *MetaConn
		start   time.Time
		lastSeq uint64
	)

	delta := SendRetryInterval
	if sendTimeLimit > MinRetryTime*1000 {
		delta = (sendTimeLimit*2/SendRetryLimit - SendRetryInterval) / SendRetryLimit // ms
	}
	log.LogDebugf("mw.metaSendTimeout: %v s, sendTimeLimit: %v ms, delta: %v ms, req %v", mw.metaSendTimeout, sendTimeLimit, delta, req)

	req.ExtentType |= proto.PacketProtocolVersionFlag

	errs := make(map[int]error, len(mp.Members))
	var j int

	addr = mp.LeaderAddr
	if addr == "" {
		err = errors.New(fmt.Sprintf("sendToMetaPartitionLeader: failed due to empty leader addr and goto retry, req(%v) mp(%v)", req, mp))
		goto retry
	}
	mc, err = mw.getConn(mp.PartitionID, addr)
	if err != nil {
		log.LogWarnf("sendToMetaPartitionLeader: getConn failed and goto retry, req(%v) mp(%v) addr(%v) err(%v)", req, mp, addr, err)
		goto retry
	}
	if mw.Client != nil { // compatible lcNode not init Client
		lastSeq = mw.Client.GetLatestVer()
	}

	if mw.IsSnapshotEnabled {
		req.ExtentType |= proto.MultiVersionFlag
		req.VerSeq = lastSeq
	}

sendWithList:
	resp, err = mc.send(req)
	if err == nil && !resp.ShouldRetry() && !resp.ShouldRetryWithVersionList() {
		mw.putConn(mc, err)
		goto out
	}
	if resp != nil && resp.ShouldRetryWithVersionList() {
		// already send with list, must be a issue happened
		if req.ExtentType&proto.VersionListFlag == proto.VersionListFlag {
			mw.putConn(mc, err)
			goto out
		}
		req.ExtentType |= proto.VersionListFlag
		req.VerList = make([]*proto.VolVersionInfo, len(mw.Client.GetVerMgr().VerList))
		copy(req.VerList, mw.Client.GetVerMgr().VerList)
		log.LogWarnf("sendToMetaPartitionLeader: leader failed and goto retry, req(%v) mp(%v) mc(%v) err(%v) resp(%v)", req, mp, mc, err, resp)
		goto sendWithList
	}
	log.LogWarnf("sendToMetaPartitionLeader: leader failed and goto retry, req(%v) mp(%v) mc(%v) err(%v), addr(%s)",
		req, mp, mc, err, addr)
	mw.putConn(mc, err)
retry:
	start = time.Now()
	for i := 0; i <= SendRetryLimit; i++ {
		for j, addr = range mp.Members {
			mc, err = mw.getConn(mp.PartitionID, addr)
			errs[j] = err
			if err != nil {
				log.LogWarnf("sendToMetaPartitionLeader: getConn failed and continue to retry, req(%v) mp(%v) addr(%v) err(%v)", req, mp, addr, err)
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
			log.LogWarnf("sendToMetaPartitionLeader: retry failed req(%v) mp(%v) mc(%v) errs(%v) resp(%v)", req, mp, mc, errs, resp)
		}
		if time.Since(start) > time.Duration(sendTimeLimit)*time.Millisecond {
			log.LogWarnf("sendToMetaPartitionLeader: retry timeout req(%v) mp(%v) time(%v)", req, mp, time.Since(start))
			break
		}
		sendRetryInterval := time.Duration(SendRetryInterval+i*delta) * time.Millisecond
		log.LogWarnf("sendToMetaPartitionLeader: req(%v) mp(%v) retry in (%v), retry_iteration (%v), retry_totalTime (%v)", req, mp,
			sendRetryInterval, i+1, time.Since(start))
		time.Sleep(sendRetryInterval)
	}

out:
	log.LogDebugf("sendToMetaPartition: succeed! req(%v) mc(%v) resp(%v)", req, mc, resp)
	if mw.Client != nil && resp != nil { // For compatibility with LcNode, the client checks whether it is nil
		mw.checkVerFromMeta(resp)
	}
	if err != nil || resp == nil {
		return nil, errors.New(fmt.Sprintf("sendToMetaPartition failed: req(%v) mp(%v) errs(%v) resp(%v)", req, mp, errs, resp))
	}
	return resp, nil
}

func (mw *MetaWrapper) sendToMetaPartition(mp *MetaPartition, req *proto.Packet) (*proto.Packet, error) {
	if req.IsReadMetaPkt() && !mw.InnerReq {
		return mw.sendReadToMP(mp, req)
	}

	var sendTimeLimit int
	if mw.metaSendTimeout < MinRetryTime {
		sendTimeLimit = MinRetryTime * 1000
	} else if mw.metaSendTimeout >= MaxRetryTime {
		sendTimeLimit = MaxRetryTime * 1000
	} else {
		sendTimeLimit = int(mw.metaSendTimeout) * 1000 // ms
	}

	return mw.sendToMetaPartitionLeader(mp, req, sendTimeLimit)
}

func (mw *MetaWrapper) sendReadToMP(mp *MetaPartition, req *proto.Packet) (resp *proto.Packet, err error) {
	leaderRetryTimeOut := mw.leaderRetryTimeout * 1000
	if leaderRetryTimeOut <= 0 {
		leaderRetryTimeOut = MinRetryTime * 1000
	}

	resp, err = mw.sendToMetaPartitionLeader(mp, req, int(leaderRetryTimeOut))
	if err == nil && !resp.ShouldRetry() {
		return
	}

	log.LogWarnf("sendReadToMP: send to leader failed, try to read quorum, req (%v), mp(%v), err(%v), resp(%v)", req, mp, err, resp)

	return mw.readQuorumFromHosts(mp, req)
}

func (mw *MetaWrapper) readQuorumFromHosts(mp *MetaPartition, req *proto.Packet) (resp *proto.Packet, err error) {
	var sendTimeLimit int
	var mc *MetaConn

	if mw.metaSendTimeout < MinRetryTime {
		sendTimeLimit = MinRetryTime * 1000
	} else if mw.metaSendTimeout >= MaxRetryTime {
		sendTimeLimit = MaxRetryTime * 1000
	} else {
		sendTimeLimit = int(mw.metaSendTimeout) * 1000 // ms
	}

	delta := (sendTimeLimit*2/SendRetryLimit - SendRetryInterval*2) / SendRetryLimit // ms
	start := time.Now()

	for i := 0; i < SendRetryLimit; i++ {

		activeHosts, quorumHosts := mw.getMpHosts(mp)
		if len(activeHosts) < mp.Quorum() {
			if !mw.FollowerRead {
				log.LogWarnf("readQuorumFromHosts: activeHosts(%d) QuorumHosts(%v) < Quorum(%v), req(%v), mp(%v)",
					len(activeHosts), len(quorumHosts), mp.Quorum(), req, mp.PartitionID)
				goto wait
			}

			log.LogWarnf("readQuorumFromHosts: follower read is open, use follower to read, active(%v), Quorum(%v) req(%v), mp (%d)",
				activeHosts, quorumHosts, *req, mp.PartitionID)
		}

		req.ArgLen = 1
		req.Arg = make([]byte, req.ArgLen)
		req.Arg[0] = proto.FollowerReadFlag

		for _, addr := range quorumHosts {
			mc, err = mw.getConn(mp.PartitionID, addr)
			if err != nil {
				log.LogWarnf("readQuorumFromHosts: getConn failed and continue to retry, req(%v) mp(%v) addr(%v) err(%v)", req, mp, addr, err)
				continue
			}

			resp, err = mc.send(req)
			mw.putConn(mc, err)
			if err == nil && !resp.ShouldRetry() {
				goto out
			}
			log.LogWarnf("readQuorumFromHosts: retry failed req(%v) mp(%v) mc(%v) errs(%v) resp(%v)", req, mp, mc, err, resp)
		}

	wait:
		if time.Since(start) > time.Duration(sendTimeLimit)*time.Millisecond {
			log.LogWarnf("readQuorumFromHosts: retry timeout req(%v) mp(%v) time(%v)", req, mp, time.Since(start))
			break
		}
		sendRetryInterval := time.Duration(SendRetryInterval+i*delta) * time.Millisecond
		log.LogWarnf("readQuorumFromHosts: req(%v) mp(%v) retry in (%v), retry_iteration (%v), retry_totalTime (%v)", req, mp,
			sendRetryInterval, i+1, time.Since(start))
		time.Sleep(sendRetryInterval)
	}

out:
	log.LogDebugf("readQuorumFromHosts: succeed! req(%v) mc(%v) resp(%v)", req, mc, resp)
	if err != nil || resp == nil {
		return nil, errors.New(fmt.Sprintf("readQuorumFromHosts failed: req(%v) mp(%v) err(%v) resp(%v)", req, mp, err, resp))
	}
	return
}

func (mw *MetaWrapper) getMpHosts(mp *MetaPartition) (activeHosts, QuorumHosts []string) {
	ids := make(map[string]uint64, len(mp.Members))
	var (
		wg           sync.WaitGroup
		lock         sync.Mutex
		maxAppliedID uint64
	)

	for _, addr := range mp.Members {
		wg.Add(1)
		go func(curAddr string) {
			defer wg.Done()

			appliedID, err := mw.getAppliedID(mp, curAddr)
			if err != nil {
				return
			}

			lock.Lock()
			ids[curAddr] = appliedID
			lock.Unlock()

			log.LogDebugf("getMpHosts: get apply id[%v] from host[%v], pid[%v]", appliedID, curAddr, mp.PartitionID)
		}(addr)
	}

	wg.Wait()

	maxID := uint64(0)
	targetHosts := make([]string, 0)
	for _, id := range ids {
		if id >= maxID {
			maxID = id
		}
	}

	for addr, id := range ids {
		if id == maxID {
			targetHosts = append(targetHosts, addr)
		}
		activeHosts = append(activeHosts, addr)
	}

	log.LogDebugf("getTargetHosts: get max apply id[%v] from hosts[%v], pid[%v]", maxAppliedID, targetHosts, mp.PartitionID)
	return activeHosts, targetHosts
}

func (mw *MetaWrapper) getAppliedID(mp *MetaPartition, addr string) (id uint64, err error) {
	req := &proto.GetAppliedIDRequest{
		PartitionId: mp.PartitionID,
	}
	packet := proto.NewPacketReqID()
	packet.Opcode = proto.OpMetaGetAppliedID
	err = packet.MarshalData(req)
	if err != nil {
		log.LogWarnf("getAppliedID err: (%v), req(%v)", err, *req)
		return
	}

	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("getAppliedID", err, bgTime, 1)
	}()

	metric := exporter.NewTPCnt(packet.GetOpMsg())
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: mw.volname})
	}()

	mc, err := mw.getConn(mp.PartitionID, addr)
	if err != nil {
		log.LogWarnf("getAppliedID: getConn failed and continue to retry, req(%v) mp(%v) addr(%v) err(%v)", req, mp, addr, err)
		return 0, err
	}
	resp, err := mc.send(packet)
	mw.putConn(mc, err)
	if err != nil || parseStatus(resp.ResultCode) != statusOK {
		log.LogWarnf("getAppliedID: packet(%v) mp(%v) addr(%v) req(%v) result(%v), err(%v)", packet, mp, addr, *req, resp, err)
		err = errors.New("getAppliedID error")
		return
	}

	id = binary.BigEndian.Uint64(resp.Data)
	log.LogDebugf("getAppliedID: get applied addr %s, mp %d, id %d, req %v", addr, mp.PartitionID, id, packet.ReqID)
	return
}

func (mc *MetaConn) send(req *proto.Packet) (resp *proto.Packet, err error) {
	req.ExtentType |= proto.PacketProtocolVersionFlag

	err = req.WriteToConn(mc.conn)
	if err != nil {
		return nil, errors.Trace(err, "Failed to write to conn, req(%v)", req)
	}
	resp = proto.NewPacket()
	err = resp.ReadFromConnWithVer(mc.conn, proto.ReadDeadlineTime)
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
