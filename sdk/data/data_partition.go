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

package data

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/tracing"
)

// DataPartition defines the wrapper of the data partition.
type DataPartition struct {
	// Will not be changed
	proto.DataPartitionResponse
	RandomWrite   bool
	PartitionType string
	NearHosts     []string
	ClientWrapper *Wrapper
	Metrics       *DataPartitionMetrics
}

// DataPartitionMetrics defines the wrapper of the metrics related to the data partition.
type DataPartitionMetrics struct {
	sync.RWMutex
	AvgReadLatencyNano  int64
	AvgWriteLatencyNano int64
	SumReadLatencyNano  int64
	SumWriteLatencyNano int64
	ReadOpNum           int64
	WriteOpNum          int64
}

// If the connection fails, take punitive measures. Punish time is 5s.
func (dp *DataPartition) RecordWrite(startT int64, punish bool) {
	if startT == 0 {
		log.LogWarnf("RecordWrite: invalid start time")
		return
	}

	cost := time.Now().UnixNano() - startT
	if punish {
		cost += 5 * 1e9
		log.LogWarnf("RecordWrite: dp[%v] punish write time[5s] because of error, avg[%v]ns", dp.PartitionID, dp.GetAvgWrite())
	}

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	dp.Metrics.WriteOpNum++
	dp.Metrics.SumWriteLatencyNano += cost

	return
}

func (dp *DataPartition) MetricsRefresh() {
	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	if dp.Metrics.ReadOpNum != 0 {
		dp.Metrics.AvgReadLatencyNano = dp.Metrics.SumReadLatencyNano / dp.Metrics.ReadOpNum
	} else {
		dp.Metrics.AvgReadLatencyNano = 0
	}

	if dp.Metrics.WriteOpNum != 0 {
		dp.Metrics.AvgWriteLatencyNano = (9*dp.Metrics.AvgWriteLatencyNano + dp.Metrics.SumWriteLatencyNano/dp.Metrics.WriteOpNum) / 10
	} else {
		dp.Metrics.AvgWriteLatencyNano = (9 * dp.Metrics.AvgWriteLatencyNano) / 10
	}

	dp.Metrics.SumReadLatencyNano = 0
	dp.Metrics.SumWriteLatencyNano = 0
	dp.Metrics.ReadOpNum = 0
	dp.Metrics.WriteOpNum = 0
}

//func (dp *DataPartition) GetAvgRead() int64 {
//	dp.Metrics.RLock()
//	defer dp.Metrics.RUnlock()
//
//	return dp.Metrics.AvgReadLatencyNano
//}

func (dp *DataPartition) GetAvgWrite() int64 {
	dp.Metrics.RLock()
	defer dp.Metrics.RUnlock()

	return dp.Metrics.AvgWriteLatencyNano
}

type DataPartitionSorter []*DataPartition

//func (ds DataPartitionSorter) Len() int {
//	return len(ds)
//}
//func (ds DataPartitionSorter) Swap(i, j int) {
//	ds[i], ds[j] = ds[j], ds[i]
//}
//func (ds DataPartitionSorter) Less(i, j int) bool {
//	return ds[i].Metrics.AvgWriteLatencyNano < ds[j].Metrics.AvgWriteLatencyNano
//}

// NewDataPartitionMetrics returns a new DataPartitionMetrics instance.
func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	return metrics
}

// String returns the string format of the data partition.
func (dp *DataPartition) String() string {
	if dp == nil {
		return ""
	}
	return fmt.Sprintf("PartitionID(%v) Status(%v) ReplicaNum(%v) PartitionType(%v) Hosts(%v) NearHosts(%v)",
		dp.PartitionID, dp.Status, dp.ReplicaNum, dp.PartitionType, dp.Hosts, dp.NearHosts)
}

func (dp *DataPartition) CheckAllHostsIsAvail(exclude map[string]struct{}) {
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)
	for i := 0; i < len(dp.Hosts); i++ {
		host := dp.Hosts[i]
		wg.Add(1)
		go func(addr string) {
			var (
				conn net.Conn
				err  error
			)
			defer wg.Done()
			if conn, err = util.DailTimeOut(addr, time.Second); err != nil {
				log.LogWarnf("Dail to Host (%v) err(%v)", addr, err.Error())
				if strings.Contains(err.Error(), syscall.ECONNREFUSED.Error()) {
					lock.Lock()
					exclude[addr] = struct{}{}
					lock.Unlock()
				}
			} else {
				conn.Close()
			}
		}(host)
	}
	wg.Wait()
	log.LogDebugf("CheckAllHostsIsAvail: dp(%v) exclude(%v)", dp.PartitionID, exclude)
}

// GetAllAddrs returns the addresses of all the replicas of the data partition.
func (dp *DataPartition) GetAllAddrs() string {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

func isExcluded(dp *DataPartition, exclude map[string]struct{}) bool {
	for _, host := range dp.Hosts {
		if _, exist := exclude[host]; exist {
			return true
		}
	}
	return false
}

func (dp *DataPartition) LeaderRead(reqPacket *Packet, req *ExtentRequest) (sc *StreamConn, readBytes int, err error) {
	var tracer = tracing.TracerFromContext(reqPacket.Ctx()).ChildTracer("DataPartition.LeaderRead").
		SetTag("reqID", reqPacket.ReqID).
		SetTag("op", reqPacket.GetOpMsg()).
		SetTag("pid", reqPacket.PartitionID)
	defer tracer.Finish()
	reqPacket.SetCtx(tracer.Context())

	sc = NewStreamConn(dp, false)
	errMap := make(map[string]error)
	tryOther := false

	var reply *Packet
	readBytes, reply, tryOther, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
	if err == nil {
		return
	}

	errMap[sc.currAddr] = err
	log.LogDebugf("LeaderRead: send to addr(%v), reqPacket(%v)", sc.currAddr, reqPacket)

	if tryOther || (reply != nil && reply.ResultCode == proto.OpTryOtherAddr) {
		hosts := sortByStatus(sc.dp, true)
		for _, addr := range hosts {
			log.LogWarnf("LeaderRead: try addr(%v) reqPacket(%v)", addr, reqPacket)
			sc.currAddr = addr
			readBytes, reply, tryOther, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
			if err == nil {
				sc.dp.LeaderAddr = sc.currAddr
				return
			}
			errMap[addr] = err
			if !tryOther && (reply != nil && reply.ResultCode != proto.OpTryOtherAddr) {
				break
			}
			log.LogWarnf("LeaderRead: try addr(%v) failed! err(%v) reqPacket(%v)", addr, err, reqPacket)
		}
	}

	log.LogWarnf("LeaderRead exit: err(%v), reqPacket(%v)", err, reqPacket)
	err = errors.New(fmt.Sprintf("LeaderRead: failed, sc(%v) reqPacket(%v) errMap(%v)", sc, reqPacket, errMap))
	return
}

func (dp *DataPartition) FollowerRead(reqPacket *Packet, req *ExtentRequest) (sc *StreamConn, readBytes int, err error) {
	var tracer = tracing.TracerFromContext(reqPacket.Ctx()).ChildTracer("DataPartition.FollowerRead").
		SetTag("reqID", reqPacket.ReqID).
		SetTag("op", reqPacket.GetOpMsg()).
		SetTag("pid", reqPacket.PartitionID)
	defer tracer.Finish()
	reqPacket.SetCtx(tracer.Context())

	sc = NewStreamConn(dp, true)
	errMap := make(map[string]error)

	readBytes, _, _, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
	log.LogDebugf("FollowerRead: send to addr(%v), reqPacket(%v)", sc.currAddr, reqPacket)
	if err == nil {
		return
	}
	errMap[sc.currAddr] = err

	startTime := time.Now()
	for i := 0; i < StreamSendReadMaxRetry; i++ {
		hosts := sortByStatus(sc.dp, true)
		for _, addr := range hosts {
			log.LogWarnf("FollowerRead: try addr(%v) reqPacket(%v)", addr, reqPacket)
			sc.currAddr = addr
			readBytes, _, _, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
			if err == nil {
				return
			}
			errMap[addr] = err
			log.LogWarnf("FollowerRead: try addr(%v) failed! reqPacket(%v) err(%v)", addr, reqPacket, err)
		}
		if time.Since(startTime) > StreamSendTimeout {
			log.LogWarnf("FollowerRead: retry timeout req(%v) time(%v)", reqPacket, time.Since(startTime))
			break
		}
		log.LogWarnf("FollowerRead: errMap(%v), reqPacket(%v), try the next round", errMap, reqPacket)
		time.Sleep(StreamSendSleepInterval)
	}
	err = errors.New(fmt.Sprintf("FollowerRead exit: retried %v times and still failed, sc(%v) reqPacket(%v) errMap(%v)", StreamSendReadMaxRetry, sc, reqPacket, errMap))
	return
}

func (dp *DataPartition) ReadConsistentFromHosts(sc *StreamConn, reqPacket *Packet, req *ExtentRequest) (readBytes int, err error) {
	var tracer = tracing.TracerFromContext(reqPacket.Ctx()).ChildTracer("DataPartition.ReadConsistentFromHosts").
		SetTag("reqID", reqPacket.ReqID).
		SetTag("op", reqPacket.GetOpMsg()).
		SetTag("pid", reqPacket.PartitionID)
	defer tracer.Finish()
	reqPacket.SetCtx(tracer.Context())

	var (
		targetHosts []string
		errMap      map[string]error
		isErr       bool
	)
	start := time.Now()

	for i := 0; i < StreamReadConsistenceRetry; i++ {
		errMap = make(map[string]error)
		targetHosts, isErr = chooseMaxAppliedDp(reqPacket.Ctx(), sc.dp.PartitionID, sc.dp.Hosts, reqPacket)
		// try all hosts with same applied ID
		if !isErr && len(targetHosts) > 0 {
			// need to read data with no leader
			reqPacket.Opcode = proto.OpStreamFollowerRead
			for _, addr := range targetHosts {
				sc.currAddr = addr
				readBytes, _, _, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
				if err == nil {
					return
				}
				errMap[addr] = err
				log.LogWarnf("readConsistentFromHosts: err(%v), addr(%v), try next host", err, addr)
			}
		}
		log.LogWarnf("readConsistentFromHost failed, try next round: sc(%v) reqPacket(%v) isErr(%v) targetHosts(%v) errMap(%v)", sc, reqPacket, isErr, targetHosts, errMap)
		if time.Since(start) > StreamReadConsistenceTimeout {
			log.LogWarnf("readConsistentFromHost failed: retry timeout sc(%v) reqPacket(%v) time(%v)", sc, reqPacket, time.Since(start))
			break
		}
	}
	return readBytes, errors.New(fmt.Sprintf("readConsistentFromHosts: failed, sc(%v) reqPacket(%v) isErr(%v) targetHosts(%v) errMap(%v)",
		sc, reqPacket, isErr, targetHosts, errMap))
}

func (dp *DataPartition) sendReadCmdToDataPartition(sc *StreamConn, reqPacket *Packet, req *ExtentRequest) (readBytes int, reply *Packet, tryOther bool, err error) {
	if sc.currAddr == "" {
		err = errors.New(fmt.Sprintf("sendReadCmdToDataPartition: failed, current address is null, reqPacket(%v)", reqPacket))
		tryOther = true
		return
	}
	var conn *net.TCPConn
	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
	}()
	if conn, err = sc.sendToDataPartition(reqPacket); err != nil {
		log.LogWarnf("sendReadCmdToDataPartition: send to curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, reqPacket, err)
		tryOther = true
		return
	}
	if readBytes, reply, tryOther, err = getReadReply(conn, reqPacket, req); err != nil {
		log.LogWarnf("sendReadCmdToDataPartition: getReply error and RETURN, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, reqPacket, err)
		return
	}
	return
}

// Send send the given packet over the network through the stream connection until success
// or the maximum number of retries is reached.
func (dp *DataPartition) OverWrite(sc *StreamConn, req *Packet, reply *Packet) (err error) {
	var tracer = tracing.TracerFromContext(req.Ctx()).ChildTracer("DataPartition.OverWrite").
		SetTag("req.ReqID", req.ReqID).
		SetTag("req.Op", req.GetOpMsg()).
		SetTag("req.PartitionID", req.PartitionID).
		SetTag("req.Size", req.Size).
		SetTag("req.ExtentID", req.ExtentID).
		SetTag("req.ExtentOffset", req.ExtentOffset)
	defer func() {
		tracer.SetTag("ret.err", err)
		tracer.Finish()
	}()
	req.SetCtx(tracer.Context())

	errMap := make(map[string]error)

	err = dp.OverWriteToDataPartitionLeader(sc, req, reply)
	if err == nil && reply.ResultCode == proto.OpOk {
		return
	}

	if err == nil && reply.ResultCode != proto.OpTryOtherAddr {
		err = errors.New(fmt.Sprintf("OverWrite failed: sc(%v) resultCode(%v) reply(%v) reqPacket(%v)", sc, reply.GetResultMsg(), reply, req))
		return
	}

	hosts := sortByStatus(sc.dp, true)
	startTime := time.Now()
	for i := 0; i < StreamSendOverWriteMaxRetry; i++ {
		for _, addr := range hosts {
			log.LogWarnf("OverWrite: try addr(%v) reqPacket(%v)", addr, req)
			sc.currAddr = addr
			err = dp.OverWriteToDataPartitionLeader(sc, req, reply)
			if err == nil && reply.ResultCode == proto.OpOk {
				sc.dp.LeaderAddr = sc.currAddr
				return
			}
			if err == nil && reply.ResultCode != proto.OpTryOtherAddr {
				err = errors.New(fmt.Sprintf("OverWrite failed: sc(%v) errMap(%v) reply(%v) reqPacket(%v)", sc, errMap, reply, req))
				return
			}
			if err == nil {
				err = errors.New(reply.GetResultMsg())
			}
			errMap[addr] = err
			log.LogWarnf("OverWrite: try addr(%v) failed! err(%v) reply(%v) reqPacket(%v) ", addr, err, reply, req)
		}
		if time.Since(startTime) > StreamSendOverWriteTimeout {
			log.LogWarnf("OverWrite: retry timeout req(%v) time(%v)", req, time.Since(startTime))
			break
		}
		log.LogWarnf("OverWrite: errMap(%v), reqPacket(%v), try the next round", errMap, req)
		time.Sleep(StreamSendSleepInterval)
	}


	return errors.New(fmt.Sprintf("OverWrite failed: sc(%v) errMap(%v) reply(%v) reqPacket(%v)", sc, errMap, reply, req))
}

func (dp *DataPartition) OverWriteToDataPartitionLeader(sc *StreamConn, req *Packet, reply *Packet) (err error) {
	var conn *net.TCPConn
	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
	}()
	if conn, err = sc.sendToDataPartition(req); err != nil {
		log.LogWarnf("OverWriteToDataPartitionLeader: send to curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
		return
	}
	reply.SetCtx(req.Ctx())
	if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		log.LogWarnf("OverWriteToDataPartitionLeader: getReply error and RETURN, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
		return
	}
	return
}
