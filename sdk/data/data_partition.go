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

package data

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type hostPingElapsed struct {
	host    string
	elapsed time.Duration
}

type PingElapsedSortedHosts struct {
	sortedHosts  []string
	updateTSUnix int64 // Timestamp (unix second) of latest update.
	getHosts     func() (hosts []string)
	getElapsed   func(host string) (elapsed time.Duration, ok bool)
}

func (h *PingElapsedSortedHosts) isNeedUpdate() bool {
	return h.updateTSUnix == 0 || time.Now().Unix()-h.updateTSUnix > 10
}

func (h *PingElapsedSortedHosts) update(getHosts func() []string, getElapsed func(host string) (time.Duration, bool)) []string {
	var hosts = getHosts()
	hostElapses := make([]*hostPingElapsed, 0, len(hosts))
	for _, host := range hosts {
		var hostElapsed *hostPingElapsed
		if elapsed, ok := getElapsed(host); ok {
			hostElapsed = &hostPingElapsed{host: host, elapsed: elapsed}
		} else {
			hostElapsed = &hostPingElapsed{host: host, elapsed: time.Duration(0)}
		}
		hostElapses = append(hostElapses, hostElapsed)
	}
	sort.SliceStable(hostElapses, func(i, j int) bool {
		return hostElapses[j].elapsed == 0 || hostElapses[i].elapsed < hostElapses[j].elapsed
	})
	sorted := make([]string, len(hostElapses))
	for i, hotElapsed := range hostElapses {
		sorted[i] = hotElapsed.host
	}
	h.sortedHosts = sorted
	h.updateTSUnix = time.Now().Unix()
	return sorted
}

func (h *PingElapsedSortedHosts) GetSortedHosts() []string {
	if h.isNeedUpdate() {
		return h.update(h.getHosts, h.getElapsed)
	}
	return h.sortedHosts
}

func NewPingElapsedSortHosts(getHosts func() []string, getElapsed func(host string) (time.Duration, bool)) *PingElapsedSortedHosts {
	return &PingElapsedSortedHosts{
		getHosts: getHosts,
		getElapsed: getElapsed,
	}
}

// DataPartition defines the wrapper of the data partition.
type DataPartition struct {
	// Will not be changed
	proto.DataPartitionResponse
	RandomWrite        bool
	PartitionType      string
	NearHosts          []string
	CrossRegionMetrics *CrossRegionMetrics
	ClientWrapper      *Wrapper
	Metrics            *proto.DataPartitionMetrics
	hostErrMap         sync.Map //key: host; value: last error access time
	ecEnable           bool
	ReadMetrics        *proto.ReadMetrics

	pingElapsedSortedHosts *PingElapsedSortedHosts
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
		log.LogWarnf("RecordWrite: dp[%v] punish write time[5s] because of error, cost[%v]ns", dp.PartitionID, cost)
	}

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	dp.Metrics.WriteOpNum++
	dp.Metrics.SumWriteLatencyNano += cost

	return
}

func (dp *DataPartition) LocalMetricsRefresh() {
	if dp.Metrics == nil {
		return
	}

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	if dp.Metrics.ReadOpNum != 0 {
		dp.Metrics.AvgReadLatencyNano = dp.Metrics.SumReadLatencyNano / dp.Metrics.ReadOpNum
	} else {
		dp.Metrics.AvgReadLatencyNano = 0
	}

	if dp.Metrics.WriteOpNum != 0 {
		atomic.StoreInt64(&dp.Metrics.AvgWriteLatencyNano, (9*dp.GetAvgWrite()+dp.Metrics.SumWriteLatencyNano/dp.Metrics.WriteOpNum)/10)
	} else {
		atomic.StoreInt64(&dp.Metrics.AvgWriteLatencyNano, (9*dp.GetAvgWrite())/10)
	}

	dp.Metrics.SumReadLatencyNano = 0
	dp.Metrics.SumWriteLatencyNano = 0
	dp.Metrics.ReadOpNum = 0
	dp.Metrics.WriteOpNum = 0
}

func (dp *DataPartition) LocalMetricsClear() {
	if dp.Metrics == nil {
		return
	}

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	dp.Metrics.SumReadLatencyNano = 0
	dp.Metrics.SumWriteLatencyNano = 0
	dp.Metrics.ReadOpNum = 0
	dp.Metrics.WriteOpNum = 0
}

func (dp *DataPartition) RemoteMetricsRefresh(newMetrics *proto.DataPartitionMetrics) {
	if dp.Metrics == nil {
		return
	}

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	if newMetrics != nil && newMetrics.WriteOpNum != 0 {
		atomic.StoreInt64(&dp.Metrics.AvgWriteLatencyNano, (9*dp.GetAvgWrite()+newMetrics.SumWriteLatencyNano/newMetrics.WriteOpNum)/10)
	} else {
		atomic.StoreInt64(&dp.Metrics.AvgWriteLatencyNano, (9*dp.GetAvgWrite())/10)
	}
}

func (dp *DataPartition) RemoteMetricsSummary() *proto.DataPartitionMetrics {
	if dp.Metrics == nil {
		return nil
	}

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	if dp.Metrics.WriteOpNum == 0 {
		return nil
	}

	summaryMetrics := &proto.DataPartitionMetrics{PartitionId: dp.PartitionID}
	summaryMetrics.SumWriteLatencyNano = dp.Metrics.SumWriteLatencyNano
	summaryMetrics.WriteOpNum = dp.Metrics.WriteOpNum
	dp.Metrics.SumWriteLatencyNano = 0
	dp.Metrics.WriteOpNum = 0

	return summaryMetrics
}

//func (dp *DataPartition) GetAvgRead() int64 {
//	dp.Metrics.RLock()
//	defer dp.Metrics.RUnlock()
//
//	return dp.Metrics.AvgReadLatencyNano
//}

func (dp *DataPartition) GetAvgWrite() int64 {
	return atomic.LoadInt64(&dp.Metrics.AvgWriteLatencyNano)
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
			if conn, err = connpool.DailTimeOut(addr, time.Duration(dp.ClientWrapper.connConfig.ConnectTimeoutNs)*time.Nanosecond); err != nil {
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

// GetAllHosts returns the addresses of all the replicas of the data partition.
func (dp *DataPartition) GetAllHosts() []string {
	return dp.Hosts
}

func isExcluded(dp *DataPartition, exclude map[string]struct{}, quorum int) bool {
	if _, exist := exclude[dp.Hosts[0]]; exist {
		return true
	}
	aliveCount := 0
	for _, host := range dp.Hosts {
		if _, exist := exclude[host]; !exist {
			aliveCount++
		}
	}
	// 'quorum == 0' means all hosts must succeed
	if quorum <= 0 && aliveCount < len(dp.Hosts) {
		return true
	}
	if quorum > 0 && aliveCount < quorum {
		return true
	}
	return false
}

func (dp *DataPartition) LeaderRead(reqPacket *common.Packet, req *ExtentRequest) (sc *StreamConn, readBytes int, err error) {
	sc = NewStreamConn(dp, false)
	errMap := make(map[string]error)
	tryOther := false

	var reply *common.Packet
	readBytes, reply, tryOther, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
	if err == nil {
		return
	}

	errMap[sc.currAddr] = err
	log.LogDebugf("LeaderRead: send to addr(%v), reqPacket(%v)", sc.currAddr, reqPacket)

	if tryOther || (reply != nil && reply.ResultCode == proto.OpTryOtherAddr) {
		hosts := sortByStatus(sc.dp, sc.currAddr)
		for _, addr := range hosts {
			log.LogWarnf("LeaderRead: try addr(%v) reqPacket(%v)", addr, reqPacket)
			sc.currAddr = addr
			readBytes, reply, tryOther, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
			if err == nil {
				sc.dp.LeaderAddr=proto.NewAtomicString(sc.currAddr)
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

func (dp *DataPartition) FollowerRead(reqPacket *common.Packet, req *ExtentRequest) (sc *StreamConn, readBytes int, err error) {
	sc = NewStreamConn(dp, true)
	errMap := make(map[string]error)

	readBytes, _, _, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
	log.LogDebugf("FollowerRead: send to addr(%v), reqPacket(%v)", sc.currAddr, reqPacket)
	if err == nil {
		return
	}
	errMap[sc.currAddr] = err

	hosts := sortByStatus(sc.dp, sc.currAddr)
	startTime := time.Now()
	for i := 0; i < StreamSendReadMaxRetry; i++ {
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
	err = errors.New(fmt.Sprintf("FollowerRead: failed %v times, reqPacket(%v) errMap(%v)", StreamSendReadMaxRetry, reqPacket, errMap))
	return
}

func (dp *DataPartition) ReadConsistentFromHosts(sc *StreamConn, reqPacket *common.Packet, req *ExtentRequest) (readBytes int, err error) {
	var (
		targetHosts []string
		errMap      map[string]error
		isErr       bool
	)
	start := time.Now()

	for i := 0; i < StreamReadConsistenceRetry; i++ {
		errMap = make(map[string]error)
		targetHosts, isErr = dp.chooseMaxAppliedDp(reqPacket.Ctx(), sc.dp.PartitionID, sc.dp.Hosts, reqPacket)
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

func (dp *DataPartition) SendReadCmdToDataPartition(sc *StreamConn, reqPacket *common.Packet, req *ExtentRequest) (readBytes int, reply *common.Packet, tryOther bool, err error) {
	return dp.sendReadCmdToDataPartition(sc, reqPacket, req)
}

func (dp *DataPartition) sendReadCmdToDataPartition(sc *StreamConn, reqPacket *common.Packet, req *ExtentRequest) (readBytes int, reply *common.Packet, tryOther bool, err error) {
	if sc.currAddr == "" {
		err = errors.New(fmt.Sprintf("sendReadCmdToDataPartition: failed, current address is null, reqPacket(%v)", reqPacket))
		tryOther = true
		return
	}
	var conn *net.TCPConn
	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
		if dp.ClientWrapper.CrossRegionHATypeQuorum() {
			// 'tryOther' means network failure
			dp.updateCrossRegionMetrics(sc.currAddr, tryOther)
		}
	}()
	if conn, err = sc.sendToDataPartition(reqPacket); err != nil {
		dp.hostErrMap.Store(sc.currAddr, time.Now().UnixNano())
		log.LogWarnf("sendReadCmdToDataPartition: send to curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, reqPacket, err)
		tryOther = true
		return
	}
	if readBytes, reply, tryOther, err = sc.getReadReply(conn, reqPacket, req); err != nil {
		dp.hostErrMap.Store(sc.currAddr, time.Now().UnixNano())
		log.LogWarnf("sendReadCmdToDataPartition: getReply error and RETURN, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, reqPacket, err)
		return
	}
	dp.RecordFollowerRead(reqPacket.SendT, sc.currAddr)
	return
}

// Send send the given packet over the network through the stream connection until success
// or the maximum number of retries is reached.
func (dp *DataPartition) OverWrite(sc *StreamConn, req *common.Packet, reply *common.Packet) (err error) {
	err = dp.OverWriteToDataPartitionLeader(sc, req, reply)
	if err == nil && reply.ResultCode == proto.OpOk {
		return
	}

	if err == nil && reply.ResultCode != proto.OpTryOtherAddr {
		err = errors.New(fmt.Sprintf("OverWrite failed: sc(%v) resultCode(%v) reply(%v) reqPacket(%v)", sc, reply.GetResultMsg(), reply, req))
		return
	}

	startTime := time.Now()
	errMap := make(map[string]error)
	for i := 0; i < StreamSendOverWriteMaxRetry; i++ {
		for _, addr := range dp.Hosts {
			log.LogWarnf("OverWrite: try addr(%v) reqPacket(%v)", addr, req)
			sc.currAddr = addr
			err = dp.OverWriteToDataPartitionLeader(sc, req, reply)
			if err == nil && reply.ResultCode == proto.OpOk {
				sc.dp.LeaderAddr=proto.NewAtomicString(sc.currAddr)
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
		//time.Sleep(StreamSendSleepInterval)
	}

	return errors.New(fmt.Sprintf("OverWrite failed: sc(%v) errMap(%v) reply(%v) reqPacket(%v)", sc, errMap, reply, req))
}

func (dp *DataPartition) OverWriteToDataPartitionLeader(sc *StreamConn, req *common.Packet, reply *common.Packet) (err error) {
	var conn *net.TCPConn
	defer func() {
		StreamConnPool.PutConnectWithErr(conn, err)
	}()
	if conn, err = sc.sendToDataPartition(req); err != nil {
		dp.hostErrMap.Store(sc.currAddr, time.Now().UnixNano())
		log.LogWarnf("OverWriteToDataPartitionLeader: send to curr addr failed, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
		return
	}
	if err = reply.ReadFromConnNs(conn, dp.ClientWrapper.connConfig.ReadTimeoutNs); err != nil {
		dp.hostErrMap.Store(sc.currAddr, time.Now().UnixNano())
		log.LogWarnf("OverWriteToDataPartitionLeader: getReply error and RETURN, addr(%v) reqPacket(%v) err(%v)", sc.currAddr, req, err)
		return
	}
	return
}

func (dp *DataPartition) getEpochReadHost(hosts []string) (err error, addr string) {
	hostsStatus := dp.ClientWrapper.HostsStatus
	epoch := dp.Epoch
	dp.Epoch += 1
	for retry := 0; retry < len(hosts); retry++ {
		addr = hosts[(epoch+uint64(retry))%uint64(len(hosts))]
		active, ok := hostsStatus[addr]
		if ok && active {
			return nil, addr
		}
	}
	return fmt.Errorf("getEpochReadHost failed: no available host"), ""
}

func chooseEcNode(hosts []string, stripeUnitSize, extentOffset uint64, dp *DataPartition) (host string) {
	div := math.Floor(float64(extentOffset) / float64(stripeUnitSize))
	index := int(div) % int(dp.EcDataNum)
	hostsStatus := dp.ClientWrapper.HostsStatus

	if status, ok := hostsStatus[hosts[index]]; ok && status {
		host = hosts[index]
	}
	return
}

func (dp *DataPartition) EcRead(reqPacket *common.Packet, req *ExtentRequest) (sc *StreamConn, readBytes int, err error) {
	errMap := make(map[string]error)
	hosts := proto.GetEcHostsByExtentId(uint64(len(dp.EcHosts)), req.ExtentKey.ExtentId, dp.EcHosts)
	stripeUnitSize := proto.CalStripeUnitSize(uint64(req.ExtentKey.Size), dp.EcMaxUnitSize, uint64(dp.EcDataNum))

	host := chooseEcNode(hosts, stripeUnitSize, uint64(reqPacket.ExtentOffset), dp)
	sc = &StreamConn{
		dp:       dp,
		currAddr: host,
	}

	readBytes, _, _, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
	log.LogDebugf("EcRead: send to addr(%v), reqPacket(%v)", sc.currAddr, reqPacket)
	if err == nil {
		return
	}
	errMap[sc.currAddr] = err

	hostsStatus := dp.ClientWrapper.HostsStatus
	for _, addr := range dp.EcHosts {
		if addr == host {
			continue
		}
		if status, ok := hostsStatus[addr]; !ok || !status {
			continue
		}
		sc.currAddr = addr
		readBytes, _, _, err = dp.sendReadCmdToDataPartition(sc, reqPacket, req)
		if err == nil {
			return
		}
		errMap[addr] = err
	}

	log.LogWarnf("EcRead exit: err(%v), reqPacket(%v)", err, reqPacket)
	err = errors.New(fmt.Sprintf("EcRead: failed, sc(%v) reqPacket(%v) errMap(%v)", sc, reqPacket, errMap))

	return
}

func (dp *DataPartition) canEcRead() bool {
	if dp.EcMigrateStatus == proto.OnlyEcExist {
		return true
	}
	if dp.ecEnable && dp.EcMigrateStatus == proto.FinishEC {
		return true
	}
	return false
}

func (dp *DataPartition) RecordFollowerRead(sendT int64, host string) {
	if !dp.ClientWrapper.dpFollowerReadDelayConfig.EnableCollect {
		return
	}
	if sendT == 0 {
		// except FollowerRead req packet, other read req packet SendT=0
		return
	}
	cost := time.Now().UnixNano() - sendT

	dp.ReadMetrics.Lock()
	defer dp.ReadMetrics.Unlock()

	dp.ReadMetrics.FollowerReadOpNum[host]++
	dp.ReadMetrics.SumFollowerReadHostDelay[host] += cost
	if log.IsDebugEnabled() {
		log.LogDebugf("RecordFollowerRead: opNum(%v), total cost(%v), host(%v)", dp.ReadMetrics.FollowerReadOpNum[host],
			dp.ReadMetrics.SumFollowerReadHostDelay[host], host)
	}
	return
}

func (dp *DataPartition) RemoteReadMetricsSummary() *proto.ReadMetrics {
	if dp.ReadMetrics == nil {
		return nil
	}
	dp.ReadMetrics.Lock()
	defer dp.ReadMetrics.Unlock()

	if dp.ReadMetrics.FollowerReadOpNum == nil || dp.ReadMetrics.SumFollowerReadHostDelay == nil {
		log.LogWarnf("RemoteReadMetricsSummary failed: dpID(%v), OpNum&Sum are nil\n", dp.PartitionID)
		return nil
	}
	if len(dp.ReadMetrics.FollowerReadOpNum) == 0 || len(dp.ReadMetrics.SumFollowerReadHostDelay) == 0 {
		log.LogDebugf("RemoteReadMetricsSummary failed: dpID(%v) ReadMetrics len = 0", dp.PartitionID)
		return nil
	}

	summaryMetrics := &proto.ReadMetrics{PartitionId: dp.PartitionID}
	summaryMetrics.SumFollowerReadHostDelay = dp.ReadMetrics.SumFollowerReadHostDelay
	summaryMetrics.FollowerReadOpNum = dp.ReadMetrics.FollowerReadOpNum

	dp.ReadMetrics.SumFollowerReadHostDelay = make(map[string]int64, 0)
	dp.ReadMetrics.FollowerReadOpNum = make(map[string]int64, 0)

	if log.IsDebugEnabled() {
		log.LogDebugf("RemoteReadMetricsSummary success: dpID(%v)", dp.PartitionID)
	}
	return summaryMetrics
}

func (dp *DataPartition) UpdateReadMetricsHost(hosts []string) {
	dp.ReadMetrics.Lock()
	defer dp.ReadMetrics.Unlock()

	dp.ReadMetrics.SortedHost = hosts
	if log.IsDebugEnabled() {
		log.LogDebugf("UpdateReadMetrics success: dpID(%v) SortedHost(%v)", dp.PartitionID, dp.ReadMetrics.SortedHost)
	}
}

func (dp *DataPartition) ClearReadMetrics() {
	dp.ReadMetrics.Lock()
	defer dp.ReadMetrics.Unlock()

	dp.ReadMetrics.SumFollowerReadHostDelay = make(map[string]int64, 0)
	dp.ReadMetrics.FollowerReadOpNum = make(map[string]int64, 0)
	dp.ReadMetrics.SortedHost = make([]string, 0)

	if log.IsDebugEnabled() {
		log.LogDebugf("ClearReadMetrics success: dpID(%v)", dp.PartitionID)
	}
}

func (dp *DataPartition) getLowestReadDelayHost(dataPartitionID uint64) (err error, addr string) {
	if dataPartitionID != dp.PartitionID {
		dataPartitionID = dp.PartitionID
	}
	dp.ReadMetrics.RLock()
	defer dp.ReadMetrics.RUnlock()

	sortedHosts := dp.ReadMetrics.SortedHost
	if sortedHosts == nil {
		return fmt.Errorf("getLowestReadDelayHost failed: dpID(%v) sortedHosts is nil", dp.PartitionID), ""
	}
	var availableHost []string
	// check hosts status to get available hosts
	hostsStatus := dp.ClientWrapper.HostsStatus
	for _, addr = range sortedHosts {
		if status, ok := hostsStatus[addr]; ok && status {
			availableHost = append(availableHost, addr)
		}
	}
	addr, err = dp.assignHostByWeight(availableHost)
	if err != nil {
		return fmt.Errorf("getLowestReadDelayHost failed: dpID(%v) err(%v)", dp.PartitionID, err), ""
	}
	log.LogDebugf("getLowestReadDelayHost success: dpID(%v), host(%v)", dataPartitionID, addr)
	return nil, addr
}

func (dp *DataPartition) assignHostByWeight(hosts []string) (host string, err error) {
	if hosts == nil {
		err = fmt.Errorf("assignHostByWeight failed: no available host")
		return "", err
	}
	num := len(hosts)
	if num == 1 {
		// only one available host
		log.LogInfof("assignHostByWeight: only one available host(%v)", hosts[0])
		return hosts[0], nil
	}
	firstWeight := dp.ClientWrapper.dpLowestDelayHostWeight
	weightListForHosts := getHostsWeight(firstWeight, num)
	if log.IsDebugEnabled() {
		log.LogDebugf("assignHostByWeight: host num: %v, weight for host: %v", num, weightListForHosts)
	}
	return getHostByWeight(weightListForHosts, hosts), nil
}

func getHostsWeight(first int, num int) (weight []int) {
	weight = make([]int, num)
	var total = 100
	if first == 0 {
		weight[0] = proto.DefaultLowestDelayHostWeight
	} else {
		weight[0] = first
	}
	total -= weight[0]
	// except the lowest delay host, other host divide equally
	for i := 1; i < num; i++ {
		weight[i] = int(math.Ceil(float64(total) / float64(num-i)))
		total -= weight[i]
	}
	return
}

func getHostByWeight(weight []int, hosts []string) (host string) {
	for i := 1; i < len(weight); i++ {
		weight[i] += weight[i-1]
	}
	rand.Seed(time.Now().UnixNano())
	target := rand.Intn(100)
	left := 0
	right := len(weight)
	for left < right {
		mid := (left + right) / 2
		if weight[mid] == target {
			return hosts[mid]
		} else if weight[mid] > target {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return hosts[left]
}

type HostDelay struct {
	host  string
	delay time.Duration
}

func (this *HostDelay) Less(that *HostDelay) bool {
	if that.delay == time.Duration(0) {
		return true
	}
	return this.delay < that.delay
}

func (dp *DataPartition) sortHostsByPingtime() []string {
	var (
		items  = make([]*HostDelay, 0)
		sorted = make([]string, 0)
	)
	for _, host := range dp.Hosts {
		var item *HostDelay
		if delay, ok := dp.ClientWrapper.HostsDelay.Load(host); ok {
			item = &HostDelay{host, delay.(time.Duration)}
		} else {
			item = &HostDelay{host, time.Duration(0)}
		}
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Less(items[j])
	})
	for _, item := range items {
		sorted = append(sorted, item.host)
	}
	return sorted
}

func (dp *DataPartition) sortHostsByPingElapsed() []string {
	if dp.pingElapsedSortedHosts == nil {
		var getHosts = func() []string {
			return dp.Hosts
		}
		var getElapsed = func (host string) (time.Duration, bool) {
			delay, ok := dp.ClientWrapper.HostsDelay.Load(host)
			if !ok {
				return 0, false
			}
			return delay.(time.Duration), true
		}
		dp.pingElapsedSortedHosts = NewPingElapsedSortHosts(getHosts, getElapsed)
	}
	return dp.pingElapsedSortedHosts.GetSortedHosts()
}

func (dp *DataPartition) getNearestHost() string {
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
	return dp.GetLeaderAddr()
}

func (dp *DataPartition) getFollowerReadHost() string {
	if len(dp.Hosts) > 0 {
		// if enableCollect is false, use getEpoch; unless, getLowest
		if dp.ClientWrapper.dpFollowerReadDelayConfig.EnableCollect {
			err, host := dp.getLowestReadDelayHost(dp.PartitionID)
			if err == nil {
				return host
			}
			log.LogWarnf("getFollowerReadHost err:(%v)", err)
		}
		err, host := dp.getEpochReadHost(dp.Hosts)
		if err == nil {
			return host
		}
	}
	return dp.GetLeaderAddr()
}

// sortByStatus will return hosts list sort by host status for DataPartition.
// The order from front to back is "status(true)/status(false)/failedHost".
func sortByStatus(dp *DataPartition, failedHost string) (hosts []string) {
	var inactiveHosts []string
	hostsStatus := dp.ClientWrapper.HostsStatus
	var dpHosts []string
	if dp.ClientWrapper.CrossRegionHATypeQuorum() {
		dpHosts = dp.getSortedCrossRegionHosts()
	} else if dp.ClientWrapper.FollowerRead() && dp.ClientWrapper.NearRead() {
		dpHosts = dp.NearHosts
	}
	if len(dpHosts) == 0 {
		dpHosts = dp.Hosts
	}

	for _, addr := range dpHosts {
		if addr == failedHost {
			continue
		}
		status, ok := hostsStatus[addr]
		if ok {
			if status {
				hosts = append(hosts, addr)
			} else {
				inactiveHosts = append(inactiveHosts, addr)
			}
		} else {
			inactiveHosts = append(inactiveHosts, addr)
			log.LogWarnf("sortByStatus: can not find host[%v] in HostsStatus, dp[%d]", addr, dp.PartitionID)
		}
	}

	sortByAccessErrTs(dp, hosts)

	hosts = append(hosts, inactiveHosts...)
	hosts = append(hosts, failedHost)

	log.LogDebugf("sortByStatus: dp(%v) sortedHost(%v) failedHost(%v)", dp, hosts, failedHost)

	return
}

func sortByAccessErrTs(dp *DataPartition, hosts []string) {

	for _, host := range hosts {
		ts, ok := dp.hostErrMap.Load(host)
		if ok && time.Now().UnixNano()-ts.(int64) > HostErrAccessTimeout*1e9 {
			dp.hostErrMap.Delete(host)
		}
	}

	sort.Slice(hosts, func(i, j int) bool {
		var iTime, jTime int64
		iTs, ok := dp.hostErrMap.Load(hosts[i])
		if ok {
			iTime = iTs.(int64)
		}
		jTs, ok := dp.hostErrMap.Load(hosts[j])
		if ok {
			jTime = jTs.(int64)
		}
		return iTime < jTime
	})
}