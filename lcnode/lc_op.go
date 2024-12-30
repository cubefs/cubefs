// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

func (l *LcNode) opMasterHeartbeat(conn net.Conn, p *proto.Packet, remoteAddr string) (err error) {
	data := p.Data
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()

	var (
		req  = &proto.HeartBeatRequest{}
		resp = &proto.LcNodeHeartbeatResponse{
			LcScanningTasks:       make(map[string]*proto.LcNodeRuleTaskResponse),
			SnapshotScanningTasks: make(map[string]*proto.SnapshotVerDelTaskResponse),
		}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)

	go func() {
		start := time.Now()
		decode := json.NewDecoder(bytes.NewBuffer(data))
		decode.UseNumber()
		if err = decode.Decode(adminTask); err != nil {
			resp.Status = proto.TaskFailed
			resp.Result = fmt.Sprintf("lcnode(%v) heartbeat decode err(%v)", l.localServerAddr, err.Error())
			goto end
		}

		l.scannerMutex.RLock()
		for _, scanner := range l.lcScanners {
			result := &proto.LcNodeRuleTaskResponse{
				ID:        scanner.ID,
				LcNode:    l.localServerAddr,
				StartTime: &scanner.now,
				Volume:    scanner.Volume,
				RcvStop:   scanner.receiveStop,
				Rule:      scanner.rule,
				LcNodeRuleTaskStatistics: proto.LcNodeRuleTaskStatistics{
					TotalFileScannedNum:      atomic.LoadInt64(&scanner.currentStat.TotalFileScannedNum),
					TotalFileExpiredNum:      atomic.LoadInt64(&scanner.currentStat.TotalFileExpiredNum),
					TotalDirScannedNum:       atomic.LoadInt64(&scanner.currentStat.TotalDirScannedNum),
					ExpiredDeleteNum:         atomic.LoadInt64(&scanner.currentStat.ExpiredDeleteNum),
					ExpiredMToHddNum:         atomic.LoadInt64(&scanner.currentStat.ExpiredMToHddNum),
					ExpiredMToBlobstoreNum:   atomic.LoadInt64(&scanner.currentStat.ExpiredMToBlobstoreNum),
					ExpiredMToHddBytes:       atomic.LoadInt64(&scanner.currentStat.ExpiredMToHddBytes),
					ExpiredMToBlobstoreBytes: atomic.LoadInt64(&scanner.currentStat.ExpiredMToBlobstoreBytes),
					ExpiredSkipNum:           atomic.LoadInt64(&scanner.currentStat.ExpiredSkipNum),
					ErrorDeleteNum:           atomic.LoadInt64(&scanner.currentStat.ErrorDeleteNum),
					ErrorMToHddNum:           atomic.LoadInt64(&scanner.currentStat.ErrorMToHddNum),
					ErrorMToBlobstoreNum:     atomic.LoadInt64(&scanner.currentStat.ErrorMToBlobstoreNum),
					ErrorReadDirNum:          atomic.LoadInt64(&scanner.currentStat.ErrorReadDirNum),
				},
			}
			resp.LcScanningTasks[scanner.ID] = result
		}
		for _, scanner := range l.snapshotScanners {
			info := &proto.SnapshotVerDelTaskResponse{
				ID:                 scanner.ID,
				LcNode:             l.localServerAddr,
				SnapshotVerDelTask: scanner.verDelReq.Task,
				SnapshotStatistics: proto.SnapshotStatistics{
					VolName:         scanner.Volume,
					VerSeq:          scanner.getTaskVerSeq(),
					TotalInodeNum:   atomic.LoadInt64(&scanner.currentStat.TotalInodeNum),
					FileNum:         atomic.LoadInt64(&scanner.currentStat.FileNum),
					DirNum:          atomic.LoadInt64(&scanner.currentStat.DirNum),
					ErrorSkippedNum: atomic.LoadInt64(&scanner.currentStat.ErrorSkippedNum),
				},
			}
			resp.SnapshotScanningTasks[scanner.ID] = info
		}
		l.scannerMutex.RUnlock()

		resp.LcTaskCountLimit = lcNodeTaskCountLimit
		resp.Status = proto.TaskSucceeds

	end:
		adminTask.Response = resp
		l.respondToMaster(adminTask)
		msg := fmt.Sprintf("from(%v), adminTask(%+v), resp(%+v), %v", remoteAddr, adminTask, resp, time.Since(start).String())
		log.LogInfof("MasterHeartbeat %v ", msg)
		auditlog.LogMasterOp("MasterHeartbeat", msg, err)
	}()

	l.lastHeartbeat = time.Now()
	log.LogDebugf("lastHeartbeat: %v", l.lastHeartbeat)
	return
}

func (l *LcNode) opLcScan(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()

	var (
		req       = &proto.LcNodeRuleTaskRequest{}
		resp      = &proto.LcNodeRuleTaskResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)

	decoder := json.NewDecoder(bytes.NewBuffer(data))
	decoder.UseNumber()
	if err = decoder.Decode(adminTask); err != nil {
		resp.LcNode = l.localServerAddr
		resp.Status = proto.TaskFailed
		resp.Done = true
		resp.StartErr = err.Error()
		adminTask.Response = resp
		l.respondToMaster(adminTask)
		return
	}

	l.startLcScan(adminTask)
	l.respondToMaster(adminTask)

	return
}

func (l *LcNode) respondToMaster(task *proto.AdminTask) {
	// handle panic
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("respondToMaster err: %v", r)
		}
	}()
	if err := l.mc.NodeAPI().ResponseLcNodeTask(task); err != nil {
		log.LogErrorf("respondToMaster err: %v, task: %v", err, task)
	}
}

func (l *LcNode) opSnapshotVerDel(conn net.Conn, p *proto.Packet) (err error) {
	data := p.Data
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()

	var (
		req       = &proto.SnapshotVerDelTaskRequest{}
		resp      = &proto.SnapshotVerDelTaskResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)

	decoder := json.NewDecoder(bytes.NewBuffer(data))
	decoder.UseNumber()
	if err = decoder.Decode(adminTask); err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		adminTask.Response = resp
		l.respondToMaster(adminTask)
		return
	}

	l.startSnapshotScan(adminTask)
	l.respondToMaster(adminTask)

	return
}
