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
	"net"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (l *LcNode) opMasterHeartbeat(conn net.Conn, p *proto.Packet, remoteAddr string) (err error) {
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()

	data := p.Data
	var (
		req  = &proto.HeartBeatRequest{}
		resp = &proto.LcNodeHeartbeatResponse{
			LcScanningTasks:       make(map[string]*proto.LcNodeRuleTaskResponse, 0),
			SnapshotScanningTasks: make(map[string]*proto.SnapshotVerDelTaskResponse, 0),
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
			resp.Result = err.Error()
			goto end
		}

		l.scannerMutex.RLock()
		for _, scanner := range l.lcScanners {
			result := &proto.LcNodeRuleTaskResponse{
				ID: scanner.ID,
				LcNodeRuleTaskStatistics: proto.LcNodeRuleTaskStatistics{
					Volume:               scanner.Volume,
					RuleId:               scanner.rule.ID,
					TotalInodeScannedNum: atomic.LoadInt64(&scanner.currentStat.TotalInodeScannedNum),
					FileScannedNum:       atomic.LoadInt64(&scanner.currentStat.FileScannedNum),
					DirScannedNum:        atomic.LoadInt64(&scanner.currentStat.DirScannedNum),
					ExpiredNum:           atomic.LoadInt64(&scanner.currentStat.ExpiredNum),
					ErrorSkippedNum:      atomic.LoadInt64(&scanner.currentStat.ErrorSkippedNum),
				},
			}
			resp.LcScanningTasks[scanner.ID] = result
		}
		for _, scanner := range l.snapshotScanners {
			info := &proto.SnapshotVerDelTaskResponse{
				ID: scanner.ID,
				SnapshotStatistics: proto.SnapshotStatistics{
					VerInfo: proto.VerInfo{
						VolName: scanner.Volume,
						VerSeq:  scanner.getTaskVerSeq(),
					},
					TotalInodeNum:   atomic.LoadInt64(&scanner.currentStat.TotalInodeNum),
					FileNum:         atomic.LoadInt64(&scanner.currentStat.FileNum),
					DirNum:          atomic.LoadInt64(&scanner.currentStat.DirNum),
					ErrorSkippedNum: atomic.LoadInt64(&scanner.currentStat.ErrorSkippedNum),
				},
			}
			resp.SnapshotScanningTasks[scanner.ID] = info
		}
		l.scannerMutex.RUnlock()

		resp.Status = proto.TaskSucceeds
	end:
		adminTask.Request = nil
		adminTask.Response = resp
		l.respondToMaster(adminTask)
		data, _ := json.Marshal(resp)
		log.LogInfof("%s pkt %s, resp success req:%v; respAdminTask: %v, resp: %v, cost %s",
			remoteAddr, p.String(), req, adminTask, string(data), time.Since(start).String())
	}()
	l.lastHeartbeat = time.Now()
	return
}

func (l *LcNode) opLcScan(conn net.Conn, p *proto.Packet) (err error) {
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
	data := p.Data
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
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		adminTask.Request = nil
		adminTask.Response = resp
		l.respondToMaster(adminTask)
		return
	}

	if err = l.startLcScan(adminTask); err != nil {
		l.respondToMaster(adminTask)
		return
	}

	return
}

func (l *LcNode) respondToMaster(task *proto.AdminTask) (err error) {
	// handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	if err = l.mc.NodeAPI().ResponseLcNodeTask(task); err != nil {
		err = errors.Trace(err, "try respondToMaster failed")
	}
	return
}

func (l *LcNode) opSnapshotVerDel(conn net.Conn, p *proto.Packet) (err error) {
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
	data := p.Data
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

	if err = l.startSnapshotScan(adminTask); err != nil {
		l.respondToMaster(adminTask)
		return
	}

	return
}
