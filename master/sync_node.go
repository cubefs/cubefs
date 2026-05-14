// Copyright 2026 The CubeFS Authors.
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

package master

import (
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

// Raft persistence keys for SyncNode. Kept local to this file to keep
// const.go untouched (Phase B-2 minimal-diff constraint). The opcodes use
// values outside the range registered in const.go's init() dedup set.
//
// TODO(B-2): Once Phase B-3/B-4 lands the full FSM apply path for syncnode
// delete/update, move these into master/const.go alongside opSyncAddLcNode
// and add them to the metadata_fsm.go delete-batch switch.
const (
	syncNodeAcronym = "sn"
	syncNodePrefix  = keySeparator + syncNodeAcronym + keySeparator

	opSyncAddSyncNode    uint32 = 0x80
	opSyncDeleteSyncNode uint32 = 0x81
)

// SyncNode is master's view of one syncnode service. Mirror of LcNode but
// with the runtime fields specific to syncnode (running tasks count, bolt
// health, etc.) populated from the heartbeat response.
type SyncNode struct {
	ID            uint64
	Addr          string
	HeartbeatPort string // unused for P0 but kept for parity with lcnode shape
	IsActive      bool
	ReportTime    time.Time
	TaskManager   *AdminTaskManager

	// Latest heartbeat snapshot. Updated under sync.RWMutex each heartbeat
	// round. P0 just exposes raw counts; load score arithmetic is on
	// master's read path (P1 / §6.3.1).
	Version        string
	UptimeSeconds  int64
	RunningTasks   int64
	QueuedTasks    int64
	ScheduledRules int
	BoltDBHealthy  bool
	BandwidthMBps  float64
	CPUPercent     float64
	MemPercent     float64
	ReloadFailures uint64

	sync.RWMutex
}

func newSyncNode(addr, clusterID string) *SyncNode {
	sn := new(SyncNode)
	sn.Addr = addr
	sn.IsActive = true
	sn.ReportTime = time.Now()
	sn.TaskManager = newAdminTaskManager(sn.Addr, clusterID)
	return sn
}

// clean is called on raft-driven deletion. Cancel any in-flight tasks
// directed at this node.
func (sn *SyncNode) clean() {
	sn.TaskManager.exitCh <- struct{}{}
}

// checkLiveness flips IsActive=false if no heartbeat arrived in
// defaultNodeTimeOutSec seconds. Same semantics as LcNode.checkLiveness.
func (sn *SyncNode) checkLiveness() {
	sn.Lock()
	defer sn.Unlock()
	log.LogInfof("action[checkLiveness] syncnode[%v, %v, %v] report time[%v], since report time[%v], need gap[%v]",
		sn.ID, sn.Addr, sn.IsActive, sn.ReportTime, time.Since(sn.ReportTime), time.Second*time.Duration(defaultNodeTimeOutSec))
	if time.Since(sn.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		sn.IsActive = false
		msg := fmt.Sprintf("syncnode[%v] report time[%v],since report time[%v], need gap [%v]",
			sn.Addr, sn.ReportTime, time.Since(sn.ReportTime), time.Second*time.Duration(defaultNodeTimeOutSec))
		log.LogWarnf("action[checkLiveness]  %v", msg)
		auditlog.LogMasterOp("SyncNodeNoLive", msg, nil)
	}
}

// createHeartbeatTask builds the AdminTask master pushes to syncnode each
// heartbeat tick. Payload is a *proto.SyncNodeHeartbeatRequest so syncnode
// learns the current master-leader on every tick (B-3 register loop relies
// on this for leader-switch follow).
func (sn *SyncNode) createHeartbeatTask(masterAddr string) *proto.AdminTask {
	req := &proto.SyncNodeHeartbeatRequest{
		Addr:       sn.Addr,
		LeaderAddr: masterAddr,
	}
	return proto.NewAdminTask(proto.OpSyncNodeHeartbeat, sn.Addr, req)
}

// syncNodeNotFound is the typed not-found error mirroring lcNodeNotFound.
func syncNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("sync node[%v]", addr))
}

// syncNodeValue is the raft-replicated identity record. Mirrors lcNodeValue.
// Only ID + Addr are persisted — runtime fields are rebuilt from heartbeats.
type syncNodeValue struct {
	ID   uint64
	Addr string
}

func newSyncNodeValue(sn *SyncNode) *syncNodeValue {
	return &syncNodeValue{
		ID:   sn.ID,
		Addr: sn.Addr,
	}
}
