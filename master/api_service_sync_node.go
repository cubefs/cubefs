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
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Phase 4 — master HTTP admin surface for syncnode lifecycle.
//
// State transitions:
//
//   Active ──/decommission?force=false──→ Draining ──(all tasks done)──→ Removed
//   Active ──/decommission?force=true ──→ Draining ──(failover redispatch)→ Removed
//   Active ──/drain                   ──→ Draining ──(/restore)─────────→ Active
//
// The SyncNode.State field is raft-replicated via opSyncUpdateSyncNode
// (master/sync_node.go) so leader switches don't lose the operator
// action.

// decommissionSyncNode handles POST /syncNode/decommission?addr=&force=.
// force=false: mark draining + return. The cluster cron will eventually
// remove the node once its active task count drops to zero (operator
// monitors via /syncNode/tasks).
// force=true: mark draining + redispatch every active task on the node
// via SyncFailover, then raft-delete the node record.
func (m *Server) decommissionSyncNode(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncNodeDecommission))
	var err error
	defer func() { doStatAndMetric(proto.SyncNodeDecommission, metric, err, nil) }()

	addr := r.URL.Query().Get("addr")
	if addr == "" {
		err = errors.New("missing addr query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	force := false
	if v := r.URL.Query().Get("force"); v != "" {
		if b, perr := strconv.ParseBool(v); perr == nil {
			force = b
		}
	}
	sn, lookupErr := m.lookupSyncNode(addr)
	if lookupErr != nil {
		err = lookupErr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: lookupErr.Error()})
		return
	}
	// 1. Flip to draining + raft-persist.
	if setErr := m.markSyncNodeDraining(sn); setErr != nil {
		err = setErr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: setErr.Error()})
		return
	}
	resp := map[string]interface{}{
		"addr":    addr,
		"state":   SyncNodeStateDraining,
		"force":   force,
		"drained": 0,
	}
	// 2. If force, cancel + failover every active task on this addr.
	if force {
		drained := m.drainSyncNodeTasks(addr)
		resp["drained"] = drained

		// 3. Tear the node record down via raft delete.
		if delErr := m.cluster.syncDeleteSyncNode(sn); delErr != nil {
			err = delErr
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: delErr.Error()})
			return
		}
		m.cluster.syncNodes.Delete(addr)
		resp["removed"] = true
	}
	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

// drainSyncNode handles POST /syncNode/drain?addr=. Same as
// /decommission?force=true minus the raft delete — node stays
// registered, just empty. Operator follows up with /restore when ready.
func (m *Server) drainSyncNode(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncNodeDrain))
	var err error
	defer func() { doStatAndMetric(proto.SyncNodeDrain, metric, err, nil) }()

	addr := r.URL.Query().Get("addr")
	if addr == "" {
		err = errors.New("missing addr query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sn, lookupErr := m.lookupSyncNode(addr)
	if lookupErr != nil {
		err = lookupErr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: lookupErr.Error()})
		return
	}
	if setErr := m.markSyncNodeDraining(sn); setErr != nil {
		err = setErr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: setErr.Error()})
		return
	}
	drained := m.drainSyncNodeTasks(addr)
	sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
		"addr":    addr,
		"state":   SyncNodeStateDraining,
		"drained": drained,
	}))
}

// restoreSyncNode handles POST /syncNode/restore?addr=. Flips draining
// → active. No effect on running tasks (the node has been receiving
// them while draining; cancellations were dispatched per /drain).
func (m *Server) restoreSyncNode(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncNodeRestore))
	var err error
	defer func() { doStatAndMetric(proto.SyncNodeRestore, metric, err, nil) }()

	addr := r.URL.Query().Get("addr")
	if addr == "" {
		err = errors.New("missing addr query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sn, lookupErr := m.lookupSyncNode(addr)
	if lookupErr != nil {
		err = lookupErr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: lookupErr.Error()})
		return
	}
	sn.Lock()
	if sn.State == SyncNodeStateActive {
		sn.Unlock()
		sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
			"addr":  addr,
			"state": SyncNodeStateActive,
		}))
		return
	}
	sn.State = SyncNodeStateActive
	sn.Unlock()
	if rerr := m.cluster.syncUpdateSyncNode(sn); rerr != nil {
		// Roll back in-memory state so the API + raft view agree.
		sn.Lock()
		sn.State = SyncNodeStateDraining
		sn.Unlock()
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: rerr.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
		"addr":  addr,
		"state": SyncNodeStateActive,
	}))
}

// listSyncNodeTasks handles GET /syncNode/tasks?addr=[&status=].
func (m *Server) listSyncNodeTasks(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncNodeTasks))
	var err error
	defer func() { doStatAndMetric(proto.SyncNodeTasks, metric, err, nil) }()

	addr := r.URL.Query().Get("addr")
	if addr == "" {
		err = errors.New("missing addr query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	status := SyncTaskStatus(r.URL.Query().Get("status"))
	out := m.cluster.syncTaskLedger.ListByOwner(addr, status)
	sendOkReply(w, r, newSuccessHTTPReply(out))
}

// lookupSyncNode resolves the SyncNode by addr and returns
// syncNodeNotFound when absent.
func (m *Server) lookupSyncNode(addr string) (*SyncNode, error) {
	value, ok := m.cluster.syncNodes.Load(addr)
	if !ok {
		return nil, syncNodeNotFound(addr)
	}
	sn, ok := value.(*SyncNode)
	if !ok || sn == nil {
		return nil, fmt.Errorf("syncnode %s entry invalid", addr)
	}
	return sn, nil
}

// markSyncNodeDraining flips the SyncNode's State to Draining (no-op if
// already), raft-persists the change, and rolls back on raft submit
// failure so master memory matches the persisted view.
func (m *Server) markSyncNodeDraining(sn *SyncNode) error {
	sn.Lock()
	if sn.State == SyncNodeStateDraining {
		sn.Unlock()
		return nil
	}
	prev := sn.State
	sn.State = SyncNodeStateDraining
	sn.Unlock()
	if err := m.cluster.syncUpdateSyncNode(sn); err != nil {
		sn.Lock()
		sn.State = prev
		sn.Unlock()
		return err
	}
	return nil
}

// drainSyncNodeTasks cancels every active task currently assigned to
// addr and asks SyncFailover to redispatch them to remaining candidate
// nodes. Returns the number of tasks dispatched cancellation packets
// for (subset is reassigned by failover separately).
func (m *Server) drainSyncNodeTasks(addr string) int {
	if m.cluster == nil || m.cluster.syncTaskLedger == nil {
		return 0
	}
	active := m.cluster.syncTaskLedger.ActiveTaskIDsOnOwner(addr)
	cancelled := 0
	for _, taskID := range active {
		if cerr := sendSyncCancelTo(m.cluster, addr, taskID); cerr != nil {
			log.LogWarnf("drainSyncNodeTasks: cancel %q on %s failed: %v", taskID, addr, cerr)
		} else {
			cancelled++
		}
		// SyncFailover's redispatch is private; the dispatcher's
		// internal failoverHook (wired at cluster init) drives it on
		// next heartbeat-driven sweep when this addr drops from
		// Candidates(). We rely on that path here rather than reaching
		// into the unexported redispatch func.
	}
	if cancelled > 0 {
		log.LogInfof("drainSyncNodeTasks: cancelled %d task(s) on %s", cancelled, addr)
	}
	return cancelled
}
