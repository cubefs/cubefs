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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Phase 4 — master HTTP admin surface for sync tasks.
//
// Backed by the LRU SyncTaskLedger (Cluster.syncTaskLedger). The
// ledger is fed by:
//   - SyncRuleManager.dispatchHash (cron-driven trigger)
//   - /syncNode/dispatch handler (legacy external trigger — TODO Phase 4
//     wire the same recordTaskDispatch hook)
//   - /syncNode/response terminal callback (recordTaskTerminal)
//
// All handlers follow the same auth + envelope conventions as the rule
// handlers (proto.HTTPReply, requireSyncAdminToken middleware).

// listSyncTasks handles GET /syncTask/list[?status=&ruleID=&owner=].
func (m *Server) listSyncTasks(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncTaskList))
	var err error
	defer func() { doStatAndMetric(proto.SyncTaskList, metric, err, nil) }()

	q := r.URL.Query()
	status := SyncTaskStatus(q.Get("status"))
	ruleID := q.Get("ruleID")
	owner := q.Get("owner")
	out := m.cluster.syncTaskLedger.List(status, ruleID, owner)
	sendOkReply(w, r, newSuccessHTTPReply(out))
}

// getSyncTask handles GET /syncTask/get?id=.
func (m *Server) getSyncTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncTaskGet))
	var err error
	defer func() { doStatAndMetric(proto.SyncTaskGet, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rec := m.cluster.syncTaskLedger.Get(id)
	if rec == nil {
		err = fmt.Errorf("sync task not found: %s", id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rec))
}

// cancelSyncTask handles POST /syncTask/cancel?id=. Reads the task's
// owner from the ledger + queues an OpSyncNodeCancelTask packet at that
// node. The actual status flip is async (executor reacts to ctx.Done).
// Caller polls /syncTask/get for the terminal status.
func (m *Server) cancelSyncTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncTaskCancel))
	var err error
	defer func() { doStatAndMetric(proto.SyncTaskCancel, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rec := m.cluster.syncTaskLedger.Get(id)
	if rec == nil {
		err = fmt.Errorf("sync task not found: %s", id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if rec.Status.IsTerminal() {
		// Idempotent: terminal task → no-op success.
		sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": id, "status": string(rec.Status)}))
		return
	}
	if rec.Owner == "" {
		// Parent of a fan-out: cancel every child. Look up children by
		// taskID prefix.
		children := m.cluster.syncTaskLedger.List("", "", "")
		cancelled := 0
		for _, c := range children {
			if c.TaskID == id || !strings.HasPrefix(c.TaskID, id+"/") {
				continue
			}
			if c.Status.IsTerminal() {
				continue
			}
			if cerr := sendSyncCancelTo(m.cluster, c.Owner, c.TaskID); cerr != nil {
				log.LogWarnf("cancelSyncTask: shard %q owner=%s: %v", c.TaskID, c.Owner, cerr)
				continue
			}
			cancelled++
		}
		sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
			"taskID":    id,
			"status":    "cancelling",
			"cancelled": cancelled,
		}))
		return
	}
	if cerr := sendSyncCancelTo(m.cluster, rec.Owner, id); cerr != nil {
		err = cerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: cerr.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": id, "status": "cancelling"}))
}

// retrySyncTask handles POST /syncTask/retry?id=. Re-dispatches the
// task's rule via SyncRuleManager, generating a fresh task ID. The
// original record is preserved.
func (m *Server) retrySyncTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncTaskRetry))
	var err error
	defer func() { doStatAndMetric(proto.SyncTaskRetry, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	prev := m.cluster.syncTaskLedger.Get(id)
	if prev == nil {
		err = fmt.Errorf("sync task not found: %s", id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	rule := m.cluster.syncRuleCache.Get(prev.RuleID)
	if rule == nil {
		err = fmt.Errorf("rule %q vanished from cache; cannot retry task %q", prev.RuleID, id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	newTaskID := fmt.Sprintf("%s/%d", rule.ID(), time.Now().UnixNano())
	if derr := m.cluster.syncRuleMgr.dispatchRule(newTaskID, rule); derr != nil {
		err = derr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: derr.Error()})
		return
	}
	out := m.cluster.syncTaskLedger.Get(newTaskID)
	sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
		"prevTaskID": id,
		"newTaskID":  newTaskID,
		"record":     out,
	}))
}

// exportSyncTasks streams the ledger as NDJSON. Each line is a JSON
// SyncTaskRecord. Optional ?since=<RFC3339> filters by StartedAt.
// Bypasses the standard envelope because the body is a stream of
// records, not a single payload.
func (m *Server) exportSyncTasks(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncTaskExport))
	var err error
	defer func() { doStatAndMetric(proto.SyncTaskExport, metric, err, nil) }()

	since := time.Time{}
	if v := r.URL.Query().Get("since"); v != "" {
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			err = fmt.Errorf("invalid since (want RFC3339): %v", perr)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		since = t
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="sync-task-history.jsonl"`)
	enc := json.NewEncoder(w)
	recs := m.cluster.syncTaskLedger.List("", "", "")
	for _, rec := range recs {
		if !since.IsZero() && rec.StartedAt.Before(since) {
			continue
		}
		if encErr := enc.Encode(rec); encErr != nil {
			// Stream already partially written; surface as a comment
			// line so a human reading the file sees the boundary.
			_, _ = w.Write([]byte("\n# error: " + encErr.Error() + "\n"))
			return
		}
	}
}

// sendSyncCancelTo pushes an OpSyncNodeCancelTask packet to the named
// addr. Mirrors the dispatch path used by cancelOrphanedTasksOnReconnect
// (master/sync_node_task.go:222). Returns an error when the addr isn't
// a registered syncnode.
func sendSyncCancelTo(c *Cluster, addr, taskID string) error {
	if c == nil || addr == "" || taskID == "" {
		return errors.New("sendSyncCancelTo: empty args")
	}
	snI, ok := c.syncNodes.Load(addr)
	if !ok {
		return fmt.Errorf("syncnode %s not registered", addr)
	}
	sn, ok := snI.(*SyncNode)
	if !ok || sn == nil || sn.TaskManager == nil {
		return fmt.Errorf("syncnode %s entry invalid", addr)
	}
	req := map[string]interface{}{"taskId": taskID}
	task := proto.NewAdminTaskEx(proto.OpSyncNodeCancelTask, addr, req, taskID)
	sn.TaskManager.AddTask(task)
	return nil
}
