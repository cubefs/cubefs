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
	"sort"
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
//   - /syncNode/dispatch handler (manual trigger via recordManualDispatch)
//   - /syncNode/response terminal callback (recordTaskTerminal)
//
// All handlers follow the same auth + envelope conventions as the rule
// handlers (proto.HTTPReply, requireSyncAdminToken middleware).

// listSyncTasks handles GET /syncTask/list[?status=&ruleID=&owner=].
// Returns []AggregatedSyncTask — one row per logical task, fan-out shards
// collapsed. status and owner filters are applied post-aggregation so that
// fan-out parent records (Owner=="") are not accidentally excluded.
func (m *Server) listSyncTasks(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncTaskList))
	var err error
	defer func() { doStatAndMetric(proto.SyncTaskList, metric, err, nil) }()

	q := r.URL.Query()
	status := SyncTaskStatus(q.Get("status"))
	ruleID := q.Get("ruleID")
	owner := q.Get("owner")

	// Fetch by ruleID only; status + owner are post-aggregation filters.
	recs := m.cluster.syncTaskLedger.List("", ruleID, "")
	agg := aggregateTaskRecords(recs)

	if status != "" || owner != "" {
		filtered := make([]AggregatedSyncTask, 0, len(agg))
		for _, t := range agg {
			if status != "" && t.Status != status {
				continue
			}
			if owner != "" {
				found := false
				for _, s := range t.Shards {
					if s.Owner == owner {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			filtered = append(filtered, t)
		}
		agg = filtered
	}

	sendOkReply(w, r, newSuccessHTTPReply(agg))
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

// deleteSyncTask handles POST /syncTask/delete?id=. Removes the task record
// from the master ledger. Idempotent — deleting a non-existent task is a
// no-op success. Does NOT cancel an in-flight task; call /syncTask/cancel first
// if the task is still running. Fan-out child shard records (taskID prefix
// "<id>/") are also removed so they cannot resurface in /syncTask/list
// aggregation after the parent is deleted.
func (m *Server) deleteSyncTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncTaskDelete))
	var err error
	defer func() { doStatAndMetric(proto.SyncTaskDelete, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	m.cluster.syncTaskLedger.Remove(id)
	// Cascade: remove child shard records so they don't resurface in aggregation.
	for _, c := range m.cluster.syncTaskLedger.List("", "", "") {
		if stripShardSuffix(c.TaskID) == id && c.TaskID != id {
			m.cluster.syncTaskLedger.Remove(c.TaskID)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": id, "status": "deleted"}))
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

// TaskShardInfo carries per-shard identity, status, and progress inside an
// AggregatedSyncTask. Single (non-fan-out) tasks have exactly one entry.
type TaskShardInfo struct {
	TaskID   string           `json:"taskID"`
	ShardIdx int              `json:"shardIdx"`
	Owner    string           `json:"owner"`
	Status   SyncTaskStatus   `json:"status"`
	Progress SyncTaskProgress `json:"progress"`
	Error    string           `json:"error,omitempty"`
}

// AggregatedSyncTask is the response shape returned by /syncTask/list.
// All shards of a fan-out task are collapsed into one row; per-shard
// detail is exposed in Shards, and the cross-shard sum in TotalProgress.
// Single (non-fan-out) tasks have ShardTotal==0 and one Shards entry.
type AggregatedSyncTask struct {
	TaskID        string           `json:"taskID"`
	RuleID        string           `json:"ruleID"`
	Type          string           `json:"type"`
	Status        SyncTaskStatus   `json:"status"`
	ShardTotal    int              `json:"shardTotal"`
	StartedAt     time.Time        `json:"startedAt"`
	DoneAt        time.Time        `json:"doneAt,omitempty"`
	Error         string           `json:"error,omitempty"`
	Shards        []TaskShardInfo  `json:"shards"`
	TotalProgress SyncTaskProgress `json:"totalProgress"`
}

// aggregateTaskRecords groups raw ledger records into per-logical-task rows.
// Fan-out shards (ShardTotal>0 && Owner!="") are collapsed under their parent.
// Single tasks (ShardTotal==0) each produce a one-Shard row. Results are
// sorted by StartedAt descending (most recent first).
func aggregateTaskRecords(recs []*SyncTaskRecord) []AggregatedSyncTask {
	type entry struct {
		parent *SyncTaskRecord
		shards []*SyncTaskRecord
	}
	byID := make(map[string]*entry, len(recs))

	for _, rec := range recs {
		if rec.ShardTotal == 0 {
			// Single task — its own parent; Shards built from the record itself.
			e, ok := byID[rec.TaskID]
			if !ok {
				e = &entry{}
				byID[rec.TaskID] = e
			}
			e.parent = rec
		} else if rec.Owner == "" {
			// Fan-out parent marker record.
			e, ok := byID[rec.TaskID]
			if !ok {
				e = &entry{}
				byID[rec.TaskID] = e
			}
			e.parent = rec
		} else {
			// Fan-out shard — group under parent task ID.
			pid := stripShardSuffix(rec.TaskID)
			e, ok := byID[pid]
			if !ok {
				e = &entry{}
				byID[pid] = e
			}
			e.shards = append(e.shards, rec)
		}
	}

	out := make([]AggregatedSyncTask, 0, len(byID))
	for pid, e := range byID {
		agg := AggregatedSyncTask{TaskID: pid}

		// Populate metadata from parent; synthesize from first shard if evicted.
		if e.parent != nil {
			agg.RuleID = e.parent.RuleID
			agg.Type = e.parent.Type
			agg.ShardTotal = e.parent.ShardTotal
			agg.StartedAt = e.parent.StartedAt
		} else if len(e.shards) > 0 {
			s0 := e.shards[0]
			agg.RuleID = s0.RuleID
			agg.Type = s0.Type
			agg.ShardTotal = s0.ShardTotal
			agg.StartedAt = s0.StartedAt
			for _, s := range e.shards[1:] {
				if s.StartedAt.Before(agg.StartedAt) {
					agg.StartedAt = s.StartedAt
				}
			}
		}

		if e.parent != nil && e.parent.ShardTotal == 0 {
			// Single task: wrap the record as a one-entry Shards list.
			agg.Shards = []TaskShardInfo{{
				TaskID:   e.parent.TaskID,
				ShardIdx: 0,
				Owner:    e.parent.Owner,
				Status:   e.parent.Status,
				Progress: e.parent.Progress,
				Error:    e.parent.Error,
			}}
			agg.Status = e.parent.Status
			agg.TotalProgress = e.parent.Progress
			agg.Error = e.parent.Error
			agg.DoneAt = e.parent.DoneAt
		} else {
			// Fan-out: aggregate shards.
			shards := make([]TaskShardInfo, 0, len(e.shards))
			for _, s := range e.shards {
				shards = append(shards, TaskShardInfo{
					TaskID:   s.TaskID,
					ShardIdx: s.ShardIdx,
					Owner:    s.Owner,
					Status:   s.Status,
					Progress: s.Progress,
					Error:    s.Error,
				})
				addProgress(&agg.TotalProgress, s.Progress)
			}
			sort.Slice(shards, func(i, j int) bool {
				return shards[i].ShardIdx < shards[j].ShardIdx
			})
			agg.Shards = shards
			agg.Status = aggregateShardStatus(e.shards)
			if agg.Status.IsTerminal() {
				for _, s := range e.shards {
					if s.DoneAt.After(agg.DoneAt) {
						agg.DoneAt = s.DoneAt
					}
				}
			}
		}

		out = append(out, agg)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.After(out[j].StartedAt)
	})
	return out
}

// stripShardSuffix removes the trailing "/<decimal-digits>" fan-out suffix
// from a child task ID, returning the parent task ID. Returns the input
// unchanged when no such suffix is present.
func stripShardSuffix(taskID string) string {
	idx := strings.LastIndex(taskID, "/")
	if idx < 0 {
		return taskID
	}
	suffix := taskID[idx+1:]
	if suffix == "" {
		return taskID
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return taskID
		}
	}
	return taskID[:idx]
}

// aggregateShardStatus returns the combined status from a fan-out task's
// shard records. Priority: running/queued > failed > cancelled > succeeded.
func aggregateShardStatus(shards []*SyncTaskRecord) SyncTaskStatus {
	if len(shards) == 0 {
		return SyncTaskStatusRunning
	}
	var hasRunning, hasFailed, hasCancelled bool
	allSucceeded := true
	for _, s := range shards {
		switch s.Status {
		case SyncTaskStatusRunning, SyncTaskStatusQueued:
			hasRunning = true
			allSucceeded = false
		case SyncTaskStatusFailed:
			hasFailed = true
			allSucceeded = false
		case SyncTaskStatusCancelled:
			hasCancelled = true
			allSucceeded = false
		}
	}
	switch {
	case hasRunning:
		return SyncTaskStatusRunning
	case hasFailed:
		return SyncTaskStatusFailed
	case hasCancelled:
		return SyncTaskStatusCancelled
	case allSucceeded:
		return SyncTaskStatusSucceeded
	default:
		return SyncTaskStatusRunning
	}
}

// addProgress accumulates the numeric fields of src into acc in place.
func addProgress(acc *SyncTaskProgress, src SyncTaskProgress) {
	acc.FilesTotal += src.FilesTotal
	acc.FilesDone += src.FilesDone
	acc.FilesSkipped += src.FilesSkipped
	acc.FilesFailed += src.FilesFailed
	acc.BytesTotal += src.BytesTotal
	acc.BytesDone += src.BytesDone
	acc.BytesSkipped += src.BytesSkipped
	acc.ThroughputMBps += src.ThroughputMBps
	acc.CurrentBandwidthMBps += src.CurrentBandwidthMBps
}
