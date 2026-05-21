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

// Bench rule / task HTTP admin surface.
//
// Routes are gated by the same requireSyncAdminToken middleware used by
// the sync rule / task handlers. All responses follow the proto.HTTPReply
// envelope (sendOkReply / sendErrReply).
//
// BenchRule CRUD (backed by BenchRuleStore — in-memory, no raft for P0):
//   GET  /benchRule/list     [?id=]            → list all or one
//   POST /benchRule/create                      → body: spec.BenchRule
//   GET  /benchRule/get      ?id=
//   POST /benchRule/update                      → body: spec.BenchRule
//   POST /benchRule/delete   ?id=
//   POST /benchRule/trigger  ?id=               → body (optional): {backendEndpoint:{...}}
//
// BenchTask observability (backed by BenchTaskLedger — bounded LRU):
//   GET  /benchTask/list     [?ruleID=&status=]
//   GET  /benchTask/get      ?id=
//   POST /benchTask/cancel   ?id=
//   POST /benchTask/retry    ?id=

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/exporter"
)

// ---- Bench rule handlers ----

// listBenchRules handles GET /benchRule/list[?id=].
// If the optional id query param is provided, returns the single matching
// rule; otherwise returns all rules.
func (m *Server) listBenchRules(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchRuleList))
	var err error
	defer func() { doStatAndMetric(proto.BenchRuleList, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id != "" {
		rule, gerr := m.cluster.benchRuleStore.Get(id)
		if gerr != nil {
			err = gerr
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: gerr.Error()})
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply(rule))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(m.cluster.benchRuleStore.List()))
}

// createBenchRule handles POST /benchRule/create.
// Body must be a JSON-encoded spec.BenchRule with a non-empty ID.
//
// Persistence: BenchRuleStore.Create is raft-backed (Phase 1, see
// docs/plan/master/bench-rule-persistence.md). Raft submit failures
// surface as HTTP 503 + ErrCodePersistenceByRaft so dashboards / CLI
// clients can retry; validation failures stay on 500.
func (m *Server) createBenchRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchRuleCreate))
	var err error
	defer func() { doStatAndMetric(proto.BenchRuleCreate, metric, err, nil) }()

	var rule spec.BenchRule
	if err = json.NewDecoder(r.Body).Decode(&rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "decode body: " + err.Error()})
		return
	}
	if rule.ID == "" {
		err = errors.New("bench rule id is required")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.benchRuleStore.Create(&rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: benchRuleErrCode(err), Msg: err.Error()})
		return
	}
	created, _ := m.cluster.benchRuleStore.Get(rule.ID)
	sendOkReply(w, r, newSuccessHTTPReply(created))
}

// getBenchRule handles GET /benchRule/get?id=.
func (m *Server) getBenchRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.BenchRuleGet, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule, gerr := m.cluster.benchRuleStore.Get(id)
	if gerr != nil {
		err = gerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: gerr.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// updateBenchRule handles POST /benchRule/update.
// Body must be a JSON-encoded spec.BenchRule with a non-empty ID matching
// an existing rule.
func (m *Server) updateBenchRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchRuleUpdate))
	var err error
	defer func() { doStatAndMetric(proto.BenchRuleUpdate, metric, err, nil) }()

	var rule spec.BenchRule
	if err = json.NewDecoder(r.Body).Decode(&rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "decode body: " + err.Error()})
		return
	}
	if rule.ID == "" {
		err = errors.New("bench rule id is required")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.benchRuleStore.Update(&rule); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: benchRuleErrCode(err), Msg: err.Error()})
		return
	}
	updated, _ := m.cluster.benchRuleStore.Get(rule.ID)
	sendOkReply(w, r, newSuccessHTTPReply(updated))
}

// deleteBenchRule handles POST /benchRule/delete?id=.
func (m *Server) deleteBenchRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.BenchRuleDelete, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.benchRuleStore.Delete(id); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: benchRuleErrCode(err), Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"id": id, "status": "deleted"}))
}

// benchRuleErrCode maps a BenchRuleStore.Create/Update/Delete error to
// the HTTP reply code. Validation errors stay as InternalError (current
// dashboard contract); anything else is treated as a raft submit failure
// and surfaces as ErrCodePersistenceByRaft so callers retry.
func benchRuleErrCode(err error) int32 {
	switch {
	case errors.Is(err, ErrBenchRuleExists), errors.Is(err, ErrBenchRuleNotFound):
		return proto.ErrCodeInternalError
	default:
		return proto.ErrCodePersistenceByRaft
	}
}

// triggerBenchRule handles POST /benchRule/trigger?id=.
// Creates a BenchTaskRecord with status "running" and dispatches the task
// to an active syncnode. On dispatch failure the record is marked failed
// but the taskID is still returned so the caller can observe the status.
//
// Optional JSON body:
//   { "backendEndpoint": { <spec.EndpointConfig> } }
//
// When present, backendEndpoint is injected into the rule's BackendEndpoint
// field before dispatch. This is required for S3/SDK storage types where the
// dashboard backend resolves credentials from MySQL and injects them here.
// The master itself has no access to the credential store.
//
// Fan-out: when rule.Parallelism > 1, one parent record + N shard records
// are created in the BenchTaskLedger. The trigger response returns the parent
// taskID and a shardTaskIDs list.
func (m *Server) triggerBenchRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchRuleTrigger))
	var err error
	defer func() { doStatAndMetric(proto.BenchRuleTrigger, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule, gerr := m.cluster.benchRuleStore.Get(id)
	if gerr != nil {
		err = gerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: gerr.Error()})
		return
	}

	// Decode optional body for backendEndpoint injection.
	if r.ContentLength != 0 {
		var body struct {
			BackendEndpoint *spec.EndpointConfig `json:"backendEndpoint"`
		}
		if derr := json.NewDecoder(r.Body).Decode(&body); derr == nil && body.BackendEndpoint != nil {
			rule.BackendEndpoint = body.BackendEndpoint
		}
	}

	parallelism := rule.Parallelism
	if parallelism <= 1 {
		// Single-node path.
		taskID := fmt.Sprintf("bench-%s-%d", id, time.Now().UnixNano())
		rec := &BenchTaskRecord{
			TaskID:    taskID,
			RuleID:    id,
			Status:    BenchTaskStatusRunning,
			CreatedAt: time.Now().UnixMilli(),
		}
		m.cluster.benchTaskLedger.Add(rec)
		if derr := m.cluster.dispatchBenchTask(taskID, rule); derr != nil {
			m.cluster.benchTaskLedger.Fail(taskID, derr.Error())
			err = derr
			sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
				"taskID":        taskID,
				"status":        string(BenchTaskStatusFailed),
				"dispatchError": derr.Error(),
			}))
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": taskID, "status": string(BenchTaskStatusRunning)}))
		return
	}

	// Multi-shard fan-out path.
	parentID := fmt.Sprintf("bench-%s-%d", id, time.Now().UnixNano())
	parent := &BenchTaskRecord{
		TaskID:     parentID,
		RuleID:     id,
		Status:     BenchTaskStatusRunning,
		ShardTotal: parallelism,
		CreatedAt:  time.Now().UnixMilli(),
	}
	shardIDs, derr := m.cluster.dispatchBenchShards(parentID, rule, parallelism)
	if derr != nil {
		// Could not dispatch any shards — mark parent failed immediately.
		parent.Status = BenchTaskStatusFailed
		parent.Error = derr.Error()
		m.cluster.benchTaskLedger.Add(parent)
		err = derr
		sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
			"taskID":        parentID,
			"status":        string(BenchTaskStatusFailed),
			"dispatchError": derr.Error(),
		}))
		return
	}
	m.cluster.benchTaskLedger.AddShards(parent, shardIDs)
	sendOkReply(w, r, newSuccessHTTPReply(map[string]interface{}{
		"taskID":       parentID,
		"shardTaskIDs": shardIDs,
		"status":       string(BenchTaskStatusRunning),
	}))
}

// ---- Bench task handlers ----

// listBenchTasks handles GET /benchTask/list[?ruleID=&status=].
func (m *Server) listBenchTasks(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchTaskList))
	var err error
	defer func() { doStatAndMetric(proto.BenchTaskList, metric, err, nil) }()

	q := r.URL.Query()
	ruleID := q.Get("ruleID")
	status := q.Get("status")
	sendOkReply(w, r, newSuccessHTTPReply(m.cluster.benchTaskLedger.List(ruleID, status)))
}

// getBenchTask handles GET /benchTask/get?id=.
func (m *Server) getBenchTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchTaskGet))
	var err error
	defer func() { doStatAndMetric(proto.BenchTaskGet, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rec := m.cluster.benchTaskLedger.Get(id)
	if rec == nil {
		err = fmt.Errorf("bench task not found: %s", id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rec))
}

// cancelBenchTask handles POST /benchTask/cancel?id=.
// Transitions a running task to cancelled in the ledger. Actual
// in-flight cancellation on the executor is P2.
func (m *Server) cancelBenchTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchTaskCancel))
	var err error
	defer func() { doStatAndMetric(proto.BenchTaskCancel, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rec := m.cluster.benchTaskLedger.Get(id)
	if rec == nil {
		err = fmt.Errorf("bench task not found: %s", id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if rec.Status.IsTerminal() {
		// Idempotent: already terminal — no-op success.
		sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": id, "status": string(rec.Status)}))
		return
	}
	m.cluster.benchTaskLedger.Cancel(id)
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": id, "status": string(BenchTaskStatusCancelled)}))
}

// retryBenchTask handles POST /benchTask/retry?id=.
// Looks up the original task's rule and creates a fresh BenchTaskRecord
// with status "running". The original record is preserved.
func (m *Server) retryBenchTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchTaskRetry))
	var err error
	defer func() { doStatAndMetric(proto.BenchTaskRetry, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	prev := m.cluster.benchTaskLedger.Get(id)
	if prev == nil {
		err = fmt.Errorf("bench task not found: %s", id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if _, gerr := m.cluster.benchRuleStore.Get(prev.RuleID); gerr != nil {
		err = fmt.Errorf("bench rule %q not found; cannot retry task %q", prev.RuleID, id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	newTaskID := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	rec := &BenchTaskRecord{
		TaskID:    newTaskID,
		RuleID:    prev.RuleID,
		Status:    BenchTaskStatusRunning,
		CreatedAt: time.Now().UnixMilli(),
	}
	m.cluster.benchTaskLedger.Add(rec)
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": newTaskID, "status": string(BenchTaskStatusRunning)}))
}

// deleteBenchTask handles POST /benchTask/delete?id=. Removes the task
// record (and its fan-out shard children "<id>/<N>") from the ledger.
// Idempotent — deleting a non-existent task is a no-op success. Does NOT
// stop an in-flight task on the executor; call /benchTask/cancel first if
// the task is still running.
func (m *Server) deleteBenchTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.BenchTaskDelete))
	var err error
	defer func() { doStatAndMetric(proto.BenchTaskDelete, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	m.cluster.benchTaskLedger.Remove(id)
	// Cascade fan-out shard records ("<parentID>/<N>") so they don't resurface
	// in /benchTask/list aggregation after the parent is gone.
	prefix := id + "/"
	for _, c := range m.cluster.benchTaskLedger.List("", "") {
		if len(c.TaskID) > len(prefix) && c.TaskID[:len(prefix)] == prefix {
			m.cluster.benchTaskLedger.Remove(c.TaskID)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"taskID": id, "status": "deleted"}))
}
