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
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// Phase 4 — master HTTP admin surface for sync rules.
//
// All handlers share these conventions (matching the existing master API):
//   - Method: handler-specific (POST for mutations, GET for reads)
//   - Auth: gated by requireSyncAdminToken middleware (registered in
//     http_server.go alongside AddSyncNode)
//   - Response: standard proto.HTTPReply envelope; success returns
//     `data=*proto.SyncRule` (single) or `[]*proto.SyncRule` (list)
//
// Error codes:
//   - 400 / ErrCodeParamError — missing or malformed body / query param
//   - 404 / inline message — rule not found (matches lcConf convention)
//   - 409 / inline message — conflict (duplicate id / overlap / cycle)
//   - 503 / ErrCodePersistenceByRaft — raft submit failure (retry-able)
//   - 500 / ErrCodeInternalError — anything else

// syncRuleBodyCap caps the JSON body size for create/update so a hostile
// caller can't OOM master. 1 MiB is generous for the SyncRule schema.
const syncRuleBodyCap = 1 << 20

// createSyncRule handles POST /syncRule/create. Body is a proto.SyncRule
// (Config + State + timestamps are accepted but server-controlled fields
// — CreatedAt/UpdatedAt/State — are reset by the server to "now" /
// active so the caller can't impersonate an existing record).
func (m *Server) createSyncRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncRuleCreate))
	var err error
	defer func() { doStatAndMetric(proto.SyncRuleCreate, metric, err, nil) }()

	rule, err := decodeSyncRuleBody(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if cerr := validateSyncRuleShape(rule); cerr != nil {
		err = cerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: cerr.Error()})
		return
	}
	// Server-controlled fields: a fresh rule is always Active with
	// timestamps stamped now. Operators that want Paused-on-create can
	// follow up with /syncRule/pause.
	now := time.Now()
	rule.State = proto.SyncRuleStateActive
	rule.CreatedAt = now
	rule.UpdatedAt = now
	rule.LastRunAt = time.Time{}
	rule.LastRunStatus = ""
	rule.LastRunError = ""

	cache := m.cluster.syncRuleCache
	if existing := cache.Get(rule.ID()); existing != nil {
		err = fmt.Errorf("rule %q already exists", rule.ID())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if cerr := m.runSyncRuleConflictCheck(rule, false); cerr != nil {
		err = cerr
		sendErrReply(w, r, syncConflictReply(cerr))
		return
	}
	if rerr := m.cluster.syncAddSyncRule(rule); rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: rerr.Error()})
		return
	}
	cache.Put(rule)
	if mgrErr := m.cluster.syncRuleMgr.Register(rule); mgrErr != nil {
		log.LogWarnf("createSyncRule: manager Register %q failed: %v", rule.ID(), mgrErr)
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// updateSyncRule handles POST /syncRule/update. Body must reference an
// existing rule by ID; UpdatedAt is refreshed server-side.
func (m *Server) updateSyncRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncRuleUpdate))
	var err error
	defer func() { doStatAndMetric(proto.SyncRuleUpdate, metric, err, nil) }()

	candidate, err := decodeSyncRuleBody(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if cerr := validateSyncRuleShape(candidate); cerr != nil {
		err = cerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: cerr.Error()})
		return
	}
	cache := m.cluster.syncRuleCache
	existing := cache.Get(candidate.ID())
	if existing == nil {
		err = syncRuleNotFound(candidate.ID())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	// Preserve server-owned fields; merge caller's Config over the
	// existing record so timestamps + state + last-run are not lost.
	updated := *existing
	updated.Config = candidate.Config
	updated.UpdatedAt = time.Now()

	if cerr := m.runSyncRuleConflictCheck(&updated, true); cerr != nil {
		err = cerr
		sendErrReply(w, r, syncConflictReply(cerr))
		return
	}
	if rerr := m.cluster.syncUpdateSyncRule(&updated); rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: rerr.Error()})
		return
	}
	cache.Put(&updated)
	if mgrErr := m.cluster.syncRuleMgr.Register(&updated); mgrErr != nil {
		log.LogWarnf("updateSyncRule: manager Register %q failed: %v", updated.ID(), mgrErr)
	}
	sendOkReply(w, r, newSuccessHTTPReply(&updated))
}

// deleteSyncRule handles POST /syncRule/delete?id=<ruleID>.
func (m *Server) deleteSyncRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncRuleDelete))
	var err error
	defer func() { doStatAndMetric(proto.SyncRuleDelete, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	cache := m.cluster.syncRuleCache
	existing := cache.Get(id)
	if existing == nil {
		err = syncRuleNotFound(id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if rerr := m.cluster.syncDeleteSyncRule(existing); rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: rerr.Error()})
		return
	}
	cache.Delete(id)
	m.cluster.syncRuleMgr.Unregister(id)
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{"id": id}))
}

// pauseSyncRule transitions the rule to State=Paused. POST ?id=.
func (m *Server) pauseSyncRule(w http.ResponseWriter, r *http.Request) {
	m.transitionSyncRule(w, r, proto.SyncRuleStatePaused, proto.SyncRulePause)
}

// resumeSyncRule transitions the rule to State=Active. POST ?id=.
func (m *Server) resumeSyncRule(w http.ResponseWriter, r *http.Request) {
	m.transitionSyncRule(w, r, proto.SyncRuleStateActive, proto.SyncRuleResume)
}

// transitionSyncRule is the shared path for pause/resume — both flip
// State, raft-commit, update the cache, and re-register / unregister
// the cron entry to match the new state.
func (m *Server) transitionSyncRule(w http.ResponseWriter, r *http.Request, next proto.SyncRuleState, route string) {
	metric := exporter.NewTPCnt(apiToMetricsName(route))
	var err error
	defer func() { doStatAndMetric(route, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	cache := m.cluster.syncRuleCache
	existing := cache.Get(id)
	if existing == nil {
		err = syncRuleNotFound(id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if existing.State == next {
		// Idempotent: same state → no-op success.
		sendOkReply(w, r, newSuccessHTTPReply(existing))
		return
	}
	updated := *existing
	updated.State = next
	updated.UpdatedAt = time.Now()
	if rerr := m.cluster.syncUpdateSyncRule(&updated); rerr != nil {
		err = rerr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: rerr.Error()})
		return
	}
	cache.Put(&updated)
	if mgrErr := m.cluster.syncRuleMgr.Register(&updated); mgrErr != nil {
		log.LogWarnf("transitionSyncRule rule=%q next=%q manager Register failed: %v", id, next, mgrErr)
	}
	sendOkReply(w, r, newSuccessHTTPReply(&updated))
}

// listSyncRules handles GET /syncRule/list[?state=]. Empty state filter
// returns every cached rule. Output is unordered (operators sort
// client-side as needed).
func (m *Server) listSyncRules(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncRuleList))
	var err error
	defer func() { doStatAndMetric(proto.SyncRuleList, metric, err, nil) }()

	stateFilter := proto.SyncRuleState(r.URL.Query().Get("state"))
	rules := m.cluster.syncRuleCache.List()
	if stateFilter != "" {
		filtered := make([]*proto.SyncRule, 0, len(rules))
		for _, rule := range rules {
			if rule.State == stateFilter {
				filtered = append(filtered, rule)
			}
		}
		rules = filtered
	}
	sendOkReply(w, r, newSuccessHTTPReply(rules))
}

// getSyncRule handles GET /syncRule/get?id=. Returns the cached rule
// or 404.
func (m *Server) getSyncRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncRuleGet))
	var err error
	defer func() { doStatAndMetric(proto.SyncRuleGet, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := m.cluster.syncRuleCache.Get(id)
	if rule == nil {
		err = syncRuleNotFound(id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(rule))
}

// triggerSyncRule handles POST /syncRule/trigger?id=. Synchronously
// fires the rule's dispatch path (same code path as the cron callback)
// and returns the new taskID + owner mapping. Useful for ops scripts +
// integration tests that don't want to wait for the cron tick.
//
// Idempotency: each call produces a fresh taskID; the underlying
// SyncFanout / Dispatcher de-duplicate against the dispatcher's ledger
// so concurrent triggers don't collide.
func (m *Server) triggerSyncRule(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SyncRuleTrigger))
	var err error
	defer func() { doStatAndMetric(proto.SyncRuleTrigger, metric, err, nil) }()

	id := r.URL.Query().Get("id")
	if id == "" {
		err = errors.New("missing id query param")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rule := m.cluster.syncRuleCache.Get(id)
	if rule == nil {
		err = syncRuleNotFound(id)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if rule.State != proto.SyncRuleStateActive {
		err = fmt.Errorf("rule %q state=%q, refusing trigger", id, rule.State)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	taskID := fmt.Sprintf("%s/%d", id, time.Now().UnixNano())
	if derr := m.cluster.syncRuleMgr.dispatchRule(taskID, rule); derr != nil {
		err = derr
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: derr.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(map[string]string{
		"ruleID": id,
		"taskID": taskID,
	}))
}

// decodeSyncRuleBody reads a SyncRuleConfig out of the request body
// and wraps it into a fresh SyncRule with server-controlled fields
// (state / timestamps / lastRun) zeroed. Cap + DisallowUnknownFields
// keep the wire schema tight; the caller (create / update handlers)
// then merges with any existing record before submit.
//
// The wire shape is the FLAT SyncRuleConfig (id, type, src, dst, ...)
// — NOT the SyncRule wrapper. Operators + console don't need to know
// about Config / State / timestamps; server owns those.
func decodeSyncRuleBody(r *http.Request) (*proto.SyncRule, error) {
	if r.Body == nil {
		return nil, errors.New("empty request body")
	}
	r.Body = http.MaxBytesReader(nil, r.Body, syncRuleBodyCap)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	cfg := &proto.SyncRuleConfig{}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	return &proto.SyncRule{Config: *cfg}, nil
}

// validateSyncRuleShape performs cheap field-level checks before the
// conflict pass + raft submit. Tests assert specific messages so any
// error-string drift here is intentional.
func validateSyncRuleShape(rule *proto.SyncRule) error {
	if rule == nil {
		return errors.New("nil rule")
	}
	if rule.Config.ID == "" {
		return errors.New("missing required field: id")
	}
	if rule.Config.Type == "" {
		return errors.New("missing required field: type")
	}
	if strings.ContainsRune(rule.Config.ID, '/') {
		return errors.New("invalid id: must not contain '/'")
	}
	switch rule.Config.ShardingStrategy {
	case "", "hash", "prefix", "auto":
		// ok
	default:
		return fmt.Errorf("invalid shardingStrategy: %q", rule.Config.ShardingStrategy)
	}
	if rule.Config.ShardingStrategy == "prefix" && len(rule.Config.ShardPrefixes) == 0 {
		return errors.New("shardingStrategy=prefix requires non-empty shardPrefixes")
	}
	return nil
}

// runSyncRuleConflictCheck validates the candidate rule against the
// current cache contents, excluding the candidate's own ID so an
// in-place Update doesn't double-fire as a duplicate. isUpdate is
// retained for symmetry with the syncnode-side flag even though the
// current implementation treats both paths the same.
func (m *Server) runSyncRuleConflictCheck(candidate *proto.SyncRule, isUpdate bool) error {
	_ = isUpdate
	existing := m.cluster.syncRuleCache.List()
	set := make([]*proto.SyncRule, 0, len(existing)+1)
	for _, r := range existing {
		if r.ID() == candidate.ID() {
			continue
		}
		set = append(set, r)
	}
	set = append(set, candidate)
	return ValidateSyncRules(set)
}

// syncConflictReply maps a SyncRuleConflictError to the HTTP envelope.
// Falls through to a generic 500 for non-conflict errors.
func syncConflictReply(err error) *proto.HTTPReply {
	var ce *SyncRuleConflictError
	if errors.As(err, &ce) {
		return &proto.HTTPReply{Code: int32(ce.Code), Msg: ce.Error()}
	}
	return &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()}
}

// syncRuleNotFound returns the canonical "rule X not found" error so
// handlers + tests use the same message.
func syncRuleNotFound(id string) error {
	return fmt.Errorf("sync rule not found: %s", id)
}
