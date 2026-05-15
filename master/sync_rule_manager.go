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
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/robfig/cron/v3"
)

// Phase 3 — master-side sync rule scheduler.
//
// The manager owns:
//   - a robfig/cron engine that fires on each rule's schedule
//   - a ruleID → cron.EntryID map so create/update/delete can rebuild
//     individual entries atomically
//   - fireRule, which is the cron callback: read the rule from
//     SyncRuleCache, build a RunTaskRequest payload (with the FULL
//     SyncRule snapshot embedded so syncnode doesn't need a local
//     store), and route through SyncDispatcher.Dispatch or
//     SyncFanout.DispatchN based on Parallelism + ShardingStrategy.
//
// Lifecycle mirrors lifecycleManager (master/lifecycle_manager.go:489):
// Start on raft leader gain, Stop on leader loss. The Start hook is
// added to master_manager.go's handleLeaderChange in Phase 6 — Phase 3
// only lands the engine itself so the cutover diff stays focused.

// SyncRuleManager schedules and dispatches sync rule executions across
// the syncnode fleet. One instance per Cluster; not safe to construct
// twice on the same Cluster (the cron engine would double-fire each rule).
type SyncRuleManager struct {
	cluster *Cluster

	mu      sync.Mutex              // guards cron + entries
	cron    *cron.Cron              // nil before Start, nil after Stop
	entries map[string]cron.EntryID // ruleID → cron entry id
	started bool                    // true between Start and Stop

	// prefixCache stores the auto-mode probe results per (ruleID +
	// source identity). Probes are asynchronous; cache hits avoid the
	// extra TCP round trip + backend list call on every fire.
	prefixCache *syncPrefixCache
}

// NewSyncRuleManager builds a manager bound to cluster. The cluster must
// have a non-nil syncRuleCache, syncDispatcher, and syncFanout — those
// are wired by Cluster construction (cluster.go:515-518) + loadMetadata
// (master_manager.go).
func NewSyncRuleManager(cluster *Cluster) *SyncRuleManager {
	return &SyncRuleManager{
		cluster:     cluster,
		entries:     make(map[string]cron.EntryID),
		prefixCache: newSyncPrefixCache(),
	}
}

// Start arms the cron engine and registers every Active rule from the
// cache. Idempotent: re-calling without an intervening Stop is a no-op.
// The cron parser uses "WithSeconds" so 6-field expressions match the
// scheduler doc.
func (m *SyncRuleManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return
	}
	m.cron = cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(cronLogger{})))
	rules := m.cluster.syncRuleCache.List()
	armed := 0
	for _, r := range rules {
		if r.State != proto.SyncRuleStateActive {
			continue
		}
		if err := m.addEntryLocked(r); err != nil {
			log.LogWarnf("SyncRuleManager.Start: register rule %q failed: %v", r.ID(), err)
			continue
		}
		armed++
	}
	m.cron.Start()
	m.started = true
	log.LogInfof("SyncRuleManager.Start: armed %d/%d rules", armed, len(rules))
}

// Stop halts cron firing and releases entries. The manager is reusable —
// a subsequent Start re-loads from the cache. Idempotent.
func (m *SyncRuleManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	ctx := m.cron.Stop() // waits for in-flight jobs to finish
	<-ctx.Done()
	m.entries = make(map[string]cron.EntryID)
	m.cron = nil
	m.started = false
	log.LogInfo("SyncRuleManager.Stop: cron halted, entries cleared")
}

// Register adds or replaces the cron entry for r. Called by admin
// handlers AFTER the raft submit succeeds AND the cache has been
// updated. Idempotent.
//
// Behaviour by state:
//   - Active   → arm new entry
//   - Paused / Degraded → remove any existing entry (no fires)
//   - empty schedule → remove any existing entry + log a warning
func (m *SyncRuleManager) Register(r *proto.SyncRule) error {
	if r == nil {
		return fmt.Errorf("SyncRuleManager.Register: nil rule")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started || m.cron == nil {
		// Manager hasn't been started yet (or has been stopped). The
		// next Start will load the rule from the cache, so we don't
		// need to track it here. Silently accept so admin handlers
		// don't need to special-case the not-leader state.
		return nil
	}
	// Remove existing entry first so Update / state-change rebuilds
	// atomically.
	if old, ok := m.entries[r.ID()]; ok {
		m.cron.Remove(old)
		delete(m.entries, r.ID())
	}
	if r.State != proto.SyncRuleStateActive {
		return nil
	}
	return m.addEntryLocked(r)
}

// Unregister removes the cron entry for ruleID. Idempotent. Called by
// the delete admin handler after the raft submit + cache eviction.
func (m *SyncRuleManager) Unregister(ruleID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started || m.cron == nil {
		return
	}
	if old, ok := m.entries[ruleID]; ok {
		m.cron.Remove(old)
		delete(m.entries, ruleID)
	}
}

// RegisteredCount returns the number of cron entries currently armed.
// Used by /syncRule/list?withScheduled=true and tests.
func (m *SyncRuleManager) RegisteredCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// addEntryLocked installs a cron entry for the rule. Caller must hold
// m.mu. The schedule is parsed with the WithSeconds parser (6 fields).
// Empty schedule is treated as "manual trigger only" and skipped.
func (m *SyncRuleManager) addEntryLocked(r *proto.SyncRule) error {
	sched := r.Config.Schedule
	if sched == "" {
		// Allowed — operator may want manual /syncRule/trigger only.
		log.LogInfof("SyncRuleManager.addEntry: rule %q has empty schedule, skipping cron arm", r.ID())
		return nil
	}
	id := r.ID()
	entryID, err := m.cron.AddFunc(sched, func() {
		m.fireRule(id)
	})
	if err != nil {
		return fmt.Errorf("cron parse %q for rule %q: %w", sched, id, err)
	}
	m.entries[id] = entryID
	return nil
}

// fireRule is the cron callback. Builds a fresh task payload from the
// rule snapshot in the cache and routes to either single-Dispatch or
// fan-out DispatchN. Recovers from panics (defensive — robfig/cron
// already wraps with cron.Recover but we want a clear log line tying the
// panic to a rule id).
func (m *SyncRuleManager) fireRule(ruleID string) {
	defer func() {
		if rec := recover(); rec != nil {
			log.LogErrorf("SyncRuleManager.fireRule rule=%q panic: %v", ruleID, rec)
		}
	}()
	rule := m.cluster.syncRuleCache.Get(ruleID)
	if rule == nil {
		log.LogWarnf("SyncRuleManager.fireRule: rule %q vanished from cache", ruleID)
		return
	}
	if rule.State != proto.SyncRuleStateActive {
		log.LogInfof("SyncRuleManager.fireRule: rule %q state=%q, skipping fire", ruleID, rule.State)
		return
	}
	taskID := fmt.Sprintf("%s/%d", ruleID, time.Now().UnixNano())
	if err := m.dispatchRule(taskID, rule); err != nil {
		log.LogErrorf("SyncRuleManager.fireRule rule=%q taskID=%q dispatch failed: %v", ruleID, taskID, err)
		return
	}
}

// dispatchRule routes a rule fire to the appropriate dispatch backend.
// Splits into:
//
//   - hash mode (default): if Parallelism > 1 and we have multiple
//     candidates, fan out via SyncFanout.DispatchN with hash-mode
//     sub-tasks. Otherwise single Dispatch to the lowest-load node.
//   - prefix mode (P2-5): bucketsForPrefix packs the operator-declared
//     ShardPrefixes into shards (≤ Parallelism); each shard gets its
//     bucket on SubTaskInfo.Prefixes. The receiving syncnode executor
//     filters via prefix match instead of hash.
//   - auto mode: probe a candidate's backend.List for top-level
//     prefixes, then route through prefix mode. (Phase 5b — wire op
//     OpSyncNodeListPrefixes lands separately.)
func (m *SyncRuleManager) dispatchRule(taskID string, rule *proto.SyncRule) error {
	disp := m.cluster.syncDispatcher
	if disp == nil {
		return fmt.Errorf("syncDispatcher not initialised")
	}
	payload := buildRunTaskRequest(taskID, rule, nil)
	strategy := rule.Config.ShardingStrategy
	parallelism := rule.Config.Parallelism

	switch strategy {
	case "prefix":
		if len(rule.Config.ShardPrefixes) == 0 {
			return fmt.Errorf("rule %q: shardingStrategy=prefix requires non-empty shardPrefixes", rule.ID())
		}
		return m.dispatchPrefix(taskID, rule, payload, rule.Config.ShardPrefixes, parallelism)
	case "auto":
		// Try cache first; on hit, dispatch via prefix mode using the
		// cached probe result. On miss, kick off an async probe and
		// fall back to hash for THIS fire (the cache will be warm by
		// the next fire).
		key := syncPrefixCacheKey(rule)
		if cached, ok := m.prefixCache.get(key); ok && len(cached) > 0 {
			log.LogInfof("SyncRuleManager.dispatchRule rule=%q auto cache HIT, %d prefix(es)", rule.ID(), len(cached))
			return m.dispatchPrefix(taskID, rule, payload, cached, parallelism)
		}
		log.LogInfof("SyncRuleManager.dispatchRule rule=%q auto cache MISS, kicking probe and falling back to hash", rule.ID())
		_, _ = m.probePrefixes(rule) // best-effort kick; ignore pending sentinel
		return m.dispatchHash(taskID, rule, payload, parallelism)
	case "", "hash":
		return m.dispatchHash(taskID, rule, payload, parallelism)
	default:
		return fmt.Errorf("rule %q unknown shardingStrategy %q", rule.ID(), strategy)
	}
}

// dispatchPrefix routes the prefix-mode fan-out path. shardTotal is
// min(len(prefixes), parallelism). Each shard owns a subset of the
// prefix list (bucketsForPrefix packs deterministically).
//
// Unlike hash-mode fan-out, prefix shards are NOT capped by candidate
// count: the goal is data partitioning (each shard filters a disjoint
// prefix subset), not load balancing. A single-node cluster runs all
// shards on the same node sequentially via its queue — distinct task
// records per prefix are still created and tracked independently.
func (m *SyncRuleManager) dispatchPrefix(taskID string, rule *proto.SyncRule, payload *SyncRunTaskRequest, prefixes []string, parallelism int) error {
	disp := m.cluster.syncDispatcher
	cands := disp.Candidates(dispatcherStaleness)
	if len(cands) == 0 {
		return fmt.Errorf("rule %q: %w", rule.ID(), ErrNoCandidates)
	}
	// Cap shard count by prefix list length and parallelism; NOT by
	// candidate count (multiple shards may share a single node).
	limit := parallelism
	if limit <= 0 || limit > len(prefixes) {
		limit = len(prefixes)
	}
	buckets := bucketsForPrefix(prefixes, limit)
	fo := m.cluster.syncFanout
	if fo == nil {
		return fmt.Errorf("syncFanout not initialised")
	}
	send := m.shardSendFn()
	owners, err := fo.DispatchNWithPrefixBuckets(taskID, rule.ID(), payload, buckets, jsonRoundTripFanoutCloner, send, 3)
	if err != nil {
		recordSyncDispatchFail(rule.ID(), "prefix_dispatch_err")
		return fmt.Errorf("DispatchNWithPrefixBuckets rule=%q: %w", rule.ID(), err)
	}
	m.cluster.recordTaskDispatch(taskID, rule, "", 0, len(buckets))
	for shard, addr := range owners {
		subID := fmt.Sprintf("%s/%d", taskID, shard)
		m.cluster.recordTaskDispatch(subID, rule, addr, shard, len(buckets))
		recordSyncShardDispatch(rule.ID())
	}
	recordSyncDispatchSuccess(rule.ID(), "prefix")
	log.LogInfof("SyncRuleManager.dispatchPrefix rule=%q taskID=%q buckets=%d", rule.ID(), taskID, len(buckets))
	return nil
}

// dispatchHash routes hash-mode dispatch — single Dispatch when no
// fan-out is requested, DispatchN when Parallelism > 1.
func (m *SyncRuleManager) dispatchHash(taskID string, rule *proto.SyncRule, payload *SyncRunTaskRequest, parallelism int) error {
	disp := m.cluster.syncDispatcher
	cands := disp.Candidates(dispatcherStaleness)
	if len(cands) == 0 {
		return fmt.Errorf("rule %q: %w", rule.ID(), ErrNoCandidates)
	}
	if parallelism <= 1 || len(cands) <= 1 {
		// Single dispatch path.
		sendFn := m.singleSendFn(payload)
		addr, err := disp.Dispatch(taskID, sendFn, 3)
		if err != nil {
			return fmt.Errorf("Dispatch rule=%q: %w", rule.ID(), err)
		}
		// Ledger: record the new task with owner so /syncTask/get +
		// /syncNode/tasks see it immediately. Terminal status arrives
		// via /syncNode/response and re-Puts the record.
		m.cluster.recordTaskDispatch(taskID, rule, addr, 0, 0)
		recordSyncDispatchSuccess(rule.ID(), "hash")
		log.LogInfof("SyncRuleManager.dispatchHash rule=%q taskID=%q owner=%s", rule.ID(), taskID, addr)
		return nil
	}
	// Fan-out path.
	shardTotal := parallelism
	if shardTotal > len(cands) {
		shardTotal = len(cands)
	}
	fo := m.cluster.syncFanout
	if fo == nil {
		return fmt.Errorf("syncFanout not initialised")
	}
	send := m.shardSendFn()
	owners, err := fo.DispatchN(taskID, rule.ID(), shardTotal, payload, jsonRoundTripFanoutCloner, send, 3)
	if err != nil {
		recordSyncDispatchFail(rule.ID(), "dispatch_n_err")
		return fmt.Errorf("DispatchN rule=%q: %w", rule.ID(), err)
	}
	// Ledger: record the parent (no owner) + one child per shard.
	m.cluster.recordTaskDispatch(taskID, rule, "", 0, shardTotal)
	for shard, addr := range owners {
		subID := fmt.Sprintf("%s/%d", taskID, shard)
		m.cluster.recordTaskDispatch(subID, rule, addr, shard, shardTotal)
		recordSyncShardDispatch(rule.ID())
	}
	recordSyncDispatchSuccess(rule.ID(), "hash")
	log.LogInfof("SyncRuleManager.dispatchHash rule=%q taskID=%q fanout shardTotal=%d", rule.ID(), taskID, shardTotal)
	return nil
}

// singleSendFn returns the send closure used by SyncDispatcher.Dispatch:
// look up the SyncNode by addr, wrap the payload in an AdminTask, hand
// to TaskManager. Mirrors the pattern from api_service.go:8168.
func (m *SyncRuleManager) singleSendFn(payload interface{}) func(addr string) error {
	return func(addr string) error {
		return sendRunTask(m.cluster, addr, payload)
	}
}

// shardSendFn returns the send closure used by SyncFanout.DispatchN. The
// payload comes from the PayloadCloner per-shard.
func (m *SyncRuleManager) shardSendFn() SendFunc {
	return func(addr string, _ int, payload interface{}) error {
		return sendRunTask(m.cluster, addr, payload)
	}
}

// sendRunTask is the shared TCP send path. Pulled out so the single +
// fan-out flows share the same lookup error messages and queueing path.
func sendRunTask(cluster *Cluster, addr string, payload interface{}) error {
	value, ok := cluster.syncNodes.Load(addr)
	if !ok {
		return fmt.Errorf("syncnode %s not registered", addr)
	}
	sn, ok := value.(*SyncNode)
	if !ok || sn == nil {
		return fmt.Errorf("syncnode %s entry invalid", addr)
	}
	// Derive a unique AdminTask ID from the embedded task ID so that
	// concurrent RunTask requests for different logical tasks are never
	// silently dropped by TaskManager's dedup map. Without a unique suffix
	// every RunTask gets the same fixed ID "addr[x]_op[121]" and a second
	// trigger is dropped while the first task's entry is still in the map.
	var reqID string
	switch p := payload.(type) {
	case *SyncRunTaskRequest:
		reqID = p.TaskID
	case map[string]interface{}:
		if v, ok := p["taskId"]; ok {
			reqID, _ = v.(string)
		}
	}
	var runTask *proto.AdminTask
	if reqID != "" {
		runTask = proto.NewAdminTaskEx(proto.OpSyncNodeRunTask, addr, payload, reqID)
	} else {
		runTask = proto.NewAdminTask(proto.OpSyncNodeRunTask, addr, payload)
	}
	sn.TaskManager.AddTask(runTask)
	return nil
}

// SyncRunTaskRequest is the master-side mirror of syncnode's
// task_handler.go::RunTaskRequest. We define a master-local copy so the
// master doesn't need to import the syncnode package (which would create
// a cycle once syncnode imports master schemas via proto).
//
// JSON shape MUST stay byte-identical to syncnode's RunTaskRequest — the
// two are tied by the wire format.
//
// Rule is the P2 addition: the master ships the full rule snapshot so
// the syncnode doesn't need a local rule store. The syncnode's
// task_handler.go is updated in Phase 6 to read this field instead of
// looking up by RuleID locally.
type SyncRunTaskRequest struct {
	TaskID   string                 `json:"taskId,omitempty"`
	RuleID   string                 `json:"ruleId"`
	Type     string                 `json:"type,omitempty"`
	Rule     *proto.SyncRule        `json:"rule,omitempty"`
	SubTask  *SyncRunSubTaskInfo    `json:"subTask,omitempty"`
	Override map[string]interface{} `json:"override,omitempty"`
}

// SyncRunSubTaskInfo mirrors syncnode/task_handler.go::RunSubTaskInfo.
// Prefixes is the Phase 5 extension (slice of literal prefix strings
// the shard owns when ShardingStrategy is "prefix" or "auto"); it's
// optional and defaults to nil (hash mode).
type SyncRunSubTaskInfo struct {
	ParentTaskID string   `json:"parentTaskId"`
	ShardIndex   int      `json:"shardIndex"`
	ShardTotal   int      `json:"shardTotal"`
	Prefixes     []string `json:"prefixes,omitempty"`
}

// buildRunTaskRequest constructs the payload pushed to syncnode for one
// rule fire. The rule snapshot is embedded so syncnode doesn't need a
// local rule lookup. sub is non-nil only for fan-out shards (set by the
// PayloadCloner round-trip, NOT by this function).
func buildRunTaskRequest(taskID string, rule *proto.SyncRule, sub *SyncRunSubTaskInfo) *SyncRunTaskRequest {
	return &SyncRunTaskRequest{
		TaskID:  taskID,
		RuleID:  rule.ID(),
		Type:    rule.Config.Type,
		Rule:    rule,
		SubTask: sub,
	}
}

// bucketsForPrefix packs a list of prefixes into at most `shardLimit`
// buckets using a deterministic byte-lex sort + greedy round-robin. The
// caller uses this in prefix-mode dispatch to produce N sub-tasks; each
// returned bucket becomes one shard's SubTaskInfo.Prefixes.
//
// Properties:
//   - input order is irrelevant — output is stable across calls
//   - len(result) == min(len(prefixes), shardLimit)
//   - balanced by count (size-aware packing is a future enhancement)
//   - shardLimit <= 0 collapses to a single bucket containing every prefix
func bucketsForPrefix(prefixes []string, shardLimit int) [][]string {
	if len(prefixes) == 0 {
		return nil
	}
	sorted := make([]string, len(prefixes))
	copy(sorted, prefixes)
	sort.Strings(sorted)

	n := shardLimit
	if n <= 0 || n > len(sorted) {
		n = len(sorted)
	}
	out := make([][]string, n)
	for i, p := range sorted {
		out[i%n] = append(out[i%n], p)
	}
	return out
}

// cronLogger adapts our log package onto robfig/cron's Logger interface.
// We only care about panics in user-registered jobs; cron's internal
// chatter goes to the same syslog channel.
type cronLogger struct{}

func (cronLogger) Info(msg string, _ ...interface{}) { log.LogInfof("cron: %s", msg) }
func (cronLogger) Error(err error, msg string, _ ...interface{}) {
	log.LogErrorf("cron: %s err=%v", msg, err)
}
