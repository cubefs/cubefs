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
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// Phase 2 — master raft persistence for sync rules.
//
// Mirrors the lifecycle config pattern at metadata_fsm_op.go:2180:
//   submit(opCode, key, json) → fsm apply → BatchDeleteAndPut → rocksdb
//
// In-memory cache is rebuilt at startup / leader change by loadSyncRules
// reading the syncRulePrefix key range from rocksdb. The cache is also
// updated by admin handlers immediately after successful raft submit so
// the just-written rule is visible to subsequent reads on the same node.
//
// Op codes 0x83-0x85 sit in the syncnode block (0x80-0x82 used by
// syncNode add/delete/update — see master/sync_node.go).

const (
	syncRuleAcronym = "sr"
	syncRulePrefix  = keySeparator + syncRuleAcronym + keySeparator

	opSyncAddSyncRule    uint32 = 0x83
	opSyncDeleteSyncRule uint32 = 0x84
	opSyncUpdateSyncRule uint32 = 0x85
)

// SyncRuleCache holds the master's in-memory view of every persisted sync
// rule. Reads are lock-free; writes go through the raft path first then
// update this cache. A nil cache pointer means "leader role not yet
// assumed" — callers must check and gracefully degrade.
type SyncRuleCache struct {
	mu    sync.RWMutex
	rules map[string]*proto.SyncRule // ID → rule
}

// NewSyncRuleCache returns an empty cache. The Cluster owns one; the
// instance is replaced (not cleared) on leader change so concurrent
// readers see a consistent snapshot until they swap pointers.
func NewSyncRuleCache() *SyncRuleCache {
	return &SyncRuleCache{rules: make(map[string]*proto.SyncRule)}
}

// Get returns the rule by ID, or nil if absent.
func (c *SyncRuleCache) Get(id string) *proto.SyncRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rules[id]
}

// List returns a snapshot slice of every rule. Caller can mutate the
// slice freely; the underlying rules are returned by reference and MUST
// be treated as read-only.
func (c *SyncRuleCache) List() []*proto.SyncRule {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*proto.SyncRule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

// Put inserts or replaces a rule. Called by FSM apply (follower path) and
// by admin handlers immediately after a successful raft submit (leader
// path).
func (c *SyncRuleCache) Put(r *proto.SyncRule) {
	if c == nil || r == nil {
		return
	}
	c.mu.Lock()
	c.rules[r.ID()] = r
	c.mu.Unlock()
}

// Delete removes a rule by ID. No-op if absent.
func (c *SyncRuleCache) Delete(id string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.rules, id)
	c.mu.Unlock()
}

// Len returns the number of rules currently cached.
func (c *SyncRuleCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.rules)
}

// syncAddSyncRule raft-replicates a Create op for the supplied rule.
// Returns nil on commit success; the caller is responsible for updating
// the in-memory cache after success.
func (c *Cluster) syncAddSyncRule(r *proto.SyncRule) error {
	return c.syncPutSyncRuleInfo(opSyncAddSyncRule, r)
}

// syncDeleteSyncRule raft-replicates a Delete op for the rule with the
// supplied ID. Body carries the rule for audit symmetry with lcConf.
func (c *Cluster) syncDeleteSyncRule(r *proto.SyncRule) error {
	return c.syncPutSyncRuleInfo(opSyncDeleteSyncRule, r)
}

// syncUpdateSyncRule raft-replicates an Update op for the supplied rule.
// State / lastRun changes also flow through this path so the historical
// summary is durable across leader changes.
func (c *Cluster) syncUpdateSyncRule(r *proto.SyncRule) error {
	return c.syncPutSyncRuleInfo(opSyncUpdateSyncRule, r)
}

// syncPutSyncRuleInfo is the shared raft submit helper. Mirrors
// metadata_fsm_op.go::syncPutLcConfInfo. Errors are wrapped into a
// stable string so handlers can map them to HTTP 503 (raft unavailable).
func (c *Cluster) syncPutSyncRuleInfo(opType uint32, r *proto.SyncRule) error {
	if r == nil || r.ID() == "" {
		return errors.New("syncPutSyncRuleInfo: nil rule or empty ID")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = syncRulePrefix + r.ID()
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutSyncRuleInfo marshal id=%s: %w", r.ID(), err)
	}
	return c.submit(metadata)
}

// loadSyncRules reconstructs the in-memory cache from the raft store on
// master start (cold load) and on leader switch. Mirrors loadLcConfs at
// metadata_fsm_op.go:2203. A fresh cluster yields an empty cache and no
// error.
//
// Must be called AFTER c.syncRuleCache has been assigned (which
// loadMetadata does) and BEFORE any admin handler is wired — readers
// from API handlers consult the cache without checking for nil.
func (c *Cluster) loadSyncRules() (err error) {
	if c.syncRuleCache == nil {
		c.syncRuleCache = NewSyncRuleCache()
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(syncRulePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadSyncRules],err:%v", err.Error())
		return err
	}
	log.LogInfof("action[loadSyncRules], result count %v", len(result))
	loaded := 0
	for k, value := range result {
		rule := &proto.SyncRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			// Don't fail the whole load on a single bad record — surface
			// the bad key and skip. Operator can inspect with seekraw on
			// rocksdb if recovery is needed.
			log.LogErrorf("action[loadSyncRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.ID() == "" {
			log.LogWarnf("action[loadSyncRules],key:%s rule has empty ID, skipping", k)
			continue
		}
		c.syncRuleCache.Put(rule)
		loaded++
		log.LogInfof("action[loadSyncRules], cached rule[%v] state[%v]", rule.ID(), rule.State)
	}
	log.LogInfof("action[loadSyncRules], loaded %d of %d records", loaded, len(result))
	return
}

// recordTaskDispatch is the centralised ledger Put used by every
// dispatch path (SyncRuleManager + /syncNode/dispatch handler). Builds a
// SyncTaskRecord with the rule snapshot context and pushes it into the
// LRU. shardTotal == 0 means single-task; shardTotal > 0 with owner=""
// records the parent of a fan-out; shardTotal > 0 with non-empty owner
// records a child shard. Idempotent — re-calling with the same taskID
// updates the record in place.
func (c *Cluster) recordTaskDispatch(taskID string, rule *proto.SyncRule, owner string, shardIdx, shardTotal int) {
	if c == nil || c.syncTaskLedger == nil || rule == nil || taskID == "" {
		return
	}
	rec := &SyncTaskRecord{
		TaskID:     taskID,
		RuleID:     rule.ID(),
		Type:       rule.Config.Type,
		Status:     SyncTaskStatusRunning,
		Owner:      owner,
		ShardIdx:   shardIdx,
		ShardTotal: shardTotal,
		StartedAt:  time.Now(),
	}
	c.syncTaskLedger.Put(rec)
}

// recordTaskTerminal updates the ledger entry for taskID with its
// terminal status + error + final progress. Invoked from
// /syncNode/response (handleSyncNodeTaskResponse) when a worker reports
// back. Missing taskID is a warning, not an error — the LRU may have
// evicted the record between dispatch and terminal report.
func (c *Cluster) recordTaskTerminal(taskID string, status SyncTaskStatus, errMsg string, progress SyncTaskProgress) {
	if c == nil || c.syncTaskLedger == nil || taskID == "" {
		return
	}
	prev := c.syncTaskLedger.Get(taskID)
	if prev == nil {
		log.LogWarnf("recordTaskTerminal: taskID %q evicted from ledger; skipping", taskID)
		return
	}
	// Clone so the LRU's previous pointer doesn't get re-Put as-is.
	updated := *prev
	updated.Status = status
	updated.Error = errMsg
	updated.Progress = progress
	updated.DoneAt = time.Now()
	c.syncTaskLedger.Put(&updated)
}
