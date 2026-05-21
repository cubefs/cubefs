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

	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// Phase 1 — master raft persistence for bench rules.
//
// Mirrors the sync rule pattern at sync_rule_store.go:
//   submit(opCode, key, json) → fsm apply → BatchDeleteAndPut → rocksdb
//
// In-memory cache is rebuilt at startup / leader change by loadBenchRules
// reading the benchRulePrefix key range from rocksdb. The cache is also
// updated by admin handlers immediately after successful raft submit so
// the just-written rule is visible to subsequent reads on the same node.
//
// Op codes 0x86-0x88 sit immediately after the sync rule block
// (0x83-0x85) inside the syncnode block (0x80-0x88).
//
// Scope: only bench rule configuration is persisted; benchTaskLedger
// remains in-memory (task history is observability data — see design doc
// docs/plan/master/bench-rule-persistence.md §B1 / §4).

const (
	benchRuleAcronym = "br"
	benchRulePrefix  = keySeparator + benchRuleAcronym + keySeparator

	opSyncAddBenchRule    uint32 = 0x86
	opSyncDeleteBenchRule uint32 = 0x87
	opSyncUpdateBenchRule uint32 = 0x88
)

// BenchRuleStore holds the master's in-memory view of every persisted
// bench rule. Reads go through the local cache; writes are first
// raft-replicated via the owning Cluster's submit() helper and then
// applied to the cache only on success.
//
// A nil cluster pointer puts the store back into legacy in-memory-only
// mode (used by unit tests that don't bring up the full Cluster). Add /
// Update / Delete will skip raft submission in that case.
type BenchRuleStore struct {
	mu      sync.RWMutex
	rules   map[string]*spec.BenchRule
	cluster *Cluster // raft submitter; nil → in-memory only
}

var (
	// ErrBenchRuleNotFound is returned when no rule with the requested ID
	// exists in the store.
	ErrBenchRuleNotFound = errors.New("bench rule not found")
	// ErrBenchRuleExists is returned when Create is called with an ID that
	// already exists in the store.
	ErrBenchRuleExists = errors.New("bench rule already exists")
)

// NewBenchRuleStore returns an empty BenchRuleStore ready for use. The
// returned store has no raft binding; callers that want persistence must
// invoke BindCluster after the owning Cluster is fully constructed (the
// Cluster value isn't ready when its NewBenchRuleStore() field
// initialiser runs).
func NewBenchRuleStore() *BenchRuleStore {
	return &BenchRuleStore{rules: make(map[string]*spec.BenchRule)}
}

// BindCluster wires the store to the owning Cluster so subsequent
// Add/Update/Delete calls go through raft. Safe to call multiple times.
func (s *BenchRuleStore) BindCluster(c *Cluster) {
	s.mu.Lock()
	s.cluster = c
	s.mu.Unlock()
}

// Create inserts r into the store. Returns ErrBenchRuleExists if a rule
// with the same ID is already present. Sets CreatedAt and UpdatedAt.
//
// Persistence path: raft submit → on success, update the in-memory
// cache. The cache is NOT updated on submit failure; the caller sees the
// raft error verbatim so the API layer can map it to HTTP 503.
func (s *BenchRuleStore) Create(r *spec.BenchRule) error {
	if r == nil || r.ID == "" {
		return errors.New("BenchRuleStore.Create: nil rule or empty ID")
	}
	// Existence check is purely a fast-fail; the authoritative state is
	// rocksdb after the raft commit. We hold the lock for the read so a
	// concurrent Create on the same ID can't race past this guard.
	s.mu.Lock()
	if _, ok := s.rules[r.ID]; ok {
		s.mu.Unlock()
		return ErrBenchRuleExists
	}
	cluster := s.cluster
	s.mu.Unlock()

	now := time.Now().UnixMilli()
	r.CreatedAt = now
	r.UpdatedAt = now

	if cluster != nil {
		if err := cluster.syncAddBenchRule(r); err != nil {
			return err
		}
	}

	cp := *r
	s.mu.Lock()
	s.rules[r.ID] = &cp
	s.mu.Unlock()
	return nil
}

// Get returns the rule with the given ID, or ErrBenchRuleNotFound.
func (s *BenchRuleStore) Get(id string) (*spec.BenchRule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rules[id]
	if !ok {
		return nil, ErrBenchRuleNotFound
	}
	cp := *r
	return &cp, nil
}

// List returns a snapshot of all rules. The returned slice and rule
// pointers are copies; callers may mutate them freely.
func (s *BenchRuleStore) List() []*spec.BenchRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*spec.BenchRule, 0, len(s.rules))
	for _, r := range s.rules {
		cp := *r
		out = append(out, &cp)
	}
	return out
}

// Update replaces the stored rule. Returns ErrBenchRuleNotFound if the ID
// is absent. Updates UpdatedAt automatically; CreatedAt is preserved
// from the existing record.
//
// Persistence path matches Create: raft submit → cache update.
func (s *BenchRuleStore) Update(r *spec.BenchRule) error {
	if r == nil || r.ID == "" {
		return errors.New("BenchRuleStore.Update: nil rule or empty ID")
	}
	s.mu.Lock()
	existing, ok := s.rules[r.ID]
	if !ok {
		s.mu.Unlock()
		return ErrBenchRuleNotFound
	}
	r.CreatedAt = existing.CreatedAt
	cluster := s.cluster
	s.mu.Unlock()

	r.UpdatedAt = time.Now().UnixMilli()

	if cluster != nil {
		if err := cluster.syncUpdateBenchRule(r); err != nil {
			return err
		}
	}

	cp := *r
	s.mu.Lock()
	s.rules[r.ID] = &cp
	s.mu.Unlock()
	return nil
}

// Delete removes the rule by ID. Returns ErrBenchRuleNotFound if absent.
//
// Persistence path: raft submit (carrying the rule snapshot for audit
// symmetry with sync rule) → cache delete.
func (s *BenchRuleStore) Delete(id string) error {
	s.mu.Lock()
	existing, ok := s.rules[id]
	if !ok {
		s.mu.Unlock()
		return ErrBenchRuleNotFound
	}
	// Snapshot for the raft payload — release the lock before calling
	// out so submit() can't deadlock on cache readers.
	snapshot := *existing
	cluster := s.cluster
	s.mu.Unlock()

	if cluster != nil {
		if err := cluster.syncDeleteBenchRule(&snapshot); err != nil {
			return err
		}
	}

	s.mu.Lock()
	delete(s.rules, id)
	s.mu.Unlock()
	return nil
}

// putLocal updates the in-memory cache without going through raft. Used
// by loadBenchRules during cold load / leader switch.
func (s *BenchRuleStore) putLocal(r *spec.BenchRule) {
	if r == nil || r.ID == "" {
		return
	}
	cp := *r
	s.mu.Lock()
	s.rules[r.ID] = &cp
	s.mu.Unlock()
}

// Len returns the number of rules currently cached.
func (s *BenchRuleStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rules)
}

// syncAddBenchRule raft-replicates a Create op for the supplied rule.
// Returns nil on commit success; the caller (BenchRuleStore.Create) is
// responsible for updating the in-memory cache after success.
func (c *Cluster) syncAddBenchRule(r *spec.BenchRule) error {
	return c.syncPutBenchRuleInfo(opSyncAddBenchRule, r)
}

// syncDeleteBenchRule raft-replicates a Delete op for the rule. Body
// carries the rule for audit symmetry with sync rule / lcConf.
func (c *Cluster) syncDeleteBenchRule(r *spec.BenchRule) error {
	return c.syncPutBenchRuleInfo(opSyncDeleteBenchRule, r)
}

// syncUpdateBenchRule raft-replicates an Update op for the supplied
// rule.
func (c *Cluster) syncUpdateBenchRule(r *spec.BenchRule) error {
	return c.syncPutBenchRuleInfo(opSyncUpdateBenchRule, r)
}

// syncPutBenchRuleInfo is the shared raft submit helper. Mirrors
// syncPutSyncRuleInfo. Errors are wrapped into a stable string so
// handlers can map them to HTTP 503 (raft unavailable).
func (c *Cluster) syncPutBenchRuleInfo(opType uint32, r *spec.BenchRule) error {
	if r == nil || r.ID == "" {
		return errors.New("syncPutBenchRuleInfo: nil rule or empty ID")
	}
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = benchRulePrefix + r.ID
	var err error
	metadata.V, err = json.Marshal(r)
	if err != nil {
		return fmt.Errorf("syncPutBenchRuleInfo marshal id=%s: %w", r.ID, err)
	}
	return c.submit(metadata)
}

// loadBenchRules reconstructs the in-memory bench rule cache from
// rocksdb on master start (cold load) and on leader switch. Mirrors
// loadSyncRules. A fresh cluster yields an empty cache and no error.
//
// Must be called AFTER c.benchRuleStore has been assigned and bound to
// the cluster (see Cluster.start / master_manager loadMetadata).
func (c *Cluster) loadBenchRules() (err error) {
	if c.benchRuleStore == nil {
		c.benchRuleStore = NewBenchRuleStore()
		c.benchRuleStore.BindCluster(c)
	}
	result, err := c.fsm.store.SeekForPrefix([]byte(benchRulePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadBenchRules],err:%v", err.Error())
		return err
	}
	log.LogInfof("action[loadBenchRules], result count %v", len(result))
	loaded := 0
	for k, value := range result {
		rule := &spec.BenchRule{}
		if err = json.Unmarshal(value, rule); err != nil {
			// Don't fail the whole load on a single bad record — surface
			// the bad key and skip. Matches loadSyncRules behaviour.
			log.LogErrorf("action[loadBenchRules],key:%s unmarshal err:%v", k, err)
			err = nil
			continue
		}
		if rule.ID == "" {
			log.LogWarnf("action[loadBenchRules],key:%s rule has empty ID, skipping", k)
			continue
		}
		c.benchRuleStore.putLocal(rule)
		loaded++
		log.LogInfof("action[loadBenchRules], cached rule[%v]", rule.ID)
	}
	log.LogInfof("action[loadBenchRules], loaded %d of %d records", loaded, len(result))
	return
}
