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

package rules

import (
	"context"
	"sort"
	"sync"
	"time"
)

// memoryStore is the in-memory Store impl used directly by tests and by
// the JSON-file store as its hot-path cache. Safe for concurrent use.
// All List/Get reads return DEEP COPIES so callers cannot mutate internal
// state and corrupt the map.
type memoryStore struct {
	mu    sync.RWMutex
	rules map[string]*Rule
}

// NewMemoryStore returns an empty in-memory Store. The concrete type is
// returned (not the interface) so callers in the same package can compose
// it (the jsonFileStore wraps one); external callers should hold it as a
// Store.
func NewMemoryStore() *memoryStore {
	return &memoryStore{rules: make(map[string]*Rule)}
}

// List returns a snapshot of every rule, sorted by ID for stable output.
func (s *memoryStore) List(_ context.Context) ([]*Rule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, cloneRule(r))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Config.ID < out[j].Config.ID
	})
	return out, nil
}

// Get fetches a single rule by ID. Returns ErrRuleNotFound if absent.
func (s *memoryStore) Get(_ context.Context, id string) (*Rule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rules[id]
	if !ok {
		return nil, ErrRuleNotFound
	}
	return cloneRule(r), nil
}

// Create inserts a new rule. Returns ErrRuleExists if the id is taken.
// CreatedAt / UpdatedAt are set to now if zero; State defaults to active.
func (s *memoryStore) Create(_ context.Context, r *Rule) error {
	if r == nil {
		return ErrInvalidState
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[r.Config.ID]; ok {
		return ErrRuleExists
	}
	now := time.Now()
	stored := cloneRule(r)
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.UpdatedAt = now
	if stored.State == "" {
		stored.State = StateActive
	}
	s.rules[stored.Config.ID] = stored
	return nil
}

// Update replaces an existing rule's Config (keeping CreatedAt). Returns
// ErrRuleNotFound if absent. State / last-run fields are preserved from
// the existing record because they belong to the runtime, not the spec.
func (s *memoryStore) Update(_ context.Context, r *Rule) error {
	if r == nil {
		return ErrInvalidState
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.rules[r.Config.ID]
	if !ok {
		return ErrRuleNotFound
	}
	stored := cloneRule(r)
	stored.CreatedAt = cur.CreatedAt
	// Preserve runtime fields unless explicitly carried in r.
	if stored.State == "" {
		stored.State = cur.State
	}
	if stored.LastRunAt.IsZero() {
		stored.LastRunAt = cur.LastRunAt
		stored.LastRunStatus = cur.LastRunStatus
		stored.LastRunError = cur.LastRunError
	}
	stored.UpdatedAt = time.Now()
	s.rules[stored.Config.ID] = stored
	return nil
}

// Delete removes a rule. Returns ErrRuleNotFound if absent.
func (s *memoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[id]; !ok {
		return ErrRuleNotFound
	}
	delete(s.rules, id)
	return nil
}

// SetState updates the lifecycle state of a rule.
func (s *memoryStore) SetState(_ context.Context, id string, st State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rules[id]
	if !ok {
		return ErrRuleNotFound
	}
	r.State = st
	r.UpdatedAt = time.Now()
	return nil
}

// UpdateLastRun writes the latest run summary back to the rule record.
func (s *memoryStore) UpdateLastRun(_ context.Context, id string, last LastRunSummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rules[id]
	if !ok {
		return ErrRuleNotFound
	}
	r.LastRunAt = last.At
	r.LastRunStatus = last.Status
	r.LastRunError = last.Error
	r.UpdatedAt = time.Now()
	return nil
}

// Close is a no-op for the in-memory impl. Present to satisfy Store.
func (s *memoryStore) Close() error { return nil }

// cloneRule returns a deep copy of r. The runtime fields are value types
// and the embedded RuleConfig contains slices (Filter.Include, Filter.Exclude)
// which we must copy to keep callers from mutating internal state.
func cloneRule(r *Rule) *Rule {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Config.Filter.Include = append([]string(nil), r.Config.Filter.Include...)
	cp.Config.Filter.Exclude = append([]string(nil), r.Config.Filter.Exclude...)
	return &cp
}
