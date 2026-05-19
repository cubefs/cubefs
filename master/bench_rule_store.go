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
	"sync"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// BenchRuleStore is an in-memory store for bench rules, protected by an
// RWMutex. For P0 we skip raft persistence; rules are lost on leader
// restart. Raft persistence (like SyncRuleCache) can be added in a later
// phase by following the syncPutSyncRuleInfo pattern.
type BenchRuleStore struct {
	mu    sync.RWMutex
	rules map[string]*spec.BenchRule
}

var (
	// ErrBenchRuleNotFound is returned when no rule with the requested ID
	// exists in the store.
	ErrBenchRuleNotFound = errors.New("bench rule not found")
	// ErrBenchRuleExists is returned when Create is called with an ID that
	// already exists in the store.
	ErrBenchRuleExists = errors.New("bench rule already exists")
)

// NewBenchRuleStore returns an empty BenchRuleStore ready for use.
func NewBenchRuleStore() *BenchRuleStore {
	return &BenchRuleStore{rules: make(map[string]*spec.BenchRule)}
}

// Create inserts r into the store. Returns ErrBenchRuleExists if a rule
// with the same ID is already present. Sets CreatedAt and UpdatedAt.
func (s *BenchRuleStore) Create(r *spec.BenchRule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[r.ID]; ok {
		return ErrBenchRuleExists
	}
	now := time.Now().UnixMilli()
	r.CreatedAt = now
	r.UpdatedAt = now
	cp := *r
	s.rules[r.ID] = &cp
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
// is absent. Updates UpdatedAt automatically.
func (s *BenchRuleStore) Update(r *spec.BenchRule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.rules[r.ID]
	if !ok {
		return ErrBenchRuleNotFound
	}
	r.CreatedAt = existing.CreatedAt
	r.UpdatedAt = time.Now().UnixMilli()
	cp := *r
	s.rules[r.ID] = &cp
	return nil
}

// Delete removes the rule by ID. Returns ErrBenchRuleNotFound if absent.
func (s *BenchRuleStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[id]; !ok {
		return ErrBenchRuleNotFound
	}
	delete(s.rules, id)
	return nil
}
