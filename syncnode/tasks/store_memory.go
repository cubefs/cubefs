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

package tasks

import (
	"context"
	"sort"
	"sync"

	"github.com/cubefs/cubefs/syncnode/executor"
)

// memoryStore is the in-memory Store impl used by tests and by ephemeral
// nodes. Safe for concurrent use. All reads return deep copies so callers
// cannot mutate internal state.
type memoryStore struct {
	mu      sync.RWMutex
	records map[string]*Record
}

// NewMemoryStore returns an empty in-memory Store. The concrete type is
// returned (not the interface) so callers in the same package can compose
// it; external callers should hold it as Store.
func NewMemoryStore() *memoryStore {
	return &memoryStore{records: make(map[string]*Record)}
}

// Put inserts or overwrites a record. A nil record is rejected to surface
// programmer error early.
func (s *memoryStore) Put(_ context.Context, r *Record) error {
	if r == nil || r.TaskID == "" {
		return ErrTaskNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[r.TaskID] = cloneRecord(r)
	return nil
}

// Get fetches a record by taskID. Returns ErrTaskNotFound when absent.
func (s *memoryStore) Get(_ context.Context, taskID string) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.records[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return cloneRecord(r), nil
}

// List returns every record matching statusFilter (empty filter = all),
// sorted by StartedAt descending.
func (s *memoryStore) List(_ context.Context, statusFilter executor.Status) ([]*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Record, 0, len(s.records))
	for _, r := range s.records {
		if statusFilter != "" && r.Status != statusFilter {
			continue
		}
		out = append(out, cloneRecord(r))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.After(out[j].StartedAt)
	})
	return out, nil
}

// Delete removes a record. Returns ErrTaskNotFound when absent.
func (s *memoryStore) Delete(_ context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[taskID]; !ok {
		return ErrTaskNotFound
	}
	delete(s.records, taskID)
	return nil
}

// Close is a no-op for the in-memory impl. Present to satisfy Store.
func (s *memoryStore) Close() error { return nil }
