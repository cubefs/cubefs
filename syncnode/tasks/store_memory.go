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
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
)

// memoryStore is the in-memory Store impl used by tests and by ephemeral
// nodes. Safe for concurrent use. All reads return deep copies so callers
// cannot mutate internal state.
//
// The store keeps two maps — `records` is the ACTIVE compartment that
// Put/Get/List/Delete operate on; `history` holds terminal records moved
// in via MoveToHistory and is the source for ListHistory /
// PurgeHistoryBefore. Both maps share the single mu (history transitions
// happen rarely relative to active-record churn, so contention is not a
// concern at the scale tasks runs at).
type memoryStore struct {
	mu      sync.RWMutex
	records map[string]*Record
	history map[string]*Record
}

// NewMemoryStore returns an empty in-memory Store. The concrete type is
// returned (not the interface) so callers in the same package can compose
// it; external callers should hold it as Store.
func NewMemoryStore() *memoryStore {
	return &memoryStore{
		records: make(map[string]*Record),
		history: make(map[string]*Record),
	}
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

// Get fetches a record by taskID from the active compartment. Returns
// ErrTaskNotFound when absent (use ListHistory to inspect aged records).
func (s *memoryStore) Get(_ context.Context, taskID string) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.records[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return cloneRecord(r), nil
}

// List returns every active record matching statusFilter (empty filter =
// all), sorted by StartedAt descending.
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

// Delete removes a record from the active compartment. Returns
// ErrTaskNotFound when absent.
func (s *memoryStore) Delete(_ context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[taskID]; !ok {
		return ErrTaskNotFound
	}
	delete(s.records, taskID)
	return nil
}

// MoveToHistory transitions an active record into history. The source must
// be in a terminal status; running tasks may not age out. Idempotent when
// the record is already in history (returns nil).
func (s *memoryStore) MoveToHistory(_ context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, already := s.history[taskID]; already {
		// Idempotent — already in history. Source may or may not still be
		// in active; remove from active to converge.
		delete(s.records, taskID)
		return nil
	}
	r, ok := s.records[taskID]
	if !ok {
		return ErrTaskNotFound
	}
	if !isTerminal(r.Status) {
		return ErrTaskNotTerminal
	}
	s.history[taskID] = cloneRecord(r)
	delete(s.records, taskID)
	return nil
}

// ListHistory returns history records with DoneAt >= since (zero `since`
// means everything). Sorted by DoneAt descending.
func (s *memoryStore) ListHistory(_ context.Context, since time.Time) ([]*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Record, 0, len(s.history))
	for _, r := range s.history {
		if !since.IsZero() && r.DoneAt.Before(since) {
			continue
		}
		out = append(out, cloneRecord(r))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].DoneAt.After(out[j].DoneAt)
	})
	return out, nil
}

// PurgeHistoryBefore removes records whose DoneAt is strictly before
// cutoff. Returns the number of records purged.
func (s *memoryStore) PurgeHistoryBefore(_ context.Context, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	purged := 0
	for id, r := range s.history {
		if r.DoneAt.Before(cutoff) {
			delete(s.history, id)
			purged++
		}
	}
	return purged, nil
}

// Close is a no-op for the in-memory impl. Present to satisfy Store.
func (s *memoryStore) Close() error { return nil }

// isTerminal reports whether status is a terminal one — eligible for being
// moved to the history compartment.
func isTerminal(s executor.Status) bool {
	switch s {
	case executor.StatusDone, executor.StatusFailed, executor.StatusCancelled:
		return true
	}
	return false
}
