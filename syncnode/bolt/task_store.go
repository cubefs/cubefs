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

package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/tasks"
	bbolt "go.etcd.io/bbolt"
)

// taskStore implements tasks.Store on top of the "tasks_active" and
// "tasks_history" buckets. Records are JSON-serialised; keys are
// task IDs.
type taskStore struct {
	db *bbolt.DB
}

// Put inserts or overwrites a record in the active bucket. Nil records
// or records with empty TaskID return tasks.ErrTaskNotFound (same
// programmer-error signal as the in-memory impl).
func (s *taskStore) Put(_ context.Context, r *tasks.Record) error {
	if r == nil || r.TaskID == "" {
		return tasks.ErrTaskNotFound
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTasksActive)
		if b == nil {
			return errBucketMissing(bucketTasksActive)
		}
		raw, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal task record: %w", err)
		}
		return b.Put([]byte(r.TaskID), raw)
	})
}

// Get fetches a single record by taskID from the active bucket. Returns
// tasks.ErrTaskNotFound when absent.
func (s *taskStore) Get(_ context.Context, taskID string) (*tasks.Record, error) {
	var rec *tasks.Record
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTasksActive)
		if b == nil {
			return errBucketMissing(bucketTasksActive)
		}
		raw := b.Get([]byte(taskID))
		if raw == nil {
			return tasks.ErrTaskNotFound
		}
		rec = &tasks.Record{}
		if err := json.Unmarshal(raw, rec); err != nil {
			return fmt.Errorf("unmarshal task record: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// List returns every active record (optionally filtered by status),
// sorted by StartedAt descending.
func (s *taskStore) List(ctx context.Context, statusFilter executor.Status) ([]*tasks.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]*tasks.Record, 0)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTasksActive)
		if b == nil {
			return errBucketMissing(bucketTasksActive)
		}
		return b.ForEach(func(_, v []byte) error {
			r := &tasks.Record{}
			if err := json.Unmarshal(v, r); err != nil {
				return fmt.Errorf("unmarshal task record: %w", err)
			}
			if statusFilter != "" && r.Status != statusFilter {
				return nil
			}
			out = append(out, r)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.After(out[j].StartedAt)
	})
	return out, nil
}

// Delete removes a record from the active bucket. Returns
// tasks.ErrTaskNotFound when absent.
func (s *taskStore) Delete(_ context.Context, taskID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTasksActive)
		if b == nil {
			return errBucketMissing(bucketTasksActive)
		}
		key := []byte(taskID)
		if b.Get(key) == nil {
			return tasks.ErrTaskNotFound
		}
		return b.Delete(key)
	})
}

// MoveToHistory atomically transfers an active record into the history
// bucket. Idempotent: when the record is already in history the call
// succeeds (and also removes any leftover active row, to converge).
// Returns tasks.ErrTaskNotFound when missing from active, or
// tasks.ErrTaskNotTerminal when Status is not terminal.
func (s *taskStore) MoveToHistory(_ context.Context, taskID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		active := tx.Bucket(bucketTasksActive)
		history := tx.Bucket(bucketTasksHistory)
		if active == nil {
			return errBucketMissing(bucketTasksActive)
		}
		if history == nil {
			return errBucketMissing(bucketTasksHistory)
		}
		key := []byte(taskID)

		if history.Get(key) != nil {
			// Already in history. Converge by removing any straggler
			// from active and return nil (idempotent).
			if active.Get(key) != nil {
				if err := active.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}
		raw := active.Get(key)
		if raw == nil {
			return tasks.ErrTaskNotFound
		}
		r := &tasks.Record{}
		if err := json.Unmarshal(raw, r); err != nil {
			return fmt.Errorf("unmarshal task record: %w", err)
		}
		if !isTerminalStatus(r.Status) {
			return tasks.ErrTaskNotTerminal
		}
		if err := history.Put(key, raw); err != nil {
			return err
		}
		return active.Delete(key)
	})
}

// ListHistory returns history records with DoneAt >= since (zero `since`
// means everything). Sorted by DoneAt descending.
func (s *taskStore) ListHistory(ctx context.Context, since time.Time) ([]*tasks.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]*tasks.Record, 0)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTasksHistory)
		if b == nil {
			return errBucketMissing(bucketTasksHistory)
		}
		return b.ForEach(func(_, v []byte) error {
			r := &tasks.Record{}
			if err := json.Unmarshal(v, r); err != nil {
				return fmt.Errorf("unmarshal task record: %w", err)
			}
			if !since.IsZero() && r.DoneAt.Before(since) {
				return nil
			}
			out = append(out, r)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].DoneAt.After(out[j].DoneAt)
	})
	return out, nil
}

// PurgeHistoryBefore removes records whose DoneAt is strictly before
// cutoff. Returns the number of records removed.
func (s *taskStore) PurgeHistoryBefore(ctx context.Context, cutoff time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	purged := 0
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTasksHistory)
		if b == nil {
			return errBucketMissing(bucketTasksHistory)
		}
		// Collect victims first; deleting during ForEach is unsafe in
		// bbolt because the cursor would skip the next key.
		var victims [][]byte
		if err := b.ForEach(func(k, v []byte) error {
			r := &tasks.Record{}
			if err := json.Unmarshal(v, r); err != nil {
				return fmt.Errorf("unmarshal task record: %w", err)
			}
			if r.DoneAt.Before(cutoff) {
				kc := make([]byte, len(k))
				copy(kc, k)
				victims = append(victims, kc)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range victims {
			if err := b.Delete(k); err != nil {
				return err
			}
			purged++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return purged, nil
}

// Close is a no-op; the DB struct owns the underlying handle.
func (s *taskStore) Close() error { return nil }

// isTerminalStatus reports whether status is one of {done, failed,
// cancelled} — the gate the design imposes before a record may age into
// history.
func isTerminalStatus(st executor.Status) bool {
	switch st {
	case executor.StatusDone, executor.StatusFailed, executor.StatusCancelled:
		return true
	}
	return false
}
