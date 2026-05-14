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
	"errors"
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
)

// Store is the persistence contract for task records. Implementations:
//
//   - in-memory (tests, ephemeral nodes — ships in this package).
//   - BoltDB-backed (Phase F-2, in syncnode/bolt).
//
// The store maintains TWO logical compartments:
//
//   - **Active** — the normal Put/Get/List/Delete surface. Records start
//     here when the Runner creates them, regardless of terminal status.
//   - **History** — a separate compartment that operators use for audit /
//     long-term retention. Records age into history via MoveToHistory
//     (driven by the TTL Runner in Phase F-4); the active compartment is
//     short-lived working set, the history compartment is the 7-day TTL'd
//     audit trail.
//
// All methods take a context so a future BoltDB / JSON-file impl can respect
// deadlines. Implementations MUST be safe for concurrent use, and List / Get
// MUST return deep copies — callers may mutate the returned records.
type Store interface {
	// Put inserts or overwrites the record under r.TaskID in the ACTIVE
	// compartment. There is no separate Create / Update; the Runner owns
	// the lifecycle.
	Put(ctx context.Context, r *Record) error

	// Get fetches a single active record by taskID. Returns ErrTaskNotFound
	// when the id is unknown in the active compartment (history is searched
	// separately via ListHistory).
	Get(ctx context.Context, taskID string) (*Record, error)

	// List returns every record in the ACTIVE compartment. If statusFilter
	// is non-empty only records matching that status are returned. Results
	// are sorted by StartedAt descending (most recent first) so operators
	// see fresh runs first.
	List(ctx context.Context, statusFilter executor.Status) ([]*Record, error)

	// Delete removes a record from the active compartment. Returns
	// ErrTaskNotFound when absent.
	Delete(ctx context.Context, taskID string) error

	// MoveToHistory atomically transfers the active record into the history
	// compartment. Returns ErrTaskNotFound when the source taskID is absent
	// from the active compartment, or ErrTaskNotTerminal when its Status is
	// not one of {done, failed, cancelled} — only terminal records may age
	// into history. Idempotent for records already in history (returns nil).
	MoveToHistory(ctx context.Context, taskID string) error

	// ListHistory returns the history compartment, sorted by DoneAt
	// descending. The optional `since` cut-off, when non-zero, filters out
	// records with DoneAt strictly before it; pass zero time to fetch
	// everything currently in history.
	ListHistory(ctx context.Context, since time.Time) ([]*Record, error)

	// PurgeHistoryBefore removes records from history whose DoneAt is
	// strictly before cutoff. Returns the number of records purged. The TTL
	// Runner calls this on a timer to bound on-disk history size.
	PurgeHistoryBefore(ctx context.Context, cutoff time.Time) (int, error)

	// Close releases any resources held by the store. Safe to call multiple
	// times.
	Close() error
}

// Sentinel errors. Handlers translate these to api.* errors.
var (
	ErrTaskNotFound    = errors.New("task not found")
	ErrTaskNotTerminal = errors.New("task is not in a terminal status; cannot move to history")
)
