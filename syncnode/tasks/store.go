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

	"github.com/cubefs/cubefs/syncnode/executor"
)

// Store is the persistence contract for task records. Implementations:
//
//   - in-memory (tests, ephemeral nodes — ships in this package).
//   - on-disk (Phase F-2; not in scope here).
//
// All methods take a context so a future BoltDB / JSON-file impl can respect
// deadlines. Implementations MUST be safe for concurrent use, and List / Get
// MUST return deep copies — callers may mutate the returned records.
type Store interface {
	// Put inserts or overwrites the record under r.TaskID. There is no
	// separate Create / Update; the Runner owns the lifecycle.
	Put(ctx context.Context, r *Record) error

	// Get fetches a single record by taskID. Returns ErrTaskNotFound when
	// the id is unknown.
	Get(ctx context.Context, taskID string) (*Record, error)

	// List returns every record. If statusFilter is non-empty only records
	// matching that status are returned. Results are sorted by StartedAt
	// descending (most recent first) so operators see fresh runs first.
	List(ctx context.Context, statusFilter executor.Status) ([]*Record, error)

	// Delete removes a record. Returns ErrTaskNotFound when absent. The
	// Runner does NOT delete records automatically — operators / GC do.
	Delete(ctx context.Context, taskID string) error

	// Close releases any resources held by the store. Safe to call multiple
	// times.
	Close() error
}

// ErrTaskNotFound is returned by Store.Get / Store.Delete when the requested
// taskID is absent. Handlers translate this to api.ErrNotFound.
var ErrTaskNotFound = errors.New("task not found")
