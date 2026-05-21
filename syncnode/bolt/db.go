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

// Package bolt provides a BoltDB-backed persistence layer for the
// syncnode service. It opens ONE bbolt.DB at {dataDir}/syncnode.db with
// four buckets (rules, tasks_active, tasks_history, in_progress) and
// exposes three Stores that implement the rules.Store / tasks.Store
// contracts plus the in-progress breakpoint store owned by this package.
//
// See design.md §7.1 + §9 F-2.
package bolt

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/tasks"
	bbolt "go.etcd.io/bbolt"
)

// dbFileName is the basename of the single on-disk BoltDB inside the
// configured dataDir.
const dbFileName = "syncnode.db"

// Bucket names. Exported via the unexported constants so tests + the
// recovery sweep refer to one source of truth.
var (
	bucketRules        = []byte("rules")
	bucketTasksActive  = []byte("tasks_active")
	bucketTasksHistory = []byte("tasks_history")
	bucketInProgress   = []byte("in_progress")
)

// allBuckets enumerates every bucket the package owns. Used by Open
// (initialise) and Health (verify presence).
var allBuckets = [][]byte{
	bucketRules,
	bucketTasksActive,
	bucketTasksHistory,
	bucketInProgress,
}

// DB is a thin wrapper over a single bbolt.DB instance. Lifecycle:
// Open -> RuleStore / TaskStore / InProgress / Recover / Health -> Close.
type DB struct {
	db   *bbolt.DB
	path string
}

// Option mutates an internal options struct passed to Open.
type Option func(*openOpts)

type openOpts struct {
	// flockTimeout caps how long we wait to acquire the file lock. A
	// stuck lock should fail fast — operators can investigate rather
	// than have the process hang on startup.
	flockTimeout time.Duration
}

// WithFlockTimeout overrides the default 5s flock acquisition timeout.
// Mainly useful in tests that want to assert the timeout path quickly.
func WithFlockTimeout(d time.Duration) Option {
	return func(o *openOpts) { o.flockTimeout = d }
}

// Open opens (or creates) the BoltDB at path. The parent directory is
// created if missing. All four buckets are initialised in a single
// Update transaction so callers see a fully-formed DB after Open returns.
//
// `path` is the directory where the syncnode.db file lives; the actual
// bbolt file is `{path}/syncnode.db`. The directory is created with 0o755
// if absent.
func Open(path string, opts ...Option) (*DB, error) {
	if path == "" {
		return nil, errors.New("bolt: path is required")
	}
	o := openOpts{flockTimeout: 5 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	file := filepath.Join(path, dbFileName)
	bdb, err := bbolt.Open(file, 0o644, &bbolt.Options{Timeout: o.flockTimeout})
	if err != nil {
		return nil, fmt.Errorf("open bbolt %s: %w", file, err)
	}
	if err := bdb.Update(func(tx *bbolt.Tx) error {
		for _, name := range allBuckets {
			if _, e := tx.CreateBucketIfNotExists(name); e != nil {
				return fmt.Errorf("create bucket %s: %w", name, e)
			}
		}
		return nil
	}); err != nil {
		_ = bdb.Close()
		return nil, err
	}
	return &DB{db: bdb, path: file}, nil
}

// Path returns the on-disk file path. Mostly useful for tests + ops.
func (d *DB) Path() string { return d.path }

// Close closes the underlying bbolt.DB. Safe to call multiple times.
func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	return err
}

// RuleStore returns a rules.Store backed by the "rules" bucket.
func (d *DB) RuleStore() rules.Store {
	return &ruleStore{db: d.db}
}

// TaskStore returns a tasks.Store backed by the "tasks_active" and
// "tasks_history" buckets.
func (d *DB) TaskStore() tasks.Store {
	return &taskStore{db: d.db}
}

// InProgress returns the breakpoint store backed by the "in_progress"
// bucket.
func (d *DB) InProgress() InProgressStore {
	return &inProgressStore{db: d.db}
}

// Health returns nil if the DB is reachable and every required bucket
// exists; non-nil otherwise. Mirrors the "BoltDB 健康检查通过" acceptance
// criterion of Phase F-2.
func (d *DB) Health() error {
	if d == nil || d.db == nil {
		return errors.New("bolt: DB is not open")
	}
	return d.db.View(func(tx *bbolt.Tx) error {
		for _, name := range allBuckets {
			if tx.Bucket(name) == nil {
				return fmt.Errorf("bolt: bucket %q missing", string(name))
			}
		}
		return nil
	})
}
