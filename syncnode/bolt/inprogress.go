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
	"errors"
	"fmt"
	"sort"
	"time"

	bbolt "go.etcd.io/bbolt"
)

// Breakpoint is the resume-from-N info for one in-flight task.
//
// P0 stores breakpoints but the executor doesn't yet read them (resume
// lands in P1). The shape is intentionally narrow so it's stable for
// future use; bytesDone / uploadId are the two atoms a chunk-resume or
// multipart-resume path needs.
type Breakpoint struct {
	TaskID    string    `json:"taskId"`
	Key       string    `json:"key"`         // object key being transferred
	BytesDone int64     `json:"bytesDone"`
	UploadID  string    `json:"uploadId,omitempty"` // s3 multipart resume token
	UpdatedAt time.Time `json:"updatedAt"`
}

// InProgressStore is the on-disk lane for chunk-offset / multipart
// breakpoints. The interface lives in this package because no other
// subsystem owns the concept — the executor (P1) will consume it via
// this contract.
type InProgressStore interface {
	Put(ctx context.Context, bp *Breakpoint) error
	Get(ctx context.Context, taskID string) (*Breakpoint, error)
	List(ctx context.Context) ([]*Breakpoint, error)
	Delete(ctx context.Context, taskID string) error
	Close() error
}

// ErrBreakpointNotFound is returned by Get / Delete when the requested
// taskID has no breakpoint in the bucket. Mirrors the sentinel style of
// the rules / tasks packages.
var ErrBreakpointNotFound = errors.New("breakpoint not found")

// inProgressStore is the bbolt-backed impl of InProgressStore.
type inProgressStore struct {
	db *bbolt.DB
}

// Put inserts or overwrites the breakpoint for bp.TaskID. UpdatedAt is
// stamped to now if the caller left it zero.
func (s *inProgressStore) Put(_ context.Context, bp *Breakpoint) error {
	if bp == nil || bp.TaskID == "" {
		return ErrBreakpointNotFound
	}
	stored := *bp
	if stored.UpdatedAt.IsZero() {
		stored.UpdatedAt = time.Now()
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketInProgress)
		if b == nil {
			return errBucketMissing(bucketInProgress)
		}
		raw, err := json.Marshal(&stored)
		if err != nil {
			return fmt.Errorf("marshal breakpoint: %w", err)
		}
		return b.Put([]byte(stored.TaskID), raw)
	})
}

// Get fetches a single breakpoint. Returns ErrBreakpointNotFound when
// absent.
func (s *inProgressStore) Get(_ context.Context, taskID string) (*Breakpoint, error) {
	var bp *Breakpoint
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketInProgress)
		if b == nil {
			return errBucketMissing(bucketInProgress)
		}
		raw := b.Get([]byte(taskID))
		if raw == nil {
			return ErrBreakpointNotFound
		}
		bp = &Breakpoint{}
		if err := json.Unmarshal(raw, bp); err != nil {
			return fmt.Errorf("unmarshal breakpoint: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return bp, nil
}

// List returns every breakpoint, sorted by UpdatedAt descending. Used
// by the recovery sweep to inspect what was in-flight at kill -9.
func (s *inProgressStore) List(ctx context.Context) ([]*Breakpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]*Breakpoint, 0)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketInProgress)
		if b == nil {
			return errBucketMissing(bucketInProgress)
		}
		return b.ForEach(func(_, v []byte) error {
			bp := &Breakpoint{}
			if err := json.Unmarshal(v, bp); err != nil {
				return fmt.Errorf("unmarshal breakpoint: %w", err)
			}
			out = append(out, bp)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

// Delete removes a breakpoint. Returns ErrBreakpointNotFound when
// absent.
func (s *inProgressStore) Delete(_ context.Context, taskID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketInProgress)
		if b == nil {
			return errBucketMissing(bucketInProgress)
		}
		key := []byte(taskID)
		if b.Get(key) == nil {
			return ErrBreakpointNotFound
		}
		return b.Delete(key)
	})
}

// Close is a no-op; the DB struct owns the underlying handle.
func (s *inProgressStore) Close() error { return nil }
