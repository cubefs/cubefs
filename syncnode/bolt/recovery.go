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
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/tasks"
	bbolt "go.etcd.io/bbolt"
)

// interruptedError is the Error string set on records adjusted by the
// crash-recovery sweep. Operators see "interrupted by node restart" as
// the cause of a Failed status — clear enough to know the task didn't
// genuinely error, it was killed mid-flight.
//
// Exported so callers (admin dashboards, tests) can match against it.
const InterruptedErrorString = "interrupted by node restart"

// Recover runs the crash-recovery sweep against the active task bucket.
// Every record whose Status is pending or running is rewritten with
// Status=Failed and Error=InterruptedErrorString. The DoneAt timestamp
// is set to now so the record can later age into history via the
// normal TTL Runner path.
//
// The call is idempotent: re-running on a clean DB (or a DB that's
// already been swept) is a no-op since neither pending nor running
// records remain.
//
// Returns the number of records adjusted.
//
// This function is intended to be called from server.go startup once
// per process boot (Phase F-3 integration), BEFORE the scheduler or
// runner accept new work — that way operators only ever see records
// in one of two states post-restart: live (newly scheduled after
// boot) or failed-with-clear-cause (interrupted).
func (d *DB) Recover(ctx context.Context) (int, error) {
	if d == nil || d.db == nil {
		return 0, fmt.Errorf("bolt: DB is not open")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	adjusted := 0
	err := d.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTasksActive)
		if b == nil {
			return errBucketMissing(bucketTasksActive)
		}
		// Collect victims first; mutating during ForEach risks cursor
		// re-positioning in bbolt.
		type victim struct {
			key []byte
			raw []byte
		}
		var victims []victim
		if err := b.ForEach(func(k, v []byte) error {
			r := &tasks.Record{}
			if err := json.Unmarshal(v, r); err != nil {
				return fmt.Errorf("unmarshal task record: %w", err)
			}
			if r.Status != executor.StatusPending && r.Status != executor.StatusRunning {
				return nil
			}
			r.Status = executor.StatusFailed
			r.Error = InterruptedErrorString
			r.DoneAt = time.Now()
			out, err := json.Marshal(r)
			if err != nil {
				return fmt.Errorf("marshal task record: %w", err)
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			victims = append(victims, victim{key: kc, raw: out})
			return nil
		}); err != nil {
			return err
		}
		for _, v := range victims {
			if err := b.Put(v.key, v.raw); err != nil {
				return err
			}
			adjusted++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return adjusted, nil
}

// OrphanBreakpoints returns breakpoints whose taskID is no longer in
// the active bucket. These are leftover resume-from-N hints that the
// recovery sweep marked Failed — there's nothing to resume against.
// Ops can use the list to purge the entries (Phase F-3 integration may
// wire this into a /admin/sync/recovery report).
//
// Always safe to call after Recover; the call is read-only.
func (d *DB) OrphanBreakpoints(ctx context.Context) ([]*Breakpoint, error) {
	if d == nil || d.db == nil {
		return nil, fmt.Errorf("bolt: DB is not open")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var orphans []*Breakpoint
	err := d.db.View(func(tx *bbolt.Tx) error {
		bp := tx.Bucket(bucketInProgress)
		active := tx.Bucket(bucketTasksActive)
		if bp == nil {
			return errBucketMissing(bucketInProgress)
		}
		if active == nil {
			return errBucketMissing(bucketTasksActive)
		}
		return bp.ForEach(func(k, v []byte) error {
			if active.Get(k) != nil {
				// Active record still exists — not an orphan.
				return nil
			}
			b := &Breakpoint{}
			if err := json.Unmarshal(v, b); err != nil {
				return fmt.Errorf("unmarshal breakpoint: %w", err)
			}
			orphans = append(orphans, b)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return orphans, nil
}
