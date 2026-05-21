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

	"github.com/cubefs/cubefs/syncnode/rules"
	bbolt "go.etcd.io/bbolt"
)

// ruleStore implements rules.Store on top of the "rules" bucket.
// Records are serialised as JSON; keys are rule IDs.
type ruleStore struct {
	db *bbolt.DB
}

// List returns every rule in the bucket, sorted by ID. Results are deep
// copies — JSON unmarshal already builds fresh slices, so callers may
// freely mutate.
func (s *ruleStore) List(ctx context.Context) ([]*rules.Rule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]*rules.Rule, 0)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRules)
		if b == nil {
			return errBucketMissing(bucketRules)
		}
		return b.ForEach(func(_, v []byte) error {
			r := &rules.Rule{}
			if err := json.Unmarshal(v, r); err != nil {
				return fmt.Errorf("unmarshal rule: %w", err)
			}
			out = append(out, r)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Config.ID < out[j].Config.ID
	})
	return out, nil
}

// Get fetches a single rule by ID. Returns rules.ErrRuleNotFound when
// absent.
func (s *ruleStore) Get(_ context.Context, id string) (*rules.Rule, error) {
	var r *rules.Rule
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRules)
		if b == nil {
			return errBucketMissing(bucketRules)
		}
		raw := b.Get([]byte(id))
		if raw == nil {
			return rules.ErrRuleNotFound
		}
		r = &rules.Rule{}
		if err := json.Unmarshal(raw, r); err != nil {
			return fmt.Errorf("unmarshal rule: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Create inserts a new rule. Returns rules.ErrRuleExists when id is
// taken. CreatedAt/UpdatedAt are set to now if zero; State defaults to
// active. The on-disk row owns the canonical timestamps.
func (s *ruleStore) Create(_ context.Context, r *rules.Rule) error {
	if r == nil || r.Config.ID == "" {
		return rules.ErrInvalidState
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRules)
		if b == nil {
			return errBucketMissing(bucketRules)
		}
		key := []byte(r.Config.ID)
		if b.Get(key) != nil {
			return rules.ErrRuleExists
		}
		now := time.Now()
		stored := *r
		if stored.CreatedAt.IsZero() {
			stored.CreatedAt = now
		}
		stored.UpdatedAt = now
		if stored.State == "" {
			stored.State = rules.StateActive
		}
		raw, err := json.Marshal(&stored)
		if err != nil {
			return fmt.Errorf("marshal rule: %w", err)
		}
		return b.Put(key, raw)
	})
}

// Update replaces an existing rule's Config. CreatedAt is preserved from
// the stored copy. Runtime fields (State, LastRun*) are preserved from
// the stored copy unless the incoming r explicitly overrides them
// (non-empty State, non-zero LastRunAt). Returns rules.ErrRuleNotFound
// when the id is absent.
func (s *ruleStore) Update(_ context.Context, r *rules.Rule) error {
	if r == nil || r.Config.ID == "" {
		return rules.ErrInvalidState
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRules)
		if b == nil {
			return errBucketMissing(bucketRules)
		}
		key := []byte(r.Config.ID)
		raw := b.Get(key)
		if raw == nil {
			return rules.ErrRuleNotFound
		}
		cur := &rules.Rule{}
		if err := json.Unmarshal(raw, cur); err != nil {
			return fmt.Errorf("unmarshal rule: %w", err)
		}
		stored := *r
		stored.CreatedAt = cur.CreatedAt
		if stored.State == "" {
			stored.State = cur.State
		}
		if stored.LastRunAt.IsZero() {
			stored.LastRunAt = cur.LastRunAt
			stored.LastRunStatus = cur.LastRunStatus
			stored.LastRunError = cur.LastRunError
		}
		stored.UpdatedAt = time.Now()
		out, err := json.Marshal(&stored)
		if err != nil {
			return fmt.Errorf("marshal rule: %w", err)
		}
		return b.Put(key, out)
	})
}

// Delete removes a rule. Returns rules.ErrRuleNotFound when absent.
func (s *ruleStore) Delete(_ context.Context, id string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRules)
		if b == nil {
			return errBucketMissing(bucketRules)
		}
		key := []byte(id)
		if b.Get(key) == nil {
			return rules.ErrRuleNotFound
		}
		return b.Delete(key)
	})
}

// SetState updates the lifecycle state and bumps UpdatedAt.
func (s *ruleStore) SetState(_ context.Context, id string, st rules.State) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRules)
		if b == nil {
			return errBucketMissing(bucketRules)
		}
		key := []byte(id)
		raw := b.Get(key)
		if raw == nil {
			return rules.ErrRuleNotFound
		}
		r := &rules.Rule{}
		if err := json.Unmarshal(raw, r); err != nil {
			return fmt.Errorf("unmarshal rule: %w", err)
		}
		r.State = st
		r.UpdatedAt = time.Now()
		out, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal rule: %w", err)
		}
		return b.Put(key, out)
	})
}

// UpdateLastRun writes the latest run summary back to the rule record.
func (s *ruleStore) UpdateLastRun(_ context.Context, id string, last rules.LastRunSummary) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRules)
		if b == nil {
			return errBucketMissing(bucketRules)
		}
		key := []byte(id)
		raw := b.Get(key)
		if raw == nil {
			return rules.ErrRuleNotFound
		}
		r := &rules.Rule{}
		if err := json.Unmarshal(raw, r); err != nil {
			return fmt.Errorf("unmarshal rule: %w", err)
		}
		r.LastRunAt = last.At
		r.LastRunStatus = last.Status
		r.LastRunError = last.Error
		r.UpdatedAt = time.Now()
		out, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal rule: %w", err)
		}
		return b.Put(key, out)
	})
}

// Close is a no-op; the DB struct owns the underlying handle.
func (s *ruleStore) Close() error { return nil }

func errBucketMissing(name []byte) error {
	return fmt.Errorf("bolt: bucket %q missing — DB not initialised", string(name))
}
