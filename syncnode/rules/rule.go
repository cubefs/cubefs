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

// Package rules defines the RuleStore interface used by the syncnode
// runtime. As of P2, the canonical Rule / State types live in
// proto/sync_rule.go (master owns the schema). This package re-exports
// them as type aliases so existing syncnode callsites compile unchanged.
package rules

import (
	"context"
	"errors"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/spec"
)

// State enumerates the lifecycle states of a Rule. Alias of
// proto.SyncRuleState.
type State = proto.SyncRuleState

// Lifecycle constants (alias the proto-side typed consts so downstream
// `rules.StateActive` etc. keep working without source edits).
const (
	StateActive   = proto.SyncRuleStateActive
	StatePaused   = proto.SyncRuleStatePaused
	StateDegraded = proto.SyncRuleStateDegraded
)

// Rule is the runtime view of a syncrule. Alias of proto.SyncRule.
type Rule = proto.SyncRule

// LastRunSummary captures the post-run state written back to the store.
// Alias of proto.SyncLastRunSummary.
type LastRunSummary = proto.SyncLastRunSummary

// NewRule constructs a Rule from a RuleConfig with sensible defaults.
// CreatedAt / UpdatedAt are set to now; State defaults to active.
func NewRule(cfg spec.RuleConfig) *Rule {
	return proto.NewSyncRule(cfg)
}

// Store is the persistence contract for rules. The syncnode-local
// implementations (jsonfile / memory / bolt) implement this so existing
// reload + handler code keeps compiling during the master-side migration
// (the master rule cache implements the same shape).
type Store interface {
	List(ctx context.Context) ([]*Rule, error)
	Get(ctx context.Context, id string) (*Rule, error)
	Create(ctx context.Context, r *Rule) error
	Update(ctx context.Context, r *Rule) error
	Delete(ctx context.Context, id string) error
	SetState(ctx context.Context, id string, s State) error
	UpdateLastRun(ctx context.Context, id string, last LastRunSummary) error
	Close() error
}

// Sentinel errors. Aliases of the proto-side error vars; handlers convert
// these to *api.APIError. Tests assert via errors.Is.
var (
	ErrRuleNotFound = proto.ErrSyncRuleNotFound
	ErrRuleExists   = proto.ErrSyncRuleExists
	ErrInvalidState = proto.ErrSyncRuleInvalidState
)

// ReasonRuleInterrupted is the canonical LastRunError string written
// when a rule is auto-degraded but no specific reason was captured.
// Alias of proto.ReasonSyncRuleInterrupted.
const ReasonRuleInterrupted = proto.ReasonSyncRuleInterrupted

// Degrade flips a rule into StateDegraded and stamps reason into
// LastRunError so operators can inspect WHY degradation fired without
// cross-referencing the task records. Idempotent.
//
// Returns ErrRuleNotFound when ruleID is empty or unknown to the store.
// If reason is empty, ReasonRuleInterrupted is used as the placeholder so
// LastRunError is never left blank for a degraded rule.
//
// See design.md §9 G-3.
func Degrade(ctx context.Context, store Store, ruleID, reason string) error {
	if store == nil {
		return errors.New("rules.Degrade: nil store")
	}
	if ruleID == "" {
		return ErrRuleNotFound
	}
	// Probe for existence so callers see ErrRuleNotFound rather than
	// SetState's downstream error.
	if _, err := store.Get(ctx, ruleID); err != nil {
		return err
	}
	if err := store.SetState(ctx, ruleID, StateDegraded); err != nil {
		return err
	}
	if reason == "" {
		reason = ReasonRuleInterrupted
	}
	return store.UpdateLastRun(ctx, ruleID, LastRunSummary{
		At:     time.Now(),
		Status: "failed",
		Error:  reason,
	})
}
