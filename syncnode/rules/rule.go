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

// Package rules defines the runtime Rule type (config + state) and the
// RuleStore interface that the admin API + scheduler talk to. See
// design.md §5.1 + §9 Phase E.
//
// A Rule is the persisted spec for "what data movement should happen and
// when". The static portion mirrors the on-disk RuleConfig schema
// (syncnode/config.go); the dynamic portion (State, timestamps, last-run
// summary) is owned by the store and updated by handlers / scheduler.
package rules

import (
	"context"
	"errors"
	"time"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// State enumerates the lifecycle states of a Rule. The scheduler only
// dispatches rules in StateActive; other states are observable via the
// admin API but produce no task runs.
type State string

const (
	StateActive   State = "active"   // default; scheduler will fire its cron
	StatePaused   State = "paused"   // explicit operator pause
	StateDegraded State = "degraded" // a precondition broke (e.g. vol missing) — see G-3
)

// Rule is the runtime view of a syncrule. The Config sub-struct is the
// on-the-wire / persisted shape (identical to spec.RuleConfig); the
// remaining fields are populated by the store.
type Rule struct {
	Config spec.RuleConfig `json:"config"`

	State     State     `json:"state"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`

	// LastRun summarises the most recent terminal run. Zero values until
	// the first task completes. The scheduler / executor update this via
	// the Store's UpdateLastRun method.
	LastRunAt     time.Time `json:"lastRunAt,omitempty"`
	LastRunStatus string    `json:"lastRunStatus,omitempty"` // "done" / "failed" / "cancelled"
	LastRunError  string    `json:"lastRunError,omitempty"`
}

// ID returns the rule's stable identifier.
func (r *Rule) ID() string { return r.Config.ID }

// NewRule constructs a Rule from a RuleConfig with sensible defaults.
// CreatedAt / UpdatedAt are set to now; State defaults to active.
func NewRule(cfg spec.RuleConfig) *Rule {
	now := time.Now()
	return &Rule{
		Config:    cfg,
		State:     StateActive,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// LastRunSummary captures the post-run state that gets written back to the
// store. Passed to Store.UpdateLastRun by the executor wrapper.
type LastRunSummary struct {
	At     time.Time
	Status string // "done" / "failed" / "cancelled"
	Error  string // empty unless Status == "failed"
}

// Store is the persistence contract for rules. Implementations:
//
//   - in-memory (tests, ephemeral nodes)
//   - JSON file (Phase E-2 default — survives restart without BoltDB dep)
//   - BoltDB-backed (Phase F-2)
//
// All methods take a context so a future BoltDB impl can respect deadlines.
// Implementations MUST be safe for concurrent use.
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

// Sentinel errors. Handlers convert these to *api.APIError. Tests assert via
// errors.Is.
var (
	ErrRuleNotFound = errors.New("rule not found")
	ErrRuleExists   = errors.New("rule already exists")
	ErrInvalidState = errors.New("invalid rule state transition")
)
