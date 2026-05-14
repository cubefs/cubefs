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

// SyncRule types — promoted from syncnode/spec + syncnode/rules so the
// master can own the canonical schema without importing syncnode. The
// syncnode side re-exports these via type aliases in syncnode/spec/types.go
// + syncnode/rules/rule.go so existing callsites compile unchanged.
//
// See docs/plan/syncnode/design.md §3 (rule-store ownership moves to
// master) and the P2 plan at /Users/tao.fang/.claude/plans/.
package proto

import (
	"errors"
	"time"
)

// SyncRuleState enumerates the lifecycle states of a sync rule. The
// master scheduler only fires rules in StateActive.
type SyncRuleState string

const (
	SyncRuleStateActive   SyncRuleState = "active"
	SyncRuleStatePaused   SyncRuleState = "paused"
	SyncRuleStateDegraded SyncRuleState = "degraded"
)

// SyncEndpointConfig describes one source or destination of a sync rule.
// Fields used depend on Kind:
//   - cfs:   Vol + Path
//   - s3:    Endpoint, Region, Bucket, Prefix, StorageClass, AccessKeyEnv, SecretKeyEnv, InsecureSkipTLS
//   - local: Path + buffer hints (BufferSizeKiB, Concurrency, DirectIO, FadviseSequential)
type SyncEndpointConfig struct {
	Kind string `json:"kind"`
	// cfs fields
	Vol  string `json:"vol"`
	Path string `json:"path"`
	// s3 fields
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix"`
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	StorageClass string `json:"storageClass"`
	// s3 credential override — names of env vars that hold the access/secret keys.
	// When set, these take precedence over the global s3Defaults in sync.json.
	AccessKeyEnv string `json:"accessKeyEnv"`
	SecretKeyEnv string `json:"secretKeyEnv"`
	// InsecureSkipTLS disables TLS certificate verification for s3 endpoints.
	// Use only in dev/test environments without a proper CA cert bundle.
	InsecureSkipTLS bool `json:"insecureSkipTLS"`
	// local fields (any host-mounted POSIX path)
	BufferSizeKiB     int  `json:"bufferSizeKiB"`
	Concurrency       int  `json:"concurrency"`
	DirectIO          bool `json:"directIO"`
	FadviseSequential bool `json:"fadviseSequential"`
}

// SyncFilterConfig is the wire / persisted shape of a rule's file filter.
// Size and age fields use human-readable strings ("1MB", "30s") at the
// boundary; syncnode's executor parses them into typed values at apply
// time.
type SyncFilterConfig struct {
	Include []string `json:"include"`
	Exclude []string `json:"exclude"`
	MinSize string   `json:"minSize"`
	MaxSize string   `json:"maxSize"`
	MinAge  string   `json:"minAge"`
	MaxAge  string   `json:"maxAge"`
}

// SyncRetentionConfig is the wire shape of a rule's destination retention
// policy.
type SyncRetentionConfig struct {
	Pattern    string `json:"pattern"`
	KeepLast   int    `json:"keepLast"`
	KeepWithin string `json:"keepWithin"`
}

// SyncRuleConfig is the on-disk schema for a single sync rule.
//
// The ShardPrefixes field is consumed only when ShardingStrategy ==
// "prefix" (operator declares the partition list explicitly) — for
// "auto" the master probes a candidate syncnode at fire time and
// fills the shard list dynamically.
type SyncRuleConfig struct {
	ID                          string              `json:"id"`
	Type                        string              `json:"type"`
	Schedule                    string              `json:"schedule"`
	Src                         SyncEndpointConfig  `json:"src"`
	Dst                         SyncEndpointConfig  `json:"dst"`
	Filter                      SyncFilterConfig    `json:"filter"`
	Retention                   SyncRetentionConfig `json:"retention"`
	AfterCopy                   string              `json:"afterCopy"`
	DownloadStrategy            string              `json:"downloadStrategy"`
	OnMismatch                  string              `json:"onMismatch"`
	SampleStrategy              string              `json:"sampleStrategy"`
	SampleRate                  float64             `json:"sampleRate"`
	BandwidthLimitMBps          int                 `json:"bandwidthLimitMBps"`
	AggregateBandwidthLimitMBps int                 `json:"aggregateBandwidthLimitMBps"`
	Parallelism                 int                 `json:"parallelism"`
	// ShardingStrategy selects how the master fans a single rule
	// trigger into N sub-tasks across the cluster:
	//   "" / "hash"  → FNV-1a hash on object key (default; even distribution)
	//   "prefix"     → use ShardPrefixes literally; len(ShardPrefixes) defines N
	//   "auto"       → master probes backend top-level prefixes at fire time
	ShardingStrategy string `json:"shardingStrategy"`
	// ShardPrefixes carries operator-declared partition prefixes for the
	// "prefix" strategy. Optional for "auto" (acts as a whitelist).
	ShardPrefixes []string `json:"shardPrefixes,omitempty"`
}

// SyncLastRunSummary captures the post-run state written back after a
// rule's task reaches a terminal status. Passed to RuleStore.UpdateLastRun
// by the executor wrapper.
type SyncLastRunSummary struct {
	At     time.Time `json:"at"`
	Status string    `json:"status"` // "done" / "failed" / "cancelled"
	Error  string    `json:"error"`  // empty unless Status == "failed"
}

// SyncRule is the runtime view of a sync rule. The Config sub-struct is
// the on-the-wire / persisted shape (SyncRuleConfig); the remaining
// fields are managed by the master rule store.
type SyncRule struct {
	Config SyncRuleConfig `json:"config"`

	State     SyncRuleState `json:"state"`
	CreatedAt time.Time     `json:"createdAt"`
	UpdatedAt time.Time     `json:"updatedAt"`

	// LastRun summarises the most recent terminal run. Zero values until
	// the first task completes.
	LastRunAt     time.Time `json:"lastRunAt,omitempty"`
	LastRunStatus string    `json:"lastRunStatus,omitempty"`
	LastRunError  string    `json:"lastRunError,omitempty"`
}

// ID returns the rule's stable identifier.
func (r *SyncRule) ID() string { return r.Config.ID }

// NewSyncRule constructs a SyncRule from a SyncRuleConfig with sensible
// defaults: CreatedAt / UpdatedAt = now, State = active.
func NewSyncRule(cfg SyncRuleConfig) *SyncRule {
	now := time.Now()
	return &SyncRule{
		Config:    cfg,
		State:     SyncRuleStateActive,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// ReasonSyncRuleInterrupted is the canonical LastRunError text written
// when a rule is auto-degraded but no specific reason was captured (e.g.
// the rule's last run was cancelled mid-flight). Operators look for this
// string in /syncRule responses to distinguish "task failed before
// reporting a reason" from "rule paused by an operator".
const ReasonSyncRuleInterrupted = "rule interrupted"

// Sentinel errors. Handlers convert these to *api.APIError / HTTP codes;
// tests assert via errors.Is.
var (
	ErrSyncRuleNotFound     = errors.New("sync rule not found")
	ErrSyncRuleExists       = errors.New("sync rule already exists")
	ErrSyncRuleInvalidState = errors.New("invalid sync rule state transition")
)
