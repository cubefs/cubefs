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
//   - s3:    Endpoint, Region, Bucket, Prefix, StorageClass, AccessKeyEnv, SecretKeyEnv, InsecureSkipTLS, UsePathStyle
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
	// Inline s3 credentials — injected by the dashboard (Approach C) so syncnode
	// does not need environment variables for per-backend credentials. When non-empty,
	// these take precedence over AccessKeyEnv / SecretKeyEnv.
	AccessKey string `json:"accessKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
	// InsecureSkipTLS disables TLS certificate verification for s3 endpoints.
	// Use only in dev/test environments without a proper CA cert bundle.
	InsecureSkipTLS bool `json:"insecureSkipTLS"`
	// UsePathStyle forces S3 path-style addressing (bucket in URL path).
	// Required for S3-compatible stores that don't support virtual-hosted
	// style (e.g. ByteDance TOS, Ceph RGW with no wildcard DNS).
	UsePathStyle bool `json:"usePathStyle"`
	// local fields (any host-mounted POSIX path)
	BufferSizeKiB     int  `json:"bufferSizeKiB"`
	Concurrency       int  `json:"concurrency"`
	DirectIO          bool `json:"directIO"`
	FadviseSequential bool `json:"fadviseSequential"`
	// OnSymlink mirrors SyncRuleConfig.OnSymlink at the endpoint level so the
	// backend builder can read the policy without an interface change. The
	// runner copies SyncRuleConfig.OnSymlink into both Src and Dst before
	// invoking the builder; rule-level validation remains authoritative.
	// Only local backends consume it; s3/cfs builders log a warning and ignore.
	OnSymlink string `json:"onSymlink,omitempty"`
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
	// Parallelism controls the per-shard, per-syncnode in-process file
	// concurrency (how many objects a single syncnode worker copies in
	// parallel). It is INDEPENDENT of ShardCount (which controls cross-
	// syncnode fan-out). 0 means "syncnode default".
	Parallelism int `json:"parallelism"`
	// ShardCount controls how many parallel sub-tasks the master fans a
	// single rule trigger into. Decoupled from Parallelism.
	//   - hash mode:  capped by min(ShardCount, online syncnodes); ≤1 = no fan-out
	//   - prefix mode: capped by min(ShardCount, len(ShardPrefixes))
	//   - auto mode:  uses ShardCount on prefix-cache hit; falls back to
	//                 hash with the same ShardCount on cache miss
	// Zero means "legacy fallback": derive from Parallelism when > 0
	// (backward compatibility for rules persisted before this field
	// landed); otherwise no fan-out (single dispatch).
	ShardCount int `json:"shardCount,omitempty"`
	// ShardingStrategy selects how the master fans a single rule
	// trigger into N sub-tasks across the cluster:
	//   "" / "hash"  → FNV-1a hash on object key (default; even distribution)
	//   "prefix"     → use ShardPrefixes literally; len(ShardPrefixes) defines N
	//   "auto"       → master probes backend top-level prefixes at fire time
	ShardingStrategy string `json:"shardingStrategy"`
	// ShardPrefixes carries operator-declared partition prefixes for the
	// "prefix" strategy. Optional for "auto" (acts as a whitelist).
	ShardPrefixes []string `json:"shardPrefixes,omitempty"`

	// ChecksumMode controls the post-copy verification strictness.
	//   ""           → "size_etag" legacy default
	//   "size_etag"  → size + etag (when both sides have one)
	//   "strong"     → ALWAYS compute sha256 src-side; compare against dst checksum.
	//                  REQUIRED for AfterCopy=verify_then_delete_src; with any other
	//                  value the executor refuses to delete src.
	ChecksumMode string `json:"checksumMode,omitempty"`

	// OnSourceMutated controls behaviour when src key changes (size/mtime/etag)
	// between pre-transfer Head and post-transfer Head.
	//   ""      → "fail" default
	//   "fail"  → error the file; counted in FilesFailed; never deletes src
	//   "skip"  → log + skip; counted in FilesSkipped; does not delete src
	//   "retry" → re-fetch & re-upload up to MaxRetries; failed after exhaustion
	OnSourceMutated string `json:"onSourceMutated,omitempty"`

	// MaxRetries is the per-file retry cap. 0 means 1 attempt total (current
	// behaviour). Recommended value: 3 with exponential backoff (1s,2s,4s,...,30s).
	MaxRetries int `json:"maxRetries,omitempty"`

	// ResumeEnabled toggles the breakpoint-resume code path. When true:
	//   - executor consults bolt.InProgressStore at file start and resumes from
	//     BytesDone (POSIX/CFS) or UploadID (s3 multipart);
	//   - on each successful Put, the breakpoint is cleared.
	ResumeEnabled bool `json:"resumeEnabled,omitempty"`

	// OnExisting selects how the executor decides whether to overwrite an
	// already-present dst object. Matches the rclone gap-fill roadmap
	// (docs/plan/syncnode/rclone-gap-roadmap.md 子项 3):
	//   ""                  → verify_then_skip (legacy default, back-compat)
	//   "verify_then_skip"  → size + checksum/ETag; skip only when equal
	//   "always_skip"       → rclone --ignore-existing; never re-upload
	//   "newer_only"        → rclone --update; skip when dst.Mtime ≥ src.Mtime
	//                         (1s cross-backend tolerance)
	//   "overwrite"         → rclone --ignore-times; always re-upload
	// For type=move only "" / "verify_then_skip" are accepted: the other
	// strategies risk silent data loss when paired with src deletion.
	OnExisting string `json:"onExisting,omitempty"`

	// OnSymlink controls how the local backend treats symbolic links during
	// List / resolve. Applies only when at least one endpoint is local; s3/cfs
	// backends ignore the field (with a warn log from the backend builder).
	//   ""       → "skip" default; legacy behaviour, back-compat
	//   "skip"   → silently skip symlinks during List; reject symlinked keys at resolve
	//   "follow" → treat each symlink as the file it points to (os.Stat
	//              semantics); EvalSymlinks may resolve across AllowedRoots
	//              boundaries, but the final path must still resolve under the
	//              configured AllowedRoots union
	//   "error"  → emit a backend.Entry{Err: ...} for each symlink and fail the
	//              List; never silently skip
	OnSymlink string `json:"onSymlink,omitempty"`

	// DryRun toggles the executor's "演练" mode (rclone --dry-run parity, 子项 2).
	// When true the executor still walks the source listing, evaluates filters
	// and idempotency checks, and emits per-file structured "would_*" events
	// (would_copy / would_skip_existing / would_server_side_copy /
	// would_delete_src) so operators can preview the effect of a rule before
	// arming destructive options. NO writes / deletes / server-side copies hit
	// either backend while DryRun is true. The task still terminates Done on
	// success — callers distinguish演练 vs real runs by reading this flag back
	// off the rule config (and the DryRunStats counters surfaced by the
	// executor).
	//
	// dry-run is a prerequisite for type=mirror (子项 6 / wave 3): the first
	// pass of a mirror rule defaults DryRun=true so the operator can see which
	// dst entries would be deleted before actually deleting them.
	DryRun bool `json:"dryRun,omitempty"`

	// Confirm pairs with DryRun for destructive task types (type=move and the
	// upcoming type=mirror). It is the operator's explicit acknowledgement
	// that they have reviewed a prior演练 (DryRun=true) and accept the
	// destructive plan; the executor refuses to start a destructive task with
	// Confirm=true unless the same task also sets DryRun=true (i.e. the
	// caller is asking for a fresh演练 with the confirmation bit pre-set), so
	// the only way to actually mutate state is: (1) run with DryRun=true and
	// review the events, (2) re-run with DryRun=false AND Confirm=false (or
	// drop Confirm entirely). The redundancy is intentional — a single
	// boolean flip should not turn a演练 into a real delete.
	//
	// Validation: Confirm=true + DryRun=false on a destructive task →
	// validateTask rejects with "dry-run confirmation required: set
	// DryRun=true to preview first". Non-destructive task types ignore
	// Confirm (the rule-level config validator forbids Confirm=true outside
	// destructive types so the field cannot accumulate unused state).
	Confirm bool `json:"confirm,omitempty"`

	// PreserveMode toggles persisting the source POSIX file mode bits
	// (rwx + setuid/setgid/sticky) on the destination. See plan doc
	// docs/plan/syncnode/posix-metadata-preservation.md.
	//   - local dst : syscall.Chmod after rename
	//   - cfs   dst : mw.Setattr with proto.AttrMode
	//   - s3    dst : x-amz-meta-syncnode-mode header (octal string);
	//                 Stat falls back to rclone naked `x-amz-meta-mode`
	// When the dst backend reports !Caps.NativeModeWrite the executor
	// honors OnMetadataUnsupported (warn / skip / error).
	PreserveMode bool `json:"preserveMode,omitempty"`

	// PreserveOwner persists POSIX uid AND gid as a single switch. uid and
	// gid are almost always set together via Chown, so a single switch
	// matches the operator mental model; split this only if a real
	// "preserve gid but not uid" use case emerges.
	//   - local dst : syscall.Lchown
	//   - cfs   dst : mw.Setattr with proto.AttrUid|proto.AttrGid
	//   - s3    dst : x-amz-meta-syncnode-uid / x-amz-meta-syncnode-gid
	//                 (decimal). Stat falls back to rclone naked
	//                 `x-amz-meta-uid` / `x-amz-meta-gid`.
	// EPERM on a non-root syncnode process is treated as Caps mismatch →
	// OnMetadataUnsupported.
	PreserveOwner bool `json:"preserveOwner,omitempty"`

	// PreserveXattr persists user.* and system.posix_acl_* extended
	// attributes. Other namespaces (security.* / trusted.* / other
	// system.*) are filtered server-side by the executor — they tend to
	// be LSM- or kernel-managed and rarely meaningful to migrate.
	//
	// Wire encoding on s3:
	//   x-amz-meta-syncnode-xattrs = base64(JSON({name: base64(value), ...}))
	// S3 user-metadata is capped at 2 KiB total; if encoded payload +
	// other syncnode headers exceed the budget the executor falls back to
	// OnMetadataUnsupported (warn/skip/error).
	//
	// POSIX ACL is intentionally NOT a first-class field: the kernel
	// stores it in system.posix_acl_access / system.posix_acl_default
	// xattrs already, so PreserveXattr covers it on local↔local and
	// local↔cfs. Cross-backend POSIX↔S3 ACL translation is explicitly
	// out of scope (rclone behaves the same way) — set the endpoint-level
	// S3 canned ACL via SyncEndpointConfig if you need a bucket-wide policy.
	PreserveXattr bool `json:"preserveXattr,omitempty"`

	// OnMetadataUnsupported controls behaviour when the dst backend cannot
	// honor a requested PreserveXxx (Caps mismatch, S3 user-metadata
	// budget overflow, non-root Chown EPERM, etc).
	//   ""      → "warn" default; record stats + log + continue with body
	//   "warn"  → same as default; the file is still transferred
	//   "skip"  → the entire file is skipped (counted in FilesSkipped)
	//   "error" → the file fails the task (counted in FilesFailed)
	// The choice trades off "best-effort migration" (warn) versus
	// "strict fidelity" (error) — pick error only when the destination
	// MUST not drift in mode/owner/xattr.
	OnMetadataUnsupported string `json:"onMetadataUnsupported,omitempty"`
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
