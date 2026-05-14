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

package proto

// Phase P2-5 — wire types for the auto-prefix probe.
//
// Flow:
//
//  1. SyncRuleManager.fireRule, for a rule with ShardingStrategy=="auto",
//     selects the lowest-load active syncnode and sends an
//     OpSyncNodeListPrefixes packet carrying SyncListPrefixesRequest.
//  2. The receiving syncnode's task_handler.handleListPrefixes builds a
//     backend (via backendBuilder) from req.Endpoint, calls
//     backend.List(prefix, recursive=false), collects CommonPrefixes
//     from the IsDir entries, and replies with SyncListPrefixesReply.
//  3. Master uses the reply to bucket-pack the discovered prefixes
//     and dispatches through SyncFanout.DispatchNWithPrefixBuckets.
//
// The probe is bounded by SyncListPrefixesTimeout server-side (the
// syncnode handler's own context) — a slow probe shouldn't block the
// cron tick. Master caches the prefix list per (ruleID + endpoint) for
// SyncListPrefixesCacheTTL to avoid hitting the backend on every fire.

// SyncListPrefixesRequest is the AdminTask.Request body for
// OpSyncNodeListPrefixes. The Endpoint is a full SyncEndpointConfig
// because the syncnode needs the credential env-var names + endpoint
// URL to build the backend instance; partial configs would force the
// syncnode to merge with stale defaults.
type SyncListPrefixesRequest struct {
	// Endpoint identifies WHERE to list — the same shape syncnode's
	// backendBuilder consumes for normal task dispatch.
	Endpoint SyncEndpointConfig `json:"endpoint"`
	// Prefix is the listing root within the endpoint. For s3, this is
	// the prefix relative to bucket; for cfs, it's the path under
	// the vol; for local, it's the directory path.
	Prefix string `json:"prefix"`
	// Delimiter is typically "/" (top-level dir listing). Empty means
	// the syncnode picks "/" (the only delimiter currently supported
	// across s3 / cfs / local backends).
	Delimiter string `json:"delimiter,omitempty"`
	// MaxPrefixes caps the result count. Zero / negative falls back to
	// SyncListPrefixesMaxDefault (256).
	MaxPrefixes int `json:"maxPrefixes,omitempty"`
}

// SyncListPrefixesReply is the AdminTask.Response body for
// OpSyncNodeListPrefixes. On success Prefixes is non-empty and Err is
// "". On failure Err carries a human-readable message and Prefixes is
// either partial or nil.
type SyncListPrefixesReply struct {
	Prefixes []string `json:"prefixes"`
	Err      string   `json:"err,omitempty"`
}

// SyncListPrefixesMaxDefault caps the number of returned prefixes when
// the request doesn't specify a limit. 256 is well above any realistic
// top-level layout (years × tenants × shards typically < 100).
const SyncListPrefixesMaxDefault = 256
