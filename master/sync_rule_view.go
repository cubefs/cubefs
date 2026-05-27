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

package master

import (
	"github.com/cubefs/cubefs/proto"
)

// redactedMask is the placeholder substituted for plaintext S3 credentials
// in all HTTP responses returning a SyncRule. The same constant is the
// signal cubefs-mcp / dashboard frontends use to detect "this came back
// already-redacted, don't echo it back into an update". See plan doc
// docs/plan/mcp/healthcheck-findings-fixes.md §P0.
const redactedMask = "***"

// redactedSyncRule returns a copy of in with AK/SK fields on both endpoints
// replaced by redactedMask when they are non-empty. The persistence path
// is unaffected: SyncRuleCache.Get/List returns shared pointers, so we MUST
// deep-copy before mutating — otherwise we'd corrupt the in-memory cache
// (and the next syncUpdateSyncRule would persist `"***"` to rocksdb).
//
// SyncRuleConfig is a value type embedded by value in SyncRule, so a plain
// `out := *in` already deep-copies Config (and SyncEndpointConfig within
// it). The only aliasing risk would be Config.ShardPrefixes (a slice), but
// we never touch it here.
//
// Returns nil when in is nil so list redactors can short-circuit safely.
func redactedSyncRule(in *proto.SyncRule) *proto.SyncRule {
	if in == nil {
		return nil
	}
	out := *in
	if out.Config.Src.AccessKey != "" {
		out.Config.Src.AccessKey = redactedMask
	}
	if out.Config.Src.SecretKey != "" {
		out.Config.Src.SecretKey = redactedMask
	}
	if out.Config.Dst.AccessKey != "" {
		out.Config.Dst.AccessKey = redactedMask
	}
	if out.Config.Dst.SecretKey != "" {
		out.Config.Dst.SecretKey = redactedMask
	}
	return &out
}

// redactedSyncRules maps redactedSyncRule over a slice for /syncRule/list.
// Preserves order; nil entries are passed through (they redact to nil).
func redactedSyncRules(in []*proto.SyncRule) []*proto.SyncRule {
	if in == nil {
		return nil
	}
	out := make([]*proto.SyncRule, 0, len(in))
	for _, r := range in {
		out = append(out, redactedSyncRule(r))
	}
	return out
}
