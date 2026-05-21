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

// Package spec carries the on-the-wire and on-disk configuration shapes
// for syncnode rules. As of P2 (rule store moved to master), the
// canonical type definitions live in proto/sync_rule.go; this package
// re-exports them via Go type aliases so existing syncnode callsites
// compile unchanged.
package spec

import "github.com/cubefs/cubefs/proto"

// RuleConfig is the on-disk schema for a single sync rule.
// Alias of proto.SyncRuleConfig.
type RuleConfig = proto.SyncRuleConfig

// EndpointConfig describes one source or destination of a rule.
// Alias of proto.SyncEndpointConfig.
type EndpointConfig = proto.SyncEndpointConfig

// FilterConfig is the wire / persisted shape of executor.Filter.
// Alias of proto.SyncFilterConfig.
type FilterConfig = proto.SyncFilterConfig

// RetentionConfig is the wire shape of executor.Retention.
// Alias of proto.SyncRetentionConfig.
type RetentionConfig = proto.SyncRetentionConfig
