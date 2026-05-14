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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// Phase P2-5b — master-side auto-prefix probe.
//
// SyncRuleManager.dispatchRule with strategy=="auto" calls
// probePrefixes to discover top-level prefixes from the rule's source
// endpoint. The probe is sent as OpSyncNodeListPrefixes via the
// lowest-LoadScore syncnode's TaskManager (one-shot fire-and-forget
// from the master side; the syncnode's response lands back on the
// /syncNode/response endpoint where we already have a wired hook).
//
// To keep the dispatch path synchronous, we cache successful probe
// results per (ruleID, endpoint signature) for syncPrefixCacheTTL (5
// min by default). A cache miss + no fresh syncnode response yields a
// FALLBACK to hash mode plus an alert metric — operators see the
// degradation instead of a silent retry storm.

// syncPrefixCacheTTL controls how long a probe result stays valid
// before master re-asks a syncnode. 5 minutes balances "react to new
// top-level dirs that operators just created" with "don't hammer the
// backend on every fire" — tweak by promoting to a SyncRule field if
// a real workload needs faster re-probe.
const syncPrefixCacheTTL = 5 * time.Minute

// syncPrefixCache stores per-rule prefix probe results with TTL.
type syncPrefixCache struct {
	mu    sync.Mutex
	items map[string]syncPrefixCacheItem
}

type syncPrefixCacheItem struct {
	prefixes  []string
	expiresAt time.Time
}

// newSyncPrefixCache constructs an empty cache.
func newSyncPrefixCache() *syncPrefixCache {
	return &syncPrefixCache{items: make(map[string]syncPrefixCacheItem)}
}

// get returns cached prefixes if the entry is fresh; nil + false
// otherwise.
func (c *syncPrefixCache) get(key string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(item.expiresAt) {
		delete(c.items, key)
		return nil, false
	}
	out := make([]string, len(item.prefixes))
	copy(out, item.prefixes)
	return out, true
}

// put inserts / replaces the entry with TTL syncPrefixCacheTTL.
func (c *syncPrefixCache) put(key string, prefixes []string) {
	if len(prefixes) == 0 {
		return
	}
	cp := make([]string, len(prefixes))
	copy(cp, prefixes)
	c.mu.Lock()
	c.items[key] = syncPrefixCacheItem{
		prefixes:  cp,
		expiresAt: time.Now().Add(syncPrefixCacheTTL),
	}
	c.mu.Unlock()
}

// invalidate drops the cache entry for key. Useful when a probe
// returned an error so the next fire re-tries instead of using a stale
// success.
func (c *syncPrefixCache) invalidate(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// syncPrefixCacheKey is the canonical key for a rule's probe cache
// entry: rule id + source kind + source identity (endpoint+bucket+vol+
// path). Two rules pointing at the same source share probe results.
func syncPrefixCacheKey(rule *proto.SyncRule) string {
	if rule == nil {
		return ""
	}
	src := rule.Config.Src
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		rule.ID(), src.Kind, src.Endpoint, src.Bucket, src.Vol, src.Path)
}

// probePrefixes asks the lowest-load syncnode to enumerate the top-
// level prefixes under rule.Src and returns them. On failure (no
// candidates, timeout, syncnode error) returns an error so the caller
// can fall back to hash mode.
//
// The reply path is asynchronous: master's TaskManager.AddTask wraps
// the AdminTask + delivers via the existing TCP channel; the syncnode
// replies on the connection, which lands back on master via the same
// AdminTaskManager loop. We don't have a synchronous wait here yet —
// the cache is updated by handleSyncNodeTaskResponse when a
// ListPrefixes reply arrives. Phase 5b leaves the probe ASYNCHRONOUS:
// the first fire returns ErrPrefixProbePending (caller falls back to
// hash), the second fire after probe lands consumes the cache.
//
// This trade-off keeps the fire path non-blocking — a slow probe
// can't stall the cron tick. Operators see one hash-mode fire followed
// by prefix-mode fires until the cache TTL expires.
func (m *SyncRuleManager) probePrefixes(rule *proto.SyncRule) ([]string, error) {
	if m == nil || m.cluster == nil {
		return nil, fmt.Errorf("nil manager")
	}
	disp := m.cluster.syncDispatcher
	if disp == nil {
		return nil, fmt.Errorf("syncDispatcher not initialised")
	}
	cands := disp.Candidates(dispatcherStaleness)
	if len(cands) == 0 {
		return nil, ErrNoCandidates
	}
	// Pick the lowest-LoadScore candidate (Candidates returns sorted).
	addr := cands[0]
	sn, ok := m.cluster.lookupSyncNodeForProbe(addr)
	if !ok {
		return nil, fmt.Errorf("syncnode %s vanished between Candidates() and probe", addr)
	}
	req := &proto.SyncListPrefixesRequest{
		Endpoint:    rule.Config.Src,
		Prefix:      syncProbePrefix(rule),
		Delimiter:   "/",
		MaxPrefixes: proto.SyncListPrefixesMaxDefault,
	}
	task := proto.NewAdminTask(proto.OpSyncNodeListPrefixes, addr, req)
	sn.TaskManager.AddTask(task)
	log.LogInfof("SyncRuleManager.probePrefixes rule=%q dispatched to %s prefix=%q",
		rule.ID(), addr, req.Prefix)
	return nil, errSyncPrefixProbePending
}

// errSyncPrefixProbePending is the sentinel returned when a probe was
// dispatched but the reply hasn't landed yet. Callers fall back to
// hash mode and try again next fire.
var errSyncPrefixProbePending = fmt.Errorf("prefix probe dispatched, falling back to hash this fire")

// syncProbePrefix returns the source-side prefix to enumerate. For s3
// this is the rule's prefix (CommonPrefixes underneath); for local /
// cfs it's the source path; sane default is "" so the backend lists
// from the root.
func syncProbePrefix(rule *proto.SyncRule) string {
	if rule == nil {
		return ""
	}
	src := rule.Config.Src
	switch src.Kind {
	case "s3":
		return src.Prefix
	case "cfs", "local":
		return src.Path
	default:
		return ""
	}
}

// handleSyncListPrefixesReply parses a SyncListPrefixesReply out of an
// AdminTask response and feeds the cache. Called from
// handleSyncNodeTaskResponse when proto.OpSyncNodeListPrefixes lands.
// Soft on errors: a bad reply doesn't break the dispatch path, it just
// leaves the cache stale.
func (m *SyncRuleManager) handleSyncListPrefixesReply(ruleID string, body []byte) {
	if m == nil || m.cluster == nil || ruleID == "" || len(body) == 0 {
		return
	}
	rule := m.cluster.syncRuleCache.Get(ruleID)
	if rule == nil {
		log.LogWarnf("handleSyncListPrefixesReply: rule %q not in cache", ruleID)
		return
	}
	var reply proto.SyncListPrefixesReply
	if err := json.Unmarshal(body, &reply); err != nil {
		log.LogWarnf("handleSyncListPrefixesReply rule=%q decode failed: %v", ruleID, err)
		return
	}
	if reply.Err != "" {
		log.LogWarnf("handleSyncListPrefixesReply rule=%q syncnode reported: %s", ruleID, reply.Err)
		return
	}
	if len(reply.Prefixes) == 0 {
		log.LogWarnf("handleSyncListPrefixesReply rule=%q empty prefix set; cache not updated", ruleID)
		return
	}
	key := syncPrefixCacheKey(rule)
	m.prefixCache.put(key, reply.Prefixes)
	log.LogInfof("handleSyncListPrefixesReply rule=%q cached %d prefix(es)", ruleID, len(reply.Prefixes))
}

// lookupSyncNodeForProbe is the Cluster-side helper the manager uses
// to resolve a SyncNode by addr. Mirrors api_service_sync_node.go's
// lookup but returns a bool (no error type) since the probe path
// already has fallback semantics.
func (c *Cluster) lookupSyncNodeForProbe(addr string) (*SyncNode, bool) {
	value, ok := c.syncNodes.Load(addr)
	if !ok {
		return nil, false
	}
	sn, ok := value.(*SyncNode)
	if !ok || sn == nil {
		return nil, false
	}
	return sn, true
}
