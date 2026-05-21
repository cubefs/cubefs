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

package syncnode

import (
	"context"
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/cfs"
	"github.com/cubefs/cubefs/syncnode/backend/local"
	"github.com/cubefs/cubefs/syncnode/backend/s3"
	"github.com/cubefs/cubefs/syncnode/spec"
)

// backendBuilder converts a rule's EndpointConfig into a constructed
// backend.Backend via the shared Pool. Implements tasks.BackendBuilder.
//
// Defaults from SyncConfig (s3Defaults, posix.allowedRoots, masterAddr) are
// applied here so individual EndpointConfigs in a rule can stay terse — see
// design.md §10.6 (Backend abstraction).
//
// FIX D — cfg pointer freshness: the builder MUST NOT cache pointers into
// the SyncNode's current *SyncConfig at construction time. A SIGHUP reload
// swaps s.cfg atomically; sub-pointers like &cfg.S3Defaults become stale.
// Instead the builder holds a cfgProvider that returns the live config on
// every Build call, with the read taken under SyncNode.cfgMu so reload
// can't race a half-swap into a Build.
type backendBuilder struct {
	pool   *backend.Pool
	cfgFn  func() *SyncConfig // returns the live SyncConfig under cfgMu
}

// newBackendBuilder takes a closure that returns the live config rather
// than caching pointers; cfgFn is invoked on every Build call so reload
// always wins. cfgFn may return nil during early shutdown — the Build
// path defensively treats nil as "no defaults".
func newBackendBuilder(pool *backend.Pool, cfgFn func() *SyncConfig) *backendBuilder {
	return &backendBuilder{pool: pool, cfgFn: cfgFn}
}

// Build returns a Backend for ep. The Pool caches per (kind, endpoint,
// region, bucket) tuple so multiple rules pointing at the same anchor
// share one HTTP/2 connection pool + credential refresher, while rules
// targeting different buckets (s3) or volumes (cfs) get distinct
// Backend instances — the concrete wrappers bake bucket/volume into
// their state at construction time.
func (b *backendBuilder) Build(_ context.Context, ep *spec.EndpointConfig) (backend.Backend, error) {
	if ep == nil {
		return nil, fmt.Errorf("backendBuilder: nil EndpointConfig")
	}
	// FIX D: read the LIVE config every call so SIGHUP-driven changes to
	// masterAddr / s3Defaults / posix take effect for the next Build.
	// Snapshot values into locals so the rest of the function is
	// race-free even if cfgFn returns a pointer the caller mutates next.
	cfg := b.cfgFn()
	var s3Defaults S3DefaultsConfig
	var posix PosixConfig
	var masterAddr string
	if cfg != nil {
		s3Defaults = cfg.S3Defaults
		posix = cfg.Posix
		masterAddr = cfg.MasterAddr
	}

	switch ep.Kind {
	case "cfs":
		return b.pool.Acquire(backend.PoolKey{Kind: "cfs", Bucket: ep.Vol}, &cfs.Config{
			Masters: splitMasters(masterAddr),
			Volume:  ep.Vol,
		})
	case "s3", "tos", "bos":
		// "tos" (Volcengine TOS) and "bos" (Baidu BOS) are S3-compatible;
		// they share the s3 backend implementation. ep.Kind is preserved in
		// PoolKey so TOS/BOS/S3 entries with identical coordinates don't collide.
		endpoint := firstNonEmpty(ep.Endpoint, s3Defaults.Endpoint)
		region := firstNonEmpty(ep.Region, s3Defaults.Region)
		storageClass := firstNonEmpty(ep.StorageClass, s3Defaults.StorageClass)
		accessKeyEnv := firstNonEmpty(ep.AccessKeyEnv, s3Defaults.AccessKeyEnv)
		secretKeyEnv := firstNonEmpty(ep.SecretKeyEnv, s3Defaults.SecretKeyEnv)
		// CredKey disambiguates pool entries that share endpoint+bucket but use
		// different credentials. For inline AK (dashboard Approach C) we use
		// the AK value itself; for env-var creds we use the env var name.
		credKey := accessKeyEnv
		if ep.AccessKey != "" {
			credKey = ep.AccessKey
		}
		return b.pool.Acquire(backend.PoolKey{
			Kind:     "s3", // tos/bos use the s3 constructor; PoolKey.Kind must match a registered kind
			Endpoint: endpoint,
			Region:   region,
			Bucket:   ep.Bucket,
			CredKey:  credKey,
		}, &s3.Config{
			Endpoint:           endpoint,
			Region:             region,
			Bucket:             ep.Bucket,
			AccessKey:          ep.AccessKey,
			SecretKey:          ep.SecretKey,
			AccessKeyEnv:       accessKeyEnv,
			SecretKeyEnv:       secretKeyEnv,
			StorageClass:       storageClass,
			InsecureSkipVerify: ep.InsecureSkipTLS,
			UsePathStyle:       ep.UsePathStyle,
		})
	case "local":
		bufKiB := ep.BufferSizeKiB
		if bufKiB == 0 {
			bufKiB = posix.DefaultBufferSizeKiB
		}
		return b.pool.Acquire(backend.PoolKey{Kind: "local"}, &local.Config{
			AllowedRoots:         posix.AllowedRoots,
			DefaultBufferSizeKiB: bufKiB,
			MaxDirDepth:          posix.MaxDirDepth,
		})
	default:
		return nil, fmt.Errorf("backendBuilder: unknown kind %q", ep.Kind)
	}
}

// splitMasters parses a comma-separated master address string into the slice
// shape cfs.Config wants. Empty input yields an empty slice — the cfs SDK
// will fail to connect, which is the right surface for missing config.
func splitMasters(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
