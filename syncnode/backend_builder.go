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
type backendBuilder struct {
	pool       *backend.Pool
	s3Defaults *S3DefaultsConfig
	posix      *PosixConfig
	masterAddr string // comma-separated; mirrored to cfs.Config.Masters
}

func newBackendBuilder(pool *backend.Pool, cfg *SyncConfig) *backendBuilder {
	return &backendBuilder{
		pool:       pool,
		s3Defaults: &cfg.S3Defaults,
		posix:      &cfg.Posix,
		masterAddr: cfg.MasterAddr,
	}
}

// Build returns a Backend for ep. The Pool caches per (kind, endpoint,
// region) tuple so multiple rules pointing at the same anchor share one
// HTTP/2 connection pool + credential refresher.
func (b *backendBuilder) Build(_ context.Context, ep *spec.EndpointConfig) (backend.Backend, error) {
	if ep == nil {
		return nil, fmt.Errorf("backendBuilder: nil EndpointConfig")
	}
	switch ep.Kind {
	case "cfs":
		return b.pool.Acquire(backend.PoolKey{Kind: "cfs"}, &cfs.Config{
			Masters: splitMasters(b.masterAddr),
			Volume:  ep.Vol,
		})
	case "s3":
		endpoint := firstNonEmpty(ep.Endpoint, b.s3Defaults.Endpoint)
		region := firstNonEmpty(ep.Region, b.s3Defaults.Region)
		storageClass := firstNonEmpty(ep.StorageClass, b.s3Defaults.StorageClass)
		return b.pool.Acquire(backend.PoolKey{
			Kind:     "s3",
			Endpoint: endpoint,
			Region:   region,
		}, &s3.Config{
			Endpoint:     endpoint,
			Region:       region,
			Bucket:       ep.Bucket,
			AccessKeyEnv: b.s3Defaults.AccessKeyEnv,
			SecretKeyEnv: b.s3Defaults.SecretKeyEnv,
			StorageClass: storageClass,
		})
	case "local":
		bufKiB := ep.BufferSizeKiB
		if bufKiB == 0 {
			bufKiB = b.posix.DefaultBufferSizeKiB
		}
		return b.pool.Acquire(backend.PoolKey{Kind: "local"}, &local.Config{
			AllowedRoots:         b.posix.AllowedRoots,
			DefaultBufferSizeKiB: bufKiB,
			MaxDirDepth:          b.posix.MaxDirDepth,
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
