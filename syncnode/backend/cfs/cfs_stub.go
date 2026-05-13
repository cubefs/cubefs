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

//go:build !linux

// Package cfs provides a stub for the cfs backend on non-linux platforms.
// The real implementation (cfs.go) requires the CubeFS SDK which depends on
// linux syscalls. On other platforms callers see a registration, but the
// constructor always fails with ErrConfigInvalid so that referencing the
// backend by kind="cfs" is detectable in tests and config validation, but
// does not silently succeed and then crash later.
package cfs

import (
	"fmt"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// Config mirrors the linux Config so cross-platform code can still build
// callers that construct a *Config. The fields are documented in cfs.go.
type Config struct {
	Masters       []string
	Volume        string
	LogDir        string
	LogLevel      string
	ReadChunkSize int
	ReadPrefetch  int
	WriteChunkMiB int
	WriteParallel int
}

func init() {
	backend.Register("cfs", New)
}

// New returns ErrConfigInvalid wrapped with a build-constraint message —
// the cfs backend cannot run on non-linux platforms because the SDK
// (sdk/data/stream, sdk/meta) only builds for linux.
func New(_ interface{}) (backend.Backend, error) {
	return nil, fmt.Errorf("%w: cfs backend requires linux build", backend.ErrConfigInvalid)
}
