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

package contract_test

import (
	"testing"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/backend/contract"
	"github.com/cubefs/cubefs/syncnode/backend/local"
)

// TestLocalBackendContract runs the shared contract suite against the
// local POSIX backend. Other backends (s3, cfs) hook into the same suite
// from their own _test.go files so the contract stays portable.
func TestLocalBackendContract(t *testing.T) {
	suite := &contract.Suite{
		Name: "local",
		Setup: func(t *testing.T) (backend.Backend, func()) {
			root := t.TempDir()
			cfg := &local.Config{
				AllowedRoots:         []string{root},
				DefaultBufferSizeKiB: 1024, // 1 MiB
				MaxDirDepth:          20,
			}
			b, err := backend.New("local", cfg)
			if err != nil {
				t.Fatalf("New local: %v", err)
			}
			return b, func() { _ = b.Close() }
		},
		// Restrict sizes to avoid blowing up test runtime: 1 KiB / 4 MiB
		// is enough to exercise the small-vs-buffered code paths.
		Sizes: []int{1 << 10, 4 << 20},
	}
	suite.Run(t)
}
