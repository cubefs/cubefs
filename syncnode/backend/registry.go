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

package backend

import (
	"fmt"
	"sort"
	"sync"
)

// Constructor builds a Backend from a typed config. Each implementation
// package (s3, local, cfs) registers its constructor via the init() pattern.
// The cfg parameter is opaque to the registry — implementations cast it to
// their own concrete config type. See e.g. backend/local.Config.
type Constructor func(cfg interface{}) (Backend, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Constructor)
)

// Register a constructor under kind. Called from each backend package's
// init(). Panics on duplicate registration (programmer error).
func Register(kind string, c Constructor) {
	if kind == "" {
		panic("backend.Register: empty kind")
	}
	if c == nil {
		panic("backend.Register: nil constructor for kind " + kind)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[kind]; exists {
		panic("backend.Register: duplicate kind " + kind)
	}
	registry[kind] = c
}

// New constructs a Backend by kind. cfg is implementation-specific (see
// each backend package's NewConfig / Config type).
func New(kind string, cfg interface{}) (Backend, error) {
	registryMu.RLock()
	c, ok := registry[kind]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("backend: unknown kind %q (registered: %v)", kind, registeredKinds())
	}
	return c(cfg)
}

// registeredKinds returns the sorted list of currently registered kinds —
// for error messages and debugging.
func registeredKinds() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// RegisteredKinds is the exported version of registeredKinds — useful for
// tests and version-info endpoints.
func RegisteredKinds() []string { return registeredKinds() }
