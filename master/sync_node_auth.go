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
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
)

// -----------------------------------------------------------------------
// SyncNode admin-token middleware (SEC1)
//
// Gate the four /syncNode/* admin routes (add, list, dispatch, getQuota)
// behind a shared bearer token. Empty configured token disables the
// check — preserves the pre-existing "open" behavior for tests and dev
// environments that don't configure cfgSyncAdminToken.
//
// Token lookup order:
//   1. Authorization: Bearer <token>
//   2. X-Sync-Token: <token>
//
// Comparison uses crypto/subtle.ConstantTimeCompare to avoid timing
// oracles. The middleware is intentionally local — a master-wide auth
// refactor is out of scope.
// -----------------------------------------------------------------------

var (
	syncAdminTokenMu sync.RWMutex
	syncAdminToken   string
)

// SetSyncAdminToken installs the shared token used by the /syncNode/*
// middleware. Empty string disables the check. Thread-safe; can be
// invoked again at runtime on config reload.
func SetSyncAdminToken(t string) {
	syncAdminTokenMu.Lock()
	defer syncAdminTokenMu.Unlock()
	syncAdminToken = t
}

// currentSyncAdminToken returns the currently-installed token.
// Exported via the unexported accessor so tests can probe it.
func currentSyncAdminToken() string {
	syncAdminTokenMu.RLock()
	defer syncAdminTokenMu.RUnlock()
	return syncAdminToken
}

// requireSyncAdminToken wraps an http.HandlerFunc and rejects requests
// whose Authorization / X-Sync-Token header doesn't match the
// configured token. If no token is configured the wrapper is a
// pass-through (preserves zero-config defaults).
func requireSyncAdminToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expected := currentSyncAdminToken()
		if expected == "" {
			next(w, r)
			return
		}
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if provided == "" {
			provided = r.Header.Get("X-Sync-Token")
		}
		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			http.Error(w, "missing or invalid sync admin token", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
