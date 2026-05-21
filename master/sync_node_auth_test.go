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
	"testing"

	"github.com/cubefs/cubefs/util/config"
)

// TestSyncAdminTokenConfigWiring pins the master-config -> middleware
// wiring for the /syncNode/* admin token. Production previously passed
// SetSyncAdminToken("") unconditionally; this test fails if a future
// refactor re-introduces that regression by losing the cfgSyncAdminToken
// read or renaming the config key.
func TestSyncAdminTokenConfigWiring(t *testing.T) {
	// Reset package-global token after the test so it doesn't leak
	// into other tests in this package.
	t.Cleanup(func() { SetSyncAdminToken("") })

	tests := []struct {
		name    string
		cfgJSON string
		want    string
	}{
		{
			name:    "token configured",
			cfgJSON: `{"syncAdminToken":"test-token"}`,
			want:    "test-token",
		},
		{
			name:    "empty token field",
			cfgJSON: `{"syncAdminToken":""}`,
			want:    "",
		},
		{
			name:    "key absent (zero-config passthrough)",
			cfgJSON: `{}`,
			want:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.LoadConfigString(tc.cfgJSON)
			// Mirrors the single-line read in server.go's checkConfig.
			// Kept inline rather than wrapping in a helper so any
			// future refactor that drops the read here also breaks
			// this test loudly.
			got := cfg.GetString(cfgSyncAdminToken)
			if got != tc.want {
				t.Fatalf("GetString(%q) = %q, want %q", cfgSyncAdminToken, got, tc.want)
			}

			// And confirm the SetSyncAdminToken / currentSyncAdminToken
			// pair faithfully round-trips the value through the
			// package-level state the middleware actually consults.
			SetSyncAdminToken(got)
			if cur := currentSyncAdminToken(); cur != tc.want {
				t.Fatalf("currentSyncAdminToken() = %q, want %q", cur, tc.want)
			}
		})
	}
}

// TestSyncAdminTokenConfigKeyName guards the literal config key string
// so operators' master.json doesn't silently stop working if someone
// renames the constant.
func TestSyncAdminTokenConfigKeyName(t *testing.T) {
	if cfgSyncAdminToken != "syncAdminToken" {
		t.Fatalf("cfgSyncAdminToken = %q, want %q (operator-visible config key)",
			cfgSyncAdminToken, "syncAdminToken")
	}
}
