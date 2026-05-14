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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cubefs/cubefs/syncnode/bolt"
	"github.com/cubefs/cubefs/syncnode/rules"
)

// newReloadTestSyncNode wires up enough of a SyncNode to exercise reload's
// validate → upsert → swap path WITHOUT the HTTP/TCP listeners or the
// scheduler. Returns the node + the on-disk config path tests can edit.
func newReloadTestSyncNode(t *testing.T, initialJSON string) *SyncNode {
	t.Helper()

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "sync.json")
	if err := os.WriteFile(cfgPath, []byte(initialJSON), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	sc, err := ParseSyncConfig([]byte(initialJSON))
	if err != nil {
		t.Fatalf("ParseSyncConfig: %v", err)
	}

	dataDir := t.TempDir()
	db, err := bolt.Open(filepath.Join(dataDir, "syncnode.db"))
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s := &SyncNode{
		cfg:       sc,
		cfgPath:   cfgPath,
		boltDB:    db,
		ruleStore: db.RuleStore(),
	}
	// Seed bootstrap rules.
	if err := s.bootstrapRulesFromConfig(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return s
}

func TestReload_SyntaxErrorKeepsOldConfig(t *testing.T) {
	original := `{
		"role": "sync",
		"masterAddr": "m:1",
		"dataDir": "/tmp/x",
		"rules": []
	}`
	s := newReloadTestSyncNode(t, original)
	oldMaster := s.cfg.MasterAddr

	// Overwrite the config file with malformed JSON.
	if err := os.WriteFile(s.cfgPath, []byte("{ this is not json"), 0o644); err != nil {
		t.Fatalf("write bad cfg: %v", err)
	}
	before := reloadFailuresTotal.Load()

	if err := s.reload(context.Background()); err == nil {
		t.Fatal("reload should fail on syntax error")
	}
	if got := reloadFailuresTotal.Load(); got != before+1 {
		t.Errorf("reloadFailuresTotal = %d, want %d", got, before+1)
	}
	if s.cfg.MasterAddr != oldMaster {
		t.Errorf("config was swapped despite error: master=%q", s.cfg.MasterAddr)
	}
}

func TestReload_ValidConfigUpserts(t *testing.T) {
	original := `{
		"role": "sync",
		"masterAddr": "m:1",
		"dataDir": "/tmp/x",
		"posix": {"allowedRoots": ["/tmp"]},
		"rules": [
			{
				"id": "r1",
				"type": "sync",
				"src": {"kind": "local", "path": "/tmp/a"},
				"dst": {"kind": "s3", "bucket": "b1", "prefix": "x"},
				"afterCopy": "keep",
				"downloadStrategy": "temp_rename",
				"onMismatch": "alert"
			}
		]
	}`
	s := newReloadTestSyncNode(t, original)

	// Confirm bootstrap landed.
	stored, err := s.ruleStore.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(stored) != 1 || stored[0].ID() != "r1" {
		t.Fatalf("bootstrap unexpected: %+v", stored)
	}

	// Edit the on-disk config: add a second rule.
	updated := `{
		"role": "sync",
		"masterAddr": "m:1",
		"dataDir": "/tmp/x",
		"posix": {"allowedRoots": ["/tmp"]},
		"rules": [
			{
				"id": "r1",
				"type": "sync",
				"src": {"kind": "local", "path": "/tmp/a"},
				"dst": {"kind": "s3", "bucket": "b1", "prefix": "x"},
				"afterCopy": "keep",
				"downloadStrategy": "temp_rename",
				"onMismatch": "alert"
			},
			{
				"id": "r2",
				"type": "sync",
				"src": {"kind": "local", "path": "/tmp/b"},
				"dst": {"kind": "s3", "bucket": "b1", "prefix": "y"},
				"afterCopy": "keep",
				"downloadStrategy": "temp_rename",
				"onMismatch": "alert"
			}
		]
	}`
	if err := os.WriteFile(s.cfgPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("rewrite cfg: %v", err)
	}
	if err := s.reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	stored, err = s.ruleStore.List(context.Background())
	if err != nil {
		t.Fatalf("list after reload: %v", err)
	}
	if len(stored) != 2 {
		t.Errorf("got %d rules after reload, want 2", len(stored))
	}
}

func TestReload_ConflictRejectsAtomically(t *testing.T) {
	original := `{
		"role": "sync",
		"masterAddr": "m:1",
		"dataDir": "/tmp/x",
		"posix": {"allowedRoots": ["/tmp"]},
		"rules": [
			{
				"id": "r1",
				"type": "sync",
				"src": {"kind": "local", "path": "/tmp/a"},
				"dst": {"kind": "s3", "bucket": "b1", "prefix": "x"},
				"afterCopy": "keep",
				"downloadStrategy": "temp_rename",
				"onMismatch": "alert"
			}
		]
	}`
	s := newReloadTestSyncNode(t, original)

	// Conflict: r2 has identical src+dst to r1 → duplicate-pair (E-4 code 1014).
	bad := `{
		"role": "sync",
		"masterAddr": "m:1",
		"dataDir": "/tmp/x",
		"posix": {"allowedRoots": ["/tmp"]},
		"rules": [
			{
				"id": "r1",
				"type": "sync",
				"src": {"kind": "local", "path": "/tmp/a"},
				"dst": {"kind": "s3", "bucket": "b1", "prefix": "x"},
				"afterCopy": "keep",
				"downloadStrategy": "temp_rename",
				"onMismatch": "alert"
			},
			{
				"id": "r2",
				"type": "sync",
				"src": {"kind": "local", "path": "/tmp/a"},
				"dst": {"kind": "s3", "bucket": "b1", "prefix": "x"},
				"afterCopy": "keep",
				"downloadStrategy": "temp_rename",
				"onMismatch": "alert"
			}
		]
	}`
	if err := os.WriteFile(s.cfgPath, []byte(bad), 0o644); err != nil {
		t.Fatalf("rewrite cfg: %v", err)
	}
	before := reloadFailuresTotal.Load()
	if err := s.reload(context.Background()); err == nil {
		t.Fatal("reload should fail on conflict")
	}
	if got := reloadFailuresTotal.Load(); got != before+1 {
		t.Errorf("reloadFailuresTotal = %d, want %d", got, before+1)
	}
	stored, _ := s.ruleStore.List(context.Background())
	if len(stored) != 1 {
		t.Errorf("rule store mutated despite conflict: have %d, want 1", len(stored))
	}
}

func TestReload_NoCfgPathErrors(t *testing.T) {
	s := &SyncNode{cfg: &SyncConfig{}}
	if err := s.reload(context.Background()); err == nil {
		t.Fatal("reload without cfgPath should fail")
	}
}

func TestHandleReload_HTTP(t *testing.T) {
	original := `{
		"role": "sync",
		"masterAddr": "m:1",
		"dataDir": "/tmp/x",
		"rules": []
	}`
	s := newReloadTestSyncNode(t, original)

	// Trigger via the HTTP handler — exercise the api envelope path too.
	req := httptest.NewRequest(http.MethodPost, "/admin/syncnode/reload", nil)
	payload, err := s.handleReload(req)
	if err != nil {
		t.Fatalf("handleReload: %v", err)
	}
	m, ok := payload.(map[string]interface{})
	if !ok {
		t.Fatalf("payload type = %T", payload)
	}
	if r, _ := m["reloaded"].(bool); !r {
		t.Errorf("reloaded = %v, want true", m["reloaded"])
	}
}

// Compile-time guard: rules.Store satisfies the runner's RuleLookup
// requirement out of the box (it has Get(ctx, id) (*Rule, error)).
var _ interface {
	Get(ctx context.Context, id string) (*rules.Rule, error)
} = (rules.Store)(nil)
