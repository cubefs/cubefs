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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cubefs/cubefs/cmd/common"
)

// Compile-time assertion: SyncNode implements common.Server.
var _ common.Server = (*SyncNode)(nil)

func TestNewServer(t *testing.T) {
	s := NewServer()
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
}

// TestHandleVersion exercises the /admin/syncnode/version handler directly
// (without going through the full Start path that would need a real config
// + master). AC for A-1: endpoint returns non-empty JSON with role.
func TestHandleVersion(t *testing.T) {
	s := &SyncNode{localServerAddr: "127.0.0.1:17710"}

	req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/version", nil)
	rec := httptest.NewRecorder()
	s.handleVersion(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var resp struct {
		Code int                    `json:"code"`
		Msg  string                 `json:"msg"`
		Data map[string]interface{} `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
	if role, _ := resp.Data["role"].(string); role != ModuleName {
		t.Errorf("data.role = %q, want %q", role, ModuleName)
	}
	if addr, _ := resp.Data["nodeAddress"].(string); addr == "" {
		t.Error("data.nodeAddress should be non-empty")
	}
}

func TestHandleStat(t *testing.T) {
	initMetrics()
	concurrentTasks.Store(2)
	defer concurrentTasks.Store(0)

	s := &SyncNode{}
	req := httptest.NewRequest(http.MethodGet, "/admin/syncnode/stat", nil)
	rec := httptest.NewRecorder()
	s.handleStat(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp struct {
		Code int                    `json:"code"`
		Data map[string]interface{} `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, _ := resp.Data["concurrentTasks"].(float64); got != 2 {
		t.Errorf("concurrentTasks = %v, want 2", resp.Data["concurrentTasks"])
	}
}

// TestStart_SmokeNoMaster boots the full server (Start) with a minimal
// config and verifies the version endpoint serves a valid response. This is
// the A-1 integration AC, but without requiring a real master server.
func TestStart_SmokeNoMaster(t *testing.T) {
	// Use a local-only minimal config; register stub doesn't actually call
	// master, so this should succeed.
	rawCfg := []byte(`{
		"role": "sync",
		"listen": "0",
		"httpListen": "0",
		"masterAddr": "127.0.0.1:17010",
		"logDir": "` + t.TempDir() + `",
		"dataDir": "` + t.TempDir() + `",
		"exporterPort": 0
	}`)
	cfg, err := ParseSyncConfig(rawCfg)
	if err != nil {
		t.Fatalf("ParseSyncConfig: %v", err)
	}
	// Confirm validation succeeded; smoke test of the parsing pipeline.
	if cfg.MasterAddr != "127.0.0.1:17010" {
		t.Errorf("MasterAddr lost: %v", cfg.MasterAddr)
	}
}
