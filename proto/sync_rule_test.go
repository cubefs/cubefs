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

package proto

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestSyncRule_JSONRoundTrip pins the wire shape that master persists in
// raft and exposes via /syncRule/*. The console will codegen against this
// schema, so any change to field names or zero-value handling must be
// intentional + reflected here.
func TestSyncRule_JSONRoundTrip(t *testing.T) {
	want := NewSyncRule(SyncRuleConfig{
		ID:       "r-1",
		Type:     "sync",
		Schedule: "*/5 * * * * *",
		Src: SyncEndpointConfig{
			Kind: "local",
			Path: "/srv/data/",
		},
		Dst: SyncEndpointConfig{
			Kind:            "s3",
			Endpoint:        "https://s3.example.com",
			Region:          "us-east-1",
			Bucket:          "backup",
			Prefix:          "cubefs/",
			StorageClass:    "STANDARD_IA",
			AccessKeyEnv:    "AWS_ACCESS_KEY_ID",
			SecretKeyEnv:    "AWS_SECRET_ACCESS_KEY",
			InsecureSkipTLS: true,
		},
		ShardingStrategy:            "prefix",
		ShardPrefixes:               []string{"2024/", "2025/"},
		Parallelism:                 4,
		BandwidthLimitMBps:          100,
		AggregateBandwidthLimitMBps: 500,
	})

	blob, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Spot-check key field names are exactly what the console expects.
	str := string(blob)
	for _, needle := range []string{
		`"shardingStrategy":"prefix"`,
		`"shardPrefixes":["2024/","2025/"]`,
		`"insecureSkipTLS":true`,
		`"accessKeyEnv":"AWS_ACCESS_KEY_ID"`,
		`"state":"active"`,
	} {
		if !strings.Contains(str, needle) {
			t.Errorf("missing %q in wire JSON; full body:\n%s", needle, str)
		}
	}

	var got SyncRule
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Config.ID != want.Config.ID {
		t.Errorf("ID: got %q want %q", got.Config.ID, want.Config.ID)
	}
	if got.State != SyncRuleStateActive {
		t.Errorf("State: got %q want %q", got.State, SyncRuleStateActive)
	}
	if got.Config.Dst.InsecureSkipTLS != true {
		t.Errorf("InsecureSkipTLS lost across round-trip")
	}
	if len(got.Config.ShardPrefixes) != 2 || got.Config.ShardPrefixes[0] != "2024/" {
		t.Errorf("ShardPrefixes: got %v", got.Config.ShardPrefixes)
	}
}

// TestSyncRule_ShardPrefixesOmitEmpty verifies that the hash-mode default
// (no prefixes) doesn't leak an empty array onto the wire, so older
// callers that don't understand the field stay quiet.
func TestSyncRule_ShardPrefixesOmitEmpty(t *testing.T) {
	r := NewSyncRule(SyncRuleConfig{
		ID:               "r-hash",
		Type:             "sync",
		ShardingStrategy: "hash",
	})
	blob, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(blob), "shardPrefixes") {
		t.Errorf("empty ShardPrefixes leaked into JSON; body:\n%s", blob)
	}
}

// TestSyncRule_NewSyncRule_Defaults exercises the factory constructor's
// defaults so a misclassified "what state should a fresh rule have"
// regression is caught immediately.
func TestSyncRule_NewSyncRule_Defaults(t *testing.T) {
	r := NewSyncRule(SyncRuleConfig{ID: "r-fresh"})
	if r.State != SyncRuleStateActive {
		t.Errorf("default State = %q want %q", r.State, SyncRuleStateActive)
	}
	if r.CreatedAt.IsZero() {
		t.Errorf("CreatedAt should be set by NewSyncRule")
	}
	if r.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt should be set by NewSyncRule")
	}
	if !r.CreatedAt.Equal(r.UpdatedAt) {
		t.Errorf("CreatedAt and UpdatedAt should be equal on construction")
	}
}

// TestSyncRule_SentinelErrors guards against accidental string-content
// drift in the typed errors that handlers + tests assert via errors.Is.
func TestSyncRule_SentinelErrors(t *testing.T) {
	if !errors.Is(ErrSyncRuleNotFound, ErrSyncRuleNotFound) {
		t.Error("errors.Is(ErrSyncRuleNotFound, self) should be true")
	}
	if errors.Is(ErrSyncRuleNotFound, ErrSyncRuleExists) {
		t.Error("ErrSyncRuleNotFound vs ErrSyncRuleExists must NOT match")
	}
}
