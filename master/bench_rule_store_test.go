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
	"encoding/json"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// TestBenchRuleStore_RawJSON_RoundTrip: Create / Get 必须保留 RawJSON 字段，
// 这是 RC8 #119 持久化原始 body 的内存层契约。
func TestBenchRuleStore_RawJSON_RoundTrip(t *testing.T) {
	s := NewBenchRuleStore() // cluster=nil → 纯 in-memory，不走 raft
	body := `{"id":"r1","name":"hello","storageType":"posix","parallelism":0,"output":{"percentiles":[50,99]}}`
	rule := &spec.BenchRule{ID: "r1", Name: "hello", RawJSON: body}
	if err := s.Create(rule); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get("r1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.RawJSON != body {
		t.Errorf("RawJSON mismatch:\n got %q\nwant %q", got.RawJSON, body)
	}
}

// TestBenchRuleStore_RawJSON_UpdateReplaces: Update 必须以新的 RawJSON
// 覆盖既有的（而不是拼接或保留旧的）。
func TestBenchRuleStore_RawJSON_UpdateReplaces(t *testing.T) {
	s := NewBenchRuleStore()
	if err := s.Create(&spec.BenchRule{ID: "r1", Name: "v1", RawJSON: `{"v":1}`}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Update(&spec.BenchRule{ID: "r1", Name: "v2", RawJSON: `{"v":2}`}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ := s.Get("r1")
	if got.RawJSON != `{"v":2}` {
		t.Errorf("RawJSON after Update = %q, want %q", got.RawJSON, `{"v":2}`)
	}
	if got.Name != "v2" {
		t.Errorf("Name after Update = %q, want v2", got.Name)
	}
}

// TestStoredBenchRuleEnvelope_Encode: storedBenchRule 必须把 RawJSON 一并
// 序列化进 raft cmd 的 V 字段（rocksdb 落盘字节）。
func TestStoredBenchRuleEnvelope_Encode(t *testing.T) {
	r := &spec.BenchRule{ID: "r1", Name: "n", RawJSON: `{"id":"r1"}`}
	v, err := json.Marshal(storedBenchRule{Rule: r, RawJSON: r.RawJSON})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// 解回必须能拿回 RawJSON。
	var back storedBenchRule
	if err := json.Unmarshal(v, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Rule == nil || back.Rule.ID != "r1" {
		t.Fatalf("envelope lost Rule: %+v", back)
	}
	if back.RawJSON != `{"id":"r1"}` {
		t.Errorf("envelope RawJSON = %q, want %q", back.RawJSON, `{"id":"r1"}`)
	}
}

// TestStoredBenchRuleEnvelope_LegacyFallback: 旧 record 是裸 BenchRule，
// loadBenchRules 用 envelope 解时 envelope.Rule 会保持 nil；这是我们用来
// 区分新旧格式的判别。这里断言这一行为以防 Go json 库语义变化。
func TestStoredBenchRuleEnvelope_LegacyFallback(t *testing.T) {
	legacy := &spec.BenchRule{ID: "r-old", Name: "legacy"}
	v, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var env storedBenchRule
	if err := json.Unmarshal(v, &env); err != nil {
		t.Fatalf("envelope unmarshal of legacy must not error: %v", err)
	}
	if env.Rule != nil {
		t.Fatalf("envelope decode of legacy must leave Rule nil, got %+v", env.Rule)
	}
	// fallback 路径：再解一次裸 BenchRule 必须成功。
	var bare spec.BenchRule
	if err := json.Unmarshal(v, &bare); err != nil {
		t.Fatalf("legacy bare decode: %v", err)
	}
	if bare.ID != "r-old" {
		t.Fatalf("legacy decode lost ID: %+v", bare)
	}
}

// TestBenchRuleStore_EmptyRawJSON_OK: 没有 RawJSON 的 rule（例如来自旧
// 持久化记录的 in-memory 回填）也必须正常 round-trip。
func TestBenchRuleStore_EmptyRawJSON_OK(t *testing.T) {
	s := NewBenchRuleStore()
	if err := s.Create(&spec.BenchRule{ID: "r1", Name: "n"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get("r1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.RawJSON != "" {
		t.Errorf("empty RawJSON must stay empty, got %q", got.RawJSON)
	}
}
