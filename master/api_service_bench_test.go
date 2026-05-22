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
	"strings"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// TestDecodeBenchRuleStrict_RejectsUnknownField: RC8 #119 — 关键回归点。
// 不能再静默丢弃未知字段；body 必须按 DisallowUnknownFields 处理，错误
// 文本里需要带上 offending field 名以便 dashboard / CLI 直接定位。
func TestDecodeBenchRuleStrict_RejectsUnknownField(t *testing.T) {
	body := `{"id":"r1","name":"x","futureFieldXYZ":42}`
	_, raw, err := decodeBenchRuleStrict(strings.NewReader(body))
	if err == nil {
		t.Fatalf("expected error for unknown field, got nil")
	}
	if !strings.Contains(err.Error(), "futureFieldXYZ") {
		t.Errorf("error must name the offending field, got %v", err)
	}
	// 即便失败，raw 应当已经被读到（便于上层做日志记录 / 审计）。
	if string(raw) != body {
		t.Errorf("raw body not preserved on decode failure: got %q", raw)
	}
}

// TestDecodeBenchRuleStrict_AcceptsValid: 合法 rule 必须正常解码，且
// 返回的 raw 必须与请求 body 字节级一致（不重新 marshal 出来）。
func TestDecodeBenchRuleStrict_AcceptsValid(t *testing.T) {
	body := `{"id":"r1","name":"hello","storageType":"posix","parallelism":2,"output":{"percentiles":[50,99]}}`
	rule, raw, err := decodeBenchRuleStrict(strings.NewReader(body))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rule.ID != "r1" || rule.Name != "hello" || rule.StorageType != spec.BenchStoragePosix || rule.Parallelism != 2 {
		t.Errorf("decoded fields mismatch: %+v", rule)
	}
	if string(raw) != body {
		t.Errorf("raw bytes mismatch:\n got %q\nwant %q", raw, body)
	}
	// rule.RawJSON 仍应为空——handler 填它，不是 decode 函数填。
	if rule.RawJSON != "" {
		t.Errorf("decodeBenchRuleStrict must not pre-fill RawJSON, got %q", rule.RawJSON)
	}
}

// TestDecodeBenchRuleStrict_RejectsRawJSONField: rawJSON 是内部字段
// (BenchRule 上是 json:"-")，调用方不得通过 body 注入。strict decode 必须
// 拒绝。
func TestDecodeBenchRuleStrict_RejectsRawJSONField(t *testing.T) {
	body := `{"id":"r1","name":"x","rawJSON":"forged"}`
	_, _, err := decodeBenchRuleStrict(strings.NewReader(body))
	if err == nil {
		t.Fatalf("expected error for rawJSON field, got nil")
	}
	if !strings.Contains(err.Error(), "rawJSON") {
		t.Errorf("error must name rawJSON, got %v", err)
	}
}

// TestDecodeBenchRuleStrict_EmptyBody: 空 body 应得到 decode error
// (json: EOF) 而不是返回空 rule —— 避免 handler 接受空 body 后落库一条
// 空规则。
func TestDecodeBenchRuleStrict_EmptyBody(t *testing.T) {
	_, _, err := decodeBenchRuleStrict(strings.NewReader(""))
	if err == nil {
		t.Fatalf("expected error for empty body")
	}
}

// TestBenchRuleView_ExposesRawJSON: GET response wrapper 必须把 RawJSON
// 提升到顶层 "rawJSON" 字段（虽然 BenchRule 本身的 JSON tag 是 "-"）。
func TestBenchRuleView_ExposesRawJSON(t *testing.T) {
	r := &spec.BenchRule{ID: "r1", Name: "n", RawJSON: `{"id":"r1"}`}
	enc, err := json.Marshal(newBenchRuleView(r))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(enc)
	if !strings.Contains(got, `"rawJSON":"{\"id\":\"r1\"}"`) {
		t.Errorf("view must expose rawJSON in output, got %s", got)
	}
	// id 同时存在——view 嵌入了 BenchRule 的所有字段。
	if !strings.Contains(got, `"id":"r1"`) {
		t.Errorf("view must keep BenchRule fields, got %s", got)
	}
}

// TestBenchRuleView_OmitsRawJSONWhenEmpty: 空 RawJSON 时 rawJSON 字段
// 必须被 omitempty 跳过，避免给老 rule 增加噪声字段。
func TestBenchRuleView_OmitsRawJSONWhenEmpty(t *testing.T) {
	r := &spec.BenchRule{ID: "r1", Name: "n"}
	enc, err := json.Marshal(newBenchRuleView(r))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(enc), "rawJSON") {
		t.Errorf("empty RawJSON must be omitted: %s", enc)
	}
}

// TestBenchRuleView_NilSafe: nil rule -> nil view，避免 GET 单条 not-found
// 时 panic。
func TestBenchRuleView_NilSafe(t *testing.T) {
	if v := newBenchRuleView(nil); v != nil {
		t.Errorf("newBenchRuleView(nil) = %+v, want nil", v)
	}
}

// TestBenchRuleViews_EmptySlice: List 返回 0 条时 wrapper 应给出空切片
// 而不是 nil（便于 dashboard 直接 .map(...)）。
func TestBenchRuleViews_EmptySlice(t *testing.T) {
	views := newBenchRuleViews(nil)
	if views == nil {
		t.Fatalf("newBenchRuleViews(nil) must return non-nil empty slice")
	}
	if len(views) != 0 {
		t.Errorf("expected empty slice, got %v", views)
	}
}
