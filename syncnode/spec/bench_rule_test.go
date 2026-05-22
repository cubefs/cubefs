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

package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestSizeClass_ClassLabel: 空 SizeClass 必须回落到 "default" 标签，
// 非空必须按字符串原样返回。
func TestSizeClass_ClassLabel(t *testing.T) {
	cases := []struct {
		in   SizeClass
		want string
	}{
		{"", "default"},
		{SizeClassSmall, "small"},
		{SizeClassMedium, "medium"},
		{SizeClassLarge, "large"},
		{SizeClass("custom"), "custom"},
	}
	for _, c := range cases {
		if got := c.in.ClassLabel(); got != c.want {
			t.Errorf("SizeClass(%q).ClassLabel() = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestFIOStageMixed_JSONRoundTrip: Mixed FIOStage 必须满足 JSON 解析-序列化
// 往返：解 → 编 → 再解出的 Mixed 字段语义不变。
func TestFIOStageMixed_JSONRoundTrip(t *testing.T) {
	src := `{
		"name": "mix-rw",
		"rw": "randrw",
		"bs": "4k",
		"reuseFiles": false,
		"runtime": 40,
		"mixed": [
			{
				"name": "small-rand",
				"size_class": "small",
				"weight": 9,
				"bs": "4k",
				"iodepth": 32,
				"numjobs": 4,
				"rw": "randread",
				"size": "1G"
			},
			{
				"name": "large-seq",
				"size_class": "large",
				"weight": 1,
				"bs": "16m",
				"rw": "read"
			}
		]
	}`

	var s FIOStage
	if err := json.Unmarshal([]byte(src), &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(s.Mixed) != 2 {
		t.Fatalf("Mixed len = %d, want 2", len(s.Mixed))
	}
	if s.Mixed[0].SizeClass != SizeClassSmall || s.Mixed[1].SizeClass != SizeClassLarge {
		t.Errorf("SizeClass mismatch: got %q + %q", s.Mixed[0].SizeClass, s.Mixed[1].SizeClass)
	}
	if s.Mixed[0].Weight != 9 || s.Mixed[1].Weight != 1 {
		t.Errorf("weight mismatch: %d/%d", s.Mixed[0].Weight, s.Mixed[1].Weight)
	}
	if s.Mixed[0].BlockSize != "4k" || s.Mixed[1].BlockSize != "16m" {
		t.Errorf("bs mismatch: %q/%q", s.Mixed[0].BlockSize, s.Mixed[1].BlockSize)
	}
	if s.Mixed[0].IODepth != 32 || s.Mixed[0].NumJobs != 4 || s.Mixed[0].Size != "1G" {
		t.Errorf("small component override fields lost: %+v", s.Mixed[0])
	}

	// re-encode + re-decode 后语义等价。
	enc, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var s2 FIOStage
	if err := json.Unmarshal(enc, &s2); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if len(s2.Mixed) != 2 || s2.Mixed[0].Name != "small-rand" || s2.Mixed[1].Name != "large-seq" {
		t.Errorf("round-trip lost component fields: %+v", s2.Mixed)
	}
}

// TestObjOp_SizeClass_JSONOmitEmpty: SizeClass 为空时不得出现在 JSON 输出里
// （omitempty 保护旧 rule 100% 兼容）。非空时必须以 "size_class" key 出现。
func TestObjOp_SizeClass_JSONOmitEmpty(t *testing.T) {
	empty := ObjOp{Type: "put", Weight: 1}
	enc, err := json.Marshal(empty)
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	if got := string(enc); strings.Contains(got, "size_class") {
		t.Errorf("empty SizeClass must be omitted, got %q", got)
	}

	withClass := ObjOp{Type: "put", Weight: 1, SizeClass: SizeClassSmall}
	enc, err = json.Marshal(withClass)
	if err != nil {
		t.Fatalf("marshal with class: %v", err)
	}
	if got := string(enc); !strings.Contains(got, `"size_class":"small"`) {
		t.Errorf("expected size_class label in JSON, got %q", got)
	}
}

// TestFIOStage_MixedOmitEmpty: Mixed 为空时 JSON 中不得出现 "mixed" key，
// 保证旧 rule 0 字段开销。
func TestFIOStage_MixedOmitEmpty(t *testing.T) {
	s := FIOStage{Name: "legacy", RW: "randwrite", BS: "4k"}
	enc, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(enc), "mixed") {
		t.Errorf("empty Mixed must be omitted, got %s", enc)
	}
}

// TestBenchRule_RawJSON_HiddenFromMarshal: RC8 #119 — BenchRule.RawJSON 必须
// 走 `json:"-"` 隐藏，否则 (a) dispatch payload 会把 RawJSON 再次嵌入；
// (b) DisallowUnknownFields 反而允许调用方伪造 RawJSON 字段。两条都会让
// "原始 body 持久化" 的语义崩塌。
func TestBenchRule_RawJSON_HiddenFromMarshal(t *testing.T) {
	r := BenchRule{ID: "r1", Name: "n", RawJSON: `{"id":"r1"}`}
	enc, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(enc), "rawJSON") || strings.Contains(string(enc), "raw_json") {
		t.Errorf("BenchRule JSON must not expose RawJSON: %s", enc)
	}
	// 反序列化时同样不接受 rawJSON 字段（json:"-"）。常规解码丢弃未知字段，
	// 严格解码（DisallowUnknownFields）拒绝该字段——这是 handler 层的契约，
	// 这里只验证字段不被填回。
	var back BenchRule
	if err := json.Unmarshal([]byte(`{"id":"r1","rawJSON":"x"}`), &back); err != nil {
		t.Fatalf("loose unmarshal: %v", err)
	}
	if back.RawJSON != "" {
		t.Errorf("RawJSON must stay empty after JSON unmarshal, got %q", back.RawJSON)
	}
}

// TestBenchRule_RawJSON_StrictDecodeRejectsField: 严格解码（master handler
// 走的路径）必须拒绝 rawJSON 字段。否则任意调用方都能伪造持久化原文。
func TestBenchRule_RawJSON_StrictDecodeRejectsField(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"id":"r1","rawJSON":"x"}`))
	dec.DisallowUnknownFields()
	var r BenchRule
	err := dec.Decode(&r)
	if err == nil {
		t.Fatalf("strict decode must reject rawJSON field, got nil err and rule=%+v", r)
	}
	if !strings.Contains(err.Error(), "rawJSON") {
		t.Errorf("error must name the offending field, got %v", err)
	}
}
