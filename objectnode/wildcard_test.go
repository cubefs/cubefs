// Copyright 2019 The CubeFS Authors.
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

package objectnode

import "testing"

func TestWildcards_Parse(t *testing.T) {

	var domains = []string{
		"object.chubao.io",
		"oss.chubao.io",
	}

	type expect struct {
		wildcard bool
		bucket   string
	}

	type sample struct {
		h string
		e expect
	}

	var samples = []sample{
		{h: "object.chubao.io", e: expect{wildcard: false}},
		{h: "object.chubao.io:8080", e: expect{wildcard: false}},
		{h: "a.object.chubao.io", e: expect{wildcard: true, bucket: "a"}},
		{h: "a.b.object.chubao.io", e: expect{wildcard: true, bucket: "a.b"}},
		{h: "a_b.object.chubao.io", e: expect{wildcard: true, bucket: "a_b"}},
		{h: "a-bc.object.chubao.io", e: expect{wildcard: true, bucket: "a-bc"}},
		{h: "ab.object.chubao.io:8080", e: expect{wildcard: true, bucket: "ab"}},
		{h: ".oss.chubao.io:a", e: expect{wildcard: false}},
		{h: ".b-c_d.oss.chubao.io:a", e: expect{wildcard: false}},
	}

	var ws Wildcards
	var err error
	if ws, err = NewWildcards(domains); err != nil {
		t.Fatalf("init wildcards fail: err(%v)", err)
	}
	for _, s := range samples {
		h := s.h
		e := s.e
		bucket, is := ws.Parse(h)
		if is != e.wildcard {
			t.Fatalf("result mismatch: h(%v) sample(%v) actual(%v)", h, e.wildcard, is)
		}
		if is && bucket != e.bucket {
			t.Fatalf("result mismatch: h(%v) sample(%v) actual(%v)", h, e.bucket, bucket)
		}
	}
}
