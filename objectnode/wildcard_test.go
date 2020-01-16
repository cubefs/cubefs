// Copyright 2018 The ChubaoFS Authors.
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

	var hosts = []string{
		"object.chubao.io",
		"object.chubao.io:8080",
		"a.object.chubao.io",
		"a.object.chubao.io:8080",
		"a_b.object.chubao.io",
		"a-b.oss.chubao.io:1024",
		".oss.chubao.io:a",
	}

	var iss = []bool{
		false, false, true, true, true, true, false,
	}

	var buckets = []string{
		"", "", "a", "a", "a_b", "a-b", "",
	}

	var ws Wildcards
	var err error
	if ws, err = NewWildcards(domains); err != nil {
		t.Fatalf("init wildcards fail: err(%v)", err)
	}
	for i := 0; i < len(hosts); i++ {
		bucket, is := ws.Parse(hosts[i])
		if is != iss[i] {
			t.Fatalf("result mismatch: host(%v) expect(%v) actual(%v)", hosts[i], iss[i], is)
		}
		if bucket != buckets[i] {
			t.Fatalf("result mismatch: host(%v) expect(%v) actual(%v)", hosts[i], buckets[i], bucket)
		}
	}
}
