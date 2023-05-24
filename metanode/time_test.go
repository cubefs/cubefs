// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"testing"
	"time"
)

func TestGetCurrentTimeUnix(t *testing.T) {
	t1 := Now.GetCurrentTimeUnix()
	t2 := Now.GetCurrentTime().Unix()
	n := time.Now().Unix()
	if n-t1 > 1 || n-t2 > 1 {
		t.Errorf("wrong time: %d %d %d", n, t1, t2)
	}

	time.Sleep(time.Second * 2)
	t1 = Now.GetCurrentTimeUnix()
	t2 = Now.GetCurrentTime().Unix()
	n = time.Now().Unix()
	if n-t1 > 1 || n-t2 > 1 {
		t.Errorf("wrong time: %d %d %d", n, t1, t2)
	}
}

func BenchmarkGetCurrentTimeUnix(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Now.GetCurrentTimeUnix()
	}
}

func BenchmarkGetCurrentTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Now.GetCurrentTime().Unix()
	}
}

func BenchmarkGetNowTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now().Unix()
	}
}
