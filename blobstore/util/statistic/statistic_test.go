// Copyright 2025 The CubeFS Authors.
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

package statistic

import (
	"math/rand"
	"testing"
	"time"
)

func TestDelayStatistic(t *testing.T) {
	ds := NewDelayStatistic("test", 0, 10*time.Microsecond, 10)
	ds.Report()
	for i := time.Duration(0); i < 100; i++ {
		ds.Add(i * time.Microsecond)
	}
	ds.Report()

	ds = NewDelayStatistic("test", time.Microsecond, 1*time.Microsecond, 10000)
	for i := time.Duration(0); i < 10000; i++ {
		ds.Add(i * time.Microsecond)
	}
	ds.Report()

	ds = NewDelayStatistic("test", time.Millisecond, time.Millisecond, 100)
	for i := time.Duration(0); i < 9999; i++ {
		if i%2 == 0 {
			ds.Add(time.Millisecond)
		} else {
			ds.Add(2 * time.Millisecond)
		}
	}
	time.Sleep(177 * time.Millisecond)
	ds.Add(time.Minute)
	ds.Report()
	for i := time.Duration(0); i < 100; i++ {
		ds.Add(i * time.Millisecond)
	}
	ds.Report()
}

func Benchmark_DelayStatistic(b *testing.B) {
	ds := NewDelayStatistic("test", 0, 100*time.Microsecond, 100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ds.Add(time.Duration(rand.Int31n(1000000)) * time.Microsecond)
		}
	})
}
