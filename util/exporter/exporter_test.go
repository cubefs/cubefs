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

package exporter

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewCounter(t *testing.T) {
	N := 100
	wg := sync.WaitGroup{}
	wg.Add(N)

	for i := 0; i < N; i++ {
		go func(i int64) {
			defer wg.Done()

			name := fmt.Sprintf("name_%d_gauge", i%2)
			label := fmt.Sprintf("label-%d:name", i)
			m := NewCounter(name)
			if m != nil {
				m.SetWithLabels(float64(i), map[string]string{"volname": label, "cluster": name})
				t.Logf("metric: %v", m.name)
			}
			name2 := fmt.Sprintf("name_%d_counter", i%2)
			c := NewGauge(name2)
			if c != nil {
				//c.Set(float64(i))
				c.SetWithLabels(float64(i), map[string]string{"volname": label, "cluster": name})
				t.Logf("metric: %v", name2)
			}

		}(int64(i))
	}

	wg.Wait()
}

func TestGetLocalIp(t *testing.T) {
	_, err := GetLocalIpAddr("")
	if err != nil {
		t.Fail()
	}
	_, err = GetLocalIpAddr("127")
	if err == nil {
		t.Fail()
	}

	_, err = GetLocalIpAddr("!127")
	if err != nil {
		t.Fail()
	}
}
