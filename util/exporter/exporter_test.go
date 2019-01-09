// Copyright 2018 The Container File System Authors.
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
	"testing"
)

func TestNewMetric(t *testing.T) {
	N := 100
	exitCh := make(chan int, 100)
	for i := 0; i < N; i++ {
		go func() {
			m := RegistMetric(fmt.Sprintf("name_%d_counter", i%17), Counter)
			if m != nil {
				m.Set(float64(i))
				t.Logf("metric: %v", m.Name)
			}
			g := RegistMetric(fmt.Sprintf("name_%d_gauge", i%17), Gauge)
			if g != nil {
				g.Set(float64(i))
				t.Logf("metric: %v", g.Name)
			}
			exitCh <- i

		}()
	}

	x := 0
	select {
	case <-exitCh:
		x += 1
		if x == N {
			return
		}
	}
}

func TestRegistGauge(t *testing.T) {
	N := 100
	exitCh := make(chan int, 100)
	for i := 0; i < N; i++ {
		go func() {
			m := RegistGauge(fmt.Sprintf("name_%d", i%7))
			if m != nil {
				t.Logf("metric: %v", m.Desc().String())
			}
			exitCh <- i

		}()
	}

	x := 0
	select {
	case <-exitCh:
		x += 1
		if x == N {
			return
		}
	}
}

func TestRegistTp(t *testing.T) {
	N := 100
	exitCh := make(chan int, 100)
	for i := 0; i < N; i++ {
		go func() {
			m := RegistTp(fmt.Sprintf("name_%d", i%7))
			if m != nil {
				t.Logf("metric: %v", m.Name)
			}

			defer m.CalcTp()

			exitCh <- i
		}()
	}

	x := 0
	select {
	case <-exitCh:
		x += 1
		if x == N {
			return
		}
	}

}
