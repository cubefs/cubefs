// Copyright 2018 The Chubao Authors.
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

//func TestNewCounter(t *testing.T) {
//    N := 100
//    exitCh := make(chan int64, N)
//    for i := 0; i < N; i++ {
//        go func(i int64) {
//            name := fmt.Sprintf("name_%d_gauge", i%2)
//            label := fmt.Sprintf("label-%d:name", i)
//            m := NewCounter(name)
//            if m != nil {
//                m.SetWithLabels(i, map[string]string{"volname": label, "cluster": name})
//                t.Logf("metric: %v", m.name)
//            }
//            name2 := fmt.Sprintf("name_%d_counter", i%2)
//            c := NewGauge(name2)
//            if c != nil {
//                //c.Set(float64(i))
//                c.SetWithLabels(i, map[string]string{"volname": label, "cluster": name})
//                t.Logf("metric: %v", name2)
//            }
//            exitCh <- i

//        }(int64(i))
//    }

//    x := 0
//    select {
//    case <-exitCh:
//        x += 1
//        if x == N {
//            return
//        }
//    }
//}

func TestNewTPCntVec(t *testing.T) {
	tpv := NewTPVec("tpv1", "", []string{})
	if tpv == nil {
		t.Logf("tpv is nil ")
	}

	N := 100
	m1 := NewTPCntVec("metric_tpcnt_vec", "", []string{"count"})
	if m1 == nil {
		t.Logf("tpcv is nil ")
	}
	exitCh := make(chan int64, N)
	for i := 0; i < N; i++ {
		go func(i int64) {
			if m1 == nil {
				t.Logf("tpcv go is nil ")
			}
			m := m1.GetWithLabelVals(fmt.Sprintf("%d", i))
			if m != nil {
				m.Count()
			}
			exitCh <- i
		}(int64(i))
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
