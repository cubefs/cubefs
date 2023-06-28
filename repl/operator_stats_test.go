// Copyright 2023 The CubeFS Authors.
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

package repl_test

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/repl"
)

func TestOperatorSampleTest(t *testing.T) {
	var stats repl.OperatorStats
	processCh := make(chan *repl.Packet, 2048)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Wait()
		stats.OnEnque(100)
		stats.OnDrop(50)
		time.Sleep(1 * time.Second)
		stats.OnDeque(100, true)
	}()
	wg.Done()
	sample := stats.Sample(2*time.Second, processCh)
	t.Logf("Enque Flow:\t%v", sample.GetEnqueFlow())
	t.Logf("Deque Flow:\t%v", sample.GetDequeFlow())
	t.Logf("Enque Packet:\t%v", sample.GetEnquePacket())
	t.Logf("Deque Packet:\t%v", sample.GetDequePacket())
	t.Logf("Wait Flow:\t%v", sample.GetWaitFlow())
	t.Logf("Wait Packet:\t%v", sample.GetWaitPacket())
	t.Logf("Wait Time:\t%v", sample.GetWaitTime())
	t.Logf("Sample Duration:\t%v", sample.GetSampleDuration())
	t.Logf("Util:\t%.1f", sample.GetUtil()*100)
	t.Logf("Drop Packet:\t%v", sample.GetDequePacket())
	t.Logf("Drop Flow:\t%v", sample.GetDropFlow())
	t.Logf("Drop Packet Percent:\t%.1f", sample.GetDropPacketRate()*100)
	t.Logf("Drop Flow Percent:\t%.1f", sample.GetDropFlowRate()*100)
	if sample.GetQueueCapacity() != 2048 {
		t.Errorf("queue capacity should equal with 0, but get %v", sample.GetQueueCapacity())
	}
	if sample.GetQueueLength() != 0 {
		t.Errorf("queue length should equal with 0, but get %v", sample.GetQueueLength())
	}
	if sample.GetEnqueFlow() != 100 {
		t.Errorf("enque flow should equal with 100, but get %v", sample.GetEnqueFlow())
	}
	if sample.GetDequeFlow() != 100 {
		t.Errorf("deque flow should equal with 100, but get %v", sample.GetDequeFlow())
	}
	if sample.GetEnquePacket() != 1 {
		t.Errorf("enque packet should equal with 1, but get %v", sample.GetEnquePacket())
	}
	if sample.GetDequePacket() != 1 {
		t.Errorf("deque packet should equal with 1, but get %v", sample.GetDequePacket())
	}
	if math.Abs(sample.GetUtil()-0.5) > 0.01 {
		t.Errorf("util should equal with 0.5, but get %v", sample.GetUtil())
	}
	if sample.GetDropPacket() != 1 {
		t.Errorf("drop packet should equal with 1, but get %v", sample.GetDropPacket())
	}
	if sample.GetDropFlow() != 50 {
		t.Errorf("drop flow should equal with 50, but get %v", sample.GetDropFlow())
	}
}
