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

package chanutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/chanutil"
)

func TestQueue(t *testing.T) {
	queue := chanutil.NewQueue(4)
	defer queue.Close()
	for i := 0; i < 4; i++ {
		queue.Enque(i)
	}
	item, ok := queue.Deque()
	if !ok {
		t.Error("failed to deque item")
	}
	if item.(int) != 0 {
		t.Error("item.(int) should equal with 0")
	}
	items := queue.DequeBatch(3)
	if len(items) != 3 {
		t.Error("len(items) should equal with 3")
	}
	val := 1
	for i := 0; i != len(items); i++ {
		if items[i].(int) != val {
			t.Errorf("items[%v] should equal with %v", i, val)
		}
		val += 1
	}
	for i := 0; i < 4; i++ {
		queue.Enque(i)
	}
	if queue.TryEnque(0) {
		t.Error("queue should be full")
	}
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	if queue.EnqueWithContext(0, ctx) {
		t.Error("queue should be full")
	}
	items = queue.DequeAll()
	if len(items) != 4 {
		t.Error("len(items) should equal with 4")
	}
}
