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

package chanutil

type Queue struct {
	queue chan interface{}
}

func NewQueue(cap int) Queue {
	return Queue{
		queue: make(chan interface{}, cap),
	}
}

func (q Queue) Enque(item interface{}) {
	q.queue <- item
}

func (q Queue) TryEnque(item interface{}) bool {
	select {
	case q.queue <- item:
		return true
	default:
		return false
	}
}

func (q Queue) Deque() (interface{}, bool) {
	item, ok := <-q.queue
	return item, ok
}

func (q Queue) DequeBatch(maxCount int) []interface{} {
	batch := make([]interface{}, 0, maxCount)
	item, ok := <-q.queue
	if ok {
		batch = append(batch, item)
		stop := false
		for !stop && len(batch) < maxCount {
			select {
			case item, ok = <-q.queue:
				if !ok {
					stop = true
					break
				}
				batch = append(batch, item)
			default:
				stop = true
			}
		}
	}
	return batch
}

func (q Queue) DequeAll() []interface{} {
	batch := make([]interface{}, 0)
	item, ok := <-q.queue
	if ok {
		batch = append(batch, item)
		stop := false
		for !stop {
			select {
			case item, ok = <-q.queue:
				if !ok {
					stop = true
					break
				}
				batch = append(batch, item)
			default:
				stop = true
			}
		}
	}
	return batch
}

func (q Queue) Len() int {
	return len(q.queue)
}

func (q Queue) Close() {
	close(q.queue)
}
