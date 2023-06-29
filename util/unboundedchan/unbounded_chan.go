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

package unboundedchan

import "sync/atomic"

type UnboundedChan struct {
	bufCount int64
	In       chan<- V //for user to write data into, BUT NOT TO READ FROM! In channel can be "close" by user
	Out      <-chan V //for user to read data from, BUT NOT TO WRITE INTO!
	buffer   *RingBuffer
}

func (uc *UnboundedChan) Len() int {
	return len(uc.In) + uc.BufLen() + len(uc.Out)
}

func (uc *UnboundedChan) BufLen() int {
	return int(atomic.LoadInt64(&uc.bufCount))
}

func NewUnboundedChan(initCapacity int) *UnboundedChan {
	return NewUnboundedChanSize(initCapacity, initCapacity, initCapacity)
}

func NewUnboundedChanSize(initInCapacity, initOutCapacity, initBufferCapacity int) *UnboundedChan {
	in := make(chan V, initInCapacity)
	out := make(chan V, initOutCapacity)
	uc := &UnboundedChan{
		In:     in,
		Out:    out,
		buffer: NewRingBuffer(uint32(initBufferCapacity)),
	}

	go run(in, out, uc)
	return uc
}

func (uc *UnboundedChan) feedBuffer(val V) {
	uc.buffer.Write(val)
	atomic.AddInt64(&uc.bufCount, 1)
}

func (uc *UnboundedChan) resetBuffer() {
	uc.buffer.Reset()
	atomic.StoreInt64(&uc.bufCount, 0)
}

func run(in, out chan V, uc *UnboundedChan) {
	defer close(out)
LOOP:
	for {
		//read data from in channel
		val, ok := <-in
		if !ok {
			break LOOP
		}

		//move data to buffer or out channel
		if atomic.LoadInt64(&uc.bufCount) > 0 {
			uc.feedBuffer(val)
		} else {
			select {
			case out <- val:
				continue
			default:
				//out channel is full
				uc.feedBuffer(val)
			}

		}

		//try feeding out channel with buffer data
		for !uc.buffer.IsEmpty() {
			select {
			//when out channel is full, data may still feed in channel
			case val, ok := <-in:
				if !ok {
					break LOOP
				}
				uc.feedBuffer(val)
			case out <- uc.buffer.Peek():
				uc.buffer.Pop()
				atomic.AddInt64(&uc.bufCount, -1)
				if uc.buffer.IsEmpty() {
					uc.resetBuffer()
				}
			}

		}
	}

	//no more in data, keep feeding out channel with buffer data until it's drained
	for !uc.buffer.IsEmpty() {
		out <- uc.buffer.Pop()
		atomic.AddInt64(&uc.bufCount, -1)
	}
	uc.resetBuffer()
}
