// Copyright 2024 The CubeFS Authors.
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

package iouring

import "sync"

type resultCh chan error

var resultChPool = sync.Pool{New: func() any {
	return make(chan error)
}}

func newRequest(op op, id reqID, buf []byte, off uint64, size int) request {
	return request{
		op:   op,
		id:   id,
		buf:  buf,
		off:  off,
		size: size,
		ret:  resultChPool.Get().(resultCh),
	}
}

type request struct {
	op   op
	id   reqID
	buf  []byte
	off  uint64
	size int
	ret  resultCh
}

func (r *request) wait() error {
	err := <-r.ret
	resultChPool.Put(r.ret)
	return err
}

func (r *request) notify(err error) {
	r.ret <- err
}

// concurrentPendingRequests is an effective data struct (concurrent map implements)
type concurrentPendingRequests struct {
	m [32]sync.Map
}

// get request from concurrentPendingRequests
func (s *concurrentPendingRequests) getRequest(id uint64) (req request) {
	idx := uint32(id) % 32
	v, ok := s.m[idx].LoadAndDelete(id)
	if !ok {
		return
	}
	return v.(request)
}

// put new request into concurrentPendingRequests
func (s *concurrentPendingRequests) putRequest(req request) {
	idx := uint32(req.id) % 32
	s.m[idx].Store(req.id, req)
}
