// Copyright 2022 The CubeFS Authors.
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

package raft

import (
	"context"
)

func newProposalQueue(bufferSize int) proposalQueue {
	return make(chan proposalRequest, bufferSize)
}

type proposalQueue chan proposalRequest

func (q proposalQueue) Push(ctx context.Context, m proposalRequest) error {
	select {
	case q <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q proposalQueue) Iter(f func(m proposalRequest) bool) {
	for {
		select {
		case m := <-q:
			if !f(m) {
				return
			}
		default:
			return
		}
	}
}

func (q proposalQueue) Len() int {
	return len(q)
}
