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
	"testing"

	"github.com/cubefs/cubefs/repl"
	"golang.org/x/time/rate"
)

const (
	flow   = 0
	packet = iota
)

func LimiterTest(t *testing.T, recv *repl.RecvLimiter, internal *rate.Limiter, mode int) {
	limiter := recv.Flow()
	if mode != flow {
		limiter = recv.Packet()
	}
	internal.SetLimit(1)
	if limiter.Limit() != 1 {
		t.Errorf("limiter should equal with 1")
	}
	internal.SetBurst(1)
	if limiter.Burst() != 1 {
		t.Errorf("burst should equal with 1")
	}
	if !limiter.Allow() {
		t.Errorf("limiter should allow 1")
	}
	if limiter.Allow() {
		t.Errorf("limiter should not allow")
	}
}

func TestRecvLimiter(t *testing.T) {
	flowLimiter := rate.NewLimiter(10, 10)
	packetLimiter := rate.NewLimiter(10, 10)
	recvLimiter := repl.NewRecvLimiter(flowLimiter, packetLimiter)
	if recvLimiter.Flow() != flowLimiter {
		t.Errorf("flow limiter should equal with %v", flowLimiter)
	}
	if recvLimiter.Packet() != packetLimiter {
		t.Errorf("packer limiter should equal witg %v", packetLimiter)
	}
	LimiterTest(t, recvLimiter, flowLimiter, flow)
	LimiterTest(t, recvLimiter, packetLimiter, packet)
}
