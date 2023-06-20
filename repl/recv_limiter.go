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

package repl

import "golang.org/x/time/rate"

type RecvLimiter struct {
	flow   *rate.Limiter
	packet *rate.Limiter
}

func NewRecvLimiter(recvLimiter *rate.Limiter, packetLimiter *rate.Limiter) *RecvLimiter {
	return &RecvLimiter{
		flow:   recvLimiter,
		packet: packetLimiter,
	}
}

func (l *RecvLimiter) Packet() *rate.Limiter {
	return l.packet
}

func (l *RecvLimiter) Flow() *rate.Limiter {
	return l.flow
}
