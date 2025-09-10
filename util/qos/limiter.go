// Copyright 2025 The CubeFS Authors.
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

package qos

import (
	"context"
	"sync/atomic"

	"github.com/cubefs/cubefs/util"
	"golang.org/x/time/rate"
)

type LimiterStatus struct {
	Enable        bool
	Limit         int
	IOConcurrency int
	IOQueue       int
	IORunning     int
	IOWaiting     int
	Factor        int
}

type Limiter struct {
	limit   int
	limiter *rate.Limiter
	enabled int32
	io      *util.IoLimiter
}

func NewLimiter(limit, ioConcurrency int) *Limiter {
	limiter := rate.NewLimiter(rate.Inf, 0)
	if limit > 0 {
		limiter = rate.NewLimiter(rate.Limit(limit), limit/2)
	}
	l := &Limiter{limit: limit, limiter: limiter}
	l.io = util.NewIOLimiter(0, ioConcurrency)
	return l
}

func (l *Limiter) ResetLimit(limit int) {
	l.limit = limit
	if limit <= 0 {
		l.limiter.SetLimit(rate.Inf)
		l.limiter.SetBurst(0)
	} else {
		l.limiter.SetLimit(rate.Limit(limit))
		l.limiter.SetBurst(limit / 2)
	}
}

func (l *Limiter) GetIo() *util.IoLimiter {
	return l.io
}

func (l *Limiter) ResetIO(ioConcurrency, factor int) {
	l.io.ResetIO(ioConcurrency, factor)
}

func (l *Limiter) ResetIOEx(ioConcurrency, factor, hangMaxMillSecond int) {
	l.io.ResetIOEx(ioConcurrency, factor, hangMaxMillSecond)
}

func (l *Limiter) Enable() {
	atomic.StoreInt32(&l.enabled, 1)
}

func (l *Limiter) Disable() {
	atomic.StoreInt32(&l.enabled, 0)
}

func (l *Limiter) IsEnabled() bool {
	return atomic.LoadInt32(&l.enabled) == 1
}

func (l *Limiter) AllocCheckLimit() {
	if l.IsEnabled() && l.limit > 0 {
		l.limiter.Wait(context.Background())
	}
}

func (l *Limiter) Run(size int, allowHang bool, taskFn func()) (err error) {
	return l.io.Run(size, allowHang, taskFn)
}

func (l *Limiter) TryRun(size int, taskFn func()) bool {
	return l.io.TryRun(size, taskFn)
}

func (l *Limiter) Status() LimiterStatus {
	s := l.io.Status(true)
	limiterStatus := LimiterStatus{
		Enable:        l.IsEnabled(),
		Limit:         l.limit,
		IOConcurrency: s.IOConcurrency,
		IOQueue:       s.IOQueue,
		IORunning:     s.IORunning,
		IOWaiting:     s.IOWaiting,
		Factor:        s.Factor,
	}
	return limiterStatus
}

func (l *Limiter) Close() {
	l.io.Close()
}
