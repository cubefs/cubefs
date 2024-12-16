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

package uring

import (
	"context"
	"runtime"
	"syscall"
	"testing"
)

func BenchmarkQueueNop(b *testing.B) {
	type opt struct {
		name    string
		entries uint32
		p       IoUringParams
	}

	ts := []opt{
		{"def-256", 256, IoUringParams{Flags: 0}},
		{"sqpoll-256-4-10000", 256, IoUringParams{Flags: IORING_SETUP_SQPOLL, SqThreadCpu: 16, SqThreadIdle: 10_000}},
	}

	consumer := func(h *IoUring, ctx context.Context, count int) {
		var cqe *IoUringCqe
		var err error
		for i := 0; i < count; i++ {
			if ctx.Err() != nil {
				return
			}
			err = h.WaitCqe(&cqe)
			if err == syscall.EINTR {
				continue // ignore INTR
			} else if err != nil {
				panic(err)
			}
			if cqe.Res < 0 {
				panic(syscall.Errno(-cqe.Res))
			}

			h.SeenCqe(cqe)
		}
	}

	for _, tc := range ts {
		b.Run(tc.name, func(b *testing.B) {
			h := testNewIoUringWithParams(b, tc.entries, &tc.p)
			defer h.Close()
			var (
				j         uint32
				sqe       *IoUringSqe
				err       error
				submitted int
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j = 0; j < tc.entries; j++ {
					for {
						// sqe could be nil if SQ is already full so we spin until we got one
						sqe = h.GetSqe()
						if sqe != nil {
							break
						}
						runtime.Gosched()
					}
					PrepNop(sqe)
					sqe.UserData.SetUint64(uint64(i + int(j)))
				}
				submitted, err = h.Submit()
				if err != nil {
					panic(err)
				}
				consumer(h, ctx, submitted)
			}
			b.StopTimer()
		})

	}
}
