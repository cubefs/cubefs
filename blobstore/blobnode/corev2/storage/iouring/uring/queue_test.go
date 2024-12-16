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
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingQueueGetSQE(t *testing.T) {
	h := testNewIoUring(t, 256, 0)
	defer h.Close()

	assert.NotEqual(t, 0, h.RingFd)
	assert.NotEqual(t, 0, h.EnterRingFd)

	sqe := h.io_uring_get_sqe()
	assert.NotNil(t, sqe)
	fmt.Printf("%+#v\n", sqe)
}

// func TestRingSqpollOnly(t *testing.T) {
// 	h := testNewIoUringWithParams(t, 256, &IoUringParams{
// 		Flags:        IORING_SETUP_SQPOLL,
// 		SqThreadCpu:  10, // ms
// 		SqThreadIdle: 10_000,
// 	})
// 	for i := 0; i < 10; i++ {
// 		sqe := h.GetSqe()
// 		PrepNop(sqe)
// 	}
// 	h.Submit()
// 	var cqe *IoUringCqe

// 	for {
// 		h.WaitCqe(&cqe)
// 		spew.Dump(cqe)
// 		h.SeenCqe(cqe)
// 	}
// }

func TestRingQueueOrderRetrieval(t *testing.T) {
	const entries = 256
	h := testNewIoUring(t, entries, 0)
	defer h.Close()

	var i uint64
	for i = 0; i < entries; i++ {
		sqe := h.GetSqe()
		PrepNop(sqe)
		sqe.UserData.SetUint64(i)
		sqe.Flags |= IOSQE_IO_LINK // ordered
	}

	submitted, err := h.SubmitAndWait(entries)
	require.NoError(t, err)
	require.Equal(t, int(entries), submitted)

	var cqe *IoUringCqe
	for i = 0; i < entries; i++ {
		err = h.WaitCqe(&cqe)
		require.NoError(t, err)
		require.NotNil(t, cqe)
		require.Equal(t, i, cqe.UserData.GetUint64())
		h.SeenCqe(cqe)
	}

}

func TestRingQueueSubmitSingleConsumer(t *testing.T) {
	type opt struct {
		name     string
		jobCount int

		entries uint32
		p       IoUringParams
	}
	ts := []opt{
		{"def-1-256", 1, 256, IoUringParams{}},
		{"def-128-256", 256, 256, IoUringParams{}}, // passed 128
		{"def-128-256", 256, 256, IoUringParams{}}, // passed 128
		{"def-8-256", 8, 256, IoUringParams{}},
		{"def-16-256", 16, 256, IoUringParams{}},
		{"def-32-256", 32, 256, IoUringParams{}},
		{"def-64-256", 64, 256, IoUringParams{}},
		{"def-128-256", 128, 256, IoUringParams{}},
		{"def-128+1-256", 128 + 1, 256, IoUringParams{}}, // passed 128
		{"def-128+2-256", 128 + 2, 256, IoUringParams{}}, // passed 128
		{"def-256-256", 256, 256, IoUringParams{}},

		{"sqpoll-127-256", 127, 256, IoUringParams{Flags: IORING_SETUP_SQPOLL, SqThreadCpu: 4, SqThreadIdle: 10_000}},
		{"sqpoll-128+2-256", 128 + 2, 256, IoUringParams{Flags: IORING_SETUP_SQPOLL, SqThreadCpu: 4, SqThreadIdle: 10_000}},
		{"sqpoll-256-256", 256, 256, IoUringParams{Flags: IORING_SETUP_SQPOLL, SqThreadCpu: 4, SqThreadIdle: 10_000}},

		// we can have other test for queue overflow.
	}
	for _, tc := range ts {

		t.Run(tc.name, func(t *testing.T) {
			ftmp, err := os.CreateTemp(os.TempDir(), "test_iouring_queue_sc_*")
			require.NoError(t, err)
			defer ftmp.Close()
			fdTemp := ftmp.Fd()

			consumer := func(h *IoUring, ctx context.Context, wg *sync.WaitGroup) {
				var cqe *IoUringCqe
				var err error
				defer func() {
					rec := recover()
					if rec != nil {
						spew.Dump(cqe)
					}
				}()
				for ctx.Err() == nil {
					err = h.io_uring_wait_cqe(&cqe)
					if err == syscall.EINTR {
						// ignore INTR
						continue
					}
					if err != nil {
						panic(err)
					}
					if cqe.Res < 0 {
						panic(syscall.Errno(-cqe.Res))
					}
					// cqe data check
					if int(cqe.Res) < len("data ") {
						panic(fmt.Sprintf("write less that it should"))
					}
					if (cqe.UserData.GetUintptr()>>(8<<2))&0xff == 0x00 {
						panic(fmt.Sprintf("cqe userdata should contain canonical address got %+#v", cqe.UserData))
					}

					bufPtr := (*[]byte)(cqe.UserData.GetUnsafe())
					buf := *bufPtr // deref check
					_ = buf
					// fmt.Printf("%+#v %s", buf, buf)

					h.io_uring_cqe_seen(cqe) // necessary
					wg.Done()
				}
			}

			submit := func(t *testing.T, opt *IoUringParams, h *IoUring, expectedSubmitCount int) {
				submitted, err := h.io_uring_submit()
				assert.NoError(t, err)
				if opt.Flags&IORING_SETUP_SQPOLL == 0 {
					assert.Equal(t, expectedSubmitCount, submitted)
				}
			}

			t.Run("submit_single", func(t *testing.T) {
				var wg sync.WaitGroup

				h := testNewIoUringWithParams(t, 256, &tc.p)
				defer h.Close()

				wg.Add(tc.jobCount)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go consumer(h, ctx, &wg)

				for i := 0; i < tc.jobCount; i++ {
					var sqe *IoUringSqe
					for { // sqe could be nil if SQ is already full so we spin until we got one
						sqe = h.io_uring_get_sqe()
						if sqe != nil {
							break
						}
					}

					var buf = new([]byte)
					*buf = append(*buf, []byte(fmt.Sprintf("data %d\n", i))...)
					reflect.ValueOf(buf) // escape the `buf`

					PrepWrite(sqe, int(fdTemp), &(*buf)[0], len((*buf)), 0)
					runtime.KeepAlive(buf)
					sqe.UserData.SetUnsafe(unsafe.Pointer(buf))

					// submit
					submit(t, &tc.p, h, 1)
				}
				runtime.GC()
				wg.Wait()
			})

			t.Run("submit_bulk", func(t *testing.T) {
				var wg sync.WaitGroup

				h := testNewIoUringWithParams(t, 256, &tc.p)
				defer h.Close()

				wg.Add(tc.jobCount)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go consumer(h, ctx, &wg)

				for i := 0; i < tc.jobCount; i++ {
					sqe := h.io_uring_get_sqe()
					if sqe == nil {
						// spin until we got one
						continue
					}

					buf := new([]byte)
					*buf = append(*buf, []byte(fmt.Sprintf("data %d\n", i))...)

					PrepWrite(sqe, int(fdTemp), &(*buf)[0], len((*buf)), 0)
					sqe.UserData.SetUnsafe(unsafe.Pointer(buf))
				}

				submit(t, &tc.p, h, tc.jobCount)
				runtime.GC()
				wg.Wait()
			})

		})
	}
}
