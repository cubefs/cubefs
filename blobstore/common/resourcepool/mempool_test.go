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

package resourcepool_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rp "github.com/cubefs/cubefs/blobstore/common/resourcepool"
)

const (
	kb  = 1 << 10
	mb  = 1 << 20
	mb4 = 4 * mb
)

func TestMemPoolBase(t *testing.T) {
	classes := map[int]int{kb: 2, mb: 1}

	pool := rp.NewMemPool(classes, false)
	require.NotNil(t, pool)

	bufm, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, mb, len(bufm))
	require.Equal(t, mb, cap(bufm))

	err = pool.Put(bufm)
	require.NoError(t, err)

	bufk, err := pool.Get(context.Background(), kb)
	require.NoError(t, err)
	require.Equal(t, kb, len(bufk))
	require.Equal(t, kb, cap(bufk))

	bufk2, err := pool.Get(context.Background(), kb/2)
	require.NoError(t, err)
	require.Equal(t, kb/2, len(bufk2))
	require.Equal(t, kb, cap(bufk2))

	err = pool.Put(bufk2)
	require.NoError(t, err)
	err = pool.Put(bufk)
	require.NoError(t, err)

	bufmx, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, bufm, bufmx)
}

func TestMemPoolEmpty(t *testing.T) {
	pool := rp.NewMemPool(nil, false)
	require.NotNil(t, pool)

	_, err := pool.Get(context.Background(), kb)
	require.ErrorIs(t, err, rp.ErrNoSuitableSizeClass)

	buf, err := pool.Alloc(context.Background(), kb)
	require.NoError(t, err)
	require.Equal(t, kb, len(buf))
	require.Equal(t, kb, cap(buf))

	err = pool.Put(buf)
	require.ErrorIs(t, err, rp.ErrNoSuitableSizeClass)
}

func TestMemPoolGetWaitAtCapacity(t *testing.T) {
	classes := map[int]int{mb: 1}
	pool := rp.NewMemPool(classes, true)
	require.NotNil(t, pool)

	buf, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_, err := pool.Get(context.Background(), mb)
		require.NoError(t, err)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, pool.Put(buf))
	<-done
}

func TestMemPoolAllocN(t *testing.T) {
	pool := rp.NewMemPool(map[int]int{mb: 4}, true)

	bufs, err := pool.AllocN(context.Background(), mb/2, 3)
	require.NoError(t, err)
	require.Len(t, bufs, 3)
	for _, buf := range bufs {
		require.Equal(t, mb/2, len(buf))
		require.Equal(t, mb, cap(buf))
	}
	require.Equal(t, 3, pool.Status()[0].Running)
	for _, buf := range bufs {
		require.NoError(t, pool.Put(buf))
	}
	require.Equal(t, 0, pool.Status()[0].Running)

	_, err = pool.AllocN(context.Background(), mb, 5)
	require.ErrorIs(t, err, rp.ErrPoolLimit)
	require.Equal(t, 0, pool.Status()[0].Running)

	oversize, err := pool.AllocN(context.Background(), mb4, 4)
	require.NoError(t, err)
	require.Len(t, oversize, 4)
	require.Equal(t, 0, pool.Status()[0].Running)
	for _, buf := range oversize {
		require.NoError(t, pool.Put(buf))
	}
	require.Equal(t, 0, pool.Status()[0].Running)
}

func TestMemPoolAllocNNoPartialDeadlock(t *testing.T) {
	pool := rp.NewMemPool(map[int]int{mb: 4}, true)
	var wg sync.WaitGroup
	for range [8]struct{}{} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bufs, err := pool.AllocN(context.Background(), mb, 3)
			if err != nil {
				t.Error(err)
				return
			}
			time.Sleep(time.Millisecond)
			for _, buf := range bufs {
				require.NoError(t, pool.Put(buf))
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deadlock: batch alloc held a partial set")
	}
}

func TestMemPoolAllocNSmallerRequestFits(t *testing.T) {
	pool := rp.NewMemPool(map[int]int{mb: 3}, true)
	held, err := pool.AllocN(context.Background(), mb, 2)
	require.NoError(t, err)

	order := make(chan string, 2)
	go func() {
		bufs, err := pool.AllocN(context.Background(), mb, 2)
		if err != nil {
			order <- "b-err"
			return
		}
		order <- "b"
		for _, buf := range bufs {
			require.NoError(t, pool.Put(buf))
		}
	}()
	time.Sleep(50 * time.Millisecond)
	go func() {
		bufs, err := pool.AllocN(context.Background(), mb, 1)
		if err != nil {
			order <- "c-err"
			return
		}
		order <- "c"
		for _, buf := range bufs {
			require.NoError(t, pool.Put(buf))
		}
	}()

	// One free slot fits the later request. It is not queued behind the batch.
	select {
	case got := <-order:
		require.Equal(t, "c", got)
	case <-time.After(2 * time.Second):
		t.Fatal("request that fits did not proceed")
	}
	select {
	case got := <-order:
		t.Fatalf("batch granted with one free slot: %s", got)
	case <-time.After(80 * time.Millisecond):
	}

	require.NoError(t, pool.Put(held[0]))
	select {
	case got := <-order:
		require.Equal(t, "b", got)
	case <-time.After(2 * time.Second):
		t.Fatal("batch was not granted after two slots were free")
	}
	require.NoError(t, pool.Put(held[1]))
}

func TestMemPoolAllocNCancelDoesNotLeak(t *testing.T) {
	pool := rp.NewMemPool(map[int]int{mb: 2}, true)
	held, err := pool.AllocN(context.Background(), mb, 2)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := pool.AllocN(ctx, mb, 2)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	for _, buf := range held {
		require.NoError(t, pool.Put(buf))
	}
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, 0, pool.Status()[0].Running)

	got := make(chan error, 1)
	go func() {
		bufs, err := pool.AllocN(context.Background(), mb, 2)
		if err != nil {
			got <- err
			return
		}
		for _, buf := range bufs {
			require.NoError(t, pool.Put(buf))
		}
		got <- nil
	}()
	select {
	case err := <-got:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("AllocN stuck after a canceled waiter")
	}
	require.Equal(t, 0, pool.Status()[0].Running)
}

func TestMemPoolChanAlloc(t *testing.T) {
	classes := map[int]int{mb: 1}
	pool := rp.NewMemPool(classes, false)
	require.NotNil(t, pool)

	bufm, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, mb, len(bufm))
	require.Equal(t, mb, cap(bufm))

	err = pool.Put(bufm)
	require.NoError(t, err)

	bufm2, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, bufm, bufm2)

	err = pool.Put(bufm2)
	require.NoError(t, err)
	require.Equal(t, 0, pool.Status()[0].Running)

	// if oversize, make new buffer
	bufm4, err := pool.Alloc(context.Background(), mb4)
	require.NoError(t, err)
	require.Equal(t, mb4, len(bufm4))
	require.Equal(t, mb4, cap(bufm4))
	require.Equal(t, 0, pool.Status()[0].Running)

	// oversize is released to GC, concurrence unchanged
	idleBefore := pool.Status()[0].Idle
	err = pool.Put(bufm4)
	require.NoError(t, err)
	require.Equal(t, 0, pool.Status()[0].Running)
	require.Equal(t, idleBefore, pool.Status()[0].Idle)

	bufm, err = pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, mb, len(bufm))
	require.Equal(t, mb, cap(bufm))
	require.Equal(t, 1, pool.Status()[0].Running)

	err = pool.Put(bufm)
	require.NoError(t, err)
	require.Equal(t, 0, pool.Status()[0].Running)
}

func TestMemPoolSyncAlloc(t *testing.T) {
	classes := map[int]int{mb: 1}
	pool := rp.NewMemPoolWith(classes, func(size, capacity int) rp.Pool {
		return rp.NewPool(func() interface{} {
			return make([]byte, size)
		}, capacity)
	})
	require.NotNil(t, pool)

	bufm, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, mb, len(bufm))
	require.Equal(t, mb, cap(bufm))

	_, err = pool.Get(context.Background(), mb)
	require.ErrorIs(t, err, rp.ErrPoolLimit)

	// if matched size class, return ErrPoolLimit
	_, err = pool.Alloc(context.Background(), mb)
	require.ErrorIs(t, err, rp.ErrPoolLimit)

	// if oversize, make new buffer
	bufm4, err := pool.Alloc(context.Background(), mb4)
	require.NoError(t, err)
	require.Equal(t, mb4, len(bufm4))
	require.Equal(t, mb4, cap(bufm4))
	require.Equal(t, 1, pool.Status()[0].Running)

	// oversize is released to GC, does not free the held slot
	err = pool.Put(bufm4)
	require.NoError(t, err)
	require.Equal(t, 1, pool.Status()[0].Running)

	_, err = pool.Get(context.Background(), mb)
	require.ErrorIs(t, err, rp.ErrPoolLimit)

	err = pool.Put(bufm)
	require.NoError(t, err)
	require.Equal(t, 0, pool.Status()[0].Running)

	bufm, err = pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, mb, len(bufm))
	require.Equal(t, mb, cap(bufm))

	err = pool.Put(bufm)
	require.NoError(t, err)
}

func TestMemPoolPutGet(t *testing.T) {
	classes := map[int]int{mb: 1}

	pool := rp.NewMemPool(classes, false)
	require.NotNil(t, pool)

	bufm, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, mb, len(bufm))
	require.Equal(t, mb, cap(bufm))

	zero := make([]byte, len(bufm))
	rand.Read(bufm)
	require.NotEqual(t, zero, bufm)

	err = pool.Put(bufm)
	require.NoError(t, err)

	bufmx, err := pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, bufm, bufmx)

	err = pool.Put(bufmx)
	require.NoError(t, err)
	require.Equal(t, 0, pool.Status()[0].Running)

	// if oversize, make new buffer
	bufm4, err := pool.Alloc(context.Background(), mb4)
	require.NoError(t, err)
	require.Equal(t, mb4, len(bufm4))
	require.Equal(t, mb4, cap(bufm4))

	rand.Read(bufm4)
	require.NotEqual(t, zero, bufm4[:mb])

	idleBefore := pool.Status()[0].Idle
	err = pool.Put(bufm4)
	require.NoError(t, err)
	require.Equal(t, 0, pool.Status()[0].Running)
	require.Equal(t, idleBefore, pool.Status()[0].Idle)

	bufm, err = pool.Get(context.Background(), mb)
	require.NoError(t, err)
	require.Equal(t, mb, len(bufm))
	require.Equal(t, mb, cap(bufm))
	require.Equal(t, bufmx, bufm)
}

func TestMemPoolZero(t *testing.T) {
	buffer := make([]byte, 1<<24)
	zero := make([]byte, 1<<24)
	cases := []struct {
		from, to int
	}{
		{0, 1 << 20},
		{0, 1 << 24},
		{1, 1 << 20},
		{1 << 20, 1 << 20},
		{1 << 20, 1 << 22},
		{1 << 20, 1 << 24},
		{1029, 1029},
		{1029, 1 << 14},
		{1029, 1 << 24},
	}

	for _, cs := range cases {
		pool := rp.NewMemPool(nil, false)
		require.NotNil(t, pool)

		buf := buffer[cs.from:cs.to]
		rand.Read(buf)
		if cs.from != cs.to {
			require.NotEqual(t, zero[cs.from:cs.to], buf)
		}
		pool.Zero(buf)
		require.Equal(t, cs.to-cs.from, len(buf))
		require.Equal(t, zero[cs.from:cs.to], buf)
	}
}

func TestMemPoolStatus(t *testing.T) {
	{
		pool := rp.NewMemPool(nil, false)
		require.NotNil(t, pool)
		t.Logf("status empty: %+v", pool.Status())
	}
	{
		classes := map[int]int{kb: 2, mb: 1}

		pool := rp.NewMemPool(classes, false)
		require.NotNil(t, pool)
		t.Logf("status init: %+v", pool.Status())

		_, err := pool.Get(context.Background(), mb)
		require.NoError(t, err)
		bufk, err := pool.Get(context.Background(), kb)
		require.NoError(t, err)
		t.Logf("status running: %+v", pool.Status())

		pool.Put(bufk)
		data, _ := json.Marshal(pool.Status())
		t.Logf("status json: %s", string(data))
	}
}

func BenchmarkMempool(b *testing.B) {
	mp := rp.NewMemPool(map[int]int{
		1 << 11: -1,
		1 << 14: -1,
		1 << 16: -1,
		1 << 18: -1,
		1 << 20: -1,
		1 << 21: -1,
		1 << 22: -1,
	}, false)

	for _, size := range []int{
		1 << 10,
		1 << 16,
		1 << 18,
		1 << 20,
		1 << 21,
		1 << 22,
	} {
		b.ResetTimer()
		b.Run(humman(size), func(b *testing.B) {
			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				if buf, err := mp.Get(context.Background(), size); err == nil {
					_ = mp.Put(buf)
				}
			}
		})
	}
}

func BenchmarkZero(b *testing.B) {
	funcZero := func(b, zero []byte) {
		for len(b) > 0 {
			n := copy(b, zero)
			b = b[n:]
		}
	}

	cases := []struct {
		size int
		zero int
	}{}

	for _, size := range []int{
		1 << 0,
		1 << 5,
		1 << 10,
		1 << 15,
		1 << 20,
		1 << 21,
		1 << 22,
		1 << 23,
		1 << 24,
		1 << 25,
	} {
		// zoro buffer from 256B - 32M
		for i := 8; i <= 24; i++ {
			cases = append(cases, struct {
				size int
				zero int
			}{size, 1 << i})
		}
	}

	for _, cs := range cases {
		b.ResetTimer()
		b.Run(fmt.Sprintf("%s-%s", humman(cs.size), humman(cs.zero)), func(b *testing.B) {
			zero := make([]byte, cs.zero)
			buff := make([]byte, cs.size)
			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				funcZero(buff, zero)
			}
		})
	}
}

var size2humman = []func(int) string{
	func(size int) string { return fmt.Sprintf("%dB", size) },
	func(size int) string { return fmt.Sprintf("%dKB", size/1024) },
	func(size int) string { return fmt.Sprintf("%dMB", size/1024/1024) },
	func(size int) string { return fmt.Sprintf("%dGB", size/1024/1024/1024) },
}

func humman(size int) string {
	for ii := len(size2humman) - 1; ii >= 0; ii-- {
		if size >= (1 << (10 * ii)) {
			return size2humman[ii](size)
		}
	}
	return "-"
}
