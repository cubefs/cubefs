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

package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/retry"
)

var (
	errTestOnly      = errors.New("test: this is a fake error")
	errTestInterrupt = errors.New("test: this is a interruptable error")
)

func TestRetryNoRetry(t *testing.T) {
	st := time.Now().Unix()
	err := retry.Timed(10, 1000000).On(func() error {
		return nil
	})
	et := time.Now().Unix()

	require.NoError(t, err)
	require.LessOrEqual(t, et, st)
}

func TestRetryOnce(t *testing.T) {
	st := time.Now()
	called := 0
	err := retry.Timed(10, 200).On(func() error {
		if called == 0 {
			called++
			return errTestOnly
		}
		return nil
	})
	duration := time.Since(st)

	require.NoError(t, err)
	require.Equal(t, 1, called)
	v := int64(duration / time.Millisecond)
	require.Less(t, int64(190), v, "duration: ", v)
}

func TestRetryMultiple(t *testing.T) {
	st := time.Now()
	called := 0
	err := retry.Timed(10, 100).On(func() error {
		if called < 5 {
			called++
			return errTestOnly
		}
		return nil
	})
	duration := time.Since(st)

	require.NoError(t, err)
	require.Equal(t, 5, called)
	v := int64(duration / time.Millisecond)
	require.Less(t, int64(450), v, "duration: ", v)
}

func TestRetryExhausted(t *testing.T) {
	st := time.Now()
	called := 0
	err := retry.Timed(2, 200).On(func() error {
		called++
		if called == 2 {
			return retry.ErrRetryFailed
		}
		return errTestOnly
	})
	duration := time.Since(st)

	require.ErrorIs(t, err, retry.ErrRetryFailed)
	v := int64(duration / time.Millisecond)
	require.Less(t, int64(190), v, "duration: ", v)
}

func TestRetryExponentialBackoff(t *testing.T) {
	st := time.Now()
	called := 0
	r := retry.ExponentialBackoff(7, 50)
	err := r.On(func() error {
		called++
		return errTestOnly
	})
	duration := time.Since(st)

	require.ErrorIs(t, err, errTestOnly)
	v := int64(duration / time.Millisecond)
	require.Equal(t, 7, called)
	require.Less(t, int64(1000), v, "duration: ", v)
	require.Greater(t, int64(1250), v, "duration: ", v)

	r.Reset()
	st = time.Now()
	err = r.On(func() error {
		called++
		return errTestOnly
	})
	duration = time.Since(st)

	require.ErrorIs(t, err, errTestOnly)
	v = int64(duration / time.Millisecond)
	require.Equal(t, 14, called)
	require.Less(t, int64(1000), v, "duration: ", v)
	require.Greater(t, int64(1250), v, "duration: ", v)
}

func TestRetryContext(t *testing.T) {
	{
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := retry.Timed(10, 0).OnContext(ctx, func() error {
			return errTestOnly
		})
		require.ErrorIs(t, err, errTestOnly)
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		err := retry.Timed(10, 0).OnContext(ctx, func() error {
			return errTestOnly
		})
		require.ErrorIs(t, err, errTestOnly)
		cancel()
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		called := 0
		err := retry.Timed(10, 400).OnContext(ctx, func() error {
			called++
			return errTestOnly
		})
		require.Equal(t, 3, called) // 0, 400, 800
		require.ErrorIs(t, err, errTestOnly)
		cancel()
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		called := 0
		err := retry.ExponentialBackoff(10, 300).RuptOnContext(ctx, func() (bool, error) {
			called++
			return false, errTestOnly
		})
		require.Equal(t, 3, called) // 0, 300, 900
		require.ErrorIs(t, err, errTestOnly)
		cancel()
	}
}

func TestRetryInterrupted(t *testing.T) {
	require.NoError(t, retry.Timed(10, 10).RuptOn(func() (bool, error) { return true, nil }))
	require.Error(t, retry.Timed(10, 10).RuptOn(func() (bool, error) { return false, errTestOnly }))

	st := time.Now()
	called := 0
	err := retry.Timed(10, 200).RuptOn(func() (bool, error) {
		if called < 2 {
			called++
			return false, errTestOnly
		}
		if called == 2 {
			return true, errTestInterrupt
		}
		return false, nil
	})
	duration := time.Since(st)

	require.ErrorIs(t, err, errTestInterrupt)
	require.Equal(t, 2, called)
	v := int64(duration / time.Millisecond)
	require.Less(t, int64(380), v, "duration: ", v)
}

func TestRetryInterruptedError(t *testing.T) {
	st := time.Now()
	called := 0
	err := retry.Timed(10, 200).RuptOn(func() (bool, error) {
		if called < 1 {
			called++
			return false, errTestOnly
		}
		if called == 1 {
			return true, retry.ErrRetryNext
		}
		return false, nil
	})
	duration := time.Since(st)

	// get last error if interrupt with ErrRetryNext
	require.ErrorIs(t, err, errTestOnly)
	require.Equal(t, 1, called)
	v := int64(duration / time.Millisecond)
	require.Less(t, int64(190), v, "duration: ", v)

	called = 0
	err = retry.Timed(10, 200).RuptOn(func() (bool, error) {
		if called < 1 {
			called++
			return false, retry.ErrRetryNext
		}
		if called == 1 {
			return true, retry.ErrRetryNext
		}
		return false, nil
	})
	duration = time.Since(st)

	// ignored the ErrRetryNext
	require.NoError(t, err)
	require.Equal(t, 1, called)
	v = int64(duration / time.Millisecond)
	require.Less(t, int64(190), v, "duration: ", v)
}

func BenchmarkWait(b *testing.B) {
	b.Run("success", func(b *testing.B) {
		r := retry.Timed(3, 1000)
		fn := func() error { return nil }
		for ii := 0; ii < b.N; ii++ {
			r.On(fn)
		}
	})
	b.Run("retry-3", func(b *testing.B) {
		r := retry.Timed(3, 0)
		fn := func() error { return errTestOnly }
		for ii := 0; ii < b.N; ii++ {
			r.On(fn)
		}
	})
}

func BenchmarkContext(b *testing.B) {
	b.Run("success", func(b *testing.B) {
		r := retry.Timed(3, 1000)
		ctx := context.Background()
		fn := func() error { return nil }
		for ii := 0; ii < b.N; ii++ {
			r.OnContext(ctx, fn)
		}
	})
	b.Run("retry-3", func(b *testing.B) {
		r := retry.Timed(3, 1000)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fn := func() error { return errTestOnly }
		for ii := 0; ii < b.N; ii++ {
			r.OnContext(ctx, fn)
		}
	})
}
