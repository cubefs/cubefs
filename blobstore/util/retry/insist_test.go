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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/retry"
)

var ms100 = 100 * time.Millisecond

func TestRetryInsistBase(t *testing.T) {
	var i int
	cases := []struct {
		exp  int
		f    func() error
		on   func(error)
		d    time.Duration
		mind time.Duration
		maxd time.Duration
	}{
		{
			exp:  0,
			f:    func() error { return nil },
			on:   func(error) {},
			d:    ms100,
			mind: 0,
			maxd: ms100,
		},
		{
			exp: 3,
			f: func() error {
				i++
				if i < 3 {
					return fmt.Errorf("fake")
				}
				return nil
			},
			on:   func(error) {},
			d:    5 * ms100,
			mind: 10 * ms100,
			maxd: 12 * ms100,
		},
		{
			exp: 4,
			f: func() error {
				i += 2
				if i < 3 {
					return fmt.Errorf("fake")
				}
				return nil
			},
			on:   func(error) {},
			d:    5 * ms100,
			mind: 5 * ms100,
			maxd: 7 * ms100,
		},
		{
			exp: 3,
			f: func() error {
				i++
				if i < 3 {
					return fmt.Errorf("fake")
				}
				return nil
			},
			on:   func(error) { i++ },
			d:    5 * ms100,
			mind: 5 * ms100,
			maxd: 7 * ms100,
		},
	}

	for _, cs := range cases {
		i = 0

		startTime := time.Now()
		retry.Insist(cs.d, cs.f, cs.on)
		require.Equal(t, cs.exp, i)

		duration := time.Since(startTime)
		require.LessOrEqual(t, cs.mind, duration, "less duration: ", duration)
		require.GreaterOrEqual(t, cs.maxd, duration, "greater duration: ", duration)
	}
}

func TestRetryInsistContext(t *testing.T) {
	var i int
	ctxDuration := 3 * ms100
	cases := []struct {
		exp  int
		f    func() error
		on   func(error)
		d    time.Duration
		mind time.Duration
		maxd time.Duration
	}{
		{
			exp:  0,
			f:    func() error { return nil },
			on:   func(error) {},
			d:    ms100,
			mind: 0,
			maxd: ms100,
		},
		{
			exp: 2,
			f: func() error {
				i++
				if i < 3 {
					return fmt.Errorf("fake")
				}
				return nil
			},
			on:   func(error) {},
			d:    2 * ms100,
			mind: 2 * ms100,
			maxd: ctxDuration + ms100,
		},
		{
			exp: 2,
			f: func() error {
				i += 2
				if i < 3 {
					return fmt.Errorf("fake")
				}
				return nil
			},
			on:   func(error) {},
			d:    5 * ms100,
			mind: ctxDuration,
			maxd: ctxDuration + ms100,
		},
		{
			exp: 3,
			f: func() error {
				i++
				if i < 3 {
					return fmt.Errorf("fake")
				}
				return nil
			},
			on:   func(error) { i++ },
			d:    ms100,
			mind: ms100,
			maxd: 2 * ms100,
		},
	}

	for _, cs := range cases {
		i = 0

		startTime := time.Now()
		ctx, cancel := context.WithDeadline(context.TODO(), startTime.Add(ctxDuration))

		retry.InsistContext(ctx, cs.d, cs.f, cs.on)
		require.Equal(t, cs.exp, i)

		duration := time.Since(startTime)
		require.LessOrEqual(t, cs.mind, duration, "less duration: ", duration)
		require.GreaterOrEqual(t, cs.maxd, duration, "greater duration: ", duration)

		cancel()
	}
}
