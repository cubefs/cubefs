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

package task_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/util/task"
)

func TestTaskRunParallel(t *testing.T) {
	err := task.Run(context.Background(),
		func() error {
			time.Sleep(time.Millisecond * 100)
			return errors.New("test")
		},
		func() error {
			time.Sleep(time.Millisecond * 500)
			return errors.New("test2")
		},
	)
	require.Error(t, err)
	require.Equal(t, "test", err.Error())
}

func TestTaskRunParallelCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := task.Run(ctx,
		func() error {
			time.Sleep(time.Second * 1)
			return errors.New("test")
		},
		func() error {
			time.Sleep(time.Second * 5)
			return errors.New("test2")
		},
		func() error {
			cancel()
			return nil
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "canceled")
}

func BenchmarkTaskRunOne(b *testing.B) {
	none := func() error { return nil }
	for ii := 0; ii < b.N; ii++ {
		task.Run(context.Background(), none)
	}
}

func BenchmarkTaskRunTwo(b *testing.B) {
	none := func() error { return nil }
	for ii := 0; ii < b.N; ii++ {
		task.Run(context.Background(), none, none)
	}
}
