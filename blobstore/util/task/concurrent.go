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

package task

import "context"

// Concurrent is tasks run concurrently.
func Concurrent[T any](f func(index int, arg T), args ...T) {
	ConcurrentContext(context.Background(), f, args...)
}

// ConcurrentContext is tasks run concurrently with context.
func ConcurrentContext[T any](ctx context.Context, f func(index int, arg T), args ...T) {
	tasks := make([]func() error, len(args))
	for ii := range args {
		index, arg := ii, args[ii]
		tasks[ii] = func() error {
			f(index, arg)
			return nil
		}
	}
	Run(ctx, tasks...)
}

// ConcurrentResult runs tasks concurrently and collects results.
func ConcurrentResult[T, R any](f func(index int, arg T) R, args ...T) []R {
	return ConcurrentResultContext(context.Background(), f, args...)
}

// ConcurrentResultContext runs tasks concurrently with context and collects results.
func ConcurrentResultContext[T, R any](ctx context.Context, f func(index int, arg T) R, args ...T) []R {
	results := make([]R, len(args))
	tasks := make([]func() error, len(args))
	for ii := range args {
		idx, arg := ii, args[ii]
		tasks[ii] = func() error {
			results[idx] = f(idx, arg)
			return nil
		}
	}
	Run(ctx, tasks...)
	return results
}
