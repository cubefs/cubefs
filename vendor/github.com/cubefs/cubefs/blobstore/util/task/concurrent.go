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

var (
	// C alias of Concurrent
	C = Concurrent
	// CC alias of ConcurrentContext
	CC = ConcurrentContext
)

// Concurrent is tasks run concurrently.
func Concurrent(f func(index int, arg interface{}), args []interface{}) {
	ConcurrentContext(context.Background(), f, args)
}

// ConcurrentContext is tasks run concurrently with context.
// How to make []interface{} see: https://golang.org/doc/faq#convert_slice_of_interface
func ConcurrentContext(ctx context.Context, f func(index int, arg interface{}), args []interface{}) {
	tasks := make([]func() error, len(args))
	for ii := 0; ii < len(args); ii++ {
		index, arg := ii, args[ii]
		tasks[ii] = func() error {
			f(index, arg)
			return nil
		}
	}
	Run(ctx, tasks...)
}
