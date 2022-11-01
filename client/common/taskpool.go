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

// Package taskpool provides limited pool running task
package common

// TaskPool limited pool
type TaskPool struct {
	pool chan func()
}

// New returns task pool with workerCount and poolSize
func New(workerCount, poolSize int) TaskPool {
	pool := make(chan func(), poolSize)
	for i := 0; i < workerCount; i++ {
		go func() {
			for {
				task, ok := <-pool
				if !ok {
					break
				}
				task()
			}
		}()
	}
	return TaskPool{pool: pool}
}

// Run add task to pool, block if pool is full
func (tp TaskPool) Run(task func()) {
	tp.pool <- task
}

// TryRun try to add task to pool, return immediately
func (tp TaskPool) TryRun(task func()) bool {
	select {
	case tp.pool <- task:
		return true
	default:
		return false
	}
}

// Close the pool, the function is concurrent unsafe
func (tp TaskPool) Close() {
	close(tp.pool)
}
