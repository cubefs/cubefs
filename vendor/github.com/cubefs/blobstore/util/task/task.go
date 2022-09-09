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

import (
	"context"
)

type semaphore struct {
	ready chan struct{}
}

func newSemaphore(n int) *semaphore {
	s := &semaphore{
		ready: make(chan struct{}, n),
	}
	for ii := 0; ii < n; ii++ {
		s.ready <- struct{}{}
	}
	return s
}

func (s *semaphore) Wait() <-chan struct{} {
	return s.ready
}

func (s *semaphore) Signal() {
	s.ready <- struct{}{}
}

// Run executes list of tasks in parallel,
// returns the first error or nil if all tasks done.
func Run(ctx context.Context, tasks ...func() error) error {
	n := len(tasks)
	semaphore := newSemaphore(n)
	errorCh := make(chan error, 1)

	for _, task := range tasks {
		<-semaphore.Wait()
		go func(task func() error) {
			err := task()
			if err == nil {
				semaphore.Signal()
				return
			}

			select {
			case errorCh <- err:
			default:
			}
		}(task)
	}

	for ii := 0; ii < n; ii++ {
		select {
		case err := <-errorCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-semaphore.Wait():
		}
	}

	return nil
}
