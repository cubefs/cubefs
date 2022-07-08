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

package base

type TaskDistribution struct {
	chs []chan func()
}

func NewTaskDistribution(concurrency int, taskBuffLength int) *TaskDistribution {
	chs := make([]chan func(), concurrency)
	for i := range chs {
		chs[i] = make(chan func(), taskBuffLength)
	}
	td := &TaskDistribution{
		chs: chs,
	}
	td.startWorker()

	return td
}

func (t *TaskDistribution) Run(taskIdx int, task func()) {
	if taskIdx < 0 || taskIdx > len(t.chs) {
		panic("invalid task index")
	}
	t.chs[taskIdx] <- task
}

func (t *TaskDistribution) Close() {
	for i := range t.chs {
		close(t.chs[i])
	}
}

func (t *TaskDistribution) startWorker() {
	for i := 0; i < len(t.chs); i++ {
		ch := t.chs[i]
		go func() {
			for {
				// get task from ch and run
				task, ok := <-ch
				if !ok {
					break
				}
				task()
			}
		}()
	}
}
