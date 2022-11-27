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

package mergetask

import (
	"errors"
)

var panicError = errors.New("mergetask: panic error")

type MergeTask struct {
	limit    int
	mergeCh  chan interface{}
	mergedCh chan struct{}
	lockCh   chan struct{}
	ackCh    chan error
	taskFunc func(interface{}) error
}

func (mt *MergeTask) Do(task interface{}) (err error) {
	select {
	case mt.mergeCh <- task:
		<-mt.mergedCh
		return <-mt.ackCh
	case mt.lockCh <- struct{}{}:
	}

	return mt.do(task)
}

func (mt *MergeTask) do(task interface{}) (err error) {
	var merged int
merge:
	for merged <= mt.limit {
		select {
		case <-mt.mergeCh:
			merged++
			mt.mergedCh <- struct{}{}
		default:
			break merge
		}
	}

	defer func() {
		if err == panicError {
			panic(err)
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			err = panicError
		}
		for i := 0; i < merged; i++ {
			mt.ackCh <- err
		}
		<-mt.lockCh
	}()

	err = mt.taskFunc(task)
	return
}

func NewMergeTask(limit int, taskFn func(interface{}) error) (mt *MergeTask) {
	if limit <= 0 {
		limit = 1024
	}
	return &MergeTask{
		limit:    limit,
		mergeCh:  make(chan interface{}),
		mergedCh: make(chan struct{}),
		lockCh:   make(chan struct{}, 1),
		ackCh:    make(chan error),
		taskFunc: taskFn,
	}
}
