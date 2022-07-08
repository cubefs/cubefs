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

package volumemgr

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
)

func TestTaskManager(t *testing.T) {
	mgr := newTaskManager(10)
	go mgr.run()

	ch := make(chan struct{}, 1)
	taskf := func(t *volTask) error {
		ch <- struct{}{}
		return nil
	}
	taskId := uuid.New().String()
	task := newVolTask(1, base.VolumeTaskTypeLock, taskId, taskf)
	mgr.AddTask(task)
	<-ch
	require.Nil(t, mgr.schedMap[task.vid])
	require.Nil(t, mgr.taskMap[task.vid])

	waitCh := make(chan struct{}, 1)
	taskf1 := func(t *volTask) error {
		ch <- struct{}{}
		<-waitCh
		return errors.New("Unexpected")
	}
	task = newVolTask(2, base.VolumeTaskTypeLock, taskId, taskf1)
	mgr.AddTask(task)
	<-ch // wait task exec
	require.Equal(t, task.taskId, mgr.schedMap[task.vid].taskId)

	taskf2 := func(t *volTask) error {
		ch <- struct{}{}
		return nil
	}
	taskId = uuid.New().String()
	task = newVolTask(2, base.VolumeTaskTypeUnlock, taskId, taskf2)
	mgr.AddTask(task)
	require.Equal(t, task.taskId, mgr.taskMap[task.vid].Value.(*volTask).taskId)
	waitCh <- struct{}{}
	<-ch
	require.Nil(t, mgr.taskMap[task.vid])
	require.Nil(t, mgr.schedMap[task.vid])

	taskId = uuid.New().String()
	taskf = func(t *volTask) error {
		select {
		case ch <- struct{}{}:
		default:
		}
		return errors.New("Unexpected")
	}
	task = newVolTask(3, base.VolumeTaskTypeUnlock, taskId, taskf)
	mgr.AddTask(task)
	<-ch // make sure the task has already been executed once
	mgr.mu.Lock()
	require.Equal(t, 1, len(mgr.taskMap)+len(mgr.schedMap))
	mgr.mu.Unlock()
	mgr.DeleteTask(3, taskId)
	require.Equal(t, 0, len(mgr.schedMap))
	require.Equal(t, 0, len(mgr.taskMap))
	mgr.Close()
}
