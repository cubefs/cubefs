// Copyright 2018 The CubeFS Authors.
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

package master

import (
	"container/list"
	"sync"
)

type MigrateTask struct {
	Status          uint8
	RetryTimes      uint8
	VolName         string
	PartitionID     uint64
	CurrentExtentID uint64
	ModifyTime      int64
	CodecNode       string
}

type migrateList struct {
	sync.Mutex
	list  *list.List
	index map[uint64]*list.Element
}

func NewMigrateList() *migrateList {
	return &migrateList{
		list:  list.New(),
		index: make(map[uint64]*list.Element),
	}
}

func (ml *migrateList) Push(migrate *MigrateTask) {
	ml.Lock()
	defer ml.Unlock()
	if _, ok := ml.index[migrate.PartitionID]; !ok {
		item := ml.list.PushBack(migrate)
		ml.index[migrate.PartitionID] = item
	}
}

func (ml *migrateList) Remove(partitionID uint64) {
	ml.Lock()
	defer ml.Unlock()
	if element, ok := ml.index[partitionID]; ok {
		ml.list.Remove(element)
		delete(ml.index, partitionID)
	}
}

func (ml *migrateList) Exists(partitionID uint64) (task *MigrateTask, exist bool) {
	ml.Lock()
	defer ml.Unlock()
	exist = true
	element, has := ml.index[partitionID]
	if !has {
		exist = false
		return
	}
	task = element.Value.(*MigrateTask)
	return
}

func (ml *migrateList) Len() int {
	ml.Lock()
	defer ml.Unlock()
	return len(ml.index)
}

func (ml *migrateList) Clear() {
	ml.Lock()
	defer ml.Unlock()
	for k, v := range ml.index {
		ml.list.Remove(v)
		delete(ml.index, k)
	}
}

func (ml *migrateList) GetAllTask() []*MigrateTask {
	ml.Lock()
	defer ml.Unlock()

	allTasks := make([]*MigrateTask, 0)
	for task := ml.list.Front(); task != nil; task = task.Next() {
		migrateTask := task.Value.(*MigrateTask)
		allTasks = append(allTasks, migrateTask)
	}
	return allTasks
}

func (ml *migrateList) GetMigrateTimeoutTask() []*MigrateTask {
	ml.Lock()
	defer ml.Unlock()

	timeoutTasks := make([]*MigrateTask, 0)
	for task := ml.list.Front(); task != nil; task = task.Next() {
		migrateTask := task.Value.(*MigrateTask)
		if migrateTask.Status == EcTaskMigrating {
			timeoutTasks = append(timeoutTasks, migrateTask)
		}
	}

	return timeoutTasks
}

func (ml *migrateList) GetRetryTask() []*MigrateTask {
	ml.Lock()
	defer ml.Unlock()

	retryTasks := make([]*MigrateTask, 0)
	for task := ml.list.Front(); task != nil; task = task.Next() {
		migrateTask := task.Value.(*MigrateTask)
		if migrateTask.Status == EcTaskRetry {
			retryTasks = append(retryTasks, migrateTask)
		}
	}

	return retryTasks
}

func (ml *migrateList) GetFailTask() []*MigrateTask {
	ml.Lock()
	defer ml.Unlock()

	failTasks := make([]*MigrateTask, 0)
	for task := ml.list.Front(); task != nil; task = task.Next() {
		migrateTask := task.Value.(*MigrateTask)
		if migrateTask.Status == EcTaskFail {
			failTasks = append(failTasks, migrateTask)
		}
	}

	return failTasks
}
