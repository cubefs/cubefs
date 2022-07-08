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
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	defaultConcurrence = 10
	maxErrCnt          = 30
)

type volTask struct {
	vid      proto.Vid
	taskType base.VolumeTaskType
	taskId   string
	f        func(task *volTask) error
	errCnt   uint32
	expire   time.Time
	context  []byte
}

func newVolTask(vid proto.Vid, taskType base.VolumeTaskType, taskId string, f func(task *volTask) error) *volTask {
	return &volTask{
		vid:      vid,
		taskType: taskType,
		taskId:   taskId,
		f:        f,
		expire:   time.Now(),
	}
}

func (task *volTask) String() string {
	return fmt.Sprintf("{\"vid\": %d, \"type\": \"%s\", \"taskId\": \"%s\", \"errCnt\": %d}", task.vid, task.taskType.String(), task.taskId, task.errCnt)
}

type taskManager struct {
	mu             sync.Mutex
	taskList       *list.List
	taskMap        map[proto.Vid]*list.Element
	schedMap       map[proto.Vid]*volTask
	maxConcurrence int
	notifyc        chan struct{}
	stopc          chan struct{}
	once           sync.Once
}

func newTaskManager(maxConcurrence int) *taskManager {
	if maxConcurrence <= 0 {
		maxConcurrence = defaultConcurrence
	}
	mgr := &taskManager{
		taskList:       list.New(),
		taskMap:        make(map[proto.Vid]*list.Element),
		schedMap:       make(map[proto.Vid]*volTask),
		maxConcurrence: maxConcurrence,
		notifyc:        make(chan struct{}, 1),
		stopc:          make(chan struct{}),
	}
	return mgr
}

func (m *taskManager) run() {
	c := make(chan struct{}, m.maxConcurrence)
	for {
		var done bool
		m.mu.Lock()
		for elem := m.taskList.Front(); elem != nil && !done; {
			next := elem.Next()
			task := elem.Value.(*volTask)

			// skip task that is running
			if _, hit := m.schedMap[task.vid]; hit {
				elem = next
				continue
			}
			if time.Since(task.expire) < 0 {
				elem = next
				continue
			}
			select {
			case c <- struct{}{}:
				delete(m.taskMap, task.vid)
				m.taskList.Remove(elem)
				m.schedMap[task.vid] = task
				go m.execTask(task, c)
			case <-m.stopc:
				m.mu.Unlock()
				return
			default:
				done = true
			}
			elem = next
		}
		m.mu.Unlock()
		select {
		case <-m.stopc:
			return
		case <-time.After(time.Second):
		case <-m.notifyc:
		}
	}
}

func (m *taskManager) reschedule(task *volTask) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, hit := m.taskMap[task.vid]; hit {
		delete(m.schedMap, task.vid) // this task had been replaced
		return
	}
	if _, hit := m.schedMap[task.vid]; !hit {
		delete(m.schedMap, task.vid) // this task had been canceled
		return
	}

	// add task head to reschedule
	delete(m.schedMap, task.vid)
	elem := m.taskList.PushBack(task)
	m.taskMap[task.vid] = elem
}

func (m *taskManager) notify() {
	select {
	case m.notifyc <- struct{}{}:
	default:
	}
}

func (m *taskManager) execTask(task *volTask, done <-chan struct{}) {
	var err error
	defer func() {
		<-done
		if err == nil {
			m.notify()
		}
	}()
	if err = task.f(task); err != nil {
		if err != errNotLeader {
			if task.errCnt < maxErrCnt {
				task.errCnt++
			}
			task.expire = time.Now().Add(time.Second * time.Duration(task.errCnt))
		}
		m.reschedule(task) // add task to taskMap and reschedule
		return
	}
	m.mu.Lock()
	delete(m.schedMap, task.vid)
	m.mu.Unlock()
}

func (m *taskManager) AddTask(task *volTask) {
	m.mu.Lock()
	defer m.mu.Unlock()
	elem, hit := m.taskMap[task.vid]
	if hit {
		m.taskList.Remove(elem)
	}
	elem = m.taskList.PushBack(task)
	m.taskMap[task.vid] = elem
	m.notify()
}

func (m *taskManager) DeleteTask(vid proto.Vid, taskId string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	elem := m.taskMap[vid]
	if elem != nil && elem.Value.(*volTask).taskId == taskId {
		delete(m.taskMap, vid)
		m.taskList.Remove(elem)
	}
	task := m.schedMap[vid]
	if task != nil && task.taskId == taskId {
		delete(m.schedMap, vid)
	}
}

func (m *taskManager) RemoveAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskList = list.New()
	m.taskMap = make(map[proto.Vid]*list.Element)
	m.schedMap = make(map[proto.Vid]*volTask)
}

func (m *taskManager) Close() {
	m.once.Do(func() {
		m.RemoveAll()
		close(m.stopc)
	})
}
