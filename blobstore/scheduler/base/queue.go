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

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// queue implement
// task queue & worker task queue

var (
	// ErrNoSuchMessageID no such message id
	ErrNoSuchMessageID = errors.New("no such message id")
	// ErrUnmatchedVuids unmatched task vuids
	ErrUnmatchedVuids    = errors.New("unmatched task vuids")
	errNoSuchIDCQueue    = errors.New("no such idc queue")
	errExistingMessageID = errors.New("existing message id")
)

const (
	msgStateTodo = iota + 1
	msgStateDoing
)

// 100 years is long enough。
const neverTimeout = time.Duration(100*365*24) * time.Hour

// Queue task queue
type Queue struct {
	mu    sync.RWMutex
	todo  *list.List
	doing *list.List
	msgs  map[string]*list.Element

	msgTimeout time.Duration // default duration of task locking
}

// NewQueue return task queue
func NewQueue(msgTimeout time.Duration) *Queue {
	if msgTimeout == 0 {
		msgTimeout = neverTimeout
	}
	q := &Queue{
		todo:       new(list.List),
		doing:      new(list.List),
		msgs:       make(map[string]*list.Element),
		msgTimeout: msgTimeout,
	}
	return q
}

type msgEx struct {
	id       string
	state    int
	deadline time.Time
	msg      interface{}
}

// Push push message to queue id is uniquely identifies。
func (q *Queue) Push(id string, msg interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.msgs[id]; ok {
		return errExistingMessageID
	}

	m := &msgEx{
		id:    id,
		state: msgStateTodo,
		msg:   msg,
	}
	elem := q.todo.PushBack(m)
	q.msgs[id] = elem

	return nil
}

// Pop  fetch a msg from queue。
func (q *Queue) Pop() (string, interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	for ele := q.doing.Front(); ele != nil; ele = ele.Next() {
		m := ele.Value.(*msgEx)
		if m.deadline.Before(now) {
			m.deadline = now.Add(q.msgTimeout)
			return m.id, m.msg, true
		}
	}

	// no timeout msg in doing ,fetch from todo
	if q.todo.Len() == 0 {
		return "", nil, false
	}
	elem := q.todo.Front()
	q.todo.Remove(elem)

	m := elem.Value.(*msgEx)
	m.state = msgStateDoing
	m.deadline = now.Add(q.msgTimeout)

	elem = q.doing.PushFront(m)
	q.msgs[m.id] = elem

	return m.id, m.msg, true
}

// Get returns message by id
func (q *Queue) Get(id string) (interface{}, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	elem, ok := q.msgs[id]
	if !ok {
		return nil, ErrNoSuchMessageID
	}
	return elem.Value.(*msgEx).msg, nil
}

// Requeue :msg while get again after delay。
func (q *Queue) Requeue(id string, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if delay < 0 {
		delay = 0
	}
	elem, ok := q.msgs[id]
	if !ok {
		return ErrNoSuchMessageID
	}
	m := elem.Value.(*msgEx)
	if m.state == msgStateTodo {
		// msg pop and new queue and reload msg, the pop msg state will chang to msgStateTodo
		// if msg in todo queue then return success。
		return nil
	}

	// msg in doing queue。
	m.deadline = time.Now().Add(delay)
	return nil
}

// Remove remove message by id
func (q *Queue) Remove(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	elem, ok := q.msgs[id]
	if !ok {
		return ErrNoSuchMessageID
	}
	m := elem.Value.(*msgEx)
	switch m.state {
	case msgStateTodo:
		q.todo.Remove(elem)
	case msgStateDoing:
		q.doing.Remove(elem)
	default:
		panic("invalid msg state")
	}
	delete(q.msgs, id)
	return nil
}

// Stats returns queue stats
func (q *Queue) Stats() (todo, doing int) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.todo.Len(), q.doing.Len()
}

// WorkerTask define worker task interface
type WorkerTask interface {
	GetSrc() []proto.VunitLocation
	GetDest() proto.VunitLocation
	SetDest(dest proto.VunitLocation)
}

// TaskQueue task queue
type TaskQueue struct {
	mu         sync.Mutex
	queue      *Queue
	retryDelay time.Duration // punish a period of time to avoid frequent failure retry。
}

// NewTaskQueue returns task queue
func NewTaskQueue(retryDelay time.Duration) *TaskQueue {
	return &TaskQueue{
		queue:      NewQueue(0),
		retryDelay: retryDelay,
	}
}

// PushTask push task to queue
func (q *TaskQueue) PushTask(taskID string, task WorkerTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.queue.Push(taskID, task)
	if err != nil {
		panic("unexpect push task fail " + err.Error())
	}
}

// PopTask return args： taskID, task, flag of task exist
func (q *TaskQueue) PopTask() (string, WorkerTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	taskID, task, exist := q.queue.Pop()
	if exist {
		return taskID, task.(WorkerTask), true
	}
	return "", nil, false
}

// RemoveTask remove task by taskID
func (q *TaskQueue) RemoveTask(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Remove(taskID)
}

// RetryTask retry task by taskID
func (q *TaskQueue) RetryTask(taskID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.queue.Requeue(taskID, q.retryDelay)
	if err != nil {
		panic("unexpect retry task fail:" + err.Error())
	}
}

// Query find task by taskID
func (q *TaskQueue) Query(taskID string) (WorkerTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	task, err := q.queue.Get(taskID)
	if err != nil {
		return nil, false
	}
	return task.(WorkerTask), true
}

// StatsTasks returns task stats
func (q *TaskQueue) StatsTasks() (todo int, doing int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Stats()
}

// WorkerTaskQueue task queue for worker
type WorkerTaskQueue struct {
	mu        sync.Mutex
	idcQueues map[string]*Queue

	cancelPunishDuration time.Duration // task cancel will punish a period of time to avoid frequent failure retry
	leaseExpiredS        time.Duration
}

// NewWorkerTaskQueue return worker task queue
func NewWorkerTaskQueue(cancelPunishDuration time.Duration) *WorkerTaskQueue {
	// extended lock duration of task leasing
	leaseExpiredS := proto.TaskLeaseExpiredS * time.Second

	return &WorkerTaskQueue{
		idcQueues:            make(map[string]*Queue),
		cancelPunishDuration: cancelPunishDuration,
		leaseExpiredS:        leaseExpiredS,
	}
}

// AddPreparedTask add prepared task
func (q *WorkerTaskQueue) AddPreparedTask(idc, taskID string, wtask WorkerTask) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idcQueue, ok := q.idcQueues[idc]
	if !ok {
		idcQueue = NewQueue(q.leaseExpiredS)
		q.idcQueues[idc] = idcQueue
	}
	err := idcQueue.Push(taskID, wtask)
	if err != nil {
		panic("unexpect add prepared task fail:" + err.Error())
	}
}

// Acquire acquire task by idc
func (q *WorkerTaskQueue) Acquire(idc string) (taskID string, wtask WorkerTask, exist bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idcQueue, ok := q.idcQueues[idc]
	if !ok {
		return "", nil, false
	}

	taskID, task, exist := idcQueue.Pop()
	if exist {
		return taskID, task.(WorkerTask), exist
	}
	return "", nil, false
}

// Cancel cancel task
func (q *WorkerTaskQueue) Cancel(idc, taskID string, src []proto.VunitLocation, dst proto.VunitLocation) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	idcQueue, ok := q.idcQueues[idc]
	if !ok {
		return errNoSuchIDCQueue
	}

	task, err := idcQueue.Get(taskID)
	if err != nil {
		return err
	}
	err = checkValid(task.(WorkerTask), src, dst)
	if err != nil {
		return err
	}

	return idcQueue.Requeue(taskID, q.cancelPunishDuration)
}

// Reclaim reclaim task
func (q *WorkerTaskQueue) Reclaim(idc, taskID string, src []proto.VunitLocation, oldDest, newDest proto.VunitLocation, newDiskID proto.DiskID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	idcQueue, ok := q.idcQueues[idc]
	if !ok {
		return errNoSuchIDCQueue
	}

	task, err := idcQueue.Get(taskID)
	if err != nil {
		return err
	}
	wtask := task.(WorkerTask)
	err = checkValid(wtask, src, oldDest)
	if err != nil {
		return err
	}
	wtask.SetDest(newDest)
	return idcQueue.Requeue(taskID, 0)
}

// Renewal renewal task
func (q *WorkerTaskQueue) Renewal(idc, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	idcQueue, ok := q.idcQueues[idc]
	if !ok {
		return errNoSuchIDCQueue
	}
	return idcQueue.Requeue(taskID, q.leaseExpiredS)
}

// Complete complete task
func (q *WorkerTaskQueue) Complete(idc, taskID string, src []proto.VunitLocation, dst proto.VunitLocation) (WorkerTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idcQueue, ok := q.idcQueues[idc]
	if !ok {
		return nil, errNoSuchIDCQueue
	}

	task, err := idcQueue.Get(taskID)
	if err != nil {
		return nil, err
	}

	t := task.(WorkerTask)
	err = checkValid(t, src, dst)
	if err != nil {
		return nil, err
	}

	err = idcQueue.Remove(taskID)
	if err != nil {
		panic(fmt.Sprintf("task %s remove form queue fail err %v", taskID, err))
	}

	return t, err
}

// StatsTasks returns task stats
func (q *WorkerTaskQueue) StatsTasks() (todo int, doing int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, queue := range q.idcQueues {
		todoTmp, doingTmp := queue.Stats()
		todo += todoTmp
		doing += doingTmp
	}
	return todo, doing
}

// Query find task by idc and taskID
func (q *WorkerTaskQueue) Query(idc, taskID string) (WorkerTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idcQueue, ok := q.idcQueues[idc]
	if !ok {
		return nil, errNoSuchIDCQueue
	}

	wt, err := idcQueue.Get(taskID)
	if err != nil {
		return nil, err
	}
	return wt.(WorkerTask), nil
}

// SetLeaseExpiredS set lease expired time
func (q *WorkerTaskQueue) SetLeaseExpiredS(dura time.Duration) {
	q.leaseExpiredS = dura
}

func checkValid(task WorkerTask, src []proto.VunitLocation, dst proto.VunitLocation) error {
	if !vunitSliceEqual(task.GetSrc(), src) || task.GetDest() != dst {
		return ErrUnmatchedVuids
	}
	return nil
}

func vunitSliceEqual(a, b []proto.VunitLocation) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
