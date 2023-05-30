// Copyright 2023 The CubeFS Authors.
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

package iopool

import "github.com/cubefs/cubefs/blobstore/util/sys"

type IoTaskType uint32

const (
	ReadIoTask  = IoTaskType(0)
	WriteIoTask = IoTaskType(iota)
	SyncIoTask  = IoTaskType(iota)
	AllocIoTask = IoTaskType(iota)
)

type FileHandle interface {
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Sync() error
	Fd() uintptr
}

type IoTask struct {
	taskType   IoTaskType
	handleID   uint64
	handle     FileHandle
	date       []byte
	offset     uint64
	resultChan chan interface{}
	// for write/sync
	isSync bool
	// for fallocate
	allocMode uint32
	allocSize uint64
	// result
	n   int
	err error
}

func newIoTask(taskType IoTaskType, handle FileHandle, handleID uint64, offset uint64, data []byte, isSync bool) *IoTask {
	return &IoTask{
		taskType:   taskType,
		handleID:   handleID,
		handle:     handle,
		date:       data,
		offset:     offset,
		resultChan: make(chan interface{}, 1),
		isSync:     isSync,
		allocMode:  0,
		allocSize:  0,
		n:          0,
		err:        nil,
	}
}

func NewReadIoTask(handle FileHandle, id uint64, offset uint64, data []byte) *IoTask {
	return newIoTask(ReadIoTask, handle, id, offset, data, false)
}

func NewWriteIoTask(handle FileHandle, id uint64, offset uint64, data []byte, isSync bool) *IoTask {
	return newIoTask(WriteIoTask, handle, id, offset, data, isSync)
}

func NewSyncIoTask(handle FileHandle, id uint64) *IoTask {
	return newIoTask(SyncIoTask, handle, id, 0, nil, true)
}

func NewAllocIoTask(handle FileHandle, id uint64, mode uint32, offset uint64, size uint64) *IoTask {
	task := newIoTask(AllocIoTask, handle, id, offset, nil, false)
	task.allocSize = size
	task.allocMode = mode
	return task
}

func (task *IoTask) WaitAndClose() (int, error) {
	<-task.resultChan
	close(task.resultChan)
	return task.n, task.err
}

func (task *IoTask) GetHandleID() uint64 {
	return task.handleID
}

func (task *IoTask) GetOffset() uint64 {
	return task.offset
}

func (task *IoTask) GetSize() uint64 {
	return uint64(len(task.date))
}

func (task *IoTask) Vailed() bool {
	return task.resultChan != nil
}

func (task *IoTask) Complete() {
	task.resultChan <- struct{}{}
}

func (task *IoTask) Exec() {
	switch task.GetType() {
	case ReadIoTask:
		task.n, task.err = task.handle.ReadAt(task.date, int64(task.GetOffset()))
	case WriteIoTask:
		task.n, task.err = task.handle.WriteAt(task.date, int64(task.GetOffset()))
	case AllocIoTask:
		task.err = sys.Fallocate(task.handle.Fd(), task.allocMode, int64(task.offset), int64(task.allocSize))
	}
}

func (task *IoTask) Sync() {
	if task.isSync {
		task.handle.Sync()
	}
}

func (task *IoTask) GetType() IoTaskType {
	return task.taskType
}

func (task *IoTask) IsSync() bool {
	return task.isSync
}
