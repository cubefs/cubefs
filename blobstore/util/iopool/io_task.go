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
	data       []byte
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
		data:       data,
		offset:     offset,
		resultChan: make(chan interface{}, 1),
		isSync:     isSync,
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

func (t *IoTask) WaitAndClose() (int, error) {
	<-t.resultChan
	close(t.resultChan)
	return t.n, t.err
}

func (t *IoTask) GetHandleID() uint64 {
	return t.handleID
}

func (t *IoTask) GetOffset() uint64 {
	return t.offset
}

func (t *IoTask) GetSize() uint64 {
	return uint64(len(t.data))
}

func (t *IoTask) Valid() bool {
	return t.resultChan != nil
}

func (t *IoTask) Complete() {
	t.resultChan <- struct{}{}
}

func (t *IoTask) Exec() {
	switch t.GetType() {
	case ReadIoTask:
		t.n, t.err = t.handle.ReadAt(t.data, int64(t.GetOffset()))
	case WriteIoTask:
		t.n, t.err = t.handle.WriteAt(t.data, int64(t.GetOffset()))
	case AllocIoTask:
		t.err = sys.Fallocate(t.handle.Fd(), t.allocMode, int64(t.offset), int64(t.allocSize))
	}
}

func (t *IoTask) Sync() {
	t.handle.Sync()
}

func (t *IoTask) GetType() IoTaskType {
	return t.taskType
}

func (t *IoTask) IsSync() bool {
	return t.isSync
}
