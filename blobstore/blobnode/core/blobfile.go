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

package core

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/sys"
	"github.com/cubefs/cubefs/blobstore/util/iopool"
	"github.com/cubefs/cubefs/blobstore/util/mergetask"
	poolsys "github.com/cubefs/cubefs/blobstore/util/sys"
)

type RawFile interface {
	Name() string
	Fd() uintptr
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Stat() (info os.FileInfo, err error)
	Sync() error
	Close() error
}

type BlobFile interface {
	RawFile
	Allocate(off int64, size int64) (err error)
	Discard(off int64, size int64) (err error)
	SysStat() (sysstat syscall.Stat_t, err error)
}

type blobFile struct {
	file          RawFile
	syncHandler   *mergetask.MergeTask
	handleIOError func(err error)
	// schedulers
	readScheduler  iopool.IoScheduler
	writeScheduler iopool.IoScheduler
}

func (ef *blobFile) Name() string {
	return ef.file.Name()
}

func (ef *blobFile) Fd() uintptr {
	return ef.file.Fd()
}

func (ef *blobFile) ReadAt(b []byte, off int64) (n int, err error) {
	if ef.readScheduler != nil {
		task := iopool.NewReadIoTask(ef.file, uint64(ef.Fd()), uint64(off), b)
		ef.readScheduler.Schedule(task)
		n, err = task.WaitAndClose()
	} else {
		n, err = ef.file.ReadAt(b, off)
	}
	ef.handleError(err)
	return
}

func (ef *blobFile) WriteAt(b []byte, off int64) (n int, err error) {
	if ef.writeScheduler != nil {
		task := iopool.NewWriteIoTask(ef.file, uint64(ef.Fd()), uint64(off), b, false)
		ef.writeScheduler.Schedule(task)
		n, err = task.WaitAndClose()
	} else {
		n, err = ef.file.WriteAt(b, off)
	}
	ef.handleError(err)
	return
}

func (ef *blobFile) Stat() (info os.FileInfo, err error) {
	info, err = ef.file.Stat()
	ef.handleError(err)
	return
}

func (ef *blobFile) Allocate(off int64, size int64) (err error) {
	if ef.writeScheduler != nil {
		task := iopool.NewAllocIoTask(ef.file, uint64(ef.Fd()), poolsys.FALLOC_FL_DEFAULT, uint64(off), uint64(size))
		ef.writeScheduler.Schedule(task)
		_, err = task.WaitAndClose()
	} else {
		err = sys.PreAllocate(ef.file.Fd(), off, size)
	}
	ef.handleError(err)
	return
}

func (ef *blobFile) Discard(off int64, size int64) (err error) {
	if ef.writeScheduler != nil {
		task := iopool.NewAllocIoTask(ef.file, uint64(ef.Fd()), poolsys.FALLOC_FL_KEEP_SIZE|poolsys.FALLOC_FL_PUNCH_HOLE, uint64(off), uint64(size))
		ef.writeScheduler.Schedule(task)
		_, err = task.WaitAndClose()
	} else {
		err = sys.PunchHole(ef.file.Fd(), off, size)
	}
	ef.handleError(err)
	return
}

func (ef *blobFile) SysStat() (sysstat syscall.Stat_t, err error) {
	stat, err := ef.file.Stat()
	ef.handleError(err)
	if err != nil {
		return sysstat, err
	}

	sysstat = *(stat.Sys().(*syscall.Stat_t))
	return sysstat, err
}

func (ef *blobFile) Sync() error {
	var err error
	if ef.writeScheduler != nil {
		task := iopool.NewSyncIoTask(ef.file, uint64(ef.Fd()))
		ef.writeScheduler.Schedule(task)
		_, err = task.WaitAndClose()
	} else {
		err = ef.syncHandler.Do(nil)
	}
	ef.handleError(err)
	return err
}

func (ef *blobFile) Close() error {
	return ef.file.Close()
}

func (ef *blobFile) handleError(err error) {
	if base.IsEIO(err) && ef.handleIOError != nil {
		ef.handleIOError(err)
	}
}

func AlignSize(p int64, bound int64) (r int64) {
	r = (p + bound - 1) & (^(bound - 1))
	return r
}

func OpenFile(filename string, createIfMiss bool) (*os.File, error) {
	fileExists, err := base.IsFileExists(filename)
	if err != nil {
		return nil, err
	}

	if !fileExists && !createIfMiss {
		return nil, os.ErrNotExist
	}

	flag := os.O_RDWR
	if !fileExists {
		if err = os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
			return nil, err
		}
		flag = os.O_RDWR | os.O_CREATE | os.O_EXCL | os.O_TRUNC
	}
	file, err := os.OpenFile(filename, flag, 0o644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func NewBlobFile(file RawFile, handleIOError func(err error), readScheduler iopool.IoScheduler, writeScheduler iopool.IoScheduler) BlobFile {
	ef := &blobFile{
		file:           file,
		handleIOError:  handleIOError,
		readScheduler:  readScheduler,
		writeScheduler: writeScheduler,
	}
	ef.syncHandler = mergetask.NewMergeTask(-1, func(interface{}) error {
		return ef.file.Sync()
	})
	return ef
}
