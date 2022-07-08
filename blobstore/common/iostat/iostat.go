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

//go:build linux || darwin
// +build linux darwin

package iostat

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	readStatType  = 0
	writeStatType = 1
	maxType       = 2
)

const (
	KiB     = 1024
	MiB     = KiB * 1024
	GiB     = MiB * 1024
	mapSize = 1 << 12
)

type StatItem struct {
	cnt       uint64
	size      uint64
	inque     int64
	latencies uint64
}

type StatMgr struct {
	Stat   *Stat
	Data   []byte
	Path   string
	DryRun bool
}

type Stat struct {
	items [maxType]StatItem
}

type StatData struct {
	Iops  uint64
	Bps   uint64
	Avgrq uint64 // bytes
	Avgqu int64  // count. io depth
	Await int64  // nanoseconds
}

type Iostat interface {
	Get(out *Stat)
	Begin(size uint64)
	End(latency uint64)
}

type StatMgrAPI interface {
	ReadBegin(size uint64)
	ReadEnd(t time.Time)
	WriteBegin(size uint64)
	WriteEnd(t time.Time)
	Writer(underlying io.Writer) io.Writer
	WriterAt(underlying io.WriterAt) io.WriterAt
	Reader(underlying io.Reader) io.Reader
	ReaderAt(underlying io.ReaderAt) io.ReaderAt
}

func (sm *StatMgr) ReadBegin(size uint64) {
	if sm.Stat == nil {
		return
	}
	sm.Stat.ReadBegin(size)
}

func (sm *StatMgr) ReadEnd(t time.Time) {
	if sm.Stat == nil {
		return
	}
	latency := time.Since(t).Nanoseconds()
	sm.Stat.ReadEnd(uint64(latency))
}

func (sm *StatMgr) WriteBegin(size uint64) {
	if sm.Stat == nil {
		return
	}
	sm.Stat.WriteBegin(size)
}

func (sm *StatMgr) WriteEnd(t time.Time) {
	if sm.Stat == nil {
		return
	}
	latency := time.Since(t).Nanoseconds()
	sm.Stat.WriteEnd(uint64(latency))
}

func OpenStat(path string, readonly int, size int64) (*os.File, error) {
	if readonly != 0 {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return nil, err
		}
		err = f.Truncate(size)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	off, _ := f.Seek(0, io.SeekEnd)
	if off < size {
		f.Truncate(size)
		off, _ = f.Seek(0, io.SeekEnd)
		if off < size {
			f.Close()
			return nil, errors.New("seek file failed")
		}
	}

	return f, nil
}

func StatInit(path string, rdonly int, dryRun bool) (*StatMgr, error) {
	if !dryRun {
		return StatInitWithShareMemory(path, rdonly)
	}

	bytes := make([]byte, mapSize)

	sm := StatMgr{
		Stat:   *(**Stat)(unsafe.Pointer(&bytes)),
		Data:   bytes,
		Path:   path,
		DryRun: true,
	}

	return &sm, nil
}

func StatInitWithShareMemory(path string, rdonly int) (*StatMgr, error) {
	flags := syscall.PROT_READ

	f, err := OpenStat(path, rdonly, mapSize)
	if err != nil {
		return nil, errors.New("open mmap file failed")
	}

	defer f.Close()

	if rdonly == 0 {
		flags |= syscall.PROT_WRITE
	}

	bytes, err := syscall.Mmap(int(f.Fd()), 0, mapSize, flags, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	sm := StatMgr{
		Stat: *(**Stat)(unsafe.Pointer(&bytes)),
		Data: bytes,
		Path: path,
	}

	return &sm, nil
}

func (i *StatItem) begin(size uint64) {
	atomic.AddUint64(&(i.cnt), 1)
	atomic.AddUint64(&(i.size), size)
	atomic.AddInt64(&(i.inque), 1)
}

func (i *StatItem) end(latency uint64) {
	atomic.AddInt64(&(i.inque), -1)
	atomic.AddUint64(&(i.latencies), latency)
}

func (i *StatItem) copyTo(dest *StatItem) {
	dest.size = atomic.LoadUint64(&(i.size))
	dest.cnt = atomic.LoadUint64(&(i.cnt))
	dest.inque = atomic.LoadInt64(&(i.inque))
	dest.latencies = atomic.LoadUint64(&(i.latencies))
}

func (stat *Stat) ReadBegin(size uint64) {
	stat.items[readStatType].begin(size)
}

func (stat *Stat) ReadEnd(latency uint64) {
	stat.items[readStatType].end(latency)
}

func (stat *Stat) WriteBegin(size uint64) {
	stat.items[writeStatType].begin(size)
}

func (stat *Stat) WriteEnd(latency uint64) {
	stat.items[writeStatType].end(latency)
}

func (stat *Stat) CopyReadStat(out *Stat) {
	stat.items[readStatType].copyTo(&(out.items[readStatType]))
}

func (stat *Stat) CopyWriteStat(out *Stat) {
	stat.items[writeStatType].copyTo(&(out.items[writeStatType]))
}
