// Copyright 2018 The Chubao Authors.
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

package stream

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/errors"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/wrapper"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) (uint64, uint64, []proto.ExtentKey, error)
type TruncateFunc func(inode, size uint64) error

const (
	MaxMountRetryLimit = 5
	MountRetryInterval = time.Second * 5
)

var (
	gDataWrapper       *wrapper.Wrapper
	openRequestPool    *sync.Pool
	writeRequestPool   *sync.Pool
	flushRequestPool   *sync.Pool
	releaseRequestPool *sync.Pool
	truncRequestPool   *sync.Pool
	evictRequestPool   *sync.Pool
)

// ExtentClient defines the struct of the extent client.
type ExtentClient struct {
	streamers       map[uint64]*Streamer
	streamerLock    sync.Mutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
	truncate        TruncateFunc
}

// NewExtentClient returns a new extent client.
func NewExtentClient(volname, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc, truncate TruncateFunc) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)

	limit := MaxMountRetryLimit
retry:
	gDataWrapper, err = wrapper.NewDataPartitionWrapper(volname, master)
	if err != nil {
		if limit <= 0 {
			return nil, errors.Trace(err, "Init data wrapper failed!")
		} else {
			limit--
			time.Sleep(MountRetryInterval)
			goto retry
		}
	}

	client.streamers = make(map[uint64]*Streamer)
	client.appendExtentKey = appendExtentKey
	client.getExtents = getExtents
	client.truncate = truncate

	// Init request pools
	openRequestPool = &sync.Pool{New: func() interface{} {
		return &OpenRequest{}
	}}
	writeRequestPool = &sync.Pool{New: func() interface{} {
		return &WriteRequest{}
	}}
	flushRequestPool = &sync.Pool{New: func() interface{} {
		return &FlushRequest{}
	}}
	releaseRequestPool = &sync.Pool{New: func() interface{} {
		return &ReleaseRequest{}
	}}
	truncRequestPool = &sync.Pool{New: func() interface{} {
		return &TruncRequest{}
	}}
	evictRequestPool = &sync.Pool{New: func() interface{} {
		return &EvictRequest{}
	}}

	return
}

// Open request shall grab the lock until request is sent to the request channel
func (client *ExtentClient) OpenStream(inode uint64) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		s = NewStreamer(client, inode)
		client.streamers[inode] = s
	}
	return s.IssueOpenRequest()
}

// Release request shall grab the lock until request is sent to the request channel
func (client *ExtentClient) CloseStream(inode uint64) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		client.streamerLock.Unlock()
		return nil
	}
	return s.IssueReleaseRequest()
}

// Evict request shall grab the lock until request is sent to the request channel
func (client *ExtentClient) EvictStream(inode uint64) error {
	client.streamerLock.Lock()
	s, ok := client.streamers[inode]
	if !ok {
		client.streamerLock.Unlock()
		return nil
	}
	err := s.IssueEvictRequest()
	if err != nil {
		return err
	}

	s.done <- struct{}{}
	return nil
}

// RefreshExtentsCache refreshes the extent cache.
func (client *ExtentClient) RefreshExtentsCache(inode uint64) error {
	s := client.GetStreamer(inode)
	if s == nil {
		return nil
	}
	return s.GetExtents()
}

// FileSize returns the file size.
func (client *ExtentClient) FileSize(inode uint64) (size int, gen uint64) {
	s := client.GetStreamer(inode)
	if s == nil {
		return
	}
	return s.extents.Size()
}

// SetFileSize set the file size.
func (client *ExtentClient) SetFileSize(inode uint64, size int) {
	s := client.GetStreamer(inode)
	if s != nil {
		log.LogDebugf("SetFileSize: ino(%v) size(%v)", inode, size)
		s.extents.SetSize(uint64(size), true)
	}
}

// Write writes the data.
func (client *ExtentClient) Write(inode uint64, offset int, data []byte, direct bool) (write int, err error) {
	prefix := fmt.Sprintf("Write{ino(%v)offset(%v)size(%v)}", inode, offset, len(data))

	s := client.GetStreamer(inode)
	if s == nil {
		return 0, fmt.Errorf("Prefix(%v): stream is not opened yet", prefix)
	}

	s.once.Do(func() {
		// TODO unhandled error
		s.GetExtents()
	})

	write, err = s.IssueWriteRequest(offset, data, direct)
	if err != nil {
		err = errors.Trace(err, prefix)
		log.LogError(errors.Stack(err))
		exporter.NewAlarm(gDataWrapper.WarningMsg())
	}
	return
}

func (client *ExtentClient) Truncate(inode uint64, size int) error {
	prefix := fmt.Sprintf("Truncate{ino(%v)size(%v)}", inode, size)
	s := client.GetStreamer(inode)
	if s == nil {
		return fmt.Errorf("Prefix(%v): stream is not opened yet", prefix)
	}

	err := s.IssueTruncRequest(size)
	if err != nil {
		err = errors.Trace(err, prefix)
		log.LogError(errors.Stack(err))
	}
	return err
}

func (client *ExtentClient) Flush(inode uint64) error {
	s := client.GetStreamer(inode)
	if s == nil {
		return fmt.Errorf("Flush: stream is not opened yet, ino(%v)", inode)
	}
	return s.IssueFlushRequest()
}

func (client *ExtentClient) Read(inode uint64, data []byte, offset int, size int) (read int, err error) {
	if size == 0 {
		return
	}

	s := client.GetStreamer(inode)
	if s == nil {
		err = fmt.Errorf("Read: stream is not opened yet, ino(%v) offset(%v) size(%v)", inode, offset, size)
		return
	}

	s.once.Do(func() {
		s.GetExtents()
	})

	err = s.IssueFlushRequest()
	if err != nil {
		return
	}

	read, err = s.read(data, offset, size)
	return
}

// GetStreamer returns the streamer.
func (client *ExtentClient) GetStreamer(inode uint64) *Streamer {
	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()
	s, ok := client.streamers[inode]
	if !ok {
		return nil
	}
	return s
}
