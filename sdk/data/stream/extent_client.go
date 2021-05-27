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
	"github.com/chubaofs/chubaofs/sdk/meta"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/wrapper"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

type AppendExtentKeyFunc func(fileSize int, parentInode, inode uint64, key proto.ExtentKey, discard []proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) (uint64, uint64, []proto.ExtentKey, error)
type TruncateFunc func(inode, size uint64) error
type EvictIcacheFunc func(inode uint64)

const (
	MaxMountRetryLimit = 5
	MountRetryInterval = time.Second * 5

	defaultReadLimitRate  = rate.Inf
	defaultReadLimitBurst = 128

	defaultWriteLimitRate  = rate.Inf
	defaultWriteLimitBurst = 128
)

var (
	// global object pools for memory optimization
	openRequestPool    *sync.Pool
	writeRequestPool   *sync.Pool
	flushRequestPool   *sync.Pool
	releaseRequestPool *sync.Pool
	truncRequestPool   *sync.Pool
	evictRequestPool   *sync.Pool
)

func init() {
	// init object pools
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
}

type ExtentConfig struct {
	Volume            string
	Masters           []string
	FollowerRead      bool
	NearRead          bool
	ReadRate          int64
	WriteRate         int64
	OnAppendExtentKey AppendExtentKeyFunc
	OnGetExtents      GetExtentsFunc
	OnTruncate        TruncateFunc
	OnEvictIcache     EvictIcacheFunc
}

// ExtentClient defines the struct of the extent client.
type ExtentClient struct {
	streamers    map[uint64]*Streamer
	streamerLock sync.Mutex

	readLimiter  *rate.Limiter
	writeLimiter *rate.Limiter

	dataWrapper     *wrapper.Wrapper
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
	truncate        TruncateFunc
	evictIcache     EvictIcacheFunc //May be null, must check before using
}

// NewExtentClient returns a new extent client.
func NewExtentClient(config *ExtentConfig) (client *ExtentClient, err error) {
	client = new(ExtentClient)

	limit := MaxMountRetryLimit
retry:
	client.dataWrapper, err = wrapper.NewDataPartitionWrapper(config.Volume, config.Masters)
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
	client.appendExtentKey = config.OnAppendExtentKey
	client.getExtents = config.OnGetExtents
	client.truncate = config.OnTruncate
	client.evictIcache = config.OnEvictIcache
	client.dataWrapper.InitFollowerRead(config.FollowerRead)
	client.dataWrapper.SetNearRead(config.NearRead)

	var readLimit, writeLimit rate.Limit
	if config.ReadRate <= 0 {
		readLimit = defaultReadLimitRate
	} else {
		readLimit = rate.Limit(config.ReadRate)
	}
	if config.WriteRate <= 0 {
		writeLimit = defaultWriteLimitRate
	} else {
		writeLimit = rate.Limit(config.WriteRate)
	}

	client.readLimiter = rate.NewLimiter(readLimit, defaultReadLimitBurst)
	client.writeLimiter = rate.NewLimiter(writeLimit, defaultWriteLimitBurst)

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
func (client *ExtentClient) FileSize(inode uint64) (size int, gen uint64, valid bool) {
	s := client.GetStreamer(inode)
	if s == nil {
		return
	}
	valid = true
	size, gen = s.extents.Size()
	return
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
func (client *ExtentClient) Write(inode uint64, offset int, data []byte, flags int) (write int, err error) {
	prefix := fmt.Sprintf("Write{ino(%v)offset(%v)size(%v)}", inode, offset, len(data))

	s := client.GetStreamer(inode)
	if s == nil {
		return 0, fmt.Errorf("Prefix(%v): stream is not opened yet", prefix)
	}

	s.once.Do(func() {
		// TODO unhandled error
		s.GetExtents()
	})

	write, err = s.IssueWriteRequest(offset, data, flags)
	if err != nil {
		err = errors.Trace(err, prefix)
		log.LogError(errors.Stack(err))
		exporter.Warning(err.Error())
	}
	return
}

func (client *ExtentClient) Truncate(mw *meta.MetaWrapper, parentIno uint64, inode uint64, size int) error {
	prefix := fmt.Sprintf("Truncate{ino(%v)size(%v)}", inode, size)
	s := client.GetStreamer(inode)
	if s == nil {
		return fmt.Errorf("Prefix(%v): stream is not opened yet", prefix)
	}
	oldSize, _, _ := client.FileSize(inode)
	err := s.IssueTruncRequest(size)
	if err != nil {
		err = errors.Trace(err, prefix)
		log.LogError(errors.Stack(err))
	}
	sizeInc := size - oldSize
	if sizeInc > 0 {
		go mw.UpdateSummary_ll(parentIno, 0, 0, int64(sizeInc))
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

func (client *ExtentClient) GetRate() string {
	return fmt.Sprintf("read: %v\nwrite: %v\n", getRate(client.readLimiter), getRate(client.writeLimiter))
}

func getRate(lim *rate.Limiter) string {
	val := int(lim.Limit())
	if val > 0 {
		return fmt.Sprintf("%v", val)
	}
	return "unlimited"
}

func (client *ExtentClient) SetReadRate(val int) string {
	return setRate(client.readLimiter, val)
}

func (client *ExtentClient) SetWriteRate(val int) string {
	return setRate(client.writeLimiter, val)
}

func setRate(lim *rate.Limiter, val int) string {
	if val > 0 {
		lim.SetLimit(rate.Limit(val))
		return fmt.Sprintf("%v", val)
	}
	lim.SetLimit(rate.Inf)
	return "unlimited"
}

func (client *ExtentClient) Close() error {
	// release streamers
	var inodes []uint64
	client.streamerLock.Lock()
	inodes = make([]uint64, 0, len(client.streamers))
	for inode := range client.streamers {
		inodes = append(inodes, inode)
	}
	client.streamerLock.Unlock()
	for _, inode := range inodes {
		_ = client.EvictStream(inode)
	}
	client.dataWrapper.Stop()
	return nil
}
