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

package data

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/tracing"
	"golang.org/x/time/rate"
)

type InsertExtentKeyFunc func(ctx context.Context, inode uint64, key proto.ExtentKey, isPreExtent bool) error
type GetExtentsFunc func(ctx context.Context, inode uint64) (uint64, uint64, []proto.ExtentKey, error)
type TruncateFunc func(ctx context.Context, inode, oldSize, size uint64) error
type EvictIcacheFunc func(ctx context.Context, inode uint64)

const (
	MaxMountRetryLimit = 5
	MountRetryInterval = time.Second * 5

	defaultReadLimitRate   = rate.Inf
	defaultReadLimitBurst  = 128
	defaultWriteLimitRate  = rate.Inf
	defaultWriteLimitBurst = 128
	updateConfigTicket     = 1 * time.Minute

	defaultMaxAlignSize = 128 * 1024
)

var (
	// global object pools for memory optimization
	openRequestPool    *sync.Pool
	writeRequestPool   *sync.Pool
	flushRequestPool   *sync.Pool
	releaseRequestPool *sync.Pool
	truncRequestPool   *sync.Pool
	evictRequestPool   *sync.Pool

	configStopC = make(chan struct{}, 0)
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
	Volume                   string
	Masters                  []string
	FollowerRead             bool
	NearRead                 bool
	ReadRate                 int64
	WriteRate                int64
	AlignSize                int64
	TinySize                 int
	ExtentSize               int
	MaxExtentNumPerAlignArea int64
	ForceAlignMerge          bool
	AutoFlush                bool
	OnInsertExtentKey        InsertExtentKeyFunc
	OnGetExtents             GetExtentsFunc
	OnTruncate               TruncateFunc
	OnEvictIcache            EvictIcacheFunc
	ExtentMerge              bool
	MetaWrapper              *meta.MetaWrapper
}

// ExtentClient defines the struct of the extent client.
type ExtentClient struct {
	//streamers    map[uint64]*Streamer
	//streamerLock sync.Mutex
	streamerConcurrentMap ConcurrentStreamerMap

	originReadRate  int64
	originWriteRate int64
	readLimiter     *rate.Limiter
	writeLimiter    *rate.Limiter
	masterClient    *masterSDK.MasterClient

	dataWrapper     *Wrapper
	metaWrapper     *meta.MetaWrapper
	insertExtentKey InsertExtentKeyFunc
	getExtents      GetExtentsFunc
	truncate        TruncateFunc
	evictIcache     EvictIcacheFunc //May be null, must check before using

	followerRead             bool
	alignSize                int64
	maxExtentNumPerAlignArea int64
	forceAlignMerge          bool

	tinySize   int
	extentSize int
	autoFlush  bool

	extentMerge        bool
	extentMergeIno     []uint64
	extentMergeChan    chan struct{}
	ExtentMergeSleepMs uint64
}

const (
	NoUseTinyExtent = -1
)

// NewExtentClient returns a new extent client.
func NewExtentClient(config *ExtentConfig) (client *ExtentClient, err error) {
	client = new(ExtentClient)

	limit := MaxMountRetryLimit
retry:
	client.dataWrapper, err = NewDataPartitionWrapper(config.Volume, config.Masters)
	if err != nil {
		if limit <= 0 {
			return nil, errors.Trace(err, "Init data wrapper failed!")
		} else {
			limit--
			time.Sleep(MountRetryInterval)
			goto retry
		}
	}
	client.metaWrapper = config.MetaWrapper

	client.streamerConcurrentMap = InitConcurrentStreamerMap()
	client.insertExtentKey = config.OnInsertExtentKey
	client.getExtents = config.OnGetExtents
	client.truncate = config.OnTruncate
	client.evictIcache = config.OnEvictIcache
	client.dataWrapper.InitFollowerRead(config.FollowerRead)
	client.dataWrapper.SetNearRead(config.NearRead)
	client.tinySize = config.TinySize
	if client.tinySize == 0 {
		client.tinySize = util.DefaultTinySizeLimit
	}
	client.SetExtentSize(config.ExtentSize)
	client.autoFlush = config.AutoFlush

	if client.tinySize == NoUseTinyExtent {
		client.tinySize = 0
	}
	var readLimit, writeLimit rate.Limit
	if config.ReadRate <= 0 {
		readLimit = defaultReadLimitRate
	} else {
		readLimit = rate.Limit(config.ReadRate)
		client.originReadRate = config.ReadRate
	}
	if config.WriteRate <= 0 {
		writeLimit = defaultWriteLimitRate
	} else {
		writeLimit = rate.Limit(config.WriteRate)
		client.originWriteRate = config.WriteRate
	}

	client.readLimiter = rate.NewLimiter(readLimit, defaultReadLimitBurst)
	client.writeLimiter = rate.NewLimiter(writeLimit, defaultWriteLimitBurst)
	client.masterClient = masterSDK.NewMasterClient(config.Masters, false)
	go client.startUpdateConfig()

	client.alignSize = config.AlignSize
	if client.alignSize > defaultMaxAlignSize {
		log.LogWarnf("config alignSize(%v) is too max, set it to default max value(%v).", client.alignSize,
			defaultMaxAlignSize)
		client.alignSize = defaultMaxAlignSize
	}
	client.maxExtentNumPerAlignArea = config.MaxExtentNumPerAlignArea
	client.forceAlignMerge = config.ForceAlignMerge

	client.extentMerge = config.ExtentMerge
	if client.extentMerge {
		client.extentMergeChan = make(chan struct{})
		go client.BackgroundExtentMerge()
	}
	return
}

// Open request shall grab the lock until request is sent to the request channel
func (client *ExtentClient) OpenStream(inode uint64) error {
	streamerMapSeg := client.streamerConcurrentMap.GetMapSegment(inode)
	streamerMapSeg.Lock()
	s, ok := streamerMapSeg.streamers[inode]
	if !ok {
		s = NewStreamer(client, inode, streamerMapSeg)
		streamerMapSeg.streamers[inode] = s
	}
	return s.IssueOpenRequest()
}

func (client *ExtentClient) OpenStreamWithSize(inode uint64, size int) (err error) {
	streamerMapSeg := client.streamerConcurrentMap.GetMapSegment(inode)
	streamerMapSeg.Lock()
	s, ok := streamerMapSeg.streamers[inode]
	if !ok {
		s = NewStreamer(client, inode, streamerMapSeg)
		streamerMapSeg.streamers[inode] = s
	} else if curSize, _ := s.extents.Size(); curSize < size {
		_ = s.GetExtents(context.Background())
	}
	return s.IssueOpenRequest()
}

// Release request shall grab the lock until request is sent to the request channel
func (client *ExtentClient) CloseStream(ctx context.Context, inode uint64) error {
	streamerMapSeg := client.streamerConcurrentMap.GetMapSegment(inode)
	streamerMapSeg.Lock()
	s, ok := streamerMapSeg.streamers[inode]
	if !ok {
		streamerMapSeg.Unlock()
		return nil
	}
	return s.IssueReleaseRequest(ctx)
}

// Evict request shall grab the lock until request is sent to the request channel
func (client *ExtentClient) EvictStream(ctx context.Context, inode uint64) error {
	streamerMapSeg := client.streamerConcurrentMap.GetMapSegment(inode)
	streamerMapSeg.Lock()
	s, ok := streamerMapSeg.streamers[inode]
	if !ok {
		streamerMapSeg.Unlock()
		return nil
	}
	err := s.IssueEvictRequest(ctx)
	if err != nil {
		return err
	}

	s.done <- struct{}{}
	return nil
}

// RefreshExtentsCache refreshes the extent cache.
func (client *ExtentClient) RefreshExtentsCache(ctx context.Context, inode uint64) error {
	s := client.GetStreamer(inode)
	if s == nil {
		return nil
	}
	return s.GetExtents(ctx)
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
//func (client *ExtentClient) SetFileSize(inode uint64, size int) {
//	s := client.GetStreamer(inode)
//	if s != nil {
//		log.LogDebugf("SetFileSize: ino(%v) size(%v)", inode, size)
//		s.extents.SetSize(uint64(size), true)
//	}
//}

// Write writes the data.
func (client *ExtentClient) Write(ctx context.Context, inode uint64, offset int, data []byte, direct bool, overWriteBuffer bool) (write int, isROW bool, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentClient.Write").
		SetTag("arg.inode", inode).
		SetTag("arg.offset", offset).
		SetTag("arg.dataLen", len(data)).
		SetTag("arg.direct", direct)
	defer func() {
		tracer.SetTag("ret.write", write)
		tracer.SetTag("ret.err", err)
		tracer.Finish()
	}()
	ctx = tracer.Context()

	if client.dataWrapper.volNotExists {
		return 0, false, proto.ErrVolNotExists
	}

	prefix := fmt.Sprintf("Write{ino(%v)offset(%v)size(%v)}", inode, offset, len(data))
	s := client.GetStreamer(inode)
	if s == nil {
		return 0, false, fmt.Errorf("Prefix(%v): stream is not opened yet", prefix)
	}

	s.once.Do(func() {
		// TODO unhandled error
		s.GetExtents(ctx)
	})

	if overWriteBuffer {
		requests := s.extents.PrepareRequests(offset, len(data), data)
		hasAppendWrite := false
		for _, req := range requests {
			if req.ExtentKey == nil {
				hasAppendWrite = true
				break
			}
		}
		if hasAppendWrite {
			write, _, err = s.IssueWriteRequest(ctx, offset, data, direct, overWriteBuffer)
		} else {
			for _, req := range requests {
				write += s.appendOverWriteReq(ctx, req, direct)
			}
		}
	} else {
		write, _, err = s.IssueWriteRequest(ctx, offset, data, direct, overWriteBuffer)
	}
	if err != nil {
		err = errors.Trace(err, prefix)
		log.LogWarnf(errors.Stack(err))
		exporter.Warning(err.Error())
	}
	return
}

func (client *ExtentClient) Truncate(ctx context.Context, inode uint64, size int) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentClient.Truncate").
		SetTag("inode", inode).
		SetTag("size", size)
	defer tracer.Finish()
	ctx = tracer.Context()

	if client.dataWrapper.volNotExists {
		return proto.ErrVolNotExists
	}

	prefix := fmt.Sprintf("Truncate{ino(%v)size(%v)}", inode, size)
	s := client.GetStreamer(inode)
	if s == nil {
		return fmt.Errorf("Prefix(%v): stream is not opened yet", prefix)
	}

	err := s.IssueTruncRequest(ctx, size)
	if err != nil {
		err = errors.Trace(err, prefix)
		log.LogError(errors.Stack(err))
	}
	return err
}

func (client *ExtentClient) Flush(ctx context.Context, inode uint64) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentClient.Flush").
		SetTag("ino", inode)
	defer tracer.Finish()
	ctx = tracer.Context()

	if client.dataWrapper.volNotExists {
		return proto.ErrVolNotExists
	}

	s := client.GetStreamer(inode)
	if s == nil {
		return fmt.Errorf("Flush: stream is not opened yet, ino(%v)", inode)
	}
	return s.IssueFlushRequest(ctx)
}

func (client *ExtentClient) Read(ctx context.Context, inode uint64, data []byte, offset int, size int) (read int, err error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentClient.Read").
		SetTag("arg.inode", inode).
		SetTag("arg.offset", offset).
		SetTag("arg.size", size)
	defer func() {
		tracer.SetTag("ret.read", read)
		tracer.SetTag("ret.err", err)
		tracer.Finish()
	}()
	ctx = tracer.Context()

	if size == 0 {
		return
	}

	if client.dataWrapper.volNotExists {
		return 0, proto.ErrVolNotExists
	}

	s := client.GetStreamer(inode)
	if s == nil {
		err = fmt.Errorf("Read: stream is not opened yet, ino(%v) offset(%v) size(%v)", inode, offset, size)
		return
	}

	s.once.Do(func() {
		s.GetExtents(ctx)
	})

	err = s.IssueFlushRequest(ctx)
	if err != nil {
		return
	}

	read, err = s.read(ctx, data, offset, size)
	return
}

func (client *ExtentClient) ExtentMerge(ctx context.Context, inode uint64) (finish bool, err error) {
	s := client.GetStreamer(inode)
	if s == nil {
		log.LogErrorf("stream is not opened yet: inode(%v)", inode)
		return true, fmt.Errorf("stream is not opened yet")
	}
	s.once.Do(func() {
		s.GetExtents(ctx)
	})
	finish, err = s.IssueExtentMergeRequest(ctx)
	return
}

// GetStreamer returns the streamer.
func (client *ExtentClient) GetStreamer(inode uint64) *Streamer {
	streamerMapSeg := client.streamerConcurrentMap.GetMapSegment(inode)
	streamerMapSeg.Lock()
	defer streamerMapSeg.Unlock()
	s, ok := streamerMapSeg.streamers[inode]
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

func (client *ExtentClient) startUpdateConfig() {
	for {
		err := client.startUpdateConfigWithRecover()
		if err == nil {
			break
		}
		log.LogErrorf("updateDataLimitConfig: err(%v) try next update", err)
	}
}

func (client *ExtentClient) startUpdateConfigWithRecover() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("updateDataLimitConfig panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("updateDataLimitConfig panic: err(%v)", r)
			handleUmpAlarm(client.dataWrapper.clusterName, client.dataWrapper.volName, "updateDataLimitConfig", msg)
			err = errors.New(msg)
		}
	}()
	ticker := time.NewTicker(updateConfigTicket)
	defer ticker.Stop()
	for {
		select {
		case <-configStopC:
			return
		case <-ticker.C:
			client.updateConfig()
		}
	}
}

func (client *ExtentClient) stopUpdateConfig() {
	configStopC <- struct{}{}
}

func (client *ExtentClient) updateConfig() {
	limitInfo, err := client.masterClient.AdminAPI().GetLimitInfo(client.dataWrapper.volName)
	if err != nil {
		log.LogWarnf("[updateConfig] %s", err.Error())
		return
	}
	// If rate from master is 0, then restore the client rate
	var readLimit, writeLimit rate.Limit
	readRate, ok := limitInfo.ClientReadVolRateLimitMap[client.dataWrapper.volName]
	if !ok {
		readRate, ok = limitInfo.ClientReadVolRateLimitMap[""]
	}
	if ok && readRate > 0 {
		client.readLimiter.SetLimit(rate.Limit(readRate))
	} else {
		if client.originReadRate > 0 {
			readLimit = rate.Limit(client.originReadRate)
		} else {
			readLimit = rate.Limit(defaultReadLimitRate)
		}
		client.readLimiter.SetLimit(readLimit)
	}
	writeRate, ok := limitInfo.ClientWriteVolRateLimitMap[client.dataWrapper.volName]
	if !ok {
		writeRate, ok = limitInfo.ClientWriteVolRateLimitMap[""]
	}
	if ok && writeRate > 0 {
		client.writeLimiter.SetLimit(rate.Limit(writeRate))
	} else {
		if client.originWriteRate > 0 {
			writeLimit = rate.Limit(client.originWriteRate)
		} else {
			writeLimit = rate.Limit(defaultWriteLimitRate)
		}
		client.writeLimiter.SetLimit(writeLimit)
	}

	if client.extentMerge {
		if len(client.extentMergeIno) == 0 && len(limitInfo.ExtentMergeIno[client.dataWrapper.volName]) > 0 {
			client.extentMergeChan <- struct{}{}
		}
		client.extentMergeIno = limitInfo.ExtentMergeIno[client.dataWrapper.volName]
		client.ExtentMergeSleepMs = limitInfo.ExtentMergeSleepMs
	}
}

func (client *ExtentClient) Close(ctx context.Context) error {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("ExtentClient.Close")
	defer tracer.Finish()
	ctx = tracer.Context()

	// release streamers
	inodes := client.streamerConcurrentMap.Keys()
	for _, inode := range inodes {
		_ = client.EvictStream(ctx, inode)
	}
	client.dataWrapper.Stop()
	client.stopUpdateConfig()
	return nil
}

//func (c *ExtentClient) AlignSize() int {
//	return int(c.alignSize)
//}
//
//func (c *ExtentClient) MaxExtentNumPerAlignArea() int {
//	return int(c.maxExtentNumPerAlignArea)
//}
//
//func (c *ExtentClient) ForceAlignMerge() bool {
//	return c.forceAlignMerge
//}

func (c *ExtentClient) SetExtentSize(size int) {
	if size == 0 {
		c.extentSize = util.ExtentSize
		return
	}
	if size > util.ExtentSize {
		log.LogWarnf("too large extent size config %v, use default value %v", size, util.ExtentSize)
		c.extentSize = util.ExtentSize
		return
	}
	if size < util.MinExtentSize {
		log.LogWarnf("too small extent size config %v, use default min value %v", size, util.MinExtentSize)
		c.extentSize = util.MinExtentSize
		return
	}
	if size&(size-1) != 0 {
		for i := util.MinExtentSize; ; {
			if i > size {
				c.extentSize = i
				break
			}
			i = i * 2
		}
		log.LogWarnf("invalid extent size %v, need power of 2, use value %v", size, c.extentSize)
		return
	}
	c.extentSize = size
}

func (c *ExtentClient) BackgroundExtentMerge() {
	ctx := context.Background()
	for {
		select {
		case <-c.extentMergeChan:
			inodes := c.extentMergeIno
			if len(inodes) == 1 && inodes[0] == 0 {
				inodes = c.lookupAllInode(proto.RootIno)
			}
			for _, inode := range inodes {
				var finish bool
				c.OpenStream(inode)
				for !finish {
					finish, _ = c.ExtentMerge(ctx, inode)
					time.Sleep(time.Duration(c.ExtentMergeSleepMs) * time.Millisecond)
				}
				c.CloseStream(ctx, inode)
				c.EvictStream(ctx, inode)
			}
		}
	}
}

func (c *ExtentClient) lookupAllInode(parent uint64) (inodes []uint64) {
	ctx := context.Background()
	dentries, err := c.metaWrapper.ReadDir_ll(ctx, parent)
	if err != nil {
		return
	}
	for _, dentry := range dentries {
		if proto.IsRegular(dentry.Type) {
			inodes = append(inodes, dentry.Inode)
		} else if proto.IsDir(dentry.Type) {
			newInodes := c.lookupAllInode(dentry.Inode)
			inodes = append(inodes, newInodes...)
		}
	}
	return
}
