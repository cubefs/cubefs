// Copyright 2018 The Containerfs Authors.
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

	"github.com/juju/errors"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) (uint64, uint64, []proto.ExtentKey, error)
type TruncateFunc func(inode, size uint64) error

var (
	gDataWrapper     *wrapper.Wrapper
	writeRequestPool *sync.Pool
	flushRequestPool *sync.Pool
	closeRequestPool *sync.Pool
	truncRequestPool *sync.Pool
)

type ExtentClient struct {
	streamers       map[uint64]*Streamer
	streamerLock    sync.Mutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
	truncate        TruncateFunc
}

func NewExtentClient(volname, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc, truncate TruncateFunc) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)
	gDataWrapper, err = wrapper.NewDataPartitionWrapper(volname, master)
	if err != nil {
		return nil, errors.Annotatef(err, "Init dp wrapper failed!")
	}
	client.streamers = make(map[uint64]*Streamer)
	client.appendExtentKey = appendExtentKey
	client.getExtents = getExtents
	client.truncate = truncate
	writeRequestPool = &sync.Pool{New: func() interface{} {
		return &WriteRequest{}
	}}
	flushRequestPool = &sync.Pool{New: func() interface{} {
		return &FlushRequest{}
	}}
	closeRequestPool = &sync.Pool{New: func() interface{} {
		return &CloseRequest{}
	}}
	truncRequestPool = &sync.Pool{New: func() interface{} {
		return &TruncRequest{}
	}}
	return
}

func (client *ExtentClient) OpenStream(inode uint64) error {
	client.streamerLock.Lock()
	stream, ok := client.streamers[inode]
	if !ok {
		stream = NewStreamer(client, inode)
		client.streamers[inode] = stream
	}
	stream.refcnt++
	log.LogDebugf("OpenStream: ino(%v) refcnt(%v)", inode, stream.refcnt)
	client.streamerLock.Unlock()
	return nil
}

func (client *ExtentClient) CloseStream(inode uint64) error {
	client.streamerLock.Lock()
	stream, ok := client.streamers[inode]
	if !ok {
		client.streamerLock.Unlock()
		return nil
	}
	stream.refcnt--
	log.LogDebugf("CloseStream: ino(%v) refcnt(%v)", inode, stream.refcnt)
	if stream.refcnt > 0 {
		client.streamerLock.Unlock()
		return nil
	}
	delete(client.streamers, inode)

	writer := stream.writer
	client.streamerLock.Unlock()

	log.LogDebugf("CloseStream: ino(%v) doing cleanup", inode)

	if writer != nil {
		err := writer.IssueCloseRequest()
		if err != nil {
			return err
		}
	}

	return nil
}

func (client *ExtentClient) RefreshExtentsCache(inode uint64) error {
	stream := client.GetStreamer(inode)
	if stream == nil {
		return nil
	}
	return stream.GetExtents()
}

func (client *ExtentClient) GetFileSize(inode uint64) (size int, gen uint64) {
	stream := client.GetStreamer(inode)
	if stream == nil {
		return
	}
	return stream.extents.Size()
}

func (client *ExtentClient) SetFileSize(inode uint64, size int) {
	stream := client.GetStreamer(inode)
	if stream != nil {
		log.LogDebugf("SetFileSize: ino(%v) size(%v)", inode, size)
		stream.extents.SetSize(uint64(size))
	}
}

func (client *ExtentClient) Write(inode uint64, offset int, data []byte) (write int, err error) {
	prefix := fmt.Sprintf("write %v_%v_%v", inode, offset, len(data))

	stream, sw := client.getStreamWriter(inode, true)
	if sw == nil {
		return 0, fmt.Errorf("Prefix(%v) cannot init StreamWriter", prefix)
	}

	stream.once.Do(func() {
		stream.GetExtents()
	})

	write, err = sw.IssueWriteRequest(offset, data)
	if err != nil {
		err = errors.Annotatef(err, prefix)
		log.LogError(errors.ErrorStack(err))
		ump.Alarm(gDataWrapper.UmpWarningKey(), err.Error())
	}
	return
}

func (client *ExtentClient) Truncate(inode uint64, size int) error {
	prefix := fmt.Sprintf("truncate: ino(%v)size(%v)", inode, size)

	_, sw := client.getStreamWriter(inode, true)
	if sw == nil {
		return fmt.Errorf("Prefix(%v) cannot init StreamWriter", prefix)
	}

	err := sw.IssueTruncRequest(size)
	if err != nil {
		err = errors.Annotatef(err, prefix)
		log.LogError(errors.ErrorStack(err))
	}
	return err
}

func (client *ExtentClient) Flush(inode uint64) error {
	_, sw := client.getStreamWriter(inode, false)
	if sw == nil {
		return nil
	}
	return sw.IssueFlushRequest()
}

func (client *ExtentClient) Read(inode uint64, data []byte, offset int, size int) (read int, err error) {
	if size == 0 {
		return
	}

	stream := client.GetStreamer(inode)
	if stream == nil {
		return
	}

	stream.once.Do(func() {
		stream.GetExtents()
	})

	err = client.Flush(inode)
	if err != nil {
		return
	}

	read, err = stream.read(data, offset, size)
	return
}

func (client *ExtentClient) GetStreamer(inode uint64) *Streamer {
	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()

	stream, ok := client.streamers[inode]
	if !ok {
		return nil
	}

	return stream
}

func (client *ExtentClient) getStreamWriter(inode uint64, init bool) (stream *Streamer, sw *StreamWriter) {
	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()
	stream, ok := client.streamers[inode]
	if !ok {
		return nil, nil
	}

	if init == true && stream.writer == nil {
		stream.writer = NewStreamWriter(stream, inode)
	}
	return stream, stream.writer
}
