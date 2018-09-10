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
	"sync"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
	"runtime"
	"sync/atomic"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) ([]proto.ExtentKey, error)

var (
	gDataWrapper     *wrapper.Wrapper
	writeRequestPool *sync.Pool
	flushRequestPool *sync.Pool
	closeRequestPool *sync.Pool
)

type ExtentClient struct {
	writers         map[uint64]*StreamWriter
	referCnt        map[uint64]uint64
	referLock       sync.Mutex
	writerLock      sync.RWMutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
}

func NewExtentClient(volname, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)
	gDataWrapper, err = wrapper.NewDataPartitionWrapper(volname, master)
	if err != nil {
		return nil, fmt.Errorf("init dp Wrapper failed (%v)", err.Error())
	}
	client.writers = make(map[uint64]*StreamWriter)
	client.appendExtentKey = appendExtentKey
	client.referCnt = make(map[uint64]uint64)
	client.getExtents = getExtents
	writeRequestPool = &sync.Pool{New: func() interface{} {
		return &WriteRequest{}
	}}
	flushRequestPool = &sync.Pool{New: func() interface{} {
		return &FlushRequest{}
	}}
	closeRequestPool = &sync.Pool{New: func() interface{} {
		return &CloseRequest{}
	}}
	return
}

func (client *ExtentClient) getStreamWriter(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	stream = client.writers[inode]
	client.writerLock.RUnlock()

	return
}

func (client *ExtentClient) getStreamWriterForRead(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	defer client.writerLock.RUnlock()
	stream = client.writers[inode]
	if stream != nil && atomic.LoadInt32(&stream.hasClosed) == HasClosed {
		return nil
	}

	return
}

func (client *ExtentClient) Write(inode uint64, offset int, data []byte) (write int, err error) {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
		return 0, fmt.Errorf("Prefix(%v) cannot init write stream", prefix)
	}

	request := writeRequestPool.Get().(*WriteRequest)
	request.data = data
	request.kernelOffset = offset
	request.size = len(data)
	request.done = make(chan struct{}, 1)
	stream.requestCh <- request
	<-request.done
	err = request.err
	write = request.canWrite
	write += request.cutSize
	if err != nil {
		prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
		err = errors.Annotatef(err, prefix)
		log.LogError(errors.ErrorStack(err))
		ump.Alarm(gDataWrapper.UmpWarningKey(), err.Error())
	}
	writeRequestPool.Put(request)
	return
}

func (client *ExtentClient) OpenForRead(inode uint64) (stream *StreamReader, err error) {
	return NewStreamReader(inode, client.getExtents)
}

func (client *ExtentClient) OpenForWrite(inode, start uint64) {
	client.referLock.Lock()
	refercnt, ok := client.referCnt[inode]
	if !ok {
		client.referCnt[inode] = 1
	} else {
		refercnt++
		client.referCnt[inode] = refercnt
	}

	client.referLock.Unlock()

	client.writerLock.RLock()
	_, ok = client.writers[inode]
	client.writerLock.RUnlock()
	if ok {
		return
	}

	client.writerLock.Lock()
	_, ok = client.writers[inode]
	if !ok {
		writer := NewStreamWriter(inode, start, client.appendExtentKey)
		client.writers[inode] = writer
	}
	client.writerLock.Unlock()

}

func (client *ExtentClient) GetWriteSize(inode uint64) uint64 {
	client.writerLock.RLock()
	defer client.writerLock.RUnlock()
	writer, ok := client.writers[inode]
	if !ok {
		return 0
	}
	return writer.getHasWriteSize()
}

func (client *ExtentClient) SetWriteSize(inode, size uint64) {
	client.writerLock.Lock()
	defer client.writerLock.Unlock()
	writer, ok := client.writers[inode]
	if ok {
		writer.setHasWriteSize(size)
	}
}

func (client *ExtentClient) deleteRefercnt(inode uint64) {
	client.referLock.Lock()
	defer client.referLock.Unlock()
	delete(client.referCnt, inode)
}

func (client *ExtentClient) Flush(inode uint64) (err error) {
	stream := client.getStreamWriterForRead(inode)
	if stream == nil {
		return nil
	}
	request := flushRequestPool.Get().(*FlushRequest)
	request.done = make(chan struct{}, 1)
	stream.requestCh <- request
	<-request.done
	err = request.err
	flushRequestPool.Put(request)
	return err
}

func (client *ExtentClient) CloseForWrite(inode uint64) (err error) {
	client.referLock.Lock()
	refercnt, ok := client.referCnt[inode]
	if !ok {
		client.referLock.Unlock()
		client.Flush(inode)
		return nil
	}
	refercnt = refercnt - 1
	client.referCnt[inode] = refercnt
	if refercnt > 0 {
		client.referLock.Unlock()
		client.Flush(inode)
		return
	}
	client.referLock.Unlock()

	streamWriter := client.getStreamWriter(inode)
	if streamWriter == nil {
		client.deleteRefercnt(inode)
		return
	}
	atomic.StoreInt32(&streamWriter.hasClosed, HasClosed)
	request := closeRequestPool.Get().(*CloseRequest)
	request.done = make(chan struct{}, 1)
	streamWriter.requestCh <- request
	<-request.done
	defer func() {
		closeRequestPool.Put(request)
	}()
	if err = request.err; err != nil {
		return
	}

	client.deleteRefercnt(inode)
	client.writerLock.Lock()
	delete(client.writers, inode)
	client.writerLock.Unlock()
	atomic.StoreInt32(&streamWriter.hasClosed, HasClosed)

	return
}

func (client *ExtentClient) Read(stream *StreamReader, inode uint64, data []byte, offset int, size int) (read int, err error) {
	if size == 0 {
		return
	}

	defer func() {
		if err != nil {
			ump.Alarm(gDataWrapper.UmpWarningKey(), err.Error())
		}
	}()

	wstream := client.getStreamWriterForRead(inode)
	if wstream != nil {
		request := flushRequestPool.Get().(*FlushRequest)
		request.done = make(chan struct{}, 1)
		wstream.requestCh <- request
		<-request.done
		err = request.err
		flushRequestPool.Put(request)
		if err != nil {
			return 0, err
		}
	}
	read, err = stream.read(data, offset, size)

	return
}
