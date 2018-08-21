// Copyright 2018 The ChuBao Authors.
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

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/sdk/data"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"runtime"
	"sync/atomic"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) ([]proto.ExtentKey, error)

var (
	gDataWrapper     *data.Wrapper
	gFlushBufferSize uint64
)

type ExtentClient struct {
	writers         map[uint64]*StreamWriter
	referCnt        map[uint64]uint64
	referLock       sync.Mutex
	writerLock      sync.RWMutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
}

func NewExtentClient(volname, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc, flushBufferSize uint64) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)
	gDataWrapper, err = data.NewDataPartitionWrapper(volname, master)
	if err != nil {
		return nil, fmt.Errorf("init dp Wrapper failed (%v)", err.Error())
	}
	client.writers = make(map[uint64]*StreamWriter)
	client.appendExtentKey = appendExtentKey
	client.referCnt = make(map[uint64]uint64)
	client.getExtents = getExtents
	gFlushBufferSize = flushBufferSize
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
	request := &WriteRequest{data: data, kernelOffset: offset, size: len(data), done: make(chan struct{}, 1)}
	stream.writeRequestCh <- request
	<-request.done
	err = request.err
	write = request.canWrite
	write += request.cutSize
	if err != nil {
		prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
		err = errors.Annotatef(err, prefix)
		log.LogError(errors.ErrorStack(err))
	}
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
	return writer.HasWriteSize
}

func (client *ExtentClient) SetWriteSize(inode, size uint64) {
	client.writerLock.Lock()
	defer client.writerLock.Unlock()
	writer, ok := client.writers[inode]
	if ok {
		writer.HasWriteSize = size
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
	request := &FlushRequest{}
	stream.flushRequestCh <- request
	request = <-stream.flushReplyCh
	return request.err
}

func (client *ExtentClient) CloseForWrite(inode uint64) (err error) {
	client.referLock.Lock()
	refercnt, ok := client.referCnt[inode]
	if !ok {
		client.Flush(inode)
		client.referLock.Unlock()
		return nil
	}
	refercnt = refercnt - 1
	client.referCnt[inode] = refercnt
	if refercnt > 0 {
		client.Flush(inode)
		client.referLock.Unlock()
		return
	}
	client.referLock.Unlock()

	streamWriter := client.getStreamWriter(inode)
	if streamWriter == nil {
		client.deleteRefercnt(inode)
		return
	}
	atomic.StoreInt32(&streamWriter.hasClosed, HasClosed)
	request := &CloseRequest{}
	streamWriter.closeRequestCh <- request
	request = <-streamWriter.closeReplyCh
	if err = request.err; err != nil {
		return
	}

	client.deleteRefercnt(inode)
	client.writerLock.Lock()
	delete(client.writers, inode)
	atomic.StoreInt32(&streamWriter.hasClosed, HasClosed)
	streamWriter.exitCh <- true
	client.writerLock.Unlock()

	return
}

func (client *ExtentClient) Read(stream *StreamReader, inode uint64, data []byte, offset int, size int) (read int, err error) {
	if size == 0 {
		return
	}
	wstream := client.getStreamWriterForRead(inode)
	if wstream != nil {
		request := &FlushRequest{}
		wstream.flushRequestCh <- request
		request = <-wstream.flushReplyCh
		if err = request.err; err != nil {
			return 0, err
		}
	}
	read, err = stream.read(data, offset, size)

	return
}

func (client *ExtentClient) Delete(keys []proto.ExtentKey) (err error) {
	//wg := &sync.WaitGroup{}
	for _, k := range keys {
		dp, err := gDataWrapper.GetDataPartition(k.PartitionId)
		if err != nil {
			continue
		}
		//wg.Add(1)
		go func(p *data.DataPartition, id uint64) {
			//defer wg.Done()
			client.delete(p, id)
		}(dp, k.ExtentId)
	}

	//wg.Wait()
	return nil
}

func (client *ExtentClient) delete(dp *data.DataPartition, extentId uint64) (err error) {
	connect, err := gDataWrapper.GetConnect(dp.Hosts[0])
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			gDataWrapper.PutConnect(connect, false)
		} else {
			gDataWrapper.PutConnect(connect, true)
		}
	}()
	p := NewDeleteExtentPacket(dp, extentId)
	if err = p.WriteToConn(connect); err != nil {
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime); err != nil {
		return
	}

	return
}
