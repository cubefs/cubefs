package stream

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/data"
	"github.com/tiglabs/baudstorage/util/log"
	"runtime"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) ([]proto.ExtentKey, error)

type ExtentClient struct {
	w               *data.Wrapper
	writers         map[uint64]*StreamWriter
	writerLock      sync.RWMutex
	referCnt        map[uint64]uint64
	referLock       sync.Mutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
	bufferSize      uint64
}

func NewExtentClient(volname, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc, bufferSize uint64) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)
	client.w, err = data.NewDataPartitionWrapper(volname, master)
	if err != nil {
		return nil, fmt.Errorf("init dp Wrapper failed [%v]", err.Error())
	}
	client.writers = make(map[uint64]*StreamWriter)
	client.referCnt = make(map[uint64]uint64)
	client.appendExtentKey = appendExtentKey
	client.getExtents = getExtents
	client.bufferSize = bufferSize
	return
}

func (client *ExtentClient) InitWriteStream(inode uint64) *StreamWriter {
	writer := NewStreamWriter(client.w, inode, client.appendExtentKey, client.bufferSize)
	client.writers[inode] = writer
	return writer
}

func (client *ExtentClient) getStreamWriter(inode uint64) *StreamWriter {
	client.writerLock.RLock()
	writer, ok := client.writers[inode]
	client.writerLock.RUnlock()
	if ok {
		return writer
	}

	client.writerLock.Lock()
	writer, ok = client.writers[inode]
	if !ok {
		writer = client.InitWriteStream(inode)
	}
	client.writerLock.Unlock()
	return writer
}

func (client *ExtentClient) getStreamWriterForClose(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	stream = client.writers[inode]
	client.writerLock.RUnlock()

	return
}

func (client *ExtentClient) Write(inode uint64, offset int, data []byte) (write int, err error) {
	prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
	stream := client.getStreamWriter(inode)
	if stream == nil {
		return 0, fmt.Errorf("Prefix[%v] cannot init write stream", prefix)
	}
	request := &WriteRequest{data: data, kernelOffset: offset, size: len(data), requestMode: RequestWriteMode}
	stream.requestCh <- request
	request = <-stream.replyCh
	err = request.err
	write = request.canWrite
	if err != nil {
		err = errors.Annotatef(err, prefix)
		log.LogError(errors.ErrorStack(err))
	}
	return
}

func (client *ExtentClient) OpenForRead(inode uint64) (stream *StreamReader, err error) {
	return NewStreamReader(inode, client.w, client.getExtents)
}

func (client *ExtentClient) Flush(inode uint64) (err error) {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		return fmt.Errorf("cannot init write stream")
	}
	request := &WriteRequest{requestMode: RequestFlushMode}
	stream.requestCh <- request
	request = <-stream.replyCh
	return request.err
}

func (client *ExtentClient) Close(inode uint64) (err error) {
	client.referLock.Lock()
	inodeReferCnt := client.referCnt[inode]
	if inodeReferCnt > 0 {
		client.referCnt[inode] = inodeReferCnt - 1

	}
	inodeReferCnt = client.referCnt[inode]
	client.referLock.Unlock()
	streamWriter := client.getStreamWriterForClose(inode)
	if streamWriter != nil {
		request := &WriteRequest{requestMode: RequestFlushMode}
		streamWriter.requestCh <- request
		request = <-streamWriter.replyCh
		if err = request.err; err != nil {
			return
		}
	}
	if inodeReferCnt != 0 {
		return
	}

	if streamWriter != nil {
		request := &WriteRequest{requestMode: RequestCloseMode}
		streamWriter.requestCh <- request
		request = <-streamWriter.replyCh
		if err = request.err; err != nil {
			return
		}
		client.writerLock.Lock()
		delete(client.writers, inode)
		client.writerLock.Unlock()
	}

	return
}

func (client *ExtentClient) Read(stream *StreamReader, inode uint64, data []byte, offset int, size int) (read int, err error) {
	if size == 0 {
		return
	}
	client.writerLock.RLock()
	wstream := client.writers[inode]
	client.writerLock.RUnlock()
	if wstream != nil {
		request := &WriteRequest{requestMode: RequestFlushMode}
		wstream.requestCh <- request
		request = <-wstream.replyCh
		if err = request.err; err != nil {
			return 0, err
		}
	}
	read, err = stream.read(data, offset, size)

	return
}

func (client *ExtentClient) Delete(keys []proto.ExtentKey) (err error) {
	wg := &sync.WaitGroup{}
	for _, k := range keys {
		dp, err := client.w.GetDataPartition(k.PartitionId)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(p *data.DataPartition, id uint64) {
			defer wg.Done()
			client.delete(p, id)
		}(dp, k.ExtentId)
	}

	wg.Wait()
	return nil
}

func (client *ExtentClient) delete(dp *data.DataPartition, extentId uint64) (err error) {
	connect, err := client.w.GetConnect(dp.Hosts[0])
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			client.w.PutConnect(connect, false)
		} else {
			client.w.PutConnect(connect, true)
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
