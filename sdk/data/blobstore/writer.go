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

package blobstore

import (
	"context"
	"fmt"
	"hash"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	MaxBufferSize = 512 * util.MB
)

type wSliceErr struct {
	err        error
	fileOffset uint64
	size       uint32
}

type Writer struct {
	volType      int
	volName      string
	blockSize    int
	ino          uint64
	err          chan *wSliceErr
	bc           *bcache.BcacheClient
	mw           *meta.MetaWrapper
	ec           *stream.ExtentClient
	ebsc         *BlobStoreClient
	wConcurrency int
	wg           sync.WaitGroup
	once         sync.Once
	bufOnce      sync.Once
	sync.RWMutex
	objExtentKeys  []proto.ObjExtentKey
	enableBcache   bool
	cacheAction    int
	buf            []byte
	fileOffset     int
	fileCache      bool
	fileSize       uint64
	flushedSize    uint64
	cacheThreshold int
	dirty          bool
	blockPosition  int
	valid          bool
	clientCnt      int32
	limitManager   *manager.LimitManager
}

func NewWriter(config ClientConfig) (writer *Writer) {
	writer = new(Writer)

	writer.volName = config.VolName
	writer.volType = config.VolType
	writer.blockSize = config.BlockSize
	writer.ino = config.Ino
	writer.err = nil
	writer.bc = config.Bc
	writer.mw = config.Mw
	writer.ec = config.Ec
	writer.ebsc = config.Ebsc
	writer.wConcurrency = config.WConcurrency
	writer.wg = sync.WaitGroup{}
	writer.once = sync.Once{}
	writer.RWMutex = sync.RWMutex{}
	writer.enableBcache = config.EnableBcache
	writer.cacheAction = config.CacheAction
	writer.fileCache = config.FileCache
	writer.fileSize = config.FileSize
	writer.cacheThreshold = config.CacheThreshold
	writer.dirty = false
	writer.AllocateCache()
	writer.clientCnt = 1
	writer.limitManager = writer.ec.LimitManager

	return
}

func (writer *Writer) String() string {
	return fmt.Sprintf("Writer{address(%v),volName(%v),volType(%v),ino(%v),blockSize(%v),fileSize(%v),enableBcache(%v),cacheAction(%v),fileCache(%v),cacheThreshold(%v)},wConcurrency(%v)",
		&writer, writer.volName, writer.volType, writer.ino, writer.blockSize, writer.fileSize, writer.enableBcache, writer.cacheAction, writer.fileCache, writer.cacheThreshold, writer.wConcurrency)
}

func (writer *Writer) WriteWithoutPool(ctx context.Context, offset int, data []byte) (size int, err error) {
	//atomic.StoreInt32(&writer.idle, 0)
	if writer == nil {
		return 0, fmt.Errorf("writer is not opened yet")
	}
	log.LogDebugf("TRACE blobStore WriteWithoutPool Enter: ino(%v) offset(%v) len(%v) fileSize(%v)",
		writer.ino, offset, len(data), writer.CacheFileSize())

	if len(data) > MaxBufferSize || offset != writer.CacheFileSize() {
		log.LogErrorf("TRACE blobStore WriteWithoutPool error,may be len(%v)>512MB,offset(%v)!=fileSize(%v)",
			len(data), offset, writer.CacheFileSize())
		err = syscall.EOPNOTSUPP
		return
	}
	//write buffer
	log.LogDebugf("TRACE blobStore WriteWithoutPool: ino(%v) offset(%v) len(%v)",
		writer.ino, offset, len(data))

	size, err = writer.doBufferWriteWithoutPool(ctx, data, offset)

	return
}

func (writer *Writer) Write(ctx context.Context, offset int, data []byte, flags int) (size int, err error) {
	//atomic.StoreInt32(&writer.idle, 0)
	if writer == nil {
		return 0, fmt.Errorf("writer is not opened yet")
	}
	log.LogDebugf("TRACE blobStore Write Enter: ino(%v) offset(%v) len(%v) flags&proto.FlagsAppend(%v) fileSize(%v)", writer.ino, offset, len(data), flags&proto.FlagsAppend, writer.CacheFileSize())

	writer.Lock()
	defer writer.Unlock()

	writer.once.Do(func() {
		writer.refreshEbsExtents()
	})
	if !writer.valid {
		writer.refreshEbsExtents()
		if !writer.valid {
			log.LogErrorf("Writer: invoke fileSize fail. ino(%v) offset(%v) len(%v)", writer.ino, offset, len(data))
			err = syscall.EIO
			return
		}
	}

	if len(data) > MaxBufferSize || offset > writer.CacheFileSize() {
		log.LogErrorf("TRACE blobStore Write error,may be len(%v)>512MB,offset(%v)>fileSize(%v)", len(data), offset, writer.CacheFileSize())
		err = syscall.EOPNOTSUPP
		return
	}
	if flags&proto.FlagsSyncWrite != 0 && writer.dirty {
		err = writer.flush(writer.ino, ctx, true)
		if err != nil {
			return
		}
	}
	if offset != writer.CacheFileSize() {
		if isOverWrite := writer.checkIfOverWrite(offset, uint64(len(data))); isOverWrite {
			log.LogErrorf("TRACE blobStore Write error, overwrite is not supported, ino(%v) offset(%v) len(%v)", writer.ino, offset, len(data))
			err = syscall.EOPNOTSUPP
			return
		}
	}
	//write buffer
	log.LogDebugf("TRACE blobStore Write: ino(%v) offset(%v) len(%v) flags&proto.FlagsSyncWrite(%v)", writer.ino, offset, len(data), flags&proto.FlagsSyncWrite)
	if flags&proto.FlagsSyncWrite == 0 {
		size, err = writer.doBufferWrite(ctx, data, offset)
		return
	}
	//parallel io write ebs direct
	size, err = writer.doParallelWrite(ctx, data, offset)
	return
}

func (writer *Writer) doParallelWrite(ctx context.Context, data []byte, offset int) (size int, err error) {
	log.LogDebugf("TRACE blobStore doDirectWrite: ino(%v) offset(%v) len(%v)", writer.ino, offset, len(data))
	wSlices := writer.prepareWriteSlice(offset, data)
	log.LogDebugf("TRACE blobStore prepareWriteSlice: wSlices(%v)", wSlices)
	sliceSize := len(wSlices)

	writer.wg.Add(sliceSize)
	writer.err = make(chan *wSliceErr, sliceSize)
	pool := New(writer.wConcurrency, sliceSize)
	defer pool.Close()
	for _, wSlice := range wSlices {
		pool.Execute(wSlice, func(param *rwSlice) {
			writer.writeSlice(ctx, param, true)
		})
	}
	writer.wg.Wait()
	for i := 0; i < sliceSize; i++ {
		if wErr := <-writer.err; wErr != nil {
			log.LogErrorf("slice write error,ino(%v) fileoffset(%v) sliceSize(%v) err(%v)",
				writer.ino, wErr.fileOffset, wErr.size, wErr.err)
			return 0, wErr.err
		}
	}
	close(writer.err)
	//update meta
	oeks := make([]proto.ObjExtentKey, 0)
	for _, wSlice := range wSlices {
		size += int(wSlice.size)
		oeks = append(oeks, wSlice.objExtentKey)
	}
	log.LogDebugf("TRACE blobStore appendObjExtentKeys: oeks(%v)", oeks)
	if err = writer.mw.AppendObjExtentKeys(writer.ino, oeks); err != nil {
		log.LogErrorf("slice write error,meta append ebsc extent keys fail,ino(%v) fileOffset(%v) len(%v) err(%v)", writer.ino, offset, len(data), err)
		return
	}
	for _, wSlice := range wSlices {
		writer.append(wSlice.objExtentKey)
	}

	if filesize := writer.CacheFileSize(); offset+size > filesize {
		atomic.StoreUint64(&writer.fileSize, uint64(offset+size))
	}
	log.LogDebugf("TRACE blobStore doDirectWrite end: ino(%v) offset(%v) len(%v)", writer.ino, offset, len(data))

	for _, wSlice := range wSlices {
		writer.cacheLevel2(wSlice)
	}

	return
}

func (writer *Writer) cacheLevel2(wSlice *rwSlice) {
	if writer.cacheAction == proto.RWCache && (wSlice.fileOffset+uint64(wSlice.size)) < uint64(writer.cacheThreshold) || writer.fileCache {
		buf := make([]byte, wSlice.size)
		offSet := int(wSlice.fileOffset)
		copy(buf, wSlice.Data)
		go writer.asyncCache(writer.ino, offSet, buf)
	}
}

func (writer *Writer) WriteFromReader(ctx context.Context, reader io.Reader, h hash.Hash) (size uint64, err error) {
	var (
		buf         = make([]byte, 2*writer.blockSize)
		exec        = NewExecutor(writer.wConcurrency)
		leftToWrite int
	)

	writer.fileOffset = 0
	writer.err = make(chan *wSliceErr)

	var oeksLock sync.RWMutex
	oeks := make([]proto.ObjExtentKey, 0)

	writeBuff := func() {
		bufSize := len(writer.buf)
		log.LogDebugf("writeBuff: bufSize(%v), leftToWrite(%v), err(%v)", bufSize, leftToWrite, err)
		if bufSize == writer.blockSize || (leftToWrite == 0 && err == io.EOF) {
			wSlice := &rwSlice{
				fileOffset: uint64(writer.fileOffset - bufSize),
				size:       uint32(bufSize),
			}
			wSlice.Data = make([]byte, bufSize)
			copy(wSlice.Data, writer.buf)
			writer.buf = writer.buf[:0]
			if (err == nil || err == io.EOF) && h != nil {
				h.Write(wSlice.Data)
				log.LogDebugf("writeBuff: bufSize(%v), md5", bufSize)
			}
			writer.wg.Add(1)

			write := func() {
				defer writer.wg.Done()
				err := writer.writeSlice(ctx, wSlice, false)
				if err != nil {
					writer.Lock()
					if len(writer.err) > 0 {
						writer.Unlock()
						return
					}
					wErr := &wSliceErr{
						err:        err,
						fileOffset: wSlice.fileOffset,
						size:       wSlice.size,
					}
					writer.err <- wErr
					writer.Unlock()
					return
				}

				oeksLock.Lock()
				oeks = append(oeks, wSlice.objExtentKey)
				oeksLock.Unlock()

				writer.cacheLevel2(wSlice)
			}

			exec.Run(write)
		}
	}

LOOP:
	for {
		position := 0
		leftToWrite, err = reader.Read(buf)
		if err != nil && err != io.EOF {
			return
		}

		for leftToWrite > 0 {
			log.LogDebugf("WriteFromReader: leftToWrite(%v), err(%v)", leftToWrite, err)
			writer.RLock()
			errNum := len(writer.err)
			writer.RUnlock()
			if errNum > 0 {
				break LOOP
			}

			freeSize := writer.blockSize - len(writer.buf)
			writeSize := util.Min(leftToWrite, freeSize)
			writer.buf = append(writer.buf, buf[position:position+writeSize]...)
			position += writeSize
			leftToWrite -= writeSize
			writer.fileOffset += writeSize
			writer.dirty = true

			writeBuff()

		}
		if err == io.EOF {
			log.LogDebugf("WriteFromReader: EOF")
			if len(writer.buf) > 0 {
				writeBuff()
			}
			err = nil
			writer.wg.Wait()
			var wErr *wSliceErr
			select {
			case wErr := <-writer.err:
				err = wErr.err
			default:
			}
			if err != nil {
				log.LogErrorf("slice write error,ino(%v) fileoffset(%v)  sliceSize(%v) err(%v)", writer.ino, wErr.fileOffset, wErr.size, err)
			}
			break
		}
	}

	log.LogDebugf("WriteFromReader before sort: %v", oeks)
	sort.Slice(oeks, func(i, j int) bool {
		return oeks[i].FileOffset < oeks[j].FileOffset
	})
	log.LogDebugf("WriteFromReader after sort: %v", oeks)
	if err = writer.mw.AppendObjExtentKeys(writer.ino, oeks); err != nil {
		log.LogErrorf("WriteFromReader error,meta append ebsc extent keys fail,ino(%v), err(%v)", writer.ino, err)
		return
	}

	size = uint64(writer.fileOffset)
	atomic.AddUint64(&writer.fileSize, size)
	return
}

func (writer *Writer) doBufferWriteWithoutPool(ctx context.Context, data []byte, offset int) (size int, err error) {
	log.LogDebugf("TRACE blobStore doBufferWriteWithoutPool Enter: ino(%v) offset(%v) len(%v)", writer.ino, offset, len(data))

	writer.fileOffset = offset
	dataSize := len(data)
	position := 0
	log.LogDebugf("TRACE blobStore doBufferWriteWithoutPool: ino(%v) writer.buf.len(%v) writer.blocksize(%v)", writer.ino, len(writer.buf), writer.blockSize)
	writer.Lock()
	defer writer.Unlock()
	for dataSize > 0 {
		freeSize := writer.blockSize - len(writer.buf)
		if dataSize < freeSize {
			freeSize = dataSize
		}
		log.LogDebugf("TRACE blobStore doBufferWriteWithoutPool: ino(%v) writer.fileSize(%v) writer.fileOffset(%v) position(%v) freeSize(%v)", writer.ino, writer.fileSize, writer.fileOffset, position, freeSize)
		writer.buf = append(writer.buf, data[position:position+freeSize]...)
		log.LogDebugf("TRACE blobStore doBufferWriteWithoutPool:ino(%v) writer.buf.len(%v)", writer.ino, len(writer.buf))
		position += freeSize
		dataSize -= freeSize
		writer.fileOffset += freeSize
		writer.dirty = true

		if len(writer.buf) == writer.blockSize {
			log.LogDebugf("TRACE blobStore doBufferWriteWithoutPool: ino(%v) writer.buf.len(%v) writer.blocksize(%v)", writer.ino, len(writer.buf), writer.blockSize)
			writer.Unlock()
			err = writer.flushWithoutPool(writer.ino, ctx, false)
			writer.Lock()
			if err != nil {
				writer.buf = writer.buf[:len(writer.buf)-len(data)]
				writer.fileOffset -= len(data)
				return
			}

		}
	}

	size = len(data)
	atomic.AddUint64(&writer.fileSize, uint64(size))

	log.LogDebugf("TRACE blobStore doBufferWriteWithoutPool Exit: ino(%v) writer.fileSize(%v) writer.fileOffset(%v)", writer.ino, writer.fileSize, writer.fileOffset)
	return size, nil
}

func (writer *Writer) doBufferWrite(ctx context.Context, data []byte, offset int) (size int, err error) {
	log.LogDebugf("TRACE blobStore doBufferWrite Enter: ino(%v) offset(%v) len(%v)", writer.ino, offset, len(data))
	if writer.fileOffset != 0 && offset != writer.fileOffset {
		err = writer.flush(writer.ino, ctx, true)
		if err != nil {
			return
		}
	}
	writer.fileOffset = offset
	dataSize := len(data)
	position := 0
	log.LogDebugf("TRACE blobStore doBufferWrite: ino(%v) writer.buf.len(%v) writer.blocksize(%v)", writer.ino, len(writer.buf), writer.blockSize)
	for dataSize > 0 {
		freeSize := writer.blockSize - writer.blockPosition
		if dataSize < freeSize {
			freeSize = dataSize
		}
		log.LogDebugf("TRACE blobStore doBufferWrite: ino(%v) writer.fileSize(%v) writer.fileOffset(%v) writer.blockPosition(%v) position(%v) freeSize(%v)", writer.ino, writer.fileSize, writer.fileOffset, writer.blockPosition, position, freeSize)
		copy(writer.buf[writer.blockPosition:], data[position:position+freeSize])
		log.LogDebugf("TRACE blobStore doBufferWrite:ino(%v) writer.buf.len(%v)", writer.ino, len(writer.buf))
		position += freeSize
		writer.blockPosition += freeSize
		dataSize -= freeSize
		writer.fileOffset += freeSize
		writer.dirty = true

		if writer.blockPosition == writer.blockSize {
			log.LogDebugf("TRACE blobStore doBufferWrite: ino(%v) writer.buf.len(%v) writer.blocksize(%v)", writer.ino, len(writer.buf), writer.blockSize)
			err = writer.flush(writer.ino, ctx, false)
			if err != nil {
				writer.buf = writer.buf[:writer.blockPosition-freeSize]
				writer.fileOffset -= freeSize
				writer.blockPosition -= freeSize
				return
			}
		}
	}

	if writer.fileOffset-writer.blockPosition > writer.CacheFileSize() {
		writer.flushedSize = uint64(writer.fileOffset - writer.blockPosition)
	} else {
		writer.flushedSize = uint64(writer.CacheFileSize())
	}
	size = len(data)
	if writer.fileOffset > writer.CacheFileSize() {
		atomic.StoreUint64(&writer.fileSize, uint64(writer.fileOffset))
	}

	log.LogDebugf("TRACE blobStore doBufferWrite Exit: ino(%v) writer.fileSize(%v) writer.fileOffset(%v)", writer.ino, writer.fileSize, writer.fileOffset)
	return size, nil
}

func (writer *Writer) FlushWithoutPool(ino uint64, ctx context.Context) (err error) {
	if writer == nil {
		return
	}
	return writer.flushWithoutPool(ino, ctx, true)
}

func (writer *Writer) Flush(ino uint64, ctx context.Context) (err error) {
	if writer == nil {
		return
	}
	writer.Lock()
	defer writer.Unlock()
	return writer.flush(ino, ctx, true)
}

func (writer *Writer) shouldCacheCfs() bool {
	return writer.cacheAction == proto.RWCache
}

func (writer *Writer) prepareWriteSlice(offset int, data []byte) []*rwSlice {
	size := len(data)
	wSlices := make([]*rwSlice, 0)
	wSliceCount := size / writer.blockSize
	remainSize := size % writer.blockSize
	for index := 0; index < wSliceCount; index++ {
		offset := offset + index*writer.blockSize
		wSlice := &rwSlice{
			index:      index,
			fileOffset: uint64(offset),
			size:       uint32(writer.blockSize),
			Data:       data[index*writer.blockSize : (index+1)*writer.blockSize],
		}
		wSlices = append(wSlices, wSlice)
	}
	offset = offset + wSliceCount*writer.blockSize
	if remainSize > 0 {
		wSlice := &rwSlice{
			index:      wSliceCount,
			fileOffset: uint64(offset),
			size:       uint32(remainSize),
			Data:       data[wSliceCount*writer.blockSize:],
		}
		wSlices = append(wSlices, wSlice)
	}

	return wSlices
}

func (writer *Writer) writeSlice(ctx context.Context, wSlice *rwSlice, wg bool) (err error) {
	if wg {
		defer writer.wg.Done()
	}
	writer.limitManager.WriteAlloc(ctx, int(wSlice.size))
	log.LogDebugf("TRACE blobStore,writeSlice to ebs. ino(%v) fileOffset(%v) len(%v)", writer.ino, wSlice.fileOffset, wSlice.size)
	location, err := writer.ebsc.Write(ctx, writer.volName, wSlice.Data, wSlice.size)
	if err != nil {
		if wg {
			writer.err <- &wSliceErr{err: err, fileOffset: wSlice.fileOffset, size: wSlice.size}
		}
		return err
	}
	log.LogDebugf("TRACE blobStore,location(%v)", location)
	blobs := make([]proto.Blob, 0)
	for _, info := range location.Blobs {
		blob := proto.Blob{
			MinBid: uint64(info.MinBid),
			Count:  uint64(info.Count),
			Vid:    uint64(info.Vid),
		}
		blobs = append(blobs, blob)
	}
	wSlice.objExtentKey = proto.ObjExtentKey{
		Cid:        uint64(location.ClusterID),
		CodeMode:   uint8(location.CodeMode),
		Size:       location.Size,
		BlobSize:   location.BlobSize,
		Blobs:      blobs,
		BlobsLen:   uint32(len(blobs)),
		FileOffset: wSlice.fileOffset,
		Crc:        location.Crc,
	}
	log.LogDebugf("TRACE blobStore,objExtentKey(%v)", wSlice.objExtentKey)

	if wg {
		writer.err <- nil
	}
	return
}

func (writer *Writer) asyncCache(ino uint64, offset int, data []byte) {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("write-async-cache", err, bgTime, 1)
	}()

	log.LogDebugf("TRACE asyncCache Enter,fileOffset(%v) len(%v)", offset, len(data))
	write, err := writer.ec.Write(ino, offset, data, proto.FlagsCache, nil)
	log.LogDebugf("TRACE asyncCache Exit,write(%v) err(%v)", write, err)

}

func (writer *Writer) resetBufferWithoutPool() {
	writer.buf = writer.buf[:0]
}

func (writer *Writer) resetBuffer() {
	//writer.buf = writer.buf[:0]
	writer.blockPosition = 0
}

func (writer *Writer) flushWithoutPool(inode uint64, ctx context.Context, flushFlag bool) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("blobstore-flush", err, bgTime, 1)
	}()

	log.LogDebugf("TRACE blobStore flushWithoutPool: ino(%v) buf-len(%v) flushFlag(%v)", inode, len(writer.buf), flushFlag)
	writer.Lock()
	defer func() {
		writer.dirty = false
		writer.Unlock()
	}()

	if len(writer.buf) == 0 || !writer.dirty {
		return
	}
	bufferSize := len(writer.buf)
	wSlice := &rwSlice{
		fileOffset: uint64(writer.fileOffset - bufferSize),
		size:       uint32(bufferSize),
		Data:       writer.buf,
	}
	err = writer.writeSlice(ctx, wSlice, false)
	if err != nil {
		if flushFlag {
			atomic.AddUint64(&writer.fileSize, -uint64(bufferSize))
		}
		return
	}

	oeks := make([]proto.ObjExtentKey, 0)
	//update meta
	oeks = append(oeks, wSlice.objExtentKey)
	if err = writer.mw.AppendObjExtentKeys(writer.ino, oeks); err != nil {
		log.LogErrorf("slice write error,meta append ebsc extent keys fail,ino(%v) fileOffset(%v) len(%v) err(%v)", inode, wSlice.fileOffset, wSlice.size, err)
		return
	}
	writer.resetBufferWithoutPool()

	writer.cacheLevel2(wSlice)
	return
}

func (writer *Writer) flush(inode uint64, ctx context.Context, flushFlag bool) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("blobstore-flush", err, bgTime, 1)
	}()

	log.LogDebugf("TRACE blobStore flush: ino(%v) buf-len(%v) flushFlag(%v)", inode, len(writer.buf), flushFlag)
	defer func() {
		writer.dirty = false
	}()

	if len(writer.buf) == 0 || !writer.dirty {
		return
	}
	bufferSize := writer.blockPosition
	wSlice := &rwSlice{
		fileOffset: uint64(writer.fileOffset - bufferSize),
		size:       uint32(bufferSize),
		Data:       writer.buf,
	}
	err = writer.writeSlice(ctx, wSlice, false)
	if err != nil {
		if flushFlag {
			atomic.StoreUint64(&writer.fileSize, writer.flushedSize)
			writer.fileOffset -= writer.blockPosition
			writer.resetBuffer()
		}
		return
	}

	oeks := make([]proto.ObjExtentKey, 0)
	//update meta
	oeks = append(oeks, wSlice.objExtentKey)
	if err = writer.mw.AppendObjExtentKeys(writer.ino, oeks); err != nil {
		log.LogErrorf("slice write error,meta append ebsc extent keys fail,ino(%v) fileOffset(%v) len(%v) err(%v)", inode, wSlice.fileOffset, wSlice.size, err)
		if flushFlag {
			atomic.StoreUint64(&writer.fileSize, writer.flushedSize)
			writer.fileOffset -= writer.blockPosition
			writer.resetBuffer()
		}
		return
	}
	writer.append(wSlice.objExtentKey)
	writer.resetBuffer()

	writer.cacheLevel2(wSlice)
	return
}

func (writer *Writer) CacheFileSize() int {
	return int(atomic.LoadUint64(&writer.fileSize))
}

func (writer *Writer) FreeCache() {
	if writer == nil {
		return
	}
	if buf.CachePool == nil {
		return
	}
	writer.bufOnce.Do(func() {
		tmpBuf := writer.buf
		writer.buf = nil
		if tmpBuf != nil {
			buf.CachePool.Put(tmpBuf)
		}
	})
}

func (writer *Writer) AllocateCache() {
	if buf.CachePool == nil {
		return
	}
	writer.buf = buf.CachePool.Get()
}

func (writer *Writer) GetClientCnt() int32 {
	return atomic.LoadInt32(&(writer.clientCnt))
}

func (writer *Writer) IncreaseClientCnt() {
	atomic.AddInt32(&(writer.clientCnt), 1)
}

func (writer *Writer) DecreaseClientCnt() {
	atomic.AddInt32(&(writer.clientCnt), -1)
}

func (writer *Writer) refreshEbsExtents() {
	_, size, _, oeks, err := writer.mw.GetObjExtents(writer.ino)
	if err != nil {
		writer.valid = false
		log.LogErrorf("TRACE blobStore writer refreshEbsExtents error. ino(%v)  err(%v) ", writer.ino, err)
		return
	}
	writer.valid = true
	writer.fileSize = size
	writer.objExtentKeys = oeks
}

func (writer *Writer) checkIfOverWrite(offset int, size uint64) (isOverWrite bool) {
	start := uint64(offset)
	end := uint64(offset + int(size))
	if writer.dirty {
		bufStart := uint64(writer.fileOffset - writer.blockPosition)
		bufEnd := uint64(writer.fileOffset)
		if start < bufStart {
			if end > bufStart {
				return true
			}
		} else if start < bufEnd {
			return true
		}
	}
	// 1. list is empty
	if len(writer.objExtentKeys) <= 0 {
		return
	}
	// 2. last key's (fileoffset+size) is not greater than new one
	lastKey := writer.objExtentKeys[len(writer.objExtentKeys)-1]
	if (lastKey.FileOffset + lastKey.Size) <= uint64(offset) {
		return
	}
	// 3. find if overlay
	L := 0
	R := len(writer.objExtentKeys) - 1
	for {
		if L > R {
			break
		}
		m := (L + R) / 2
		curEk := writer.objExtentKeys[m]
		ekStart := curEk.FileOffset
		ekEnd := curEk.FileOffset + curEk.Size

		if start < ekStart {
			if end <= ekStart {
				R = m - 1
			} else {
				return true
			}
		} else if start < ekEnd {
			return true
		} else {
			L = m + 1
		}
	}
	return
}

func (writer *Writer) append(oek proto.ObjExtentKey) {
	// 1. list is empty
	if len(writer.objExtentKeys) <= 0 {
		writer.objExtentKeys = append(writer.objExtentKeys, oek)
		return
	}
	// 2. last key's (fileoffset+size) is not greater than new one
	lastKey := writer.objExtentKeys[len(writer.objExtentKeys)-1]
	if (lastKey.FileOffset + lastKey.Size) <= oek.FileOffset {
		writer.objExtentKeys = append(writer.objExtentKeys, oek)
		return
	}

	// 3. find one key is equals to the new one, if not and overlay, log warn
	start := oek.FileOffset
	end := oek.FileOffset + oek.Size
	var overlayEk *proto.ObjExtentKey
	L := 0
	R := len(writer.objExtentKeys) - 1
	appendIdx := R
	for {
		if L > R {
			break
		}
		m := (L + R) / 2
		curEk := writer.objExtentKeys[m]
		if oek.IsEquals(&curEk) {
			return
		}
		ekStart := curEk.FileOffset
		ekEnd := curEk.FileOffset + curEk.Size

		if start < ekStart {
			if end <= ekStart {
				R = m - 1
				appendIdx = m
			} else {
				overlayEk = &curEk
				break
			}
		} else if start < ekEnd {
			overlayEk = &curEk
			break
		} else {
			L = m + 1
		}
	}

	if overlayEk != nil {
		log.LogWarnf("obj extentkeys exist overlay! the new obj extent key cannot cover existed extend key(%s), new(%s)",
			overlayEk.String(), oek.String())
		writer.valid = false
		return
	}

	writer.objExtentKeys = append(writer.objExtentKeys, oek)
	copy(writer.objExtentKeys[appendIdx+1:], writer.objExtentKeys[appendIdx:])
	writer.objExtentKeys[appendIdx] = oek

	return
}

func (writer *Writer) Truncate(ino uint64, ctx context.Context, truncateSize uint64) (err error) {
	if writer == nil {
		return fmt.Errorf("writer is not opened yet")
	}
	writer.Lock()
	defer writer.Unlock()
	if err = writer.flush(ino, ctx, true); err != nil {
		return
	}
	var nodeInfo *proto.InodeInfo
	if nodeInfo, err = writer.mw.InodeGet_ll(ino); err != nil {
		return
	}
	fileSize := uint64(writer.CacheFileSize())
	if nodeInfo.Size > fileSize {
		fileSize = nodeInfo.Size
	}
	if truncateSize < fileSize {
		log.LogErrorf("Truncate: truncate size(%v) is less than file size(%v)", truncateSize, fileSize)
		return fmt.Errorf("truncate size is less than file size")
	}
	if err = writer.mw.Truncate(ino, truncateSize); err != nil {
		return
	}
	atomic.StoreUint64(&writer.fileSize, truncateSize)
	return
}
