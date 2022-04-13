// Copyright 2022 The ChubaoFS Authors.
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
	"github.com/cubefs/cubefs/util/buf"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/cubefs/cubefs/util/stat"

	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const (
	MaxBufferSize = 512 * util.MB
)

type Writer struct {
	volType      int
	volName      string
	blockSize    int
	ino          uint64
	err          chan error
	bc           *bcache.BcacheClient
	mw           *meta.MetaWrapper
	ec           *stream.ExtentClient
	ebsc         *BlobStoreClient
	wConcurrency int
	wg           sync.WaitGroup
	once         sync.Once
	sync.Mutex
	enableBcache   bool
	cacheAction    int
	buf            []byte
	fileOffset     int
	fileCache      bool
	fileSize       uint64
	cacheThreshold int
	dirty          bool
	blockPosition  int
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
	writer.Mutex = sync.Mutex{}
	writer.enableBcache = config.EnableBcache
	writer.cacheAction = config.CacheAction
	writer.fileCache = config.FileCache
	writer.fileSize = config.FileSize
	writer.cacheThreshold = config.CacheThreshold
	writer.dirty = false
	writer.AllocateCache()
	return
}

func (writer *Writer) String() string {
	return fmt.Sprintf("Writer{address(%v),volName(%v),volType(%v),ino(%v),blockSize(%v),fileSize(%v),enableBcache(%v),cacheAction(%v),fileCache(%v),cacheThreshold(%v)},wConcurrency(%v)",
		&writer, writer.volName, writer.volType, writer.ino, writer.blockSize, writer.fileSize, writer.enableBcache, writer.cacheAction, writer.fileCache, writer.cacheThreshold, writer.wConcurrency)
}

func (writer *Writer) Write(ctx context.Context, offset int, data []byte, flags int) (size int, err error) {
	//atomic.StoreInt32(&writer.idle, 0)
	if writer == nil {
		return 0, fmt.Errorf("writer is not opened yet")
	}
	log.LogDebugf("TRACE blobStore Write Enter: ino(%v) offset(%v) len(%v) flags&proto.FlagsAppend(%v) fileSize(%v)", writer.ino, offset, len(data), flags&proto.FlagsAppend, writer.CacheFileSize())

	if len(data) > MaxBufferSize || flags&proto.FlagsAppend == 0 || offset != writer.CacheFileSize() {
		log.LogErrorf("TRACE blobStore Write error,may be len(%v)>512MB,flags(%v)!=flagAppend,offset(%v)!=fileSize(%v)", len(data), flags&proto.FlagsAppend, offset, writer.CacheFileSize())
		err = syscall.EOPNOTSUPP
		return
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
	writer.Lock()
	defer writer.Unlock()
	wSlices := writer.prepareWriteSlice(offset, data)
	log.LogDebugf("TRACE blobStore prepareWriteSlice: wSlices(%v)", wSlices)
	sliceSize := len(wSlices)

	writer.wg.Add(sliceSize)
	writer.err = make(chan error, sliceSize)
	pool := New(writer.wConcurrency, sliceSize)
	defer pool.Close()
	for _, wSlice := range wSlices {
		pool.Execute(wSlice, func(param *rwSlice) {
			writer.writeSlice(ctx, param, true)
		})
	}
	writer.wg.Wait()
	for i := 0; i < sliceSize; i++ {
		if err, ok := <-writer.err; !ok || err != nil {
			log.LogErrorf("slice write error,ino(%v) fileoffset(%v) sliceOffset(%v) sliceSize(%v) err(%v)", writer.ino, wSlices[i].fileOffset, wSlices[i].rOffset, wSlices[i].rSize, err)
			return 0, err
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
	atomic.AddUint64(&writer.fileSize, uint64(size))

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

func (writer *Writer) doBufferWrite(ctx context.Context, data []byte, offset int) (size int, err error) {
	log.LogDebugf("TRACE blobStore doBufferWrite Enter: ino(%v) offset(%v) len(%v)", writer.ino, offset, len(data))

	writer.fileOffset = offset
	dataSize := len(data)
	position := 0
	log.LogDebugf("TRACE blobStore doBufferWrite: ino(%v) writer.buf.len(%v) writer.blocksize(%v)", writer.ino, len(writer.buf), writer.blockSize)
	writer.Lock()
	defer writer.Unlock()
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
			writer.Unlock()
			err = writer.flush(writer.ino, ctx, false)
			writer.Lock()
			if err != nil {
				writer.buf = writer.buf[:writer.blockPosition-freeSize]
				writer.fileOffset -= freeSize
				writer.blockPosition -= freeSize
				return
			}
		}
	}

	size = len(data)
	atomic.AddUint64(&writer.fileSize, uint64(size))

	log.LogDebugf("TRACE blobStore doBufferWrite Exit: ino(%v) writer.fileSize(%v) writer.fileOffset(%v)", writer.ino, writer.fileSize, writer.fileOffset)
	return size, nil
}

func (writer *Writer) Flush(ino uint64, ctx context.Context) (err error) {
	if writer == nil {
		return
	}
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
	log.LogDebugf("TRACE blobStore,writeSlice to ebs. ino(%v) fileOffset(%v) len(%v)", writer.ino, wSlice.fileOffset, wSlice.size)
	location, err := writer.ebsc.Write(ctx, writer.volName, wSlice.Data, wSlice.size)
	if err != nil {
		if wg {
			writer.err <- err
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
	write, err := writer.ec.Write(ino, offset, data, 0)
	log.LogDebugf("TRACE asyncCache Exit,write(%v) err(%v)", write, err)

}

func (writer *Writer) resetBuffer() {
	//writer.buf = writer.buf[:0]
	writer.blockPosition = 0
}

func (writer *Writer) flush(inode uint64, ctx context.Context, flushFlag bool) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("blobstore-flush", err, bgTime, 1)
	}()

	log.LogDebugf("TRACE blobStore flush: ino(%v) buf-len(%v) flushFlag(%v)", inode, len(writer.buf), flushFlag)
	writer.Lock()
	defer func() {
		writer.dirty = false
		writer.Unlock()
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
	writer.resetBuffer()

	writer.cacheLevel2(wSlice)
	return
}

func (writer *Writer) CacheFileSize() int {
	return int(atomic.LoadUint64(&writer.fileSize))
}

func (writer *Writer) FreeCache() {
	buf.CachePool.Put(writer.buf)
}

func (writer *Writer) AllocateCache() {
	writer.buf = buf.CachePool.Get()
}
