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
	"github.com/cubefs/cubefs/sdk/data/manager"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/stat"

	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

type rwSlice struct {
	index        int
	fileOffset   uint64
	size         uint32
	rOffset      uint64
	rSize        uint32
	read         int
	Data         []byte
	extentKey    proto.ExtentKey
	objExtentKey proto.ObjExtentKey
	empty        bool
}

func (reader *Reader) NewRwSlice(index int, rOffset uint64, rSize uint32, objExtentKey *proto.ObjExtentKey) *rwSlice {
	rs := new(rwSlice)
	rs.rOffset = rOffset
	rs.rSize = rSize
	rs.Data = make([]byte, rs.rSize)

	if objExtentKey != nil {
		rs.index = index
		rs.fileOffset = objExtentKey.FileOffset
		rs.size = uint32(objExtentKey.Size)
		rs.objExtentKey = *objExtentKey
		reader.buildExtentKey(rs)
	} else {
		rs.empty = true
		for i := range rs.Data {
			rs.Data[i] = 0
		}
	}
	return rs
}

func (s rwSlice) String() string {
	return fmt.Sprintf("rwSlice{fileOffset(%v),size(%v),rOffset(%v),rSize(%v),read(%v),extentKey(%v),objExtentKey(%v)}", s.fileOffset, s.size, s.rOffset, s.rSize, s.read, s.extentKey, s.objExtentKey)
}

func (reader *Reader) String() string {
	return fmt.Sprintf("Reader{address(%v),volName(%v),volType(%v),ino(%v),fileSize(%v),enableBcache(%v),cacheAction(%v),fileCache(%v),cacheThreshold(%v)},readConcurrency(%v)",
		&reader, reader.volName, reader.volType, reader.ino, reader.fileLength, reader.enableBcache, reader.cacheAction, reader.fileCache, reader.cacheThreshold, reader.readConcurrency)
}

type Reader struct {
	volName         string
	volType         int
	ino             uint64
	offset          uint64
	data            []byte
	err             chan error
	bc              *bcache.BcacheClient
	mw              *meta.MetaWrapper
	ec              *stream.ExtentClient
	ebs             *BlobStoreClient
	readConcurrency int
	cacheTimeout    time.Duration
	wg              sync.WaitGroup
	once            sync.Once
	sync.Mutex
	close           bool
	extentKeys      []proto.ExtentKey
	missExtentKeys  []proto.ExtentKey
	objExtentKeys   []proto.ObjExtentKey
	enableBcache    bool
	cacheAction     int
	fileCache       bool
	cacheThreshold  int
	fileLength      uint64
	valid           bool
	inflightL2cache sync.Map
	limitManager    *manager.LimitManager
}

type ClientConfig struct {
	VolName         string
	VolType         int
	BlockSize       int
	Ino             uint64
	Bc              *bcache.BcacheClient
	Mw              *meta.MetaWrapper
	Ec              *stream.ExtentClient
	Ebsc            *BlobStoreClient
	EnableBcache    bool
	WConcurrency    int
	ReadConcurrency int
	CacheAction     int
	FileCache       bool
	FileSize        uint64
	CacheThreshold  int
}

func NewReader(config ClientConfig) (reader *Reader) {
	reader = new(Reader)

	reader.volName = config.VolName
	reader.volType = config.VolType
	reader.ino = config.Ino
	reader.bc = config.Bc
	reader.ebs = config.Ebsc
	reader.mw = config.Mw
	reader.ec = config.Ec
	reader.enableBcache = config.EnableBcache
	reader.readConcurrency = config.ReadConcurrency
	reader.cacheAction = config.CacheAction
	reader.fileCache = config.FileCache
	reader.cacheThreshold = config.CacheThreshold

	if proto.IsCold(reader.volType) {
		reader.ec.UpdateDataPartitionForColdVolume()
	}

	reader.limitManager = reader.ec.LimitManager
	return
}

func (reader *Reader) Read(ctx context.Context, buf []byte, offset int, size int) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("reader is not opened yet")
	}
	log.LogDebugf("TRACE reader Read Enter. ino(%v) offset(%v) len(%v)", reader.ino, offset, size)
	var (
		read = 0
		err  error
	)
	if reader.close {
		return 0, os.ErrInvalid
	}

	reader.Lock()
	defer reader.Unlock()
	// cold volume,slice read
	var rSlices []*rwSlice
	if size != len(buf) {
		size = len(buf)
	}

	rSlices, err = reader.prepareEbsSlice(offset, uint32(size))
	log.LogDebugf("TRACE reader Read. ino(%v)  rSlices-length(%v) ", reader.ino, len(rSlices))

	if err != nil {
		return 0, err
	}

	emptySliceNum := 0
	for _, rs := range rSlices {
		if rs.empty {
			emptySliceNum += 1
		}
	}
	sliceSize := len(rSlices)
	nonEmptySliceSize := len(rSlices) - emptySliceNum
	if nonEmptySliceSize > 0 {
		reader.wg.Add(nonEmptySliceSize)
		reader.err = make(chan error, nonEmptySliceSize)

		pool := New(reader.readConcurrency, nonEmptySliceSize)
		defer pool.Close()
		for _, rs := range rSlices {
			if rs.empty {
				continue
			}
			pool.Execute(rs, func(param *rwSlice) {
				reader.readSliceRange(ctx, param)
			})
		}

		reader.wg.Wait()
		for i := 0; i < nonEmptySliceSize; i++ {
			if err, ok := <-reader.err; !ok || err != nil {
				return 0, err
			}
		}
		close(reader.err)
	}
	for i := 0; i < sliceSize; i++ {
		read += copy(buf[read:], rSlices[i].Data)
	}
	log.LogDebugf("TRACE reader Read Exit. ino(%v)  readN(%v) buf-len(%v)", reader.ino, read, len(buf))
	return read, nil

}

func (reader *Reader) Close(ctx context.Context) {
	reader.Lock()
	reader.close = true
	reader.Unlock()
}

func (reader *Reader) prepareEbsSlice(offset int, size uint32) ([]*rwSlice, error) {
	if offset < 0 {
		return nil, syscall.EIO
	}
	chunks := make([]*rwSlice, 0)

	reader.once.Do(func() {
		reader.refreshEbsExtents()
	})
	if !reader.valid {
		log.LogErrorf("Reader: invoke fileSize fail. ino(%v)  offset(%v) size(%v)", reader.ino, offset, size)
		return nil, syscall.EIO
	}
	fileSize := reader.fileLength
	log.LogDebugf("TRACE blobStore prepareEbsSlice Enter. ino(%v) fileSize(%v) offset(%v) size(%v)", reader.ino, fileSize, offset, size)
	if uint64(offset) >= fileSize {
		return nil, io.EOF
	}

	start := uint64(offset)
	if uint64(offset)+uint64(size) > fileSize {
		size = uint32(fileSize - uint64(offset))
	}
	end := uint64(offset + int(size))
	log.LogDebugf("TRACE blobStore prepareEbsSlice. ino(%v) fileSize(%v) offset(%v) size(%v) start(%v) end(%v)", reader.ino, fileSize, offset, size, start, end)
	for index, oek := range reader.objExtentKeys {
		ekStart := oek.FileOffset
		ekEnd := oek.FileOffset + oek.Size

		if start < ekStart {
			if end <= ekStart {
				break
			} else if end < ekEnd {
				// add hole (start, ekStart)
				rs := reader.NewRwSlice(-1, 0, uint32(ekStart-start), nil)
				chunks = append(chunks, rs)
				// add non-hole (ekStart, end)
				rs = reader.NewRwSlice(index, 0, uint32(end-ekStart), &oek)
				chunks = append(chunks, rs)
				start = end
				break
			} else {
				// add hole (start, ekStart)
				rs := reader.NewRwSlice(-1, 0, uint32(ekStart-start), nil)
				chunks = append(chunks, rs)

				// add non-hole (ekStart, ekEnd)
				rs = reader.NewRwSlice(index, 0, uint32(ekEnd-ekStart), &oek)
				chunks = append(chunks, rs)

				start = ekEnd
				continue
			}
		} else if start < ekEnd {
			if end <= ekEnd {
				// add non-hole (start, end)
				rs := reader.NewRwSlice(index, start-ekStart, uint32(end-start), &oek)
				chunks = append(chunks, rs)
				start = end
				break
			} else {
				// add non-hole (start, ekEnd), start = ekEnd
				rs := reader.NewRwSlice(index, start-ekStart, uint32(ekEnd-start), &oek)
				chunks = append(chunks, rs)
				start = ekEnd
				continue
			}
		} else {
			continue
		}
	}
	if start < end {
		// add hole (start, end)
		rs := reader.NewRwSlice(-1, start, uint32(end-start), nil)
		chunks = append(chunks, rs)
	}
	for _, rs := range chunks {
		log.LogDebugf("TRACE blobStore prepareEbsSlice. ino(%v)  offset(%v) size(%v) rwSlice(%v)", reader.ino, offset, size, rs)
	}
	log.LogDebugf("TRACE blobStore prepareEbsSlice Exit. ino(%v)  offset(%v) size(%v) rwSlices(%v)", reader.ino, offset, size, chunks)
	return chunks, nil
}

func (reader *Reader) buildExtentKey(rs *rwSlice) {
	if len(reader.extentKeys) <= 0 {
		rs.extentKey = proto.ExtentKey{}
	} else {
		low := 0
		high := len(reader.extentKeys) - 1
		for low <= high {
			mid := (high + low) / 2
			target := reader.extentKeys[mid]
			if target.FileOffset == rs.objExtentKey.FileOffset {
				rs.extentKey = target
				return
			} else if target.FileOffset > rs.objExtentKey.FileOffset {
				high = mid - 1
			} else {
				low = mid + 1
			}
		}
		rs.extentKey = proto.ExtentKey{}
	}

}

func (reader *Reader) readSliceRange(ctx context.Context, rs *rwSlice) (err error) {
	defer reader.wg.Done()
	log.LogDebugf("TRACE blobStore readSliceRange Enter. ino(%v)  rs.fileOffset(%v),rs.rOffset(%v),rs.rSize(%v) ", reader.ino, rs.fileOffset, rs.rOffset, rs.rSize)
	cacheKey := util.GenerateKey(reader.volName, reader.ino, rs.fileOffset)
	log.LogDebugf("TRACE blobStore readSliceRange. ino(%v)  cacheKey(%v) ", reader.ino, cacheKey)
	buf := make([]byte, rs.rSize)
	var (
		readN int
	)

	bgTime := stat.BeginStat()
	stat.EndStat("CacheGet", nil, bgTime, 1)
	// all request for each block.
	metric := exporter.NewTPCnt("CacheGet")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: reader.volName})
	}()

	//read local cache
	if reader.enableBcache {
		readN, err = reader.bc.Get(cacheKey, buf, rs.rOffset, rs.rSize)
		if err == nil {
			reader.ec.BcacheHealth = true
			if readN == int(rs.rSize) {

				// L1 cache hit.
				metric := exporter.NewTPCnt("L1CacheGetHit")
				stat.EndStat("CacheHit-L1", nil, bgTime, 1)
				defer func() {
					metric.SetWithLabels(err, map[string]string{exporter.Vol: reader.volName})
				}()

				copy(rs.Data, buf)
				reader.err <- nil
				return
			}
		}
	}

	readLimitOn := false
	//read cfs and cache to bcache
	if rs.extentKey != (proto.ExtentKey{}) {

		//check if dp is exist in preload sence
		err = reader.ec.CheckDataPartitionExsit(rs.extentKey.PartitionId)
		if err == nil || ctx.Value("objectnode") != nil {
			readN, err, readLimitOn = reader.ec.ReadExtent(reader.ino, &rs.extentKey, buf, int(rs.rOffset), int(rs.rSize))
			if err == nil && readN == int(rs.rSize) {

				// L2 cache hit.
				metric := exporter.NewTPCnt("L2CacheGetHit")
				stat.EndStat("CacheHit-L2", nil, bgTime, 1)
				defer func() {
					metric.SetWithLabels(err, map[string]string{exporter.Vol: reader.volName})
				}()

				copy(rs.Data, buf)
				reader.err <- nil
				return
			}
		} else {
			log.LogDebugf("checkDataPartitionExsit failed (%v)", err)
		}
		log.LogDebugf("TRACE blobStore readSliceRange. cfs block miss.extentKey=%v,err=%v", rs.extentKey, err)
	}
	if !readLimitOn {
		reader.limitManager.ReadAlloc(ctx, int(rs.rSize))
	}

	readN, err = reader.ebs.Read(ctx, reader.volName, buf, rs.rOffset, uint64(rs.rSize), rs.objExtentKey)
	if err != nil {
		reader.err <- err
		return
	}
	read := copy(rs.Data, buf)
	reader.err <- nil

	//cache full block
	if !reader.needCacheL1() && !reader.needCacheL2() || reader.ec.IsPreloadMode() {
		log.LogDebugf("TRACE blobStore readSliceRange exit without cache. read counter=%v", read)
		return nil
	}

	asyncCtx := context.Background()
	go reader.asyncCache(asyncCtx, cacheKey, rs.objExtentKey)

	log.LogDebugf("TRACE blobStore readSliceRange exit with cache. read counter=%v", read)
	return nil
}

func (reader *Reader) asyncCache(ctx context.Context, cacheKey string, objExtentKey proto.ObjExtentKey) {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("read-async-cache", err, bgTime, 1)
	}()

	log.LogDebugf("TRACE blobStore asyncCache Enter. cacheKey=%v", cacheKey)

	// block is go loading.
	if _, ok := reader.inflightL2cache.Load(cacheKey); ok {
		return
	}

	reader.inflightL2cache.Store(cacheKey, true)
	defer reader.inflightL2cache.Delete(cacheKey)

	buf := make([]byte, objExtentKey.Size)
	read, err := reader.ebs.Read(ctx, reader.volName, buf, 0, uint64(len(buf)), objExtentKey)
	if err != nil || read != len(buf) {
		log.LogErrorf("ERROR blobStore asyncCache fail, size no match. cacheKey=%v, objExtentKey.size=%v, read=%v",
			cacheKey, len(buf), read)
		return
	}

	if reader.needCacheL2() {
		reader.ec.Write(reader.ino, int(objExtentKey.FileOffset), buf, proto.FlagsCache, nil)
		log.LogDebugf("TRACE blobStore asyncCache(L2) Exit. cacheKey=%v", cacheKey)
		return
	}

	if reader.needCacheL1() {
		reader.bc.Put(cacheKey, buf)
	}

	log.LogDebugf("TRACE blobStore asyncCache(L1) Exit. cacheKey=%v", cacheKey)
}

func (reader *Reader) needCacheL2() bool {
	if reader.cacheAction > proto.NoCache && reader.fileLength < uint64(reader.cacheThreshold) || reader.fileCache {
		return true
	}
	return false
}

func (reader *Reader) needCacheL1() bool {
	return reader.enableBcache
}

func (reader *Reader) refreshEbsExtents() {
	_, size, eks, oeks, err := reader.mw.GetObjExtents(reader.ino)
	if err != nil {
		reader.valid = false
		log.LogErrorf("TRACE blobStore refreshEbsExtents error. ino(%v)  err(%v) ", reader.ino, err)
		return
	}
	reader.valid = true
	reader.extentKeys = eks
	reader.objExtentKeys = oeks
	reader.fileLength = size
	log.LogDebugf("TRACE blobStore refreshEbsExtents ok. extentKeys(%v)  objExtentKeys(%v) ", reader.extentKeys, reader.objExtentKeys)
}

func (reader *Reader) fileSize() (uint64, bool) {
	objKeys := reader.objExtentKeys
	if !reader.valid {
		return 0, false
	}
	if len(objKeys) > 0 {
		lastIndex := len(objKeys) - 1
		return objKeys[lastIndex].FileOffset + objKeys[lastIndex].Size, true
	}
	return 0, true
}
