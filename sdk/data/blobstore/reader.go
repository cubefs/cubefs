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
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/client/blockcache/bcache"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/prometheus/client_golang/prometheus"
)

var readerMetric = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Namespace:  "cubefs",
		Subsystem:  "client",
		Name:       "reader_cost_time",
		Help:       "time cost in cubefs sdk",
		Objectives: map[float64]float64{0.5: 0.05, 0.75: 0.025, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001, 0.999: 0.0001, 0.9999: 0.00001},
	}, []string{"api"})

func init() {
	prometheus.MustRegister(readerMetric)
}

type rwSlice struct {
	index        int
	fileOffset   uint64
	size         uint32
	rOffset      uint64
	rSize        uint32
	read         int
	Data         []byte
	objExtentKey proto.ObjExtentKey
}

func (s rwSlice) String() string {
	return fmt.Sprintf("rwSlice{fileOffset(%v),size(%v),rOffset(%v),rSize(%v),read(%v),objExtentKey(%v)}", s.fileOffset, s.size, s.rOffset, s.rSize, s.read, s.objExtentKey)
}

func (reader *Reader) String() string {
	return fmt.Sprintf("Reader{address(%v),volName(%v),volType(%v),ino(%v),fileSize(%v),enableBcache(%v),fileCache(%v),cacheThreshold(%v)},readConcurrency(%v)",
		&reader, reader.volName, reader.volType, reader.ino, reader.fileLength, reader.enableBcache, reader.fileCache, reader.cacheThreshold, reader.readConcurrency)
}

type Reader struct {
	volName         string
	volType         int
	ino             uint64
	err             chan error
	bc              *bcache.BcacheClient
	mw              *meta.MetaWrapper
	ec              *stream.ExtentClient
	ebs             *BlobStoreClient
	readConcurrency int
	wg              sync.WaitGroup
	once            sync.Once
	sync.Mutex
	close          bool
	extentKeys     []proto.ExtentKey
	objExtentKeys  []proto.ObjExtentKey
	enableBcache   bool
	fileCache      bool
	cacheThreshold int
	fileLength     uint64
	valid          bool
	inflightCache  sync.Map
	limitManager   *manager.LimitManager
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
	FileCache       bool
	FileSize        uint64
	CacheThreshold  int
	StorageClass    uint32
}

func NewReader(config ClientConfig) (reader *Reader) {
	reader = new(Reader)

	reader.volName = config.VolName
	reader.volType = config.VolType
	reader.ino = config.Ino
	reader.bc = config.Bc
	reader.ec = config.Ec
	reader.ebs = config.Ebsc
	reader.mw = config.Mw
	reader.enableBcache = config.EnableBcache
	reader.readConcurrency = config.ReadConcurrency
	reader.fileCache = config.FileCache
	reader.cacheThreshold = config.CacheThreshold

	reader.limitManager = config.Ec.LimitManager
	return
}

func (reader *Reader) Read(ctx context.Context, buf []byte, offset int, size int) (int, error) {
	beg := time.Now()
	defer func() {
		readerMetric.WithLabelValues("BlobstorRead").Observe(float64(time.Since(beg).Microseconds()))
	}()

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
	sliceSize := len(rSlices)
	if sliceSize > 0 {
		reader.wg.Add(sliceSize)
		pool := New(reader.readConcurrency, sliceSize)
		defer pool.Close()
		reader.err = make(chan error, sliceSize)
		for _, rs := range rSlices {
			pool.Execute(rs, func(param *rwSlice) {
				reader.readSliceRange(ctx, param)
			})
		}

		reader.wg.Wait()
		for i := 0; i < sliceSize; i++ {
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
	endflag := false
	selected := false

	reader.once.Do(func() {
		reader.refreshEbsExtents()
	})
	fileSize, valid := reader.fileSize()
	reader.fileLength = fileSize
	log.LogDebugf("TRACE blobStore prepareEbsSlice Enter. ino(%v)  fileSize(%v) ", reader.ino, fileSize)
	if !valid {
		log.LogErrorf("Reader: invoke fileSize fail. ino(%v)  offset(%v) size(%v)", reader.ino, offset, size)
		return nil, syscall.EIO
	}
	log.LogDebugf("TRACE blobStore prepareEbsSlice. ino(%v)  offset(%v) size(%v)", reader.ino, offset, size)
	if uint64(offset) >= fileSize {
		return nil, io.EOF
	}

	start := uint64(offset)
	if uint64(offset)+uint64(size) > fileSize {
		size = uint32(fileSize - uint64(offset))
	}
	end := uint64(offset + int(size))
	for index, oek := range reader.objExtentKeys {
		rs := &rwSlice{}
		if oek.FileOffset <= start && start < oek.FileOffset+(oek.Size) {
			rs.index = index
			rs.fileOffset = oek.FileOffset
			rs.size = uint32(oek.Size)
			rs.rOffset = start - oek.FileOffset
			rs.rSize = uint32(oek.FileOffset + oek.Size - start)
			selected = true
		}
		if end <= oek.FileOffset+oek.Size {
			rs.rSize = uint32(end - start)
			selected = true
			endflag = true
		}
		if selected {
			rs.objExtentKey = oek
			rs.Data = make([]byte, rs.rSize)
			start = oek.FileOffset + oek.Size
			chunks = append(chunks, rs)
			log.LogDebugf("TRACE blobStore prepareEbsSlice. ino(%v)  offset(%v) size(%v) rwSlice(%v)", reader.ino, offset, size, rs)
		}
		if endflag {
			break
		}
	}
	log.LogDebugf("TRACE blobStore prepareEbsSlice Exit. ino(%v)  offset(%v) size(%v) rwSlices(%v)", reader.ino, offset, size, chunks)
	return chunks, nil
}

func (reader *Reader) readSliceRange(ctx context.Context, rs *rwSlice) (err error) {
	defer reader.wg.Done()
	log.LogDebugf("TRACE blobStore readSliceRange Enter. ino(%v)  rs.fileOffset(%v),rs.rOffset(%v),rs.rSize(%v) ", reader.ino, rs.fileOffset, rs.rOffset, rs.rSize)
	cacheKey := util.GenerateKey(reader.volName, reader.ino, rs.fileOffset)
	log.LogDebugf("TRACE blobStore readSliceRange. ino(%v)  cacheKey(%v) ", reader.ino, cacheKey)
	buf := make([]byte, rs.rSize)
	var readN int

	bgTime := stat.BeginStat()
	stat.EndStat("CacheGet", nil, bgTime, 1)
	// all request for each block.
	metric := exporter.NewTPCnt("CacheGet")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: reader.volName})
	}()

	// read local cache
	if reader.enableBcache {
		readN, err = reader.bc.Get(reader.volName, cacheKey, buf, rs.rOffset, rs.rSize)
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
	if !readLimitOn {
		reader.limitManager.ReadAlloc(ctx, int(rs.rSize))
	}

	_, err = reader.ebs.Read(ctx, reader.volName, buf, rs.rOffset, uint64(rs.rSize), rs.objExtentKey)
	if err != nil {
		reader.err <- err
		return
	}
	read := copy(rs.Data, buf)
	reader.err <- nil

	// cache full block
	if !reader.needCacheL1() {
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
	if _, ok := reader.inflightCache.Load(cacheKey); ok {
		return
	}

	reader.inflightCache.Store(cacheKey, true)
	defer reader.inflightCache.Delete(cacheKey)

	buf := make([]byte, objExtentKey.Size)
	read, err := reader.ebs.Read(ctx, reader.volName, buf, 0, uint64(len(buf)), objExtentKey)
	if err != nil || read != len(buf) {
		log.LogErrorf("ERROR blobStore asyncCache fail, size no match. cacheKey=%v, objExtentKey.size=%v, read=%v",
			cacheKey, len(buf), read)
		return
	}

	if reader.needCacheL1() {
		reader.bc.Put(reader.volName, cacheKey, buf)
	}

	log.LogDebugf("TRACE blobStore asyncCache(L1) Exit. cacheKey=%v", cacheKey)
}

func (reader *Reader) needCacheL1() bool {
	return reader.enableBcache
}

func (reader *Reader) refreshEbsExtents() {
	_, _, eks, oeks, err := reader.mw.GetObjExtents(reader.ino)
	if err != nil {
		reader.valid = false
		log.LogErrorf("TRACE blobStore refreshEbsExtents error. ino(%v)  err(%v) ", reader.ino, err)
		return
	}
	reader.valid = true
	reader.extentKeys = eks
	reader.objExtentKeys = oeks
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
