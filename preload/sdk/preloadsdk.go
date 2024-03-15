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

package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"path"
	gopath "path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	blog "github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LimitParameters struct {
	TraverseDirConcurrency int64
	PreloadFileConcurrency int64
	ReadBlockConcurrency   int32
	PreloadFileSizeLimit   int64
	ClearFileConcurrency   int64
}

type PreLoadClient struct {
	mw   *meta.MetaWrapper
	ec   *stream.ExtentClient
	mc   *masterSDK.MasterClient
	ebsc *blobstore.BlobStoreClient
	sync.RWMutex
	fileCache             []fileInfo // file list for target dir
	vol                   string
	limitParam            LimitParameters
	cacheAction           int
	cacheThreshold        int
	ebsBlockSize          int
	preloadFileNumTotal   int64
	preloadFileNumSucceed int64
}

type fileInfo struct {
	ino  uint64
	name string
	size uint64
}

type PreloadConfig struct {
	Volume     string
	Masters    []string
	LogDir     string
	LogLevel   string
	ProfPort   string
	LimitParam LimitParameters
}

var getSpan = proto.SpanFromContext

func NewClient(ctx context.Context, config PreloadConfig) *PreLoadClient {
	c := &PreLoadClient{
		vol: config.Volume,
	}

	level := log.ParseLevel(config.LogLevel, log.Ldebug)
	if config.LogDir != "" {
		log.SetOutputLevel(level)
		log.SetOutput(&lumberjack.Logger{
			Filename: path.Join(config.LogDir, "preload", "preload.log"),
			MaxSize:  1024, MaxAge: 7, MaxBackups: 7, LocalTime: true, Compress: true,
		})
		stat.NewStatistic(config.LogDir, "preload", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
	}

	if config.ProfPort != "" {
		go func() {
			mainMux := http.NewServeMux()
			mux := http.NewServeMux()
			http.HandleFunc(log.ChangeDefaultLevelHandler())
			mux.Handle("/debug/pprof", http.HandlerFunc(pprof.Index))
			mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
			mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
			mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
			mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
			mux.Handle("/debug/", http.HandlerFunc(pprof.Index))
			mainHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasPrefix(req.URL.Path, "/debug/") {
					mux.ServeHTTP(w, req)
				} else {
					http.DefaultServeMux.ServeHTTP(w, req)
				}
			})
			mainMux.Handle("/", mainHandler)
			e := http.ListenAndServe(fmt.Sprintf(":%v", config.ProfPort), mainMux)
			if e != nil {
				log.Warnf("newClient newEBSClient cannot listen pprof (%v)", config.ProfPort)
			}
		}()
	}

	span := getSpan(ctx)
	var (
		mw  *meta.MetaWrapper
		ec  *stream.ExtentClient
		err error
	)
	c.mc = masterSDK.NewMasterClient(config.Masters, false)
	if err = c.newEBSClient(ctx, config.Masters, config.LogDir, level); err != nil {
		span.Errorf("newClient newEBSClient failed(%v)", err)
		return nil
	}

	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        config.Volume,
		Masters:       config.Masters,
		ValidateOwner: false,
	}); err != nil {
		span.Errorf("newClient NewMetaWrapper failed(%v)", err)
		return nil
	}

	if ec, err = stream.NewExtentClient(ctx, &stream.ExtentConfig{
		Volume:            config.Volume,
		Masters:           config.Masters,
		Preload:           true,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnSplitExtentKey:  mw.SplitExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		VolumeType:        proto.VolumeTypeCold,
	}); err != nil {
		span.Errorf("newClient NewExtentClient failed(%v)", err)
		return nil
	}
	c.mw = mw
	c.ec = ec
	c.limitParam.PreloadFileConcurrency = config.LimitParam.PreloadFileConcurrency

	if c.limitParam.PreloadFileConcurrency == 0 {
		c.limitParam.PreloadFileConcurrency = 3
	}
	c.limitParam.TraverseDirConcurrency = config.LimitParam.TraverseDirConcurrency

	if c.limitParam.PreloadFileConcurrency == 0 {
		c.limitParam.PreloadFileConcurrency = 4
	}
	c.limitParam.PreloadFileSizeLimit = config.LimitParam.PreloadFileSizeLimit
	c.limitParam.ReadBlockConcurrency = config.LimitParam.ReadBlockConcurrency
	if c.limitParam.ReadBlockConcurrency == 0 {
		c.limitParam.ReadBlockConcurrency = 3
	}
	c.limitParam.ClearFileConcurrency = config.LimitParam.ClearFileConcurrency
	if c.limitParam.ClearFileConcurrency == 0 {
		c.limitParam.ClearFileConcurrency = 4
	}
	span.Debugf("Client is created:(%v)", c)
	return c
}

func (c *PreLoadClient) newEBSClient(ctx context.Context, masters []string, logDir string, level blog.Level) (err error) {
	var (
		volumeInfo  *proto.SimpleVolView
		clusterInfo *proto.ClusterInfo
		ebsc        *blobstore.BlobStoreClient
	)
	volumeInfo, err = c.mc.AdminAPI().GetVolumeSimpleInfo(ctx, c.vol)
	if err != nil {
		return
	}
	c.ebsBlockSize = volumeInfo.ObjBlockSize
	c.cacheAction = volumeInfo.CacheAction
	c.cacheThreshold = volumeInfo.CacheThreshold

	clusterInfo, err = c.mc.AdminAPI().GetClusterInfo(ctx)
	if err != nil {
		return
	}
	ebsEndpoint := clusterInfo.EbsAddr

	if ebsc, err = blobstore.NewEbsClient(access.Config{
		ConnMode: access.NoLimitConnMode,
		Consul: access.ConsulConfig{
			Address: ebsEndpoint,
		},
		MaxSizePutOnce: int64(c.ebsBlockSize),
		LogLevel:       level,
		Logger: &access.Logger{
			Filename: gopath.Join(logDir, "ebs/ebs.log"),
		},
	}); err != nil {
		getSpan(ctx).Errorf("newClient newEBSClient failed(%v)", err)
		return
	}
	c.ebsc = ebsc
	buf.InitCachePool(c.ebsBlockSize)
	return
}

func (c *PreLoadClient) walker(ctx context.Context, dir string,
	wg *sync.WaitGroup, currentGoroutineNum *int64, ch chan<- fileInfo, newGoroutine bool,
) (err error) {
	defer func() {
		if newGoroutine {
			atomic.AddInt64(currentGoroutineNum, -1)
			wg.Done()
		}
	}()

	var (
		ino  uint64
		info *proto.InodeInfo
	)

	span := getSpan(ctx)
	ino, err = c.mw.LookupPath(ctx, gopath.Clean(dir))
	if err != nil {
		span.Errorf("LookupPath path(%v) faild(%v)", dir, err)
		return err
	}

	info, err = c.mw.InodeGet_ll(ctx, ino)
	if err != nil {
		span.Errorf("InodeGet_ll path(%v) faild(%v)", dir, err)
		return err
	}

	if proto.IsRegular(info.Mode) {
		fInfo := fileInfo{ino: info.Inode, name: gopath.Base(dir)}
		span.Debugf("Target is a file:(%v)", fInfo)
		ch <- fInfo
		return err
	}

	children, err := c.mw.ReadDir_ll(ctx, info.Inode)
	if err != nil {
		span.Errorf("ReadDir_ll path(%v) faild(%v)", dir, err)
		return err
	}

	for _, child := range children {
		if proto.IsDir(child.Type) {
			span.Debugf("preloadTravrseDir:(%v) is dir", path.Join(dir, child.Name))
			if atomic.LoadInt64(currentGoroutineNum) < c.limitParam.PreloadFileConcurrency {
				wg.Add(1)
				atomic.AddInt64(currentGoroutineNum, 1)
				go c.walker(ctx, path.Join(dir, child.Name), wg, currentGoroutineNum, ch, true)
			} else {
				c.walker(ctx, path.Join(dir, child.Name), wg, currentGoroutineNum, ch, false)
			}

		} else if proto.IsRegular(child.Type) {
			fInfo := fileInfo{ino: child.Inode, name: child.Name}
			span.Debugf("preloadTravrseDir send:(%v)", fInfo)
			ch <- fInfo
		}
	}
	return nil
}

func (c *PreLoadClient) allocatePreloadDPWorker(ctx context.Context, ch <-chan fileInfo) uint64 {
	span := getSpan(ctx)
	var (
		inodes []uint64
		total  uint64
		cache  map[uint64]fileInfo
	)
	cache = make(map[uint64]fileInfo)
	for fInfo := range ch {
		inodes = append(inodes, fInfo.ino)

		cache[fInfo.ino] = fInfo
		if len(inodes) < 100 {
			continue
		}
		infos := c.mw.BatchInodeGet(ctx, inodes)
		for _, info := range infos {
			if int64(info.Size) <= c.limitParam.PreloadFileSizeLimit {
				total += info.Size
				cachefino := cache[info.Inode]
				cachefino.size = info.Size
				// append
				c.fileCache = append(c.fileCache, cachefino)
				span.Debugf("allocatePreloadDPWorker append:%v", cachefino)
			}
		}
		inodes = inodes[:0]
		for key := range cache {
			delete(cache, key)
		}
	}

	// flush cache
	if len(inodes) != 0 {
		infos := c.mw.BatchInodeGet(ctx, inodes)
		for _, info := range infos {
			if int64(info.Size) <= c.limitParam.PreloadFileSizeLimit {
				total += info.Size
				cachefino := cache[info.Inode]
				cachefino.size = info.Size
				// append
				c.fileCache = append(c.fileCache, cachefino)
				span.Debugf("allocatePreloadDPWorker append#2:%v", cachefino)
			}
		}
	}
	return total
}

func (c *PreLoadClient) allocatePreloadDP(ctx context.Context, target string, count int, ttl uint64, zones string) (err error) {
	span := getSpan(ctx)
	span.Debug("allocatePreloadDP enter")
	ch := make(chan fileInfo, 100)
	var (
		wg                  sync.WaitGroup
		currentGoroutineNum int64 = 0
	)
	wg.Add(1)
	atomic.AddInt64(&currentGoroutineNum, 1)
	go c.walker(ctx, target, &wg, &currentGoroutineNum, ch, true)
	go func() {
		wg.Wait()
		close(ch)
	}()
	need := c.allocatePreloadDPWorker(ctx, ch)
	if need == 0 {
		return errors.New("No file would be preloaded")
	}
	//#test1
	if (need % (1024 * 1024 * 1024)) == 0 {
		need = uint64(need / (1024 * 1024 * 1024))
	} else {
		need = uint64(need/(1024*1024*1024)) + 1
	}
	span.Debugf("preloadFile:need total space %v GB, ttl %v second, zones=%v", need, ttl, zones)
	total, used := c.getVolumeCacheCapacity(ctx)

	if float64(need+used) > float64(total) {
		span.Errorf("AllocatePreLoadDataPartition failed: need space (%v) GB, volume total(%v)GB used(%v)GB", need, total, used)
		return errors.New(fmt.Sprintf("AllocatePreLoadDataPartition failed: need space (%v) GB, volume total(%v)GB used(%v)GB", need, total, used))
	}
	err = c.ec.AllocatePreLoadDataPartition(ctx, c.vol, count, need, ttl, zones)

	if err != nil {
		span.Errorf("AllocatePreLoadDataPartition failed:%v", err)
		return err
	}
	span.Debug("allocatePreloadDP leave")
	return nil
}

func (c *PreLoadClient) clearPreloadDPWorker(ctx context.Context, ch <-chan fileInfo) {
	var routineNum int64 = 0
	var wg sync.WaitGroup

	span := getSpan(ctx)
	for fInfo := range ch {
		if atomic.LoadInt64(&routineNum) < c.limitParam.ClearFileConcurrency {
			wg.Add(1)
			go func(f fileInfo) {
				defer func() {
					atomic.AddInt64(&routineNum, -1)
					wg.Done()
				}()
				span.Debugf("clearPreloadDPWorker:clear file %v", f.name)
				c.mw.InodeClearPreloadCache_ll(ctx, f.ino)
			}(fInfo)
			atomic.AddInt64(&routineNum, 1)
		} else {
			span.Debugf("clearPreloadDPWorker:clear file %v", fInfo.name)
			c.mw.InodeClearPreloadCache_ll(ctx, fInfo.ino)
		}
	}
	wg.Wait()
}

func (c *PreLoadClient) ClearPreloadDP(ctx context.Context, target string) (err error) {
	ch := make(chan fileInfo, 100)
	var (
		wg                  sync.WaitGroup
		currentGoroutineNum int64 = 0
	)
	wg.Add(1)
	atomic.AddInt64(&currentGoroutineNum, 1)
	go c.walker(ctx, target, &wg, &currentGoroutineNum, ch, true)
	go func() {
		wg.Wait()
		close(ch)
	}()
	c.clearPreloadDPWorker(ctx, ch)
	return nil
}

func (c *PreLoadClient) preloadFileWorker(ctx context.Context, id int64, jobs <-chan fileInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	var total int64 = 0
	var succeed int64 = 0
	noWritableDP := false
	span := getSpan(ctx)
	for job := range jobs {
		if noWritableDP == true {
			span.Warnf("no writable dp,ingnore (%v) to cbfs", job.name)
			continue // consume the job
		}
		total += 1
		span.Debugf("worker %v ready to preload(%v)", id, job.name)
		ino := job.ino
		//#1 open
		c.ec.OpenStream(ctx, ino)
		//#2 write
		var (
			objExtents []proto.ObjExtentKey
			err        error
		)
		if _, _, _, objExtents, err = c.mw.GetObjExtents(ctx, ino); err != nil {
			span.Warnf("GetObjExtents (%v) faild(%v)", job.name, err)
			continue
		}
		clientConf := blobstore.ClientConfig{
			VolName:         c.vol,
			VolType:         proto.VolumeTypeCold,
			Ino:             ino,
			Mw:              c.mw,
			Ec:              c.ec,
			Ebsc:            c.ebsc,
			EnableBcache:    false,
			ReadConcurrency: int(c.limitParam.ReadBlockConcurrency),
			CacheAction:     c.cacheAction,
			FileCache:       false,
			CacheThreshold:  c.cacheThreshold,
		}

		fileReader := blobstore.NewReader(clientConf)
		subErr := false

		for _, objExtent := range objExtents {
			size := objExtent.Size
			buf := make([]byte, size)
			var n int
			n, err = fileReader.Read(ctx, buf, int(objExtent.FileOffset), int(size))

			if err != nil {
				subErr = true
				span.Warnf("Read (%v) from ebs failed (%v)", objExtent, err)
				continue
			}

			if uint64(n) != size {
				span.Warnf("Read (%v) wrong size:(%v)", objExtent, n)
				continue
			}
			_, err = c.ec.Write(ctx, ino, int(objExtent.FileOffset), buf, 0, nil)
			// in preload mode,onece extend_hander set to error, streamer is set to error
			// so write should failed immediately
			if err != nil {
				subErr = true
				span.Warnf("preload (%v) to cbfs failed (%v)", job.name, err)
				if err = c.ec.GetDataPartitionForWrite(ctx); err != nil {
					span.Errorf("worker %v end for %v", id, err)
					noWritableDP = true
				}
				break
			}
		}
		c.ec.CloseStream(ino)
		if subErr == false {
			span.Infof("worker %v preload (%v) to cbfs success", id, job.name)
			succeed += 1
		}
	}
	atomic.AddInt64(&c.preloadFileNumTotal, total)
	atomic.AddInt64(&c.preloadFileNumSucceed, succeed)
	span.Infof("worker %v end:total %v, succeed %v", id, total, succeed)
}

func (c *PreLoadClient) preloadFile(ctx context.Context) error {
	span := getSpan(ctx)
	span.Debug("preloadFile enter")
	var (
		wg sync.WaitGroup
		w  int64
	)
	jobs := make(chan fileInfo, 100)

	for w = 1; w <= c.limitParam.PreloadFileConcurrency; w++ {
		wg.Add(1)
		go c.preloadFileWorker(ctx, w, jobs, &wg)
	}

	for _, fileInfo := range c.fileCache {
		jobs <- fileInfo
	}
	close(jobs)
	wg.Wait()
	span.Infof("preloadFile end:total %v, succeed %v", c.preloadFileNumTotal, c.preloadFileNumSucceed)
	if c.preloadFileNumTotal == c.preloadFileNumSucceed {
		return nil
	} else if c.preloadFileNumSucceed < c.preloadFileNumTotal {
		return errors.New("Preload partially succeed")
	} else {
		return errors.New("Preload failed")
	}
}

func (c *PreLoadClient) CheckColdVolume(ctx context.Context) bool {
	view, err := c.mc.AdminAPI().GetVolumeSimpleInfo(ctx, c.vol)
	if err != nil {
		getSpan(ctx).Errorf("get volume simple info fail: volume(%v) err(%v)", c.vol, err)
		return false
	}
	return view.VolType == proto.VolumeTypeCold
}

func (c *PreLoadClient) getVolumeCacheCapacity(ctx context.Context) (total uint64, used uint64) {
	view, err := c.mc.AdminAPI().GetVolumeSimpleInfo(ctx, c.vol)
	if err != nil {
		getSpan(ctx).Errorf("get volume simple info fail: volume(%v) err(%v)", c.vol, err)
		return 0, 0
	}
	return view.CacheCapacity, view.PreloadCapacity
}

func (c *PreLoadClient) PreloadDir(ctx context.Context, target string, count int, ttl uint64, zones string) (err error) {
	span := getSpan(ctx)
	span.Debugf("PreloadDir (%v)", target)
	if err = c.allocatePreloadDP(ctx, target, count, ttl, zones); err != nil {
		span.Errorf("PreloadDir failed(%v)", err)
		return
	}
	span.Debug("Wait 100s for preload dp get ready")
	time.Sleep(time.Duration(100) * time.Second)
	span.Debug("Sleep end")
	// Step3.2  preload the file
	return c.preloadFile(ctx)
}

func (c *PreLoadClient) GetPreloadResult(ctx context.Context) (int64, int64) {
	return c.preloadFileNumTotal, c.preloadFileNumSucceed
}
