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

package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"path"
	gopath "path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/blobstore/api/access"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/hashicorp/consul/api"
)

type LimitParameters struct {
	TraverseDirConcurrency int64
	PreloadFileConcurrency int64
	ReadBlockConcurrency   int64
	PreloadFileSizeLimit   int64
	ClearFileConcurrency   int64
}

type PreLoadClient struct {
	mw   *meta.MetaWrapper
	ec   *stream.ExtentClient
	mc   *masterSDK.MasterClient
	ebsc *blobstore.BlobStoreClient
	sync.RWMutex
	fileCache             []fileInfo //file list for target dir
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

func FlushLog() {
	log.LogFlush()
}

func NewClient(config PreloadConfig) *PreLoadClient {
	defer log.LogFlush()
	c := &PreLoadClient{
		vol: config.Volume,
	}

	if config.LogDir != "" {
		log.InitLog(config.LogDir, "preload", convertLogLevel(config.LogLevel), nil)
		stat.NewStatistic(config.LogDir, "preload", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
	}

	if config.ProfPort != "" {
		go func() {
			http.HandleFunc(log.SetLogLevelPath, log.SetLogLevel)
			e := http.ListenAndServe(fmt.Sprintf(":%v", config.ProfPort), nil)
			if e != nil {
				log.LogWarnf("newClient newEBSClient cannot listen pprof (%v)", config.ProfPort)
			}
		}()
	}

	var (
		mw  *meta.MetaWrapper
		ec  *stream.ExtentClient
		err error
	)
	c.mc = masterSDK.NewMasterClient(config.Masters, false)
	if err = c.newEBSClient(config.Masters, config.LogDir); err != nil {
		log.LogErrorf("newClient newEBSClient failed(%v)", err)
		return nil
	}

	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        config.Volume,
		Masters:       config.Masters,
		ValidateOwner: false,
	}); err != nil {
		log.LogErrorf("newClient NewMetaWrapper failed(%v)", err)
		return nil
	}

	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:            config.Volume,
		Masters:           config.Masters,
		Preload:           true,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		VolumeType:        proto.VolumeTypeCold,
	}); err != nil {
		log.LogErrorf("newClient NewExtentClient failed(%v)", err)
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

	log.LogDebugf("Client is created:(%v)", c)
	return c
}

func convertLogLevel(level string) log.Level {
	switch level {
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	case "warn":
		return log.WarnLevel
	case "error":
		return log.ErrorLevel
	case "fatal":
		return log.FatalLevel
	case "critical":
		return log.CriticalLevel
	case "read":
		return log.ReadLevel
	case "update":
		return log.UpdateLevel
	default:
		return log.DebugLevel
	}
}

func (c *PreLoadClient) newEBSClient(masters []string, logDir string) (err error) {
	var (
		volumeInfo  *proto.SimpleVolView
		clusterInfo *proto.ClusterInfo
		ebsc        *blobstore.BlobStoreClient
	)
	volumeInfo, err = c.mc.AdminAPI().GetVolumeSimpleInfo(c.vol)
	if err != nil {
		return
	}
	c.ebsBlockSize = volumeInfo.ObjBlockSize
	c.cacheAction = volumeInfo.CacheAction
	c.cacheThreshold = volumeInfo.CacheThreshold

	clusterInfo, err = c.mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return
	}
	ebsEndpoint := clusterInfo.EbsAddr

	if ebsc, err = blobstore.NewEbsClient(access.Config{
		ConnMode: access.NoLimitConnMode,
		Consul: api.Config{
			Address: ebsEndpoint,
		},
		MaxSizePutOnce: int64(c.ebsBlockSize),
		Logger: &access.Logger{
			Filename: gopath.Join(logDir, "ebs/ebs.log"),
		},
	}); err != nil {
		log.LogErrorf("newClient newEBSClient failed(%v)", err)
		return
	}
	c.ebsc = ebsc
	return
}

func (c *PreLoadClient) ctx(cid int64, ino uint64) context.Context {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", fmt.Sprintf("cid=%v,ino=%v", cid, ino))
	return ctx
}

func (c *PreLoadClient) walker(dir string, wg *sync.WaitGroup, currentGoroutineNum *int64, ch chan<- fileInfo, newGoroutine bool) (err error) {
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

	ino, err = c.mw.LookupPath(gopath.Clean(dir))
	if err != nil {
		log.LogErrorf("LookupPath path(%v) faild(%v)", dir, err)
		return err
	}
	info, err = c.mw.InodeGet_ll(ino)

	if err != nil {
		log.LogErrorf("InodeGet_ll path(%v) faild(%v)", dir, err)
		return err
	}

	if proto.IsRegular(info.Mode) {
		fInfo := fileInfo{ino: info.Inode, name: gopath.Base(dir)}
		log.LogDebugf("Target is a file:(%v)", fInfo)
		ch <- fInfo
		return err
	}

	children, err := c.mw.ReadDir_ll(info.Inode)

	if err != nil {
		log.LogErrorf("ReadDir_ll path(%v) faild(%v)", dir, err)
		return err
	}

	for _, child := range children {
		if proto.IsDir(child.Type) {
			log.LogDebugf("preloadTravrseDir:(%v) is dir", path.Join(dir, child.Name))
			if atomic.LoadInt64(currentGoroutineNum) < c.limitParam.PreloadFileConcurrency {
				wg.Add(1)
				atomic.AddInt64(currentGoroutineNum, 1)
				go c.walker(path.Join(dir, child.Name), wg, currentGoroutineNum, ch, true)
			} else {
				c.walker(path.Join(dir, child.Name), wg, currentGoroutineNum, ch, false)
			}

		} else if proto.IsRegular(child.Type) {
			fInfo := fileInfo{ino: child.Inode, name: child.Name}
			log.LogDebugf("preloadTravrseDir send:(%v)", fInfo)
			ch <- fInfo
		}
	}
	return nil
}

func (c *PreLoadClient) allocatePreloadDPWorker(ch <-chan fileInfo) uint64 {
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
		infos := c.mw.BatchInodeGet(inodes)
		for _, info := range infos {
			if int64(info.Size) <= c.limitParam.PreloadFileSizeLimit {
				total += info.Size
				cachefino := cache[info.Inode]
				cachefino.size = info.Size
				//append
				c.fileCache = append(c.fileCache, cachefino)
				log.LogDebugf("allocatePreloadDPWorker append:%v", cachefino)
			}
		}
		inodes = inodes[:0]
		for key := range cache {
			delete(cache, key)
		}
	}
	//flush cache
	if len(inodes) != 0 {
		infos := c.mw.BatchInodeGet(inodes)

		for _, info := range infos {
			if int64(info.Size) <= c.limitParam.PreloadFileSizeLimit {
				total += info.Size
				cachefino := cache[info.Inode]
				cachefino.size = info.Size
				//append
				c.fileCache = append(c.fileCache, cachefino)
				log.LogDebugf("allocatePreloadDPWorker append#2:%v", cachefino)
			}
		}
	}
	return total
}

func (c *PreLoadClient) allocatePreloadDP(target string, count int, ttl uint64, zones string) (err error) {
	log.LogDebugf("allocatePreloadDP enter")
	ch := make(chan fileInfo, 100)
	var (
		wg                  sync.WaitGroup
		currentGoroutineNum int64 = 0
	)
	wg.Add(1)
	atomic.AddInt64(&currentGoroutineNum, 1)
	go c.walker(target, &wg, &currentGoroutineNum, ch, true)
	go func() {
		wg.Wait()
		close(ch)
	}()
	need := c.allocatePreloadDPWorker(ch)
	if need == 0 {
		return errors.New("No file would be preloaded")
	}
	//#test1
	if (need % (1024 * 1024 * 1024)) == 0 {
		need = uint64(need / (1024 * 1024 * 1024))
	} else {
		need = uint64(need/(1024*1024*1024)) + 1
	}
	log.LogDebugf("preloadFile:need total space %v GB, ttl %v second, zones=%v", need, ttl, zones)
	total, used := c.getVolumeCacheCapacity()

	if float64(need+used) > float64(total) {
		log.LogErrorf("AllocatePreLoadDataPartition failed: need space (%v) GB, volume total(%v)GB used(%v)GB", need, total, used)
		return errors.New(fmt.Sprintf("AllocatePreLoadDataPartition failed: need space (%v) GB, volume total(%v)GB used(%v)GB", need, total, used))
	}
	err = c.ec.AllocatePreLoadDataPartition(c.vol, count, need, ttl, zones)

	if err != nil {
		log.LogErrorf("AllocatePreLoadDataPartition failed:%v", err)
		return err
	}
	log.LogDebugf("allocatePreloadDP leave")
	return nil
}

func (c *PreLoadClient) clearPreloadDPWorker(ch <-chan fileInfo) {
	var routineNum int64 = 0
	var wg sync.WaitGroup

	for fInfo := range ch {
		if atomic.LoadInt64(&routineNum) < c.limitParam.ClearFileConcurrency {
			wg.Add(1)
			go func(f fileInfo) {
				defer func() {
					atomic.AddInt64(&routineNum, -1)
					wg.Done()
				}()
				log.LogDebugf("clearPreloadDPWorker:clear file %v", f.name)
				c.mw.InodeClearPreloadCache_ll(f.ino)
			}(fInfo)
			atomic.AddInt64(&routineNum, 1)
		} else {
			log.LogDebugf("clearPreloadDPWorker:clear file %v", fInfo.name)
			c.mw.InodeClearPreloadCache_ll(fInfo.ino)
		}
	}
	wg.Wait()
}

func (c *PreLoadClient) ClearPreloadDP(target string) (err error) {
	ch := make(chan fileInfo, 100)
	var (
		wg                  sync.WaitGroup
		currentGoroutineNum int64 = 0
	)
	wg.Add(1)
	atomic.AddInt64(&currentGoroutineNum, 1)
	go c.walker(target, &wg, &currentGoroutineNum, ch, true)
	go func() {
		wg.Wait()
		close(ch)
	}()
	c.clearPreloadDPWorker(ch)

	return nil
}

func (c *PreLoadClient) preloadFileWorker(id int64, jobs <-chan fileInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	var total int64 = 0
	var succeed int64 = 0
	noWritableDP := false
	for job := range jobs {
		if noWritableDP == true {
			log.LogWarnf("no writable dp,ingnore (%v) to cbfs", job.name)
			continue //consume the job
		}
		total += 1
		log.LogDebugf("worker %v ready to preload(%v)", id, job.name)
		ino := job.ino
		//#1 open
		c.ec.OpenStream(ino)
		//#2 write
		var (
			objExtents []proto.ObjExtentKey
			err        error
		)
		if _, _, _, objExtents, err = c.mw.GetObjExtents(ino); err != nil {
			log.LogWarnf("GetObjExtents (%v) faild(%v)", job.name, err)
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
			var buf = make([]byte, size)
			var n int
			n, err = fileReader.Read(c.ctx(0, ino), buf, int(objExtent.FileOffset), int(size))

			if err != nil {
				subErr = true
				log.LogWarnf("Read (%v) from ebs failed (%v)", objExtent, err)
				continue
			}

			if uint64(n) != size {
				log.LogWarnf("Read (%v) wrong size:(%v)", objExtent, n)
				continue
			}
			_, err = c.ec.Write(ino, int(objExtent.FileOffset), buf, 0)
			//in preload mode,onece extend_hander set to error, streamer is set to error
			// so write should failed immediately
			if err != nil {
				subErr = true
				log.LogWarnf("preload (%v) to cbfs failed (%v)", job.name, err)
				if err = c.ec.GetDataPartitionForWrite(); err != nil {
					log.LogErrorf("worker %v end for %v", id, err)
					noWritableDP = true
				}
				break
			}
		}
		c.ec.CloseStream(ino)
		if subErr == false {
			log.LogInfof("worker %v preload (%v) to cbfs success", id, job.name)
			succeed += 1
		}
	}
	atomic.AddInt64(&c.preloadFileNumTotal, total)
	atomic.AddInt64(&c.preloadFileNumSucceed, succeed)
	log.LogInfof("worker %v end:total %v, succeed %v", id, total, succeed)
}

func (c *PreLoadClient) preloadFile() error {
	log.LogDebug("preloadFile enter")
	var (
		wg sync.WaitGroup
		w  int64
	)
	jobs := make(chan fileInfo, 100)

	for w = 1; w <= c.limitParam.PreloadFileConcurrency; w++ {
		wg.Add(1)
		go c.preloadFileWorker(w, jobs, &wg)
	}

	for _, fileInfo := range c.fileCache {
		jobs <- fileInfo
	}
	close(jobs)
	wg.Wait()
	log.LogInfof("preloadFile end:total %v, succeed %v", c.preloadFileNumSucceed, c.preloadFileNumTotal)
	if c.preloadFileNumTotal == c.preloadFileNumSucceed {
		return nil
	} else if c.preloadFileNumSucceed < c.preloadFileNumTotal {
		return errors.New("Preload partitionly succeed")
	} else {
		return errors.New("Preload failed")
	}

}

func (c *PreLoadClient) CheckColdVolume() bool {
	var (
		err  error
		view *proto.SimpleVolView
	)

	if view, err = c.mc.AdminAPI().GetVolumeSimpleInfo(c.vol); err != nil {
		log.LogErrorf("getSimpleVolView: get volume simple info fail: volume(%v) err(%v)", c.vol, err)
		return false
	}
	return view.VolType == proto.VolumeTypeCold
}

func (c *PreLoadClient) getVolumeCacheCapacity() (total uint64, used uint64) {
	var (
		err  error
		view *proto.SimpleVolView
	)

	if view, err = c.mc.AdminAPI().GetVolumeSimpleInfo(c.vol); err != nil {
		log.LogErrorf("getSimpleVolView: get volume simple info fail: volume(%v) err(%v)", c.vol, err)
		return 0, 0
	}
	return view.CacheCapacity, view.PreloadCapacity
}

func (c *PreLoadClient) PreloadDir(target string, count int, ttl uint64, zones string) (err error) {
	log.LogDebugf("PreloadDir (%v)", target)
	if err = c.allocatePreloadDP(target, count, ttl, zones); err != nil {
		log.LogErrorf("PreloadDir failed(%v)", err)
		return
	}
	log.LogDebugf("Wait 100s for preload dp get ready")
	time.Sleep(time.Duration(100) * time.Second)
	log.LogDebugf("Sleep end")
	//Step3.2  preload the file
	return c.preloadFile()

}
