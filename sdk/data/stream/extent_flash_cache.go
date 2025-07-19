// Copyright 2018 The CubeFS Authors.
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
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/remotecache"
	"github.com/cubefs/cubefs/util/bloom"
	"github.com/cubefs/cubefs/util/log"
)

const (
	BloomBits          = 10 * 1024 * 1024 * 8
	BloomHashNum       = 7
	cachePathSeparator = ","
)

type ZoneRankType int

type CacheConfig struct {
	Cluster string
	Volume  string
	Masters []string
	MW      *meta.MetaWrapper

	SameZoneWeight int
}

type RemoteCache struct {
	cluster     string
	volname     string
	stopOnce    sync.Once
	metaWrapper *meta.MetaWrapper
	cacheBloom  *bloom.BloomFilter
	lock        sync.Mutex

	Started       bool
	VolumeEnabled bool
	Path          string
	AutoPrepare   bool
	PrepareCh     chan *PrepareRemoteCacheRequest

	remoteCacheMaxFileSizeGB int64
	remoteCacheOnlyForNotSSD bool
	remoteCacheClient        *remotecache.RemoteCacheClient
	WarmUpMetaPaths          sync.Map
	WarmPathWorked           int32
}

type AddressPingStats struct {
	sync.Mutex
	durations []time.Duration
	index     int
}

func (as *AddressPingStats) Add(duration time.Duration) {
	as.Lock()
	defer as.Unlock()
	if as.index < 5 {
		as.durations = append(as.durations, duration)
	} else {
		as.durations[as.index%5] = duration
	}
	as.index++
}

func (as *AddressPingStats) Average() time.Duration {
	as.Lock()
	defer as.Unlock()
	if len(as.durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range as.durations {
		total += d
	}
	return total / time.Duration(len(as.durations))
}

func (rc *RemoteCache) UpdateRemoteCacheConfig(client *ExtentClient, view *proto.SimpleVolView) {
	// cannot set vol's RemoteCacheReadTimeoutSec <= 0
	if view.RemoteCacheReadTimeout <= 0 {
		view.RemoteCacheReadTimeout = proto.DefaultRemoteCacheClientReadTimeout
	}
	if view.RemoteCacheSameZoneTimeout <= 0 {
		view.RemoteCacheSameZoneTimeout = proto.DefaultRemoteCacheSameZoneTimeout
	}
	if view.RemoteCacheSameRegionTimeout <= 0 {
		view.RemoteCacheSameRegionTimeout = proto.DefaultRemoteCacheSameRegionTimeout
	}
	if rc.VolumeEnabled != view.RemoteCacheEnable {
		log.LogInfof("RcVolumeEnabled: %v -> %v", rc.VolumeEnabled, view.RemoteCacheEnable)
		rc.VolumeEnabled = view.RemoteCacheEnable
	}
	if rc.Path != view.RemoteCachePath {
		oldPath := client.RemoteCache.Path
		rc.Path = view.RemoteCachePath
		log.LogInfof("RcPath: %v -> %v, but(%v)", oldPath, view.RemoteCachePath, rc.Path)
	}
	if rc.AutoPrepare != view.RemoteCacheAutoPrepare {
		log.LogInfof("RcAutoPrepare: %v -> %v", rc.AutoPrepare, view.RemoteCacheAutoPrepare)
		rc.AutoPrepare = view.RemoteCacheAutoPrepare
	}
	if rc.remoteCacheMaxFileSizeGB != view.RemoteCacheMaxFileSizeGB {
		log.LogInfof("RcMaxFileSizeGB: %d(GB) -> %d(GB)", rc.remoteCacheMaxFileSizeGB, view.RemoteCacheMaxFileSizeGB)
		rc.remoteCacheMaxFileSizeGB = view.RemoteCacheMaxFileSizeGB
	}
	if rc.remoteCacheOnlyForNotSSD != view.RemoteCacheOnlyForNotSSD {
		log.LogInfof("RcOnlyForNotSSD: %v -> %v", rc.remoteCacheOnlyForNotSSD, view.RemoteCacheOnlyForNotSSD)
		rc.remoteCacheOnlyForNotSSD = view.RemoteCacheOnlyForNotSSD
	}
	if rc.remoteCacheClient == nil {
		return
	}
	// check if RemoteCache.ClusterEnabled is set to true after it has been set to false last time
	if !client.RemoteCache.remoteCacheClient.IsClusterEnable() {
		if err := client.RemoteCache.remoteCacheClient.UpdateClusterEnable(); err != nil {
			log.LogWarnf("UpdateClusterEnable: err(%v)", err)
			return
		}
	}

	// RemoteCache may be nil if the first initialization failed, it will not be set nil anymore even if remote cache is disabled
	if client.IsRemoteCacheEnabled() {
		if !rc.Started {
			log.LogInfof("UpdateRemoteCacheConfig: initRemoteCache")
			if err := rc.Init(client); err != nil {
				log.LogErrorf("updateRemoteCacheConfig: initRemoteCache failed, err: %v", err)
				return
			}
		}
	} else if rc.Started {
		client.RemoteCache.Stop()
		log.LogInfo("stop RemoteCache")
	}

	if rc.remoteCacheClient.TTL != view.RemoteCacheTTL {
		log.LogInfof("RcTTL: %d -> %d", rc.remoteCacheClient.TTL, view.RemoteCacheTTL)
		rc.remoteCacheClient.TTL = view.RemoteCacheTTL
	}
	if rc.remoteCacheClient.ReadTimeout != view.RemoteCacheReadTimeout {
		log.LogInfof("RcReadTimeoutSec: %d(ms) -> %d(ms)", rc.remoteCacheClient.ReadTimeout, view.RemoteCacheReadTimeout)
		rc.remoteCacheClient.ReadTimeout = view.RemoteCacheReadTimeout
	}
	if rc.remoteCacheClient.RemoteCacheMultiRead != view.RemoteCacheMultiRead {
		log.LogInfof("RcFollowerRead: %v -> %v", rc.remoteCacheClient.RemoteCacheMultiRead, view.RemoteCacheMultiRead)
		rc.remoteCacheClient.RemoteCacheMultiRead = view.RemoteCacheMultiRead
	}

	if rc.remoteCacheClient.FlashNodeTimeoutCount != int32(view.FlashNodeTimeoutCount) {
		log.LogInfof("RcFlashNodeTimeoutCount: %d -> %d", rc.remoteCacheClient.FlashNodeTimeoutCount, int32(view.FlashNodeTimeoutCount))
		rc.remoteCacheClient.FlashNodeTimeoutCount = int32(view.FlashNodeTimeoutCount)
	}
	if rc.remoteCacheClient.SameZoneTimeout != view.RemoteCacheSameZoneTimeout {
		log.LogInfof("RcSameZoneTimeout: %d -> %d", rc.remoteCacheClient.SameZoneTimeout, view.RemoteCacheSameZoneTimeout)
		rc.remoteCacheClient.SameZoneTimeout = view.RemoteCacheSameZoneTimeout
	}
	if rc.remoteCacheClient.SameRegionTimeout != view.RemoteCacheSameRegionTimeout {
		log.LogInfof("RcSameRegionTimeout: %d -> %d", rc.remoteCacheClient.SameRegionTimeout, view.RemoteCacheSameRegionTimeout)
		rc.remoteCacheClient.SameRegionTimeout = view.RemoteCacheSameRegionTimeout
	}
}

func (rc *RemoteCache) DoRemoteCachePrepare(c *ExtentClient) {
	defer c.wg.Done()
	workerWg := sync.WaitGroup{}
	for range [5]struct{}{} {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for {
				select {
				case <-c.stopCh:
					return
				case req := <-c.RemoteCache.PrepareCh:
					c.servePrepareRequest(req)
				}
			}
		}()
	}
	workerWg.Wait()
}

func (rc *RemoteCache) Init(client *ExtentClient) (err error) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if rc.Started {
		log.LogInfof("RemoteCache already started")
		return
	}

	log.LogDebugf("RemoteCache: Init")
	rc.cluster = client.dataWrapper.ClusterName
	rc.volname = client.extentConfig.Volume
	rc.metaWrapper = client.metaWrapper
	cfg := &remotecache.ClientConfig{
		Masters:            client.extentConfig.Masters,
		BlockSize:          proto.CACHE_BLOCK_SIZE,
		NeedInitLog:        false,
		LogLevelStr:        "",
		LogDir:             "",
		ConnectTimeout:     500,
		FirstPacketTimeout: 1000,
		FromFuse:           true,
	}
	rc.remoteCacheClient, err = remotecache.NewRemoteCacheClient(cfg)
	if err != nil {
		log.LogWarnf("RemoteCache: new client err %v", err)
	}
	rc.remoteCacheClient.UpdateWarmPath = rc.UpdateWarmPath
	err = rc.remoteCacheClient.UpdateFlashGroups()
	if err != nil {
		log.LogWarnf("RemoteCache: update flashgroups err %v", err)
	}
	rc.cacheBloom = bloom.New(BloomBits, BloomHashNum)
	rc.PrepareCh = make(chan *PrepareRemoteCacheRequest, 1024)
	client.wg.Add(1)
	go rc.DoRemoteCachePrepare(client)
	rc.Started = true
	log.LogDebugf("Init: NewRemoteCache sucess")
	return
}

func (rc *RemoteCache) Stop() {
	rc.stopOnce.Do(func() {
		rc.Started = false
		rc.remoteCacheClient.Stop()
	})
}

func (rc *RemoteCache) GetRemoteCacheBloom() *bloom.BloomFilter {
	log.LogDebugf("GetRemoteCacheBloom. cacheBloom %v", rc.cacheBloom)
	return rc.cacheBloom
}

func (rc *RemoteCache) ResetPathToBloom(cachePath string) bool {
	if cachePath == "" {
		cachePath = "/"
	}
	res := true
	rc.cacheBloom.ClearAll()
	for _, path := range strings.Split(cachePath, cachePathSeparator) {
		path = strings.TrimSpace(path)
		if len(path) == 0 {
			continue
		}
		if ino, err := rc.getPathInode(path); err != nil {
			log.LogWarnf("RemoteCache: lookup cachePath %s err: %v", path, err)
			res = false
			continue
		} else {
			n := make([]byte, 8)
			binary.BigEndian.PutUint64(n, ino)
			rc.cacheBloom.Add(n)
			log.LogDebugf("RemoteCache: add path %s, inode %d to bloomFilter", path, ino)
		}
	}
	return res
}

func (rc *RemoteCache) getPathInode(path string) (ino uint64, err error) {
	ino = proto.RootIno
	if path == "/" {
		return ino, nil
	}
	if path != "" && path != "/" {
		dirs := strings.Split(path, "/")
		var childIno uint64
		for _, dir := range dirs {
			if dir == "/" || dir == "" {
				continue
			}
			childIno, _, err = rc.metaWrapper.Lookup_ll(ino, dir)
			if err != nil {
				ino = 0
				return
			}
			ino = childIno
		}
	}
	return
}

func hasPathIntersection(dir1, dir2 string) (bool, string) {
	sep := "/"
	if dir1 == "" {
		dir1 = sep
	}
	if dir2 == "" {
		dir2 = sep
	}
	if !strings.HasPrefix(dir1, sep) {
		dir1 = sep + dir1
	}
	if !strings.HasPrefix(dir2, sep) {
		dir2 = sep + dir2
	}
	if !strings.HasSuffix(dir1, sep) {
		dir1 += sep
	}
	if !strings.HasSuffix(dir2, sep) {
		dir2 += sep
	}
	if strings.HasPrefix(dir1, dir2) {
		return true, dir1[:len(dir1)-1]
	}
	if strings.HasPrefix(dir2, dir1) {
		return true, dir2[:len(dir2)-1]
	}
	return false, ""
}

func (rc *RemoteCache) ApplyWarmupMetaToken(flashNodeAddr string, clientId string, requestType uint8) (bool, error) {
	if rc.remoteCacheClient == nil || !rc.Started {
		return false, errors.New("remote cache client is nil or not started")
	} else {
		return rc.remoteCacheClient.ApplyWarmupMetaToken(flashNodeAddr, clientId, requestType)
	}
}

func (rc *RemoteCache) UpdateWarmPath(data []byte, addr string) {
	warmUpPaths, err := proto.UnmarshalBinaryWPSlice(data)
	if err != nil {
		log.LogWarnf("UpdateWarmPath UnmarshalBinaryWPSlice err(%v)", err)
		return
	}
	for _, warmUpPath := range warmUpPaths {
		if rc.volname != warmUpPath.VolName {
			continue
		}
		inst, path := hasPathIntersection(warmUpPath.DirPath, rc.metaWrapper.GetSubDir())
		if !inst {
			continue
		}
		if rc.isPathAlreadyCovered(path) {
			log.LogDebugf("UpdateWarmPath: path %s is already covered by existing warmup paths", path)
			continue
		}
		warmUpPath.DirPath = path
		warmUpPath.Status = proto.WarmStatusInitializing
		warmUpPath.FlashAddr = addr
		if actual, loaded := rc.WarmUpMetaPaths.LoadOrStore(path, warmUpPath); loaded {
			info := actual.(*proto.WarmUpPathInfo)
			if info.Status == proto.WarmStatusCompleted || info.Status == proto.WarmStatusFailed {
				if time.Now().Add(-5*time.Minute).UnixNano() > info.Expiration {
					rc.WarmUpMetaPaths.Delete(path)
				}
			}
		}
	}
}

func (rc *RemoteCache) isPathAlreadyCovered(newPath string) bool {
	var isCovered bool
	rc.WarmUpMetaPaths.Range(func(_, value interface{}) bool {
		warmUpPath := value.(*proto.WarmUpPathInfo)
		existingPath := warmUpPath.DirPath
		if warmUpPath.Status == proto.WarmStatusCompleted || warmUpPath.Status == proto.WarmStatusFailed || warmUpPath.Status == proto.WarmStatusRunning {
			return true
		}
		if strings.HasPrefix(newPath, existingPath) {
			log.LogDebugf("isPathAlreadyCovered: new path %s is covered by existing path %s", newPath, existingPath)
			isCovered = true
			return false
		}
		if strings.HasPrefix(existingPath, newPath) {
			log.LogDebugf("isPathAlreadyCovered: existing path %s is covered by new path %s, will replace", existingPath, newPath)
			rc.WarmUpMetaPaths.Delete(existingPath)
		}
		return true
	})
	return isCovered
}
