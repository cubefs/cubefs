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
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/bloom"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	BloomBits    = 10 * 1024 * 1024 * 8
	BloomHashNum = 7

	cachePathSeparator = ","

	pingCount           = 3
	pingTimeout         = 50 * time.Millisecond
	_connIdelTimeout    = 30 // 30 second
	_connReadTimeoutSec = 3

	RefreshFlashNodesInterval  = time.Minute
	RefreshHostLatencyInterval = 15 * time.Minute

	sameZoneTimeout   = 400 * time.Microsecond
	SameRegionTimeout = 2 * time.Millisecond
	sameZoneWeight    = 70
	InvalidCachePath  = ""
)

type ZoneRankType int

const (
	SameZoneRank ZoneRankType = iota
	SameRegionRank
	CrossRegionRank
	UnknownZoneRank
)

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
	mc          *master.MasterClient
	conns       *util.ConnectPool
	hostLatency sync.Map
	flashGroups *btree.BTree
	stopOnce    sync.Once
	stopC       chan struct{}
	wg          sync.WaitGroup
	metaWrapper *meta.MetaWrapper
	cacheBloom  *bloom.BloomFilter

	readTimeoutSec int
	Started        bool
	ClusterEnabled bool
	VolumeEnabled  bool
	Path           string
	AutoPrepare    bool
	TTL            int64
	ReadTimeoutSec int64
	PrepareCh      chan *PrepareRemoteCacheRequest

	clusterEnable func(bool)
	lock          sync.Mutex

	remoteCacheMaxFileSizeGB int64
	remoteCacheOnlyForNotSSD bool
	remoteCacheFollowerRead  bool
}

func (rc *RemoteCache) UpdateRemoteCacheConfig(client *ExtentClient, view *proto.SimpleVolView) {
	if rc.VolumeEnabled != view.RemoteCacheEnable {
		log.LogInfof("RcVolumeEnabled: %v -> %v", rc.VolumeEnabled, view.RemoteCacheEnable)
		rc.VolumeEnabled = view.RemoteCacheEnable
	}

	// check if RemoteCache.ClusterEnabled is set to true after it has been set to false last time
	if !client.RemoteCache.ClusterEnabled {
		if fgv, err := rc.mc.AdminAPI().ClientFlashGroups(); err != nil {
			log.LogWarnf("updateFlashGroups: err(%v)", err)
			return
		} else {
			rc.clusterEnable(fgv.Enable)
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

	if rc.Path != view.RemoteCachePath {
		oldPath := client.RemoteCache.Path
		rc.Path = view.RemoteCachePath
		//if client.IsRemoteCacheEnabled() {
		//	if !rc.ResetPathToBloom(view.RemoteCachePath) {
		//		rc.Path = InvalidCachePath
		//	}
		//}
		log.LogInfof("RcPath: %v -> %v, but(%v)", oldPath, view.RemoteCachePath, rc.Path)
	}

	if rc.AutoPrepare != view.RemoteCacheAutoPrepare {
		log.LogInfof("RcAutoPrepare: %v -> %v", rc.AutoPrepare, view.RemoteCacheAutoPrepare)
		rc.AutoPrepare = view.RemoteCacheAutoPrepare
	}
	if rc.TTL != view.RemoteCacheTTL {
		log.LogInfof("RcTTL: %d -> %d", rc.TTL, view.RemoteCacheTTL)
		rc.TTL = view.RemoteCacheTTL
	}
	if rc.ReadTimeoutSec != view.RemoteCacheReadTimeoutSec {
		log.LogInfof("RcReadTimeoutSec: %d -> %d", rc.ReadTimeoutSec, view.RemoteCacheReadTimeoutSec)
		rc.ReadTimeoutSec = view.RemoteCacheReadTimeoutSec
	}

	if rc.remoteCacheMaxFileSizeGB != view.RemoteCacheMaxFileSizeGB {
		log.LogInfof("RcMaxFileSizeGB: %d(GB) -> %d(GB)", rc.remoteCacheMaxFileSizeGB, view.RemoteCacheMaxFileSizeGB)
		rc.remoteCacheMaxFileSizeGB = view.RemoteCacheMaxFileSizeGB
	}

	if rc.remoteCacheOnlyForNotSSD != view.RemoteCacheOnlyForNotSSD {
		log.LogInfof("RcOnlyForNotSSD: %v -> %v", rc.remoteCacheOnlyForNotSSD, view.RemoteCacheOnlyForNotSSD)
		rc.remoteCacheOnlyForNotSSD = view.RemoteCacheOnlyForNotSSD
	}

	if rc.remoteCacheFollowerRead != view.RemoteCacheFollowerRead {
		log.LogInfof("RcFollowerRead: %v -> %v", rc.remoteCacheFollowerRead, view.RemoteCacheFollowerRead)
		rc.remoteCacheFollowerRead = view.RemoteCacheFollowerRead
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
	fmt.Println("RemoteCache: Init")
	rc.stopC = make(chan struct{})
	rc.cluster = client.dataWrapper.ClusterName
	rc.volname = client.extentConfig.Volume
	rc.metaWrapper = client.metaWrapper
	rc.flashGroups = btree.New(32)
	rc.mc = master.NewMasterClient(client.extentConfig.Masters, false)

	rc.readTimeoutSec = int(client.RemoteCache.ReadTimeoutSec)
	if rc.readTimeoutSec <= 0 {
		rc.readTimeoutSec = _connReadTimeoutSec
	}

	rc.clusterEnable = client.enableRemoteCacheCluster
	err = rc.updateFlashGroups()
	if err != nil {
		log.LogDebugf("RemoteCache: Init err %v", err)
		return
	}
	rc.conns = util.NewConnectPoolWithTimeoutAndCap(5, 500, _connIdelTimeout, int64(rc.readTimeoutSec))
	rc.cacheBloom = bloom.New(BloomBits, BloomHashNum)
	rc.wg.Add(1)
	go rc.refresh()

	//if !rc.ResetPathToBloom(client.RemoteCache.Path) {
	//	rc.Path = InvalidCachePath
	//}
	rc.PrepareCh = make(chan *PrepareRemoteCacheRequest, 1024)
	client.wg.Add(1)
	go rc.DoRemoteCachePrepare(client)
	rc.Started = true

	log.LogDebugf("Init: NewRemoteCache sucess")
	return
}

func (rc *RemoteCache) Read(ctx context.Context, fg *FlashGroup, inode uint64, req *CacheReadRequest) (read int, err error) {
	var (
		conn      *net.TCPConn
		moved     bool
		addr      string
		reqPacket *Packet
	)

	for {
		addr = fg.getFlashHost()
		if addr == "" {
			err = fmt.Errorf("getFlashHost failed: cannot find any available host")
			log.LogWarnf("FlashGroup read failed: fg(%v) err(%v)", fg, err)
			return
		}
		reqPacket = NewFlashCachePacket(inode, proto.OpFlashNodeCacheRead)
		if err = reqPacket.MarshalDataPb(&req.CacheReadRequest); err != nil {
			log.LogWarnf("FlashGroup Read: failed to MarshalData (%+v). err(%v)", req, err)
			return
		}
		if conn, err = rc.conns.GetConnect(addr); err != nil {
			log.LogWarnf("FlashGroup read: get connection failed, addr(%v) reqPacket(%v) err(%v) remoteCacheFollowerRead(%v)", addr, req, err, rc.remoteCacheFollowerRead)
			moved = fg.moveToUnknownRank(addr, err)
			if rc.remoteCacheFollowerRead {
				log.LogInfof("Retrying due to GetConnect of addr(%v) failure err(%v)", addr, err)
				continue
			}
			return
		}

		if err = reqPacket.WriteToConn(conn); err != nil {
			log.LogWarnf("FlashGroup Read: failed to write to addr(%v) err(%v) remoteCacheFollowerRead(%v)", addr, err, rc.remoteCacheFollowerRead)
			rc.conns.PutConnect(conn, err != nil)
			moved = fg.moveToUnknownRank(addr, err)
			if rc.remoteCacheFollowerRead {
				log.LogInfof("Retrying due to write to addr(%v) failure err(%v)", addr, err)
				continue
			}
			return
		}
		if read, err = rc.getReadReply(conn, reqPacket, req); err != nil {
			log.LogWarnf("FlashGroup Read: getReadReply from addr(%v) err(%v) remoteCacheFollowerRead(%v)", addr, err, rc.remoteCacheFollowerRead)
			rc.conns.PutConnect(conn, err != nil)
			moved = fg.moveToUnknownRank(addr, err)
			if rc.remoteCacheFollowerRead {
				log.LogInfof("Retrying due to getReadReply from addr(%v) failure  err(%v)", addr, err)
				continue
			}
		}
		break
	}
	rc.conns.PutConnect(conn, err != nil)

	log.LogDebugf("FlashGroup Read: flashGroup(%v) addr(%v) CacheReadRequest(%v) reqPacket(%v) err(%v) moved(%v) remoteCacheFollowerRead(%v)", fg, addr, req, reqPacket, err, moved, rc.remoteCacheFollowerRead)
	return
}

func (rc *RemoteCache) getReadReply(conn *net.TCPConn, reqPacket *Packet, req *CacheReadRequest) (readBytes int, err error) {
	for readBytes < int(req.Size_) {
		replyPacket := NewFlashCacheReply()
		err = replyPacket.ReadFromConn(conn, rc.readTimeoutSec)
		if err != nil {
			log.LogWarnf("getReadReply: failed to read from connect, req(%v) readBytes(%v) err(%v)", reqPacket, readBytes, err)
			return
		}
		if replyPacket.ResultCode != proto.OpOk {
			err = fmt.Errorf("ResultCode NOK (%v)", replyPacket.ResultCode)
			log.LogWarnf("getReadReply: ResultCode NOK, req(%v) reply(%v) ResultCode(%v)", reqPacket, replyPacket, replyPacket.ResultCode)
			return
		}
		expectCrc := crc32.ChecksumIEEE(replyPacket.Data[:replyPacket.Size])
		if replyPacket.CRC != expectCrc {
			err = fmt.Errorf("inconsistent CRC, expect(%v) reply(%v)", expectCrc, replyPacket.CRC)
			log.LogWarnf("getReadReply: req(%v) err(%v)", req, err)
			return
		}
		copy(req.Data[readBytes:], replyPacket.Data)
		readBytes += int(replyPacket.Size)
	}
	return
}

func (rc *RemoteCache) Prepare(ctx context.Context, fg *FlashGroup, inode uint64, req *proto.CachePrepareRequest) (err error) {
	var (
		conn  *net.TCPConn
		moved bool
	)
	addr := fg.getFlashHost()
	if addr == "" {
		err = fmt.Errorf("getFlashHost failed: can not find host")
		log.LogWarnf("FlashGroup prepare failed: err(%v)", err)
		return
	}
	reqPacket := NewFlashCachePacket(inode, proto.OpFlashNodeCachePrepare)
	if err = reqPacket.MarshalDataPb(req); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to MarshalData (%v), err(%v)", req, err)
		return
	}
	defer func() {
		if err != nil {
			moved = fg.moveToUnknownRank(addr, err)
		}
	}()
	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("FlashGroup prepare: get connection to curr addr failed, addr(%v) reqPacket(%v) err(%v)", addr, req, err)
		return
	}
	defer func() {
		rc.conns.PutConnect(conn, err != nil)
	}()

	if err = reqPacket.WriteToConn(conn); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to write to addr(%v) err(%v)", addr, err)
		return
	}

	replyPacket := NewFlashCacheReply()
	if err = replyPacket.ReadFromConn(conn, rc.readTimeoutSec); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to ReadFromConn, replyPacket(%v), fg host(%v) moved(%v), err(%v)", replyPacket, addr, moved, err)
		return
	}
	if replyPacket.ResultCode != proto.OpOk {
		log.LogWarnf("FlashGroup Prepare: ResultCode NOK, replyPacket(%v), fg host(%v), ResultCode(%v)", replyPacket, addr, replyPacket.ResultCode)
		err = fmt.Errorf("ResultCode NOK (%v)", replyPacket.ResultCode)
		return
	}
	log.LogDebugf("FlashGroup Prepare successful: flashGroup(%v) addr(%v) CachePrepareRequest(%v) reqPacket(%v) replyPacket(%v) err(%v) moved(%v)", fg, addr, req, reqPacket, replyPacket, err, moved)
	return
}

func (rc *RemoteCache) Stop() {
	rc.stopOnce.Do(func() {
		rc.Started = false
		close(rc.stopC)
		rc.conns.Close()
		rc.wg.Wait()
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

func (rc *RemoteCache) refresh() {
	defer rc.wg.Done()
	for {
		err := rc.refreshWithRecover()
		if err == nil {
			log.LogInfof("refresh: exit")
			break
		}
		log.LogErrorf("refresh: err(%v) try next update", err)
	}
}

func (rc *RemoteCache) refreshWithRecover() (panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("refreshFlashNode panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("refreshFlashNode panic: err(%v)", r)
			panicErr = errors.New(msg)
		}
	}()

	refreshView := time.NewTicker(RefreshFlashNodesInterval)
	defer refreshView.Stop()

	refreshLatency := time.NewTimer(0)
	defer refreshLatency.Stop()

	var err error
	for {
		select {
		case <-rc.stopC:
			log.LogInfof("refreshWithRecover: remote stop")
			return
		case <-refreshView.C:
			if err = rc.updateFlashGroups(); err != nil {
				log.LogErrorf("updateFlashGroups err: %v", err)
			}
		case <-refreshLatency.C:
			rc.refreshHostLatency()
			refreshLatency.Reset(RefreshHostLatencyInterval)
		}
	}
}

func (rc *RemoteCache) updateFlashGroups() (err error) {
	var (
		fgv            proto.FlashGroupView
		newFlashGroups = btree.New(32)
	)
	if fgv, err = rc.mc.AdminAPI().ClientFlashGroups(); err != nil {
		log.LogWarnf("updateFlashGroups: err(%v)", err)
		return
	}
	log.LogDebugf("updateFlashGroups. get flashGroupView [%v]", fgv)
	rc.clusterEnable(fgv.Enable)
	if !fgv.Enable {
		rc.flashGroups = newFlashGroups
		return
	}

	for _, fg := range fgv.FlashGroups {
		newAdded := make([]string, 0)
		for _, host := range fg.Hosts {
			if _, ok := rc.hostLatency.Load(host); !ok {
				newAdded = append(newAdded, host)
			}
		}
		log.LogDebugf("updateFlashGroups: fgID(%v) newAdded hosts: %v", fg.ID, newAdded)

		rc.updateHostLatency(newAdded)
		sortedHosts := rc.ClassifyHostsByAvgDelay(fg.ID, fg.Hosts)

		flashGroup := NewFlashGroup(fg, sortedHosts)
		for _, slot := range fg.Slot {
			slotItem := &SlotItem{
				slot:       slot,
				FlashGroup: flashGroup,
			}
			newFlashGroups.ReplaceOrInsert(slotItem)
		}
	}
	rc.flashGroups = newFlashGroups

	return
}

func (rc *RemoteCache) ClassifyHostsByAvgDelay(fgID uint64, hosts []string) (sortedHosts map[ZoneRankType][]string) {
	sortedHosts = make(map[ZoneRankType][]string)

	for _, host := range hosts {
		avgTime := time.Duration(0)
		v, ok := rc.hostLatency.Load(host)
		if ok {
			avgTime = v.(time.Duration)
		}
		if avgTime <= time.Duration(0) {
			sortedHosts[UnknownZoneRank] = append(sortedHosts[UnknownZoneRank], host)
		} else if avgTime <= sameZoneTimeout {
			sortedHosts[SameZoneRank] = append(sortedHosts[SameZoneRank], host)
		} else if avgTime <= SameRegionTimeout {
			sortedHosts[SameRegionRank] = append(sortedHosts[SameRegionRank], host)
		} else {
			sortedHosts[CrossRegionRank] = append(sortedHosts[CrossRegionRank], host)
		}
	}
	log.LogInfof("ClassifyHostsByAvgDelay: fgID(%v) sortedHost:%v", fgID, sortedHosts)
	return sortedHosts
}

func (rc *RemoteCache) refreshHostLatency() {
	hosts := rc.getFlashHostsMap()

	needPings := make([]string, 0)
	rc.hostLatency.Range(func(key, value interface{}) bool {
		host := key.(string)
		if _, exist := hosts[host]; !exist {
			rc.hostLatency.Delete(host)
			log.LogInfof("remove flashNode(%v)", host)
		} else {
			needPings = append(needPings, host)
		}
		return true
	})
	rc.updateHostLatency(needPings)
	log.LogInfof("updateHostLatencyByLatency: needPings(%v)", len(needPings))
}

func (rc *RemoteCache) updateHostLatency(hosts []string) {
	for _, host := range hosts {
		avgRtt, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], pingCount, pingTimeout*pingCount)
		if err == nil {
			rc.hostLatency.Store(host, avgRtt)
		} else {
			rc.hostLatency.Delete(host)
			log.LogWarnf("updateHostLatency: host(%v) err(%v)", host, err)
		}
	}
}

func (rc *RemoteCache) GetFlashGroupBySlot(slot uint32) *FlashGroup {
	var item *SlotItem

	pivot := &SlotItem{slot: slot}
	rangeFunc := func(i btree.Item) bool {
		item = i.(*SlotItem)
		return false
	}
	rc.rangeFlashGroups(pivot, rangeFunc)

	if item == nil {
		return rc.getMinFlashGroup()
	}
	return item.FlashGroup
}

func (rc *RemoteCache) getFlashHostsMap() map[string]bool {
	allHosts := make(map[string]bool)

	rangeFunc := func(i btree.Item) bool {
		fgItem := i.(*SlotItem)
		for _, host := range fgItem.FlashGroup.Hosts {
			allHosts[host] = true
		}
		return true
	}
	rc.rangeFlashGroups(nil, rangeFunc)

	return allHosts
}

type CacheReadRequest struct {
	proto.CacheReadRequest
	Data []byte
}

func (rc *RemoteCache) rangeFlashGroups(pivot *SlotItem, rangeFunc func(item btree.Item) bool) {
	flashGroups := rc.flashGroups

	if pivot == nil {
		flashGroups.Ascend(rangeFunc)
	} else {
		flashGroups.AscendGreaterOrEqual(pivot, rangeFunc)
	}
}

func (rc *RemoteCache) getMinFlashGroup() *FlashGroup {
	flashGroups := rc.flashGroups

	if flashGroups.Len() > 0 {
		item := flashGroups.Min().(*SlotItem)
		if item != nil {
			return item.FlashGroup
		}
	}
	return nil
}
