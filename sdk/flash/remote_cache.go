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

package flash

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bits-and-blooms/bloom"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	BloomBits    = 10 * 1024 * 1024 * 8
	BloomHashNum = 7

	cacheBoostPathSeparator = ","

	pingCount            = 3
	pingCostPerHost      = 20 * time.Millisecond
	IdleConnTimeoutData  = 30
	ConnectTimeoutDataMs = 200

	RefreshFlashNodesInterval  = time.Minute
	RefreshHostLatencyInterval = 15 * time.Minute

	sameZoneTimeout   = 400 * time.Microsecond
	SameRegionTimeout = 2 * time.Millisecond

	limitedHostPunishTime = time.Duration(5 * time.Second)
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

	ReadTimeoutMs int64
}

type RemoteCache struct {
	cluster     string
	volname     string
	mc          *masterSDK.MasterClient
	conns       *connpool.ConnectPool
	connConfig  *proto.ConnConfig
	hostLatency sync.Map
	flashGroups *btree.BTree
	stopOnce    sync.Once
	stopC       chan struct{}
	wg          sync.WaitGroup
	metaWrapper *meta.MetaWrapper
	cacheBloom  *bloom.BloomFilter

	readTimeoutMs   int64
	limitedHost     map[string]time.Time
	limitedHostLock sync.RWMutex
}

func NewRemoteCache(config *CacheConfig) *RemoteCache {
	rc := new(RemoteCache)
	rc.stopC = make(chan struct{})
	rc.cluster = config.Cluster
	rc.volname = config.Volume
	rc.metaWrapper = config.MW
	rc.flashGroups = btree.New(32)
	rc.mc = masterSDK.NewMasterClient(config.Masters, false)
	if config.ReadTimeoutMs <= 0 {
		rc.readTimeoutMs = ConnectTimeoutDataMs
	} else {
		rc.readTimeoutMs = config.ReadTimeoutMs
	}
	rc.connConfig = &proto.ConnConfig{
		IdleTimeoutSec:   IdleConnTimeoutData,
		ConnectTimeoutNs: ConnectTimeoutDataMs * int64(time.Millisecond),
		ReadTimeoutNs:    rc.readTimeoutMs * int64(time.Millisecond),
		WriteTimeoutNs:   rc.readTimeoutMs * int64(time.Millisecond),
	}
	rc.conns = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, time.Duration(rc.connConfig.IdleTimeoutSec)*time.Second, time.Duration(rc.connConfig.ConnectTimeoutNs))
	rc.cacheBloom = bloom.New(BloomBits, BloomHashNum)
	rc.limitedHost = make(map[string]time.Time)

	rc.wg.Add(1)
	go rc.refresh()
	return rc
}

func (rc *RemoteCache) Read(ctx context.Context, fg *FlashGroup, inode uint64, req *CacheReadRequest) (read int, err error) {
	var (
		conn  *net.TCPConn
		reply *common.Packet
		moved bool
	)

	addr := rc.selectFlashHost(fg)
	if addr == "" {
		err = fmt.Errorf("getFlashHost failed: cannot find any available host")
		log.LogWarnf("FlashGroup read failed: err(%v)", err)
		return
	}
	reqPacket := common.NewCachePacket(ctx, inode, proto.OpCacheRead)
	if err = reqPacket.MarshalDataPb(&req.CacheReadRequest); err != nil {
		log.LogWarnf("FlashGroup Read: failed to Marshal req(%v) err(%v)", req, err)
		return
	}
	defer func() {
		if err != nil && (os.IsTimeout(err) || strings.Contains(err.Error(), syscall.ECONNREFUSED.Error())) {
			moved = fg.moveToUnknownRank(addr)
		}
	}()
	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("FlashGroup read: get connection to curr addr failed, addr(%v) reqPacket(%v) err(%v)", addr, req, err)
		return
	}
	defer func() {
		rc.conns.PutConnectWithErr(conn, err)
	}()
	if err = reqPacket.WriteToConnNs(conn, rc.connConfig.WriteTimeoutNs); err != nil {
		log.LogWarnf("FlashGroup Read: failed to write to addr(%v) err(%v)", addr, err)
		return
	}
	if read, reply, err = rc.getReadReply(conn, reqPacket, req); err != nil {
		log.LogWarnf("FlashGroup Read: getReadReply from addr(%v) err(%v)", addr, err)
	}
	// check resultCode is limited or not
	if reply.ResultCode == proto.OpAgain {
		rc.putLimitedHost(addr)
		log.LogWarnf("FlashGroup Read: put limited flash host(%v), fg(%v)", addr, fg.ID)
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("FlashGroup Read: flashGroup(%v) addr(%v) CacheReadRequest(%v) reqPacket(%v) err(%v) moved(%v)", fg, addr, req, reqPacket, err, moved)
	}
	return
}

func (rc *RemoteCache) getReadReply(conn *net.TCPConn, reqPacket *common.Packet, req *CacheReadRequest) (readBytes int, replyPacket *common.Packet, err error) {
	for readBytes < int(req.Size_) {
		replyPacket = common.NewCacheReply(reqPacket.Ctx())
		err = replyPacket.ReadFromConnNs(conn, rc.connConfig.ReadTimeoutNs)
		if err != nil {
			log.LogWarnf("getReadReply: failed to read from connect, req(%v) readBytes(%v) err(%v)", reqPacket, readBytes, err)
			return
		}
		if replyPacket.ReqID != reqPacket.ReqID {
			err = fmt.Errorf("mismatch packet")
			log.LogWarnf("getReadReply: err(%v) req(%v) reply(%v)", err, reqPacket, replyPacket)
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
	reqPacket := common.NewCachePacket(ctx, inode, proto.OpCachePrepare)
	if err = reqPacket.MarshalDataPb(req); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to MarshalData (%v), err(%v)", req, err)
		return
	}
	defer func() {
		if err != nil && (os.IsTimeout(err) || strings.Contains(err.Error(), syscall.ECONNREFUSED.Error())) {
			moved = fg.moveToUnknownRank(addr)
		}
	}()
	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("FlashGroup prepare: get connection to curr addr failed, addr(%v) reqPacket(%v) err(%v)", addr, req, err)
		return
	}
	defer func() {
		rc.conns.PutConnectWithErr(conn, err)
	}()

	if err = reqPacket.WriteToConnNs(conn, rc.connConfig.WriteTimeoutNs); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to write to addr(%v) err(%v)", addr, err)
		return
	}

	replyPacket := common.NewCacheReply(reqPacket.Ctx())
	if err = replyPacket.ReadFromConnNs(conn, rc.connConfig.ReadTimeoutNs); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to ReadFromConn, replyPacket(%v), fg host(%v) moved(%v), err(%v)", replyPacket, addr, moved, err)
		return
	}
	if replyPacket.ReqID != reqPacket.ReqID {
		err = fmt.Errorf("mismatch packet")
		log.LogWarnf("FlashGroup prepare: err(%v) req(%v) reply(%v)", err, reqPacket, replyPacket)
		return
	}
	if replyPacket.ResultCode != proto.OpOk {
		log.LogWarnf("FlashGroup Prepare: ResultCode NOK, replyPacket(%v), fg host(%v), ResultCode(%v)", replyPacket, addr, replyPacket.ResultCode)
		err = fmt.Errorf("ResultCode NOK (%v)", replyPacket.ResultCode)
		return
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("FlashGroup Prepare successful: flashGroup(%v) addr(%v) CachePrepareRequest(%v) reqPacket(%v) replyPacket(%v) err(%v) moved(%v)", fg, addr, req, reqPacket, replyPacket, err, moved)
	}
	return
}

func (rc *RemoteCache) Stop() {
	rc.stopOnce.Do(func() {
		close(rc.stopC)
		rc.conns.Close()
		rc.wg.Wait()
	})
}

func (rc *RemoteCache) GetRemoteCacheBloom() *bloom.BloomFilter {
	return rc.cacheBloom
}

func (rc *RemoteCache) ResetConnConfig(timeoutNs int64) {
	config := rc.connConfig
	if timeoutNs > 0 {
		if timeoutNs != config.ReadTimeoutNs {
			log.LogInfof("ResetConnConfig: old(%v) new(%v)", rc.connConfig.ReadTimeoutNs, timeoutNs)
			atomic.StoreInt64(&config.ReadTimeoutNs, timeoutNs)
		}
	}
}

func (rc *RemoteCache) ResetCacheBoostPathToBloom(cacheBoostPath string) bool {
	res := true
	rc.cacheBloom.ClearAll()
	for _, path := range strings.Split(cacheBoostPath, cacheBoostPathSeparator) {
		path = strings.TrimSpace(path)
		if len(path) == 0 {
			continue
		}
		if ino, err := rc.getPathInode(path); err != nil {
			log.LogErrorf("RemoteCache: lookup cacheBoostPath %s err: %v", path, err)
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
	if path != "" && path != "/" {
		dirs := strings.Split(path, "/")
		var childIno uint64
		for _, dir := range dirs {
			if dir == "/" || dir == "" {
				continue
			}
			childIno, _, err = rc.metaWrapper.Lookup_ll(nil, ino, dir)
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
			common.HandleUmpAlarm(rc.cluster, rc.volname, "refreshFlashNode", msg)
			panicErr = errors.New(msg)
		}
	}()

	refreshView := time.NewTimer(0)
	defer refreshView.Stop()

	refreshLatency := time.NewTicker(RefreshHostLatencyInterval)
	defer refreshLatency.Stop()

	var (
		err error
	)
	for {
		select {
		case <-rc.stopC:
			return
		case <-refreshView.C:
			if err = rc.updateFlashGroups(); err != nil {
				log.LogErrorf("updateFlashGroups err: %v", err)
			}
			refreshView.Reset(RefreshFlashNodesInterval)
		case <-refreshLatency.C:
			rc.refreshHostLatency()
		}
	}
}

func (rc *RemoteCache) updateFlashGroups() (err error) {
	var (
		fgv            *proto.FlashGroupView
		newFlashGroups = btree.New(32)
	)
	if fgv, err = rc.mc.AdminAPI().ClientFlashGroups(); err != nil {
		log.LogWarnf("updateFlashGroups: err(%v)", err)
		return
	}

	for _, fg := range fgv.FlashGroups {
		newAdded := make([]string, 0)
		for _, host := range fg.Hosts {
			if _, ok := rc.hostLatency.Load(host); !ok {
				newAdded = append(newAdded, host)
			}
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("updateFlashGroups: fgID(%v) newAdded hosts: %v", fg.ID, newAdded)
		}
		// 这个方法耗时久
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
	sortedHosts = make(map[ZoneRankType][]string, 0)

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
	if hosts == nil || len(hosts) == 0 {
		return
	}
	for _, host := range hosts {
		avgTime, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], pingCount, pingCostPerHost)
		if err == nil {
			rc.hostLatency.Store(host, avgTime)
		} else {
			rc.hostLatency.Delete(host)
			log.LogWarnf("updateHostLatency: host(%v) err(%v)", host, err)
		}
	}
	return
}

func (rc *RemoteCache) GetFlashGroupBySlot(slot uint32) *FlashGroup {
	var item *SlotItem

	pivot := &SlotItem{slot: slot}
	var rangeFunc = func(item btree.Item) bool {
		item = item.(*SlotItem)
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

	var rangeFunc = func(item btree.Item) bool {
		fgItem := item.(*SlotItem)
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

func (rc *RemoteCache) selectFlashHost(fg *FlashGroup) (host string) {
	visited := make(map[string]bool)
	for {
		host = fg.getFlashHost()
		if host == "" {
			return
		}
		if visited[host] {
			return ""
		}
		if !rc.isLimitedHost(host) {
			return
		}
		visited[host] = true
	}
}

func (rc *RemoteCache) putLimitedHost(host string) {
	rc.limitedHostLock.Lock()
	defer rc.limitedHostLock.Unlock()

	rc.limitedHost[host] = time.Now()
}

func (rc *RemoteCache) removeLimitedHost(host string) {
	rc.limitedHostLock.Lock()
	defer rc.limitedHostLock.Unlock()

	delete(rc.limitedHost, host)
}

func (rc *RemoteCache) isLimitedHost(host string) bool {
	rc.limitedHostLock.RLock()
	insertTime, ok := rc.limitedHost[host]
	if !ok {
		rc.limitedHostLock.RUnlock()
		return false
	}
	rc.limitedHostLock.RUnlock()

	if time.Since(insertTime) >= limitedHostPunishTime {
		rc.removeLimitedHost(host)
		return false
	}
	return true
}
