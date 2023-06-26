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
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	BloomBits    = 10 * 1024 * 1024 * 8
	BloomHashNum = 7

	cacheBoostPathSeparator = ","

	pingCount            = 3
	pingTimeout          = 50 * time.Millisecond
	IdleConnTimeoutData  = 30
	ConnectTimeoutDataMs = 500

	RefreshFlashNodesInterval  = time.Minute
	RefreshHostLatencyInterval = 15 * time.Minute

	sameZoneTimeout   = 400 * time.Microsecond
	SameRegionTimeout = 2 * time.Millisecond
	sameZoneWeight    = 70
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
	ConnTimeoutUs  int64 // 读写超时
}

type RemoteCache struct {
	sync.RWMutex
	cluster     string
	volname     string
	mc          *masterSDK.MasterClient
	conns       *connpool.ConnectPool
	connConfig  *proto.ConnConfig
	hostLatency sync.Map
	flashGroups *btree.BTree
	allSlots    map[uint32]bool
	stopOnce    sync.Once
	stopC       chan struct{}
	wg          sync.WaitGroup
	metaWrapper *meta.MetaWrapper
	cacheBloom  *bloom.BloomFilter

	sameZoneWeight int
	connTimeoutUs  int64
}

func NewRemoteCache(config *CacheConfig) (*RemoteCache, error) {
	rc := new(RemoteCache)
	rc.stopC = make(chan struct{})
	rc.cluster = config.Cluster
	rc.volname = config.Volume
	rc.metaWrapper = config.MW
	rc.flashGroups = btree.New(32)
	rc.mc = masterSDK.NewMasterClient(config.Masters, false)
	if config.SameZoneWeight <= 0 {
		rc.sameZoneWeight = sameZoneWeight
	} else {
		rc.sameZoneWeight = config.SameZoneWeight
	}
	if config.ConnTimeoutUs <= 0 {
		rc.connTimeoutUs = ConnectTimeoutDataMs * int64(time.Microsecond)
	} else {
		rc.connTimeoutUs = config.ConnTimeoutUs
	}
	rc.connConfig = &proto.ConnConfig{
		IdleTimeoutSec:   IdleConnTimeoutData,
		ConnectTimeoutNs: ConnectTimeoutDataMs * int64(time.Millisecond),
		ReadTimeoutNs:    rc.connTimeoutUs * int64(time.Microsecond),
		WriteTimeoutNs:   rc.connTimeoutUs * int64(time.Microsecond),
	}

	err := rc.updateFlashGroups()
	if err != nil {
		return nil, err
	}
	rc.conns = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, rc.connConfig.IdleTimeoutSec, rc.connConfig.ConnectTimeoutNs)
	rc.cacheBloom = bloom.New(BloomBits, BloomHashNum)

	rc.wg.Add(1)
	go rc.refresh()
	return rc, nil
}

func (rc *RemoteCache) Read(ctx context.Context, fg *FlashGroup, inode uint64, req *CacheReadRequest) (read int, err error) {
	var tp = exporter.NewVolumeTPUs("fgRead", rc.volname)
	defer func() {
		tp.Set(err)
	}()

	var (
		conn  *net.TCPConn
		moved bool
	)
	reqPacket := common.NewCachePacket(ctx, inode, proto.OpCacheRead)
	if err = reqPacket.MarshalDataPb(&req.CacheReadRequest); err != nil {
		log.LogWarnf("FlashGroup Read: failed to MarshalData (%+v). err(%v)", req, err)
		return
	}
	addr := fg.getFlashHost()
	if addr == "" {
		err = fmt.Errorf("getFlashHost failed: cannot find any available host")
		log.LogErrorf("FlashGroup read failed: err(%v)", err)
		return
	}
	defer func() {
		if err != nil && (os.IsTimeout(err) || strings.Contains(err.Error(), syscall.ECONNREFUSED.Error())) {
			moved = fg.moveToUnknownRank(addr)
			rc.hostLatency.Store(addr, time.Duration(0))
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
	if read, err = rc.getReadReply(conn, reqPacket, req); err != nil {
		log.LogWarnf("FlashGroup Read: getReadReply from addr(%v) err(%v)", addr, err)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("FlashGroup Read: flashGroup(%v) addr(%v) CacheReadRequest(%v) reqPacket(%v) err(%v) moved(%v)", fg, addr, req, reqPacket, err, moved)
	}
	return
}

func (rc *RemoteCache) getReadReply(conn *net.TCPConn, reqPacket *common.Packet, req *CacheReadRequest) (readBytes int, err error) {
	for readBytes < int(req.Size_) {
		replyPacket := common.NewCacheReply(reqPacket.Ctx())
		err = replyPacket.ReadFromConnNs(conn, rc.connConfig.ReadTimeoutNs)
		if err != nil {
			log.LogWarnf("getReadReply: failed to read from connect, req(%v) readBytes(%v) err(%v)", reqPacket, readBytes, err)
			return
		}
		//err = common.CheckReadReplyValid(reqPacket, replyPacket)
		//if err != nil {
		//	log.LogWarnf("getReadReply: failed to checkReadReplyValid, err(%v)", err)
		//	return
		//}
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
	reqPacket := common.NewCachePacket(ctx, inode, proto.OpCachePrepare)
	if err = reqPacket.MarshalDataPb(req); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to MarshalData (%+v). err(%v)", req, err)
		return
	}
	addr := fg.getFlashHost()
	if addr == "" {
		err = fmt.Errorf("getFlashHost failed: can not find host")
		log.LogErrorf("FlashGroup prepare failed: err(%v)", err)
		return
	}
	defer func() {
		if err != nil && (os.IsTimeout(err) || strings.Contains(err.Error(), syscall.ECONNREFUSED.Error())) {
			moved = fg.moveToUnknownRank(addr)
			rc.hostLatency.Store(addr, time.Duration(0))
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
	if rc != nil {
		return rc.cacheBloom
	}
	return nil
}

func (rc *RemoteCache) ResetConnConfig(timeouts int64) {
	if rc != nil {
		config := rc.connConfig
		if timeouts > 0 {
			log.LogInfof("ResetConnConfig: %v", timeouts)
			if timeouts != config.ReadTimeoutNs {
				atomic.StoreInt64(&config.ReadTimeoutNs, timeouts)
			}
			if timeouts != config.WriteTimeoutNs {
				atomic.StoreInt64(&config.WriteTimeoutNs, timeouts)
			}
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
		log.LogErrorf("refreshMetaInfo: err(%v) try next update", err)
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

	refreshView := time.NewTicker(RefreshFlashNodesInterval)
	defer refreshView.Stop()

	refreshLatency := time.NewTimer(0)
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
		case <-refreshLatency.C:
			allHosts := make(map[string]bool)
			rc.flashGroups.Ascend(func(item btree.Item) bool {
				fgItem := item.(*SlotItem)
				for _, host := range fgItem.FlashGroup.Hosts {
					allHosts[host] = true
				}
				return true
			})
			rc.refreshHostLatency(allHosts)
			refreshLatency.Reset(RefreshHostLatencyInterval)
		}
	}
}

func (rc *RemoteCache) updateFlashGroups() (err error) {
	var fgv *proto.FlashGroupView
	if fgv, err = rc.mc.AdminAPI().ClientFlashGroups(); err != nil {
		log.LogWarnf("updateFlashGroups: err(%v)", err)
		return
	}

	recordSlots := make(map[uint32]bool)

	rc.Lock()
	defer rc.Unlock()
	rc.retryHostLatency()
	for _, fg := range fgv.FlashGroups {
		newAdded := make([]string, 0)
		for _, host := range fg.Hosts {
			if _, ok := rc.hostLatency.Load(host); !ok {
				rc.hostLatency.Store(host, time.Duration(0))
				newAdded = append(newAdded, host)
			}
		}
		rc.updateHostLatency(newAdded)
		sortedHosts := rc.ClassifyHostsByAvgDelay(fg.ID, fg.Hosts)
		if log.IsDebugEnabled() {
			log.LogDebugf("updateFlashGroups: new hosts: %v", newAdded)
		}
		flashGroup := NewFlashGroup(fg, sortedHosts)
		for _, slot := range fg.Slot {
			slotItem := &SlotItem{
				slot:       slot,
				FlashGroup: flashGroup,
			}
			rc.flashGroups.ReplaceOrInsert(slotItem)
			delete(rc.allSlots, slot)
			recordSlots[slot] = true
		}
	}
	for slot := range rc.allSlots {
		fgItem := &SlotItem{slot, nil}
		rc.flashGroups.Delete(fgItem)
	}

	rc.allSlots = recordSlots
	log.LogInfof("updateFlashGroups: %d", len(fgv.FlashGroups))
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
	log.LogInfof("ClassifyHostsByAvgDelay: fg(%v) sortedHost:%v", fgID, sortedHosts)
	return sortedHosts
}

func (rc *RemoteCache) refreshHostLatency(hosts map[string]bool) {
	needPings := make([]string, 0)
	rc.hostLatency.Range(func(key, value interface{}) bool {
		host := key.(string)
		if _, exist := hosts[host]; !exist {
			rc.hostLatency.Delete(host)
			log.LogInfof("remove flashNode(%v)", host)
		}
		needPings = append(needPings, host)
		return true
	})
	rc.updateHostLatency(needPings)
	log.LogInfof("updateHostLatencyByLatency: needPings(%v)", len(needPings))
}

func (rc *RemoteCache) updateHostLatency(hosts []string) (failedHosts map[string]bool) {
	if hosts == nil || len(hosts) == 0 {
		return
	}
	failedHosts = make(map[string]bool)
	for _, host := range hosts {
		avgTime, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], pingCount, pingTimeout*pingCount)
		if err != nil {
			avgTime = time.Duration(0)
			failedHosts[host] = true
			log.LogWarnf("updateHostLatency: host(%v) err(%v)", host, err)
		}
		rc.hostLatency.Store(host, avgTime)
	}
	log.LogDebugf("updateHostLatency: failedHosts(%v)", failedHosts)
	return
}

func (rc *RemoteCache) retryHostLatency() {
	retryHosts := make([]string, 0)
	rc.hostLatency.Range(func(key, value interface{}) bool {
		host := key.(string)
		delay := value.(time.Duration)
		if delay == 0 {
			retryHosts = append(retryHosts, host)
		}
		return true
	})
	rc.updateHostLatency(retryHosts)
	if log.IsDebugEnabled() {
		log.LogDebugf("retryHostLatency: retry hosts(%v)", retryHosts)
	}
}

func (rc *RemoteCache) GetFlashGroupBySlot(slot uint32) *FlashGroup {
	var item *SlotItem
	pivot := &SlotItem{slot: slot}

	rc.RLock()
	defer rc.RUnlock()
	rc.flashGroups.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		item = i.(*SlotItem)
		return false
	})

	if item == nil && rc.flashGroups.Len() > 0 {
		item = rc.flashGroups.Min().(*SlotItem)
	}
	if item == nil {
		return nil
	}
	return item.FlashGroup
}

type CacheReadRequest struct {
	proto.CacheReadRequest
	Data []byte
}