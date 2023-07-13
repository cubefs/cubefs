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

package data

import (
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/sdk/flash"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/scheduler"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
)

var (
	LocalIP                      string
	MinWriteAbleDataPartitionCnt = 10
	MasterNoCacheAPIRetryTimeout = 5 * time.Minute
)

const (
	VolNotExistInterceptThresholdMin = 60 * 24
	VolNotExistClearViewThresholdMin = 0

	RefreshHostLatencyInterval = time.Hour
)

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

// Wrapper TODO rename. This name does not reflect what it is doing.
type Wrapper struct {
	sync.RWMutex
	clusterName           string
	volName               string
	masters               []string
	umpJmtpAddr           string
	volNotExistCount      int32
	partitions            *sync.Map //key: dpID; value: *DataPartition
	followerRead          bool
	followerReadClientCfg bool
	nearRead              bool
	forceROW              bool
	enableWriteCache      bool
	extentCacheExpireSec  int64
	dpSelectorChanged     bool
	dpSelectorName        string
	dpSelectorParm        string
	mc                    *masterSDK.MasterClient
	metaWrapper           *meta.MetaWrapper
	stopOnce              sync.Once
	stopC                 chan struct{}
	wg                    sync.WaitGroup

	dpSelector DataPartitionSelector

	HostsStatus map[string]bool

	crossRegionHAType      proto.CrossRegionHAType
	crossRegionHostLatency sync.Map // key: host, value: ping time
	quorum                 int

	connConfig *proto.ConnConfig

	schedulerClient        *scheduler.SchedulerClient
	dpMetricsReportDomain  string
	dpMetricsReportConfig  *proto.DpMetricsReportConfig
	dpMetricsRefreshCount  uint
	dpMetricsFetchErrCount uint
	ecEnable               bool

	dpFollowerReadDelayConfig *proto.DpFollowerReadDelayConfig
	dpLowestDelayHostWeight   int

	oldCacheStatus         bool
	clusterEnableCache     bool
	connTimeoutUs          int64
	enableRemoteCache      bool
	cacheBoostPath         string
	enableCacheAutoPrepare bool
	cacheTTL               int64
	remoteCache            *flash.RemoteCache
	HostsDelay             sync.Map
}

type DataState struct {
	ClusterName      string
	LocalIP          string
	VolNotExistCount int32
	VolView          *proto.SimpleVolView
	DpView           *proto.DataPartitionsView
	ClusterView      *proto.ClientClusterConf
}

// NewDataPartitionWrapper returns a new data partition wrapper.
func NewDataPartitionWrapper(volName string, masters []string) (w *Wrapper, err error) {
	w = new(Wrapper)
	w.stopC = make(chan struct{})
	w.masters = masters
	w.mc = masterSDK.NewMasterClient(masters, false)
	w.schedulerClient = scheduler.NewSchedulerClient(w.dpMetricsReportDomain, false)
	w.volName = volName
	w.partitions = new(sync.Map)
	w.HostsStatus = make(map[string]bool)
	w.connConfig = &proto.ConnConfig{
		IdleTimeoutSec:   IdleConnTimeoutData,
		ConnectTimeoutNs: ConnectTimeoutDataMs * int64(time.Millisecond),
		WriteTimeoutNs:   WriteTimeoutData * int64(time.Second),
		ReadTimeoutNs:    ReadTimeoutData * int64(time.Second),
	}
	w.dpMetricsReportConfig = &proto.DpMetricsReportConfig{
		EnableReport:      false,
		ReportIntervalSec: defaultMetricReportSec,
		FetchIntervalSec:  defaultMetricFetchSec,
	}
	w.dpFollowerReadDelayConfig = &proto.DpFollowerReadDelayConfig{
		EnableCollect:        true,
		DelaySummaryInterval: followerReadDelaySummaryInterval,
	}
	if err = w.updateClusterInfo(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.getSimpleVolView(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.initDpSelector(); err != nil {
		log.LogErrorf("NewDataPartitionWrapper: init initDpSelector failed, [%v]", err)
	}
	if err = w.updateDataPartition(true); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.updateClientClusterView(); err != nil {
		log.LogErrorf("NewDataPartitionWrapper: init DataNodeStatus failed, [%v]", err)
	}

	err = nil
	StreamConnPoolInitOnce.Do(func() {
		StreamConnPool = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, w.connConfig.IdleTimeoutSec, w.connConfig.ConnectTimeoutNs)
	})

	w.wg.Add(4)
	go w.update()
	go w.updateCrossRegionHostStatus()
	go w.ScheduleDataPartitionMetricsReport()
	go w.dpFollowerReadDelayCollect()

	return
}

func RebuildDataPartitionWrapper(volName string, masters []string, dataState *DataState) (w *Wrapper) {
	w = new(Wrapper)
	w.stopC = make(chan struct{})
	w.masters = masters
	w.mc = masterSDK.NewMasterClient(masters, false)
	w.schedulerClient = scheduler.NewSchedulerClient(w.dpMetricsReportDomain, false)
	w.volName = volName
	w.partitions = new(sync.Map)
	w.HostsStatus = make(map[string]bool)
	w.connConfig = &proto.ConnConfig{
		IdleTimeoutSec:   IdleConnTimeoutData,
		ConnectTimeoutNs: ConnectTimeoutDataMs * int64(time.Millisecond),
		WriteTimeoutNs:   WriteTimeoutData * int64(time.Second),
		ReadTimeoutNs:    ReadTimeoutData * int64(time.Second),
	}
	w.dpMetricsReportConfig = &proto.DpMetricsReportConfig{
		EnableReport:      false,
		ReportIntervalSec: defaultMetricReportSec,
		FetchIntervalSec:  defaultMetricFetchSec,
	}
	w.dpFollowerReadDelayConfig = &proto.DpFollowerReadDelayConfig{
		EnableCollect:        true,
		DelaySummaryInterval: followerReadDelaySummaryInterval,
	}
	w.clusterName = dataState.ClusterName
	LocalIP = dataState.LocalIP

	view := dataState.VolView
	w.followerRead = view.FollowerRead
	w.nearRead = view.NearRead
	w.forceROW = view.ForceROW
	w.dpSelectorName = view.DpSelectorName
	w.dpSelectorParm = view.DpSelectorParm
	w.crossRegionHAType = view.CrossRegionHAType
	w.quorum = view.Quorum
	w.ecEnable = view.EcEnable
	w.extentCacheExpireSec = view.ExtentCacheExpireSec
	w.updateConnConfig(view.ConnConfig)
	w.updateDpMetricsReportConfig(view.DpMetricsReportConfig)
	w.updateDpFollowerReadDelayConfig(&view.DpFolReadDelayConfig)
	w.initDpSelector()

	w.volNotExistCount = dataState.VolNotExistCount
	if !w.VolNotExists() {
		w.convertDataPartition(dataState.DpView, true)
	}

	w._updateDataNodeStatus(dataState.ClusterView)

	StreamConnPoolInitOnce.Do(func() {
		StreamConnPool = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, w.connConfig.IdleTimeoutSec, w.connConfig.ConnectTimeoutNs)
	})

	w.wg.Add(4)
	go w.update()
	go w.updateCrossRegionHostStatus()
	go w.ScheduleDataPartitionMetricsReport()
	go w.dpFollowerReadDelayCollect()

	return
}

func (w *Wrapper) setClusterTimeoutUs(timeoutUs int64) {
	if timeoutUs <= 0 {
		timeoutUs = ConnectTimeoutDataMs * int64(time.Microsecond)
	}
	w.connTimeoutUs = timeoutUs
}

func (w *Wrapper) setClusterBoostEnable(enableBoost bool) {
	w.oldCacheStatus = w.EnableRemoteCache()
	oldClusterEnableCache := w.clusterEnableCache
	if oldClusterEnableCache != enableBoost {
		w.clusterEnableCache = enableBoost
		log.LogInfof("setClusterBoostEnable: from old(%v) to new(%v)", oldClusterEnableCache, enableBoost)
	}
}

func (w *Wrapper) EnableRemoteCache() bool {
	return w.enableRemoteCache && w.clusterEnableCache
}

func (w *Wrapper) initRemoteCache() (err error) {
	cacheConfig := &flash.CacheConfig{
		Cluster:       w.clusterName,
		Volume:        w.volName,
		Masters:       w.masters,
		MW:            w.metaWrapper,
		ConnTimeoutUs: w.connTimeoutUs,
	}
	if w.remoteCache, err = flash.NewRemoteCache(cacheConfig); err != nil {
		return
	}
	if !w.remoteCache.ResetCacheBoostPathToBloom(w.cacheBoostPath) {
		w.cacheBoostPath = ""
	}
	return
}

func (w *Wrapper) saveDataState() *DataState {
	dataState := new(DataState)
	dataState.ClusterName = w.clusterName
	dataState.LocalIP = LocalIP
	dataState.VolNotExistCount = w.volNotExistCount

	dataState.VolView = w.saveSimpleVolView()
	dataState.DpView = w.saveDataPartition()
	dataState.ClusterView = w.saveClientClusterView()

	return dataState
}

func (w *Wrapper) Stop() {
	w.stopOnce.Do(func() {
		if w.remoteCache != nil {
			w.remoteCache.Stop()
		}
		close(w.stopC)
		w.wg.Wait()
	})
}

func (w *Wrapper) InitFollowerRead(clientConfig bool) {
	w.followerReadClientCfg = clientConfig
	w.followerRead = w.followerReadClientCfg || w.followerRead
}

func (w *Wrapper) FollowerRead() bool {
	return w.followerRead
}

func (w *Wrapper) updateClusterInfo() (err error) {
	var (
		info    *proto.ClusterInfo
		localIp string
	)
	if info, err = w.mc.AdminAPI().GetClusterInfo(); err != nil {
		log.LogWarnf("UpdateClusterInfo: get cluster info fail: err(%v)", err)
		return
	}
	log.LogInfof("UpdateClusterInfo: get cluster info: cluster(%v) localIP(%v)", info.Cluster, info.Ip)
	w.clusterName = info.Cluster
	if localIp, err = iputil.GetLocalIPByDial(w.mc.Nodes(), iputil.GetLocalIPTimeout); err != nil {
		log.LogWarnf("UpdateClusterInfo: get local ip fail: err(%v)", err)
		return
	}
	LocalIP = localIp
	return
}

func (w *Wrapper) getSimpleVolView() (err error) {
	var view *proto.SimpleVolView

	if view, err = w.mc.AdminAPI().GetVolumeSimpleInfo(w.volName); err != nil {
		log.LogWarnf("getSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.volName, err)
		return
	}
	w.followerRead = view.FollowerRead
	w.nearRead = view.NearRead
	w.forceROW = view.ForceROW
	w.dpSelectorName = view.DpSelectorName
	w.dpSelectorParm = view.DpSelectorParm
	w.crossRegionHAType = view.CrossRegionHAType
	w.quorum = view.Quorum
	w.ecEnable = view.EcEnable
	w.extentCacheExpireSec = view.ExtentCacheExpireSec
	if view.UmpCollectWay != exporter.UMPCollectMethodUnknown {
		exporter.SetUMPCollectMethod(view.UmpCollectWay)
	}
	w.enableRemoteCache = view.RemoteCacheBoostEnable
	w.cacheBoostPath = view.RemoteCacheBoostPath
	w.enableCacheAutoPrepare = view.RemoteCacheAutoPrepare
	w.cacheTTL = view.RemoteCacheTTL
	w.updateConnConfig(view.ConnConfig)
	w.updateDpMetricsReportConfig(view.DpMetricsReportConfig)
	w.updateDpFollowerReadDelayConfig(&view.DpFolReadDelayConfig)

	log.LogInfof("getSimpleVolView: get volume simple info: ID(%v) name(%v) owner(%v) status(%v) capacity(%v) "+
		"metaReplicas(%v) dataReplicas(%v) mpCnt(%v) dpCnt(%v) followerRead(%v) forceROW(%v) enableWriteCache(%v) createTime(%v) dpSelectorName(%v) "+
		"dpSelectorParm(%v) quorum(%v) extentCacheExpireSecond(%v) dpFolReadDelayConfig(%v)",
		view.ID, view.Name, view.Owner, view.Status, view.Capacity, view.MpReplicaNum, view.DpReplicaNum, view.MpCnt,
		view.DpCnt, view.FollowerRead, view.ForceROW, view.EnableWriteCache, view.CreateTime, view.DpSelectorName, view.DpSelectorParm,
		view.Quorum, view.ExtentCacheExpireSec, view.DpFolReadDelayConfig)
	return nil
}

func (w *Wrapper) saveSimpleVolView() *proto.SimpleVolView {
	view := &proto.SimpleVolView{
		FollowerRead:         w.followerRead,
		NearRead:             w.nearRead,
		ForceROW:             w.forceROW,
		DpSelectorName:       w.dpSelectorName,
		DpSelectorParm:       w.dpSelectorParm,
		CrossRegionHAType:    w.crossRegionHAType,
		Quorum:               w.quorum,
		EcEnable:             w.ecEnable,
		ExtentCacheExpireSec: w.extentCacheExpireSec,
	}
	view.ConnConfig = &proto.ConnConfig{
		IdleTimeoutSec:   w.connConfig.IdleTimeoutSec,
		ConnectTimeoutNs: w.connConfig.ConnectTimeoutNs,
		WriteTimeoutNs:   w.connConfig.WriteTimeoutNs,
		ReadTimeoutNs:    w.connConfig.ReadTimeoutNs,
	}

	view.DpMetricsReportConfig = &proto.DpMetricsReportConfig{
		EnableReport:      w.dpMetricsReportConfig.EnableReport,
		ReportIntervalSec: w.dpMetricsReportConfig.ReportIntervalSec,
		FetchIntervalSec:  w.dpMetricsReportConfig.FetchIntervalSec,
	}
	view.DpFolReadDelayConfig = proto.DpFollowerReadDelayConfig{
		EnableCollect:        w.dpFollowerReadDelayConfig.EnableCollect,
		DelaySummaryInterval: w.dpFollowerReadDelayConfig.DelaySummaryInterval,
	}
	return view
}

func (w *Wrapper) update() {
	defer w.wg.Done()
	for {
		err := w.updateWithRecover()
		if err == nil {
			break
		}
		log.LogErrorf("updateDataInfo: err(%v) try next update", err)
	}
}

func (w *Wrapper) updateWithRecover() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("updateWithRecover panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("updateDataInfo panic: err(%v)", r)
			common.HandleUmpAlarm(w.clusterName, w.volName, "updateDataInfo", msg)
			err = errors.New(msg)
		}
	}()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	refreshLatency := time.NewTimer(0)
	defer refreshLatency.Stop()

	var (
		retryHosts map[string]bool
		hostsLock  sync.Mutex
	)
	for {
		select {
		case <-w.stopC:
			return
		case <-ticker.C:
			w.updateClientClusterView()
			w.updateSimpleVolView()
			w.updateDataPartition(false)


			hostsLock.Lock()
			retryHosts = w.retryHostsPingtime(retryHosts)
			hostsLock.Unlock()

		case <-refreshLatency.C:
			hostsLock.Lock()
			retryHosts = w.updateHostsPingtime()
			hostsLock.Unlock()

			refreshLatency.Reset(RefreshHostLatencyInterval)
		}
	}
}

func (w *Wrapper) updateHostsPingtime() map[string]bool {
	failedHosts := make(map[string]bool)
	allHosts := make(map[string]bool)
	w.partitions.Range(func(id, value interface{}) bool {
		dp := value.(*DataPartition)
		for _, host := range dp.Hosts {
			if _, ok := allHosts[host]; ok {
				continue
			}
			allHosts[host] = true
			avgTime, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], pingCount, pingTimeout*pingCount)
			if err != nil {
				avgTime = time.Duration(0)
				failedHosts[host] = true
				log.LogWarnf("updateHostsPingtime: host(%v) err(%v)", host, err)
			} else {
				log.LogDebugf("updateHostsPingtime: host(%v) ping time(%v)", host, avgTime)
			}
			w.HostsDelay.Store(host, avgTime)
		}
		return true
	})
	return failedHosts
}

func (w *Wrapper) retryHostsPingtime(retryHosts map[string]bool) map[string]bool {
	if retryHosts == nil || len(retryHosts) == 0 {
		return nil
	}
	failedHosts := make(map[string]bool)
	for host, _ := range retryHosts {
		avgTime, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], pingCount, pingTimeout*pingCount)
		if err != nil {
			avgTime = time.Duration(0)
			failedHosts[host] = true
		} else {
			w.HostsDelay.Store(host, avgTime)
		}
	}
	return failedHosts
}

func (w *Wrapper) updateSimpleVolView() (err error) {
	var view *proto.SimpleVolView

	if view, err = w.mc.AdminAPI().GetVolumeSimpleInfo(w.volName); err != nil {
		log.LogWarnf("updateSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.volName, err)
		return
	}

	if w.followerRead != view.FollowerRead && !w.followerReadClientCfg {
		log.LogInfof("updateSimpleVolView: update followerRead from old(%v) to new(%v)",
			w.followerRead, view.FollowerRead)
		w.followerRead = view.FollowerRead
	}

	if w.nearRead != view.NearRead {
		log.LogInfof("updateSimpleVolView: update nearRead from old(%v) to new(%v)", w.nearRead, view.NearRead)
		w.nearRead = view.NearRead
	}

	if exporter.GetUmpCollectMethod() != view.UmpCollectWay && view.UmpCollectWay != exporter.UMPCollectMethodUnknown {
		log.LogInfof("updateSimpleVolView: update umpCollectWay from old(%v) to new(%v)", exporter.GetUmpCollectMethod(), view.UmpCollectWay)
		exporter.SetUMPCollectMethod(view.UmpCollectWay)
	}

	if w.dpSelectorName != view.DpSelectorName || w.dpSelectorParm != view.DpSelectorParm || w.quorum != view.Quorum {
		log.LogInfof("updateSimpleVolView: update dpSelector from old(%v %v) to new(%v %v), update quorum from old(%v) to new(%v)",
			w.dpSelectorName, w.dpSelectorParm, view.DpSelectorName, view.DpSelectorParm, w.quorum, view.Quorum)
		w.Lock()
		w.dpSelectorName = view.DpSelectorName
		w.dpSelectorParm = view.DpSelectorParm
		w.quorum = view.Quorum
		w.dpSelectorChanged = true
		w.Unlock()
	}

	if w.forceROW != view.ForceROW {
		log.LogInfof("updateSimpleVolView: update forceROW from old(%v) to new(%v)", w.forceROW, view.ForceROW)
		w.forceROW = view.ForceROW
	}

	if w.crossRegionHAType != view.CrossRegionHAType {
		log.LogInfof("updateSimpleVolView: update crossRegionHAType from old(%v) to new(%v)", w.crossRegionHAType, view.CrossRegionHAType)
		w.crossRegionHAType = view.CrossRegionHAType
	}

	if w.extentCacheExpireSec != view.ExtentCacheExpireSec {
		log.LogInfof("updateSimpleVolView: update ExtentCacheExpireSec from old(%v) to new(%v)", w.extentCacheExpireSec, view.ExtentCacheExpireSec)
		w.extentCacheExpireSec = view.ExtentCacheExpireSec
	}

	if w.ecEnable != view.EcEnable {
		log.LogInfof("updateSimpleVolView: update EcEnable from old(%v) to new(%v)", w.ecEnable, view.EcEnable)
		w.ecEnable = view.EcEnable
	}

	w.updateRemoteCacheConfig(view)
	w.updateConnConfig(view.ConnConfig)
	w.updateDpMetricsReportConfig(view.DpMetricsReportConfig)
	w.updateDpFollowerReadDelayConfig(&view.DpFolReadDelayConfig)
	if w.dpLowestDelayHostWeight != view.FolReadHostWeight {
		log.LogInfof("updateSimpleVolView: update FolReadHostWeight from old(%v) to new(%v)", w.dpLowestDelayHostWeight, view.FolReadHostWeight)
		w.dpLowestDelayHostWeight = view.FolReadHostWeight
	}
	return nil
}

func (w *Wrapper) updateRemoteCacheConfig(view *proto.SimpleVolView) {
	if w.enableRemoteCache != view.RemoteCacheBoostEnable {
		log.LogInfof("updateRemoteCacheConfig: RemoteCacheBoostEnable from old(%v) to new(%v)", w.enableRemoteCache, view.RemoteCacheBoostEnable)
		w.enableRemoteCache = view.RemoteCacheBoostEnable
	}
	if w.oldCacheStatus != w.EnableRemoteCache() {
		log.LogInfof("updateRemoteCacheConfig: enable from old(%v) to new(%v)", w.oldCacheStatus, w.EnableRemoteCache())
	}
	// remoteCache may be nil if the first initialization failed, it will not be set nil anymore even if remote cache is disabled
	if w.EnableRemoteCache() {
		if !w.oldCacheStatus || w.remoteCache == nil {
			log.LogInfof("updateRemoteCacheConfig: initRemoteCache: enable(%v -> %v) remoteCache isNil(%v)", w.oldCacheStatus, w.EnableRemoteCache(), w.remoteCache == nil)
			if err := w.initRemoteCache(); err != nil {
				log.LogErrorf("updateRemoteCacheConfig: initRemoteCache failed, err: %v", err)
			}
		}
	} else if w.oldCacheStatus && w.remoteCache != nil {
		w.remoteCache.Stop()
		log.LogInfof("updateRemoteCacheConfig: stop remoteCache")
	}

	if w.cacheBoostPath != view.RemoteCacheBoostPath {
		oldBoostPath := w.cacheBoostPath
		w.cacheBoostPath = view.RemoteCacheBoostPath
		if w.EnableRemoteCache() && w.remoteCache != nil {
			if !w.remoteCache.ResetCacheBoostPathToBloom(view.RemoteCacheBoostPath) {
				w.cacheBoostPath = ""
			}
		}
		log.LogInfof("updateRemoteCacheConfig: RemoteCacheBoostPath from old(%v) to want(%v), but(%v)", oldBoostPath, view.RemoteCacheBoostPath, w.cacheBoostPath)
	}

	if w.enableCacheAutoPrepare != view.RemoteCacheAutoPrepare {
		log.LogInfof("updateRemoteCacheConfig: RemoteCacheAutoPrepare from old(%v) to new(%v)", w.enableCacheAutoPrepare, view.RemoteCacheAutoPrepare)
		w.enableCacheAutoPrepare = view.RemoteCacheAutoPrepare
	}

	if w.cacheTTL != view.RemoteCacheTTL {
		log.LogInfof("updateRemoteCacheConfig: RemoteCacheTTL from old(%d) to new(%d)", w.cacheTTL, view.RemoteCacheTTL)
		w.cacheTTL = view.RemoteCacheTTL
	}

}

func (w *Wrapper) updateDataPartition(isInit bool) (err error) {
	var dpv *proto.DataPartitionsView
	if dpv, err = w.fetchDataPartition(); err != nil {
		return
	}
	return w.convertDataPartition(dpv, isInit)
}

func (w *Wrapper) fetchDataPartition() (dpv *proto.DataPartitionsView, err error) {
	if dpv, err = w.mc.ClientAPI().GetDataPartitions(w.volName); err != nil {
		if err == proto.ErrVolNotExists {
			w.volNotExistCount++
		}
		log.LogWarnf("updateDataPartition: get data partitions fail: volume(%v) notExistCount(%v) err(%v)", w.volName, w.volNotExistCount, err)
		return
	}
	if w.volNotExistCount > VolNotExistClearViewThresholdMin {
		w.partitions = new(sync.Map)
		log.LogInfof("updateDataPartition: clear volNotExistCount(%v) and data partitions", w.volNotExistCount)
	}
	w.volNotExistCount = 0
	log.LogInfof("updateDataPartition: get data partitions: volume(%v) partitions(%v) notExistCount(%v)", w.volName, len(dpv.DataPartitions), w.volNotExistCount)
	return
}

func (w *Wrapper) convertDataPartition(dpv *proto.DataPartitionsView, isInit bool) (err error) {
	var convert = func(response *proto.DataPartitionResponse) *DataPartition {
		return &DataPartition{
			DataPartitionResponse: *response,
			ClientWrapper:         w,
			CrossRegionMetrics:    NewCrossRegionMetrics(),
		}
	}

	rwPartitionGroups := make([]*DataPartition, 0)
	for _, partition := range dpv.DataPartitions {
		dp := convert(partition)
		if len(dp.Hosts) == 0 {
			log.LogWarnf("updateDataPartition: no host in dp(%v)", dp)
			continue
		}
		//log.LogInfof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(dp)
		if dp.Status == proto.ReadWrite {
			rwPartitionGroups = append(rwPartitionGroups, dp)
		}
	}

	// isInit used to identify whether this call is caused by mount action
	if isInit || (len(rwPartitionGroups) >= MinWriteAbleDataPartitionCnt) {
		log.LogInfof("updateDataPartition: update rwPartitionGroups count(%v)", len(rwPartitionGroups))
		w.refreshDpSelector(rwPartitionGroups)
	} else {
		err = errors.New("updateDataPartition: no writable data partition")
	}

	log.LogInfof("updateDataPartition: finish")
	return err
}

func (w *Wrapper) saveDataPartition() *proto.DataPartitionsView {
	dpv := &proto.DataPartitionsView{
		DataPartitions: make([]*proto.DataPartitionResponse, 0),
	}
	w.partitions.Range(func(k, v interface{}) bool {
		dp := v.(*DataPartition)
		dpv.DataPartitions = append(dpv.DataPartitions, &dp.DataPartitionResponse)
		return true
	})
	return dpv
}

func (w *Wrapper) replaceOrInsertPartition(dp *DataPartition) {
	if w.CrossRegionHATypeQuorum() {
		w.initCrossRegionHostStatus(dp)
		dp.CrossRegionMetrics.Lock()
		dp.CrossRegionMetrics.CrossRegionHosts = w.classifyCrossRegionHosts(dp.Hosts)
		log.LogDebugf("classifyCrossRegionHosts: dp(%v) hosts(%v) crossRegionMetrics(%v)", dp.PartitionID, dp.Hosts, dp.CrossRegionMetrics)
		dp.CrossRegionMetrics.Unlock()
	} else if w.followerRead && w.nearRead {
		dp.NearHosts = w.sortHostsByDistance(dp.Hosts)
	}

	w.Lock()
	value, ok := w.partitions.Load(dp.PartitionID)
	if ok {
		old := value.(*DataPartition)
		if old.Status != dp.Status || old.ReplicaNum != dp.ReplicaNum ||
			old.EcMigrateStatus != dp.EcMigrateStatus || old.ecEnable != w.ecEnable ||
			strings.Join(old.EcHosts, ",") != strings.Join(dp.EcHosts, ",") ||
			strings.Join(old.Hosts, ",") != strings.Join(dp.Hosts, ",") {
			log.LogInfof("updateDataPartition: dp (%v) --> (%v)", old, dp)
		}
		if !isLeaderExist(old.GetLeaderAddr(), dp.Hosts) {
			if dp.GetLeaderAddr() != "" {
				old.LeaderAddr = proto.NewAtomicString(dp.GetLeaderAddr())
			} else {
				old.LeaderAddr = proto.NewAtomicString(dp.Hosts[0])
			}
		}
		old.Status = dp.Status
		old.ReplicaNum = dp.ReplicaNum
		old.Hosts = dp.Hosts
		old.NearHosts = dp.NearHosts
		old.EcMigrateStatus = dp.EcMigrateStatus
		old.EcHosts = dp.EcHosts
		old.EcMaxUnitSize = dp.EcMaxUnitSize
		old.EcDataNum = dp.EcDataNum
		old.CrossRegionMetrics.Lock()
		old.CrossRegionMetrics.CrossRegionHosts = dp.CrossRegionMetrics.CrossRegionHosts
		old.CrossRegionMetrics.Unlock()
		dp.Metrics = old.Metrics
		dp.ReadMetrics = old.ReadMetrics
		dp.timeoutCnt = atomic.LoadInt32(&old.timeoutCnt)
		old.ecEnable = w.ecEnable
	} else {
		dp.Metrics = proto.NewDataPartitionMetrics()
		dp.ReadMetrics = proto.NewDPReadMetrics()
		dp.ecEnable = w.ecEnable
		w.partitions.Store(dp.PartitionID, dp)
		log.LogInfof("updateDataPartition: new dp (%v) EcMigrateStatus (%v)", dp, dp.EcMigrateStatus)
	}
	w.Unlock()

}

func isLeaderExist(addr string, hosts []string) bool {
	for _, host := range hosts {
		if addr == host {
			return true
		}
	}
	return false
}

func (w *Wrapper) getDataPartitionByPid(partitionID uint64) (err error) {
	var dpInfo *proto.DataPartitionInfo
	start := time.Now()
	for {
		if dpInfo, err = w.mc.AdminAPI().GetDataPartition(w.volName, partitionID); err == nil {
			if len(dpInfo.Hosts) > 0 {
				log.LogInfof("getDataPartitionByPid: pid(%v) vol(%v)", partitionID, w.volName)
				break
			}
			err = fmt.Errorf("master return empty host list")
		}
		if err != nil && time.Since(start) > MasterNoCacheAPIRetryTimeout {
			log.LogWarnf("getDataPartitionByPid: err(%v) pid(%v) vol(%v) retry timeout(%v)", err, partitionID, w.volName, time.Since(start))
			return
		}
		log.LogWarnf("getDataPartitionByPid: err(%v) pid(%v) vol(%v) retry next round", err, partitionID, w.volName)
		time.Sleep(1 * time.Second)
	}
	var convert = func(dpInfo *proto.DataPartitionInfo) *DataPartition {
		dp := &DataPartition{
			ClientWrapper: w,
			DataPartitionResponse: proto.DataPartitionResponse{
				PartitionID: dpInfo.PartitionID,
				Status:      dpInfo.Status,
				ReplicaNum:  dpInfo.ReplicaNum,
				Hosts:       dpInfo.Hosts,
				LeaderAddr:  proto.NewAtomicString(getDpInfoLeaderAddr(dpInfo)),
			},
			CrossRegionMetrics: NewCrossRegionMetrics(),
		}
		return dp
	}
	dp := convert(dpInfo)
	log.LogInfof("getDataPartitionByPid: dp(%v) leader(%v)", dp, dp.GetLeaderAddr())
	w.replaceOrInsertPartition(dp)
	return nil
}

func getDpInfoLeaderAddr(partition *proto.DataPartitionInfo) (leaderAddr string) {
	for _, replica := range partition.Replicas {
		if replica.IsLeader {
			return replica.Addr
		}
	}
	return
}

// GetDataPartition returns the data partition based on the given partition ID.
func (w *Wrapper) GetDataPartition(partitionID uint64) (dp *DataPartition, err error) {
	value, ok := w.partitions.Load(partitionID)
	if !ok {
		w.getDataPartitionByPid(partitionID)
		value, ok = w.partitions.Load(partitionID)
		if !ok {
			return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
		}
	}
	dp = value.(*DataPartition)
	return dp, nil
}

//// WarningMsg returns the warning message that contains the cluster name.
//func (w *Wrapper) WarningMsg() string {
//	return fmt.Sprintf("%s_client_warning", w.clusterName)
//}

func (w *Wrapper) fetchClusterView() (cv *proto.ClusterView, err error) {
	cv, err = w.mc.AdminAPI().GetCluster()
	if err != nil {
		log.LogWarnf("updateDataNodeStatus: get cluster fail: err(%v)", err)
	}
	return
}

func (w *Wrapper) fetchClientClusterView() (cf *proto.ClientClusterConf, err error) {
	cf, err = w.mc.AdminAPI().GetClientConf()
	if err != nil {
		log.LogWarnf("fetchClientConfView: getClientConf fail: err(%v)", err)
	}
	return
}

func (w *Wrapper) updateClientClusterView() (err error) {
	var cf *proto.ClientClusterConf
	if cf, err = w.fetchClientClusterView(); err != nil {
		return
	}
	w._updateDataNodeStatus(cf)

	w.umpJmtpAddr = cf.UmpJmtpAddr
	exporter.SetUMPJMTPAddress(w.umpJmtpAddr)
	exporter.SetUmpJMTPBatch(uint(cf.UmpJmtpBatch))

	w.setClusterBoostEnable(cf.RemoteCacheBoostEnable)
	w.setClusterTimeoutUs(cf.NetConnTimeoutUs)
	if w.EnableRemoteCache() && w.remoteCache != nil {
		w.remoteCache.ResetConnConfig(cf.NetConnTimeoutUs)
	}
	return
}

func (w *Wrapper) _updateDataNodeStatus(cf *proto.ClientClusterConf) {
	newHostsStatus := make(map[string]bool)
	for _, node := range cf.DataNodes {
		newHostsStatus[node.Addr] = node.Status
	}

	for _, node := range cf.EcNodes {
		newHostsStatus[node.Addr] = node.Status
	}
	log.LogInfof("updateDataNodeStatus: update %d hosts status", len(newHostsStatus))

	w.Lock()
	w.HostsStatus = newHostsStatus
	w.Unlock()

	if w.dpMetricsReportDomain != cf.SchedulerDomain {
		log.LogInfof("updateDataNodeStatus: update scheduler domain from old(%v) to new(%v)", w.dpMetricsReportDomain, cf.SchedulerDomain)
		w.dpMetricsReportDomain = cf.SchedulerDomain
		w.schedulerClient.UpdateSchedulerDomain(w.dpMetricsReportDomain)
	}
	return
}

func (w *Wrapper) saveClientClusterView() *proto.ClientClusterConf {
	w.RLock()
	defer w.RUnlock()
	cf := &proto.ClientClusterConf{
		DataNodes:       make([]proto.NodeView, 0, len(w.HostsStatus)),
		SchedulerDomain: w.dpMetricsReportDomain,
	}
	for addr, status := range w.HostsStatus {
		cf.DataNodes = append(cf.DataNodes, proto.NodeView{Addr: addr, Status: status})
	}
	return cf
}

func (w *Wrapper) SetNearRead(nearRead bool) {
	w.nearRead = w.nearRead || nearRead
	log.LogInfof("SetNearRead: set nearRead to %v", w.nearRead)
}

func (w *Wrapper) NearRead() bool {
	return w.nearRead
}

func (w *Wrapper) SetMetaWrapper(metaWrapper *meta.MetaWrapper) {
	w.metaWrapper = metaWrapper
}

func (w *Wrapper) SetConnConfig() {
	w.connConfig = &proto.ConnConfig{
		IdleTimeoutSec:   IdleConnTimeoutData,
		ConnectTimeoutNs: ConnectTimeoutDataMs * int64(time.Millisecond),
		WriteTimeoutNs:   WriteTimeoutData * int64(time.Second),
		ReadTimeoutNs:    ReadTimeoutData * int64(time.Second),
	}
}

func (w *Wrapper) SetDpFollowerReadDelayConfig(enableCollect bool, delaySummaryInterval int64) {
	if w.dpFollowerReadDelayConfig == nil {
		w.dpFollowerReadDelayConfig = &proto.DpFollowerReadDelayConfig{}
	}
	w.dpFollowerReadDelayConfig.EnableCollect = enableCollect
	w.dpFollowerReadDelayConfig.DelaySummaryInterval = delaySummaryInterval
}

func (w *Wrapper) CrossRegionHATypeQuorum() bool {
	return w.crossRegionHAType == proto.CrossRegionHATypeQuorum
}

// Sort hosts by distance form local
func (w *Wrapper) sortHostsByDistance(dpHosts []string) []string {
	nearHost := make([]string, len(dpHosts))
	copy(nearHost, dpHosts)
	for i := 0; i < len(nearHost); i++ {
		for j := i + 1; j < len(nearHost); j++ {
			if distanceFromLocal(nearHost[i]) > distanceFromLocal(nearHost[j]) {
				nearHost[i], nearHost[j] = nearHost[j], nearHost[i]
			}
		}
	}
	return nearHost
}

func (w *Wrapper) updateConnConfig(config *proto.ConnConfig) {
	if config == nil {
		return
	}
	log.LogInfof("updateConnConfig: (%v)", config)
	updateConnPool := false
	if config.IdleTimeoutSec > 0 && config.IdleTimeoutSec != w.connConfig.IdleTimeoutSec {
		w.connConfig.IdleTimeoutSec = config.IdleTimeoutSec
		updateConnPool = true
	}
	if config.ConnectTimeoutNs > 0 && config.ConnectTimeoutNs != w.connConfig.ConnectTimeoutNs {
		w.connConfig.ConnectTimeoutNs = config.ConnectTimeoutNs
		updateConnPool = true
	}
	if config.WriteTimeoutNs > 0 && config.WriteTimeoutNs != w.connConfig.WriteTimeoutNs {
		atomic.StoreInt64(&w.connConfig.WriteTimeoutNs, config.WriteTimeoutNs)
	}
	if config.ReadTimeoutNs > 0 && config.ReadTimeoutNs != w.connConfig.ReadTimeoutNs {
		atomic.StoreInt64(&w.connConfig.ReadTimeoutNs, config.ReadTimeoutNs)
	}
	if updateConnPool && StreamConnPool != nil {
		StreamConnPool.UpdateTimeout(w.connConfig.IdleTimeoutSec, w.connConfig.ConnectTimeoutNs)
	}
}

func (w *Wrapper) updateDpMetricsReportConfig(config *proto.DpMetricsReportConfig) {
	if config == nil {
		return
	}
	log.LogInfof("updateDpMetricsReportConfig: (%v)", config)
	if w.dpMetricsReportConfig.EnableReport != config.EnableReport {
		w.dpMetricsReportConfig.EnableReport = config.EnableReport
	}
	if config.ReportIntervalSec > 0 && w.dpMetricsReportConfig.ReportIntervalSec != config.ReportIntervalSec {
		atomic.StoreInt64(&w.dpMetricsReportConfig.ReportIntervalSec, config.ReportIntervalSec)
	}
	if config.FetchIntervalSec > 0 && w.dpMetricsReportConfig.FetchIntervalSec != config.FetchIntervalSec {
		atomic.StoreInt64(&w.dpMetricsReportConfig.FetchIntervalSec, config.FetchIntervalSec)
	}
}

func (w *Wrapper) updateDpFollowerReadDelayConfig(config *proto.DpFollowerReadDelayConfig) {
	if config == nil || w.dpFollowerReadDelayConfig == nil {
		return
	}
	if w.dpFollowerReadDelayConfig.EnableCollect != config.EnableCollect {
		w.dpFollowerReadDelayConfig.EnableCollect = config.EnableCollect
	}
	if config.DelaySummaryInterval >= 0 && w.dpFollowerReadDelayConfig.DelaySummaryInterval != config.DelaySummaryInterval {
		atomic.StoreInt64(&w.dpFollowerReadDelayConfig.DelaySummaryInterval, config.DelaySummaryInterval)
	}
	log.LogInfof("updateDpFollowerReadDelayConfig: (%v)", w.dpFollowerReadDelayConfig)
}

func (w *Wrapper) VolNotExists() bool {
	if w.volNotExistCount > VolNotExistInterceptThresholdMin {
		log.LogWarnf("VolNotExists: vol(%v) count(%v) threshold(%v)", w.volName, w.volNotExistCount, VolNotExistInterceptThresholdMin)
		return true
	}
	return false
}

func distanceFromLocal(b string) int {
	remote := strings.Split(b, ":")[0]

	return iputil.GetDistance(net.ParseIP(LocalIP), net.ParseIP(remote))
}

func handleUmpAlarm(cluster, vol, act, msg string) {
	umpKeyCluster := fmt.Sprintf("%s_client_warning", cluster)
	umpMsgCluster := fmt.Sprintf("volume(%s) %s", vol, msg)
	exporter.WarningBySpecialUMPKey(umpKeyCluster, umpMsgCluster)

	umpKeyVol := fmt.Sprintf("%s_%s_warning", cluster, vol)
	umpMsgVol := fmt.Sprintf("act(%s) - %s", act, msg)
	exporter.WarningBySpecialUMPKey(umpKeyVol, umpMsgVol)
}
