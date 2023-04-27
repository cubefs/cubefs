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

package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/stat"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func apiToMetricsName(api string) (reqMetricName string) {
	var builder strings.Builder
	builder.WriteString("req")
	// prometheus metric not allow '/' in name, need to transfer to '_'
	builder.WriteString(strings.Replace(api, "/", "_", -1))
	return builder.String()
}

func doStatAndMetric(statName string, metric *exporter.TimePointCount, err error, metricLabels map[string]string) {
	if metric == nil {
		return
	}
	if metricLabels == nil {
		metric.Set(err)
	} else {
		metric.SetWithLabels(err, metricLabels)
	}

	startTime := metric.GetStartTime()
	stat.EndStat(statName, err, &startTime, 1)
}

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	Status     bool
	ID         uint64
	IsWritable bool
}

// NodeView provides the view of the data or meta node.
type InvalidNodeView struct {
	Addr     string
	ID       uint64
	OldID    uint64
	NodeType string
}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	Zones []*ZoneView
}

type NodeSetView struct {
	DataNodeLen int
	MetaNodeLen int
	MetaNodes   []proto.NodeView
	DataNodes   []proto.NodeView
}

func newNodeSetView(dataNodeLen, metaNodeLen int) *NodeSetView {
	return &NodeSetView{DataNodes: make([]proto.NodeView, 0), MetaNodes: make([]proto.NodeView, 0), DataNodeLen: dataNodeLen, MetaNodeLen: metaNodeLen}
}

// ZoneView define the view of zone
type ZoneView struct {
	Name    string
	Status  string
	NodeSet map[uint64]*NodeSetView
}

func newZoneView(name string) *ZoneView {
	return &ZoneView{NodeSet: make(map[uint64]*NodeSetView, 0), Name: name}
}

type badPartitionView = proto.BadPartitionView

func (m *Server) setClusterInfo(w http.ResponseWriter, r *http.Request) {
	var (
		quota uint32
		err   error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetClusterInfo))
	defer func() {
		doStatAndMetric(proto.AdminSetClusterInfo, metric, err, nil)
	}()

	if quota, err = parseAndExtractDirQuota(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if quota < proto.MinDirChildrenNumLimit {
		quota = proto.MinDirChildrenNumLimit
	}
	if err = m.cluster.setClusterInfo(quota); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set dir quota(min:%v, max:%v) to %v successfully",
		proto.MinDirChildrenNumLimit, math.MaxUint32, quota)))
}

// Set the threshold of the memory usage on each meta node.
// If the memory usage reaches this threshold, then all the mata partition will be marked as readOnly.
func (m *Server) setMetaNodeThreshold(w http.ResponseWriter, r *http.Request) {
	var (
		threshold float64
		err       error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetMetaNodeThreshold))
	defer func() {
		doStatAndMetric(proto.AdminSetMetaNodeThreshold, metric, err, nil)
	}()

	if threshold, err = parseAndExtractThreshold(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setMetaNodeThreshold(float32(threshold)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set threshold to %v successfully", threshold)))
}

// Turn on or off the automatic allocation of the data partitions.
// If DisableAutoAllocate == off, then we WILL NOT automatically allocate new data partitions for the volume when:
//  1. the used space is below the max capacity,
//  2. and the number of r&w data partition is less than 20.
//
// If DisableAutoAllocate == on, then we WILL automatically allocate new data partitions for the volume when:
//  1. the used space is below the max capacity,
//  2. and the number of r&w data partition is less than 20.
func (m *Server) setupAutoAllocation(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminClusterFreeze))
	defer func() {
		doStatAndMetric(proto.AdminClusterFreeze, metric, err, nil)
	}()

	if status, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setDisableAutoAllocate(status); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set DisableAutoAllocate to %v successfully", status)))
}

// View the topology of the cluster.
func (m *Server) getTopology(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetTopologyView))
	defer func() {
		doStatAndMetric(proto.GetTopologyView, metric, nil, nil)
	}()

	tv := &TopologyView{
		Zones: make([]*ZoneView, 0),
	}
	zones := m.cluster.t.getAllZones()
	for _, zone := range zones {
		cv := newZoneView(zone.name)
		cv.Status = zone.getStatusToString()
		tv.Zones = append(tv.Zones, cv)
		nsc := zone.getAllNodeSet()
		for _, ns := range nsc {
			nsView := newNodeSetView(ns.dataNodeLen(), ns.metaNodeLen())
			cv.NodeSet[ns.ID] = nsView
			ns.dataNodes.Range(func(key, value interface{}) bool {
				dataNode := value.(*DataNode)
				nsView.DataNodes = append(nsView.DataNodes, proto.NodeView{ID: dataNode.ID, Addr: dataNode.Addr,
					DomainAddr: dataNode.DomainAddr, Status: dataNode.isActive, IsWritable: dataNode.isWriteAble()})
				return true
			})
			ns.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)
				nsView.MetaNodes = append(nsView.MetaNodes, proto.NodeView{ID: metaNode.ID, Addr: metaNode.Addr,
					DomainAddr: metaNode.DomainAddr, Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
				return true
			})
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(tv))
}

func (m *Server) updateZone(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.UpdateZone))
	defer func() {
		doStatAndMetric(proto.UpdateZone, metric, err, nil)
	}()

	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(nameKey)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	status, err := extractStatus(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	zone, err := m.cluster.t.getZone(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeZoneNotExists, Msg: err.Error()})
		return
	}
	if status {
		zone.setStatus(normalZone)
	} else {
		zone.setStatus(unavailableZone)
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("update zone status to [%v] successfully", status)))
}

func (m *Server) listZone(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetAllZones))
	defer func() {
		doStatAndMetric(proto.GetAllZones, metric, nil, nil)
	}()

	zones := m.cluster.t.getAllZones()
	zoneViews := make([]*ZoneView, 0)
	for _, zone := range zones {
		cv := newZoneView(zone.name)
		cv.Status = zone.getStatusToString()
		zoneViews = append(zoneViews, cv)
	}
	sendOkReply(w, r, newSuccessHTTPReply(zoneViews))
}

func (m *Server) clusterStat(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminClusterStat))
	defer func() {
		doStatAndMetric(proto.AdminClusterStat, metric, nil, nil)
	}()

	cs := &proto.ClusterStatInfo{
		DataNodeStatInfo: m.cluster.dataNodeStatInfo,
		MetaNodeStatInfo: m.cluster.metaNodeStatInfo,
		ZoneStatInfo:     make(map[string]*proto.ZoneStat, 0),
	}
	for zoneName, zoneStat := range m.cluster.zoneStatInfos {
		cs.ZoneStatInfo[zoneName] = zoneStat
	}
	sendOkReply(w, r, newSuccessHTTPReply(cs))
}

func (m *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetCluster))
	defer func() {
		doStatAndMetric(proto.AdminGetCluster, metric, nil, nil)
	}()

	cv := &proto.ClusterView{
		Name:                m.cluster.Name,
		CreateTime:          time.Unix(m.cluster.CreateTime, 0).Format(proto.TimeFormat),
		LeaderAddr:          m.leaderInfo.addr,
		DisableAutoAlloc:    m.cluster.DisableAutoAllocate,
		MetaNodeThreshold:   m.cluster.cfg.MetaNodeThreshold,
		Applied:             m.fsm.applied,
		MaxDataPartitionID:  m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:       m.cluster.idAlloc.commonID,
		MaxMetaPartitionID:  m.cluster.idAlloc.metaPartitionID,
		MetaNodes:           make([]proto.NodeView, 0),
		DataNodes:           make([]proto.NodeView, 0),
		VolStatInfo:         make([]*proto.VolStatInfo, 0),
		BadPartitionIDs:     make([]proto.BadPartitionView, 0),
		BadMetaPartitionIDs: make([]proto.BadPartitionView, 0),
	}

	vols := m.cluster.allVolNames()
	cv.MetaNodes = m.cluster.allMetaNodes()
	cv.DataNodes = m.cluster.allDataNodes()
	cv.DataNodeStatInfo = m.cluster.dataNodeStatInfo
	cv.MetaNodeStatInfo = m.cluster.metaNodeStatInfo
	for _, name := range vols {
		stat, ok := m.cluster.volStatInfo.Load(name)
		if !ok {
			cv.VolStatInfo = append(cv.VolStatInfo, newVolStatInfo(name, 0, 0, 0, 0, 0))
			continue
		}
		cv.VolStatInfo = append(cv.VolStatInfo, stat.(*volStatInfo))
	}
	cv.BadPartitionIDs = m.cluster.getBadDataPartitionsView()
	cv.BadMetaPartitionIDs = m.cluster.getBadMetaPartitionsView()

	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *Server) getApiList(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetMasterApiList))
	defer func() {
		doStatAndMetric(proto.AdminGetMasterApiList, metric, nil, nil)
	}()

	sendOkReply(w, r, newSuccessHTTPReply(proto.GApiInfo))
}

func (m *Server) setApiQpsLimit(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		limit   uint32
		timeout uint32
		err     error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetApiQpsLimit))
	defer func() {
		doStatAndMetric(proto.AdminSetApiQpsLimit, metric, err, nil)
	}()

	if name, limit, timeout, err = parseRequestToSetApiQpsLimit(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.apiLimiter.SetLimiter(name, limit, timeout); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	//persist to rocksdb
	var qPath string
	if err, _, qPath = m.cluster.apiLimiter.IsApiNameValid(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.syncPutApiLimiterInfo(m.cluster.apiLimiter.IsFollowerLimiter(qPath)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("set api qps limit failed: %v", err)))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set api qps limit success: name: %v, limit: %v, timeout: %v",
		name, limit, timeout)))
	return
}

func (m *Server) getApiQpsLimit(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetMasterApiList))
	defer func() {
		doStatAndMetric(proto.AdminGetMasterApiList, metric, nil, nil)
	}()

	m.cluster.apiLimiter.m.RLock()
	v, err := json.Marshal(m.cluster.apiLimiter.limiterInfos)
	m.cluster.apiLimiter.m.RUnlock()
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("get api qps limit failed: %v", err)))
		return
	}

	limiterInfos := make(map[string]*ApiLimitInfo)
	json.Unmarshal(v, &limiterInfos)
	sendOkReply(w, r, newSuccessHTTPReply(limiterInfos))
}

func (m *Server) rmApiQpsLimit(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminRemoveApiQpsLimit))
	defer func() {
		doStatAndMetric(proto.AdminRemoveApiQpsLimit, metric, err, nil)
	}()

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.apiLimiter.RmLimiter(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	//persist to rocksdb
	var qPath string
	if err, _, qPath = m.cluster.apiLimiter.IsApiNameValid(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.syncPutApiLimiterInfo(m.cluster.apiLimiter.IsFollowerLimiter(qPath)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("set api qps limit failed: %v", err)))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("rm api qps limit success: name: %v",
		name)))

}

func (m *Server) getIPAddr(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetIP))
	defer func() {
		doStatAndMetric(proto.AdminGetIP, metric, nil, nil)
	}()

	m.cluster.loadClusterValue()
	batchCount := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteBatchCount)
	limitRate := atomic.LoadUint64(&m.cluster.cfg.DataNodeDeleteLimitRate)
	deleteSleepMs := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)
	autoRepairRate := atomic.LoadUint64(&m.cluster.cfg.DataNodeAutoRepairLimitRate)
	dirChildrenNumLimit := atomic.LoadUint32(&m.cluster.cfg.DirChildrenNumLimit)

	cInfo := &proto.ClusterInfo{
		Cluster:                     m.cluster.Name,
		MetaNodeDeleteBatchCount:    batchCount,
		MetaNodeDeleteWorkerSleepMs: deleteSleepMs,
		DataNodeDeleteLimitRate:     limitRate,
		DataNodeAutoRepairLimitRate: autoRepairRate,
		DirChildrenNumLimit:         dirChildrenNumLimit,
		Ip:                          strings.Split(r.RemoteAddr, ":")[0],
		EbsAddr:                     m.bStoreAddr,
		ServicePath:                 m.servicePath,
		ClusterUuid:                 m.cluster.clusterUuid,
		ClusterUuidEnable:           m.cluster.clusterUuidEnable,
	}

	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}

func (m *Server) createMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		start   uint64
		err     error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreateMetaPartition))
	defer func() {
		doStatAndMetric(proto.AdminCreateMetaPartition, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if volName, start, err = validateRequestToCreateMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.updateInodeIDRange(volName, start); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprint("create meta partition successfully")))
}

func parsePreloadDpReq(r *http.Request, preload *DataPartitionPreLoad) (err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	preload.preloadZoneName = r.FormValue(zoneNameKey)

	if preload.PreloadCacheTTL, err = extractPositiveUint64(r, cacheTTLKey); err != nil {
		return
	}

	if preload.preloadCacheCapacity, err = extractPositiveUint(r, volCapacityKey); err != nil {
		return
	}

	if preload.preloadReplicaNum, err = extractUintWithDefault(r, replicaNumKey, 1); err != nil {
		return
	}

	if preload.preloadReplicaNum < 1 || preload.preloadReplicaNum > 16 {
		return fmt.Errorf("preload replicaNum must be between [%d] to [%d], now[%d]", 1, 16, preload.preloadReplicaNum)
	}

	return
}

func (m *Server) createPreLoadDataPartition(w http.ResponseWriter, r *http.Request) {

	var (
		volName string
		vol     *Vol
		err     error
		dps     []*DataPartition
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreatePreLoadDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminCreatePreLoadDataPartition, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if volName, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	log.LogInfof("action[createPreLoadDataPartition]")
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if !proto.IsCold(vol.VolType) {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("only low frequency volume can create preloadDp")))
		return
	}

	preload := new(DataPartitionPreLoad)
	err = parsePreloadDpReq(r, preload)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	total := vol.getPreloadCapacity() + uint64(preload.preloadCacheCapacity)
	if total > vol.CacheCapacity {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("preload total capacity[%d] can't be bigger than cache capacity [%d]",
			total, vol.CacheCapacity)))
		return
	}

	log.LogInfof("[createPreLoadDataPartition] start create preload dataPartition, vol(%s), req(%s)", volName, preload.toString())
	err, dps = m.cluster.batchCreatePreLoadDataPartition(vol, preload)
	if err != nil {
		log.LogErrorf("create data partition fail: volume(%v), req(%v) err(%v)", volName, preload.toString(), err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if len(dps) == 0 {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("create zero datapartition")))
		return
	}

	cv := proto.NewDataPartitionsView()
	dpResps := make([]*proto.DataPartitionResponse, 0)

	for _, dp := range dps {
		dpResp := dp.convertToDataPartitionResponse()
		dpResps = append(dpResps, dpResp)
	}

	log.LogDebugf("action[createPreLoadDataPartition] dps cnt[%v]  content[%v]", len(dps), dpResps)
	cv.DataPartitions = dpResps
	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *Server) getQosStatus(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		err     error
		vol     *Vol
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosGetStatus))
	defer func() {
		doStatAndMetric(proto.QosGetStatus, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if volName, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(vol.getQosStatus(m.cluster)))
}

func (m *Server) getClientQosInfo(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		err     error
		vol     *Vol
		host    string
		id      uint64
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosGetClientsLimitInfo))
	defer func() {
		doStatAndMetric(proto.QosGetClientsLimitInfo, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if volName, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if host = r.FormValue(addrKey); host != "" {
		log.LogInfof("action[getClientQosInfo] host %v", host)
		if !checkIp(host) {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
			return
		}
	}

	if value := r.FormValue(idKey); value != "" {
		if id, err = strconv.ParseUint(value, 10, 64); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	var rsp interface{}
	if rsp, err = vol.getClientLimitInfo(id, host); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	} else {
		sendOkReply(w, r, newSuccessHTTPReply(rsp))
	}
}

func (m *Server) getQosUpdateMasterLimit(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		value string
		limit uint64
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosUpdateMasterLimit))
	defer func() {
		doStatAndMetric(proto.QosUpdateMasterLimit, metric, err, nil)
	}()

	if value = r.FormValue(QosMasterLimit); value != "" {
		if limit, err = strconv.ParseUint(value, 10, 64); err != nil {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("wrong param of limit")))
			return
		}
		if limit < QosMasterAcceptCnt {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("limit too less than %v", QosMasterAcceptCnt)))
			return
		}

		m.cluster.cfg.QosMasterAcceptLimit = limit
		m.cluster.QosAcceptLimit.SetLimit(rate.Limit(limit))
		if err = m.cluster.syncPutCluster(); err != nil {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("set master not worked %v", err)))
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply("success"))
		return
	}
	sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("no param of limit")))
}

func (m *Server) QosUpdateClientParam(w http.ResponseWriter, r *http.Request) {
	var (
		volName            string
		value              string
		period, triggerCnt uint64
		err                error
		vol                *Vol
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosUpdateClientParam))
	defer func() {
		doStatAndMetric(proto.QosUpdateClientParam, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if volName, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if value = r.FormValue(ClientReqPeriod); value != "" {
		if period, err = strconv.ParseUint(value, 10, 64); err != nil || period == 0 {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("wrong param of peroid")))
		}
	}
	if value = r.FormValue(ClientTriggerCnt); value != "" {
		if triggerCnt, err = strconv.ParseUint(value, 10, 64); err != nil || triggerCnt == 0 {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("wrong param of triggerCnt")))
		}
	}
	if err = vol.updateClientParam(m.cluster, period, triggerCnt); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func parseRequestQos(r *http.Request, isMagnify bool, isEnableIops bool) (qosParam *qosArgs, err error) {
	qosParam = &qosArgs{}

	var value int
	var flowFmt int

	if isMagnify {
		flowFmt = 1
	} else {
		flowFmt = util.MB
	}

	if qosEnableStr := r.FormValue(QosEnableKey); qosEnableStr != "" {
		qosParam.qosEnable, _ = strconv.ParseBool(qosEnableStr)
	}

	if isEnableIops {
		if iopsRLimitStr := r.FormValue(IopsRKey); iopsRLimitStr != "" {
			log.LogInfof("actin[parseRequestQos] iopsRLimitStr %v", iopsRLimitStr)
			if value, err = strconv.Atoi(iopsRLimitStr); err == nil {
				qosParam.iopsRVal = uint64(value)
				if !isMagnify && qosParam.iopsRVal < MinIoLimit {
					err = fmt.Errorf("iops read %v need larger than 100", value)
					return
				}
				if isMagnify && (qosParam.iopsRVal < MinMagnify || qosParam.iopsRVal > MaxMagnify) {
					err = fmt.Errorf("iops read magnify %v must between %v and %v", value, MinMagnify, MaxMagnify)
					log.LogErrorf("acttion[parseRequestQos] %v", err.Error())
					return
				}
			}
		}

		if iopsWLimitStr := r.FormValue(IopsWKey); iopsWLimitStr != "" {
			log.LogInfof("actin[parseRequestQos] iopsWLimitStr %v", iopsWLimitStr)
			if value, err = strconv.Atoi(iopsWLimitStr); err == nil {
				qosParam.iopsWVal = uint64(value)
				if !isMagnify && qosParam.iopsWVal < MinIoLimit {
					err = fmt.Errorf("iops %v write write io larger than 100", value)
					return
				}
				if isMagnify && (qosParam.iopsWVal < MinMagnify || qosParam.iopsWVal > MaxMagnify) {
					err = fmt.Errorf("iops write magnify %v must between %v and %v", value, MinMagnify, MaxMagnify)
					log.LogErrorf("acttion[parseRequestQos] %v", err.Error())
					return
				}
			}
		}

	}

	if flowRLimitStr := r.FormValue(FlowRKey); flowRLimitStr != "" {
		log.LogInfof("actin[parseRequestQos] flowRLimitStr %v", flowRLimitStr)
		if value, err = strconv.Atoi(flowRLimitStr); err == nil {
			qosParam.flowRVal = uint64(value * flowFmt)
			if !isMagnify && (qosParam.flowRVal < MinFlowLimit || qosParam.flowRVal > MaxFlowLimit) {
				err = fmt.Errorf("flow read %v should be between 100M and 10TB ", value)
				return
			}
			if isMagnify && (qosParam.flowRVal < MinMagnify || qosParam.flowRVal > MaxMagnify) {
				err = fmt.Errorf("flow read magnify %v must between %v and %v", value, MinMagnify, MaxMagnify)
				log.LogErrorf("acttion[parseRequestQos] %v", err.Error())
				return
			}
		}
	}
	if flowWLimitStr := r.FormValue(FlowWKey); flowWLimitStr != "" {
		log.LogInfof("actin[parseRequestQos] flowWLimitStr %v", flowWLimitStr)
		if value, err = strconv.Atoi(flowWLimitStr); err == nil {
			qosParam.flowWVal = uint64(value * flowFmt)
			if !isMagnify && (qosParam.flowWVal < MinFlowLimit || qosParam.flowWVal > MaxFlowLimit) {
				err = fmt.Errorf("flow write %v should be between 100M and 10TB", value)
				log.LogErrorf("acttion[parseRequestQos] %v", err.Error())
				return
			}
			if isMagnify && (qosParam.flowWVal < MinMagnify || qosParam.flowWVal > MaxMagnify) {
				err = fmt.Errorf("flow write magnify %v must between %v and %v", value, MinMagnify, MaxMagnify)
				log.LogErrorf("acttion[parseRequestQos] %v", err.Error())
				return
			}
		}
	}

	log.LogInfof("action[parseRequestQos] result %v", qosParam)

	return
}

func (m *Server) QosUpdateMagnify(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		err         error
		vol         *Vol
		magnifyArgs *qosArgs
	)
	if volName, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if vol, err = m.cluster.getVol(volName); err == nil {
		if magnifyArgs, err = parseRequestQos(r, true, false); err == nil {
			_ = vol.volQosUpdateMagnify(m.cluster, magnifyArgs)
			sendOkReply(w, r, newSuccessHTTPReply("success"))
		}
	}
	sendErrReply(w, r, newErrHTTPReply(err))
}

// flowRVal, flowWVal take MB as unit
func (m *Server) QosUpdateZoneLimit(w http.ResponseWriter, r *http.Request) {
	var (
		value    interface{}
		ok       bool
		err      error
		qosParam *qosArgs
		enable   bool
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosUpdateZoneLimit))
	defer func() {
		doStatAndMetric(proto.QosUpdateZoneLimit, metric, err, nil)
	}()

	var zoneName string
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	if qosParam, err = parseRequestQos(r, false, true); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if sVal := r.FormValue(DiskEnableKey); sVal != "" {
		if enable, err = strconv.ParseBool(sVal); err == nil {
			log.LogInfof("action[DiskQosUpdate] enable be set [%v]", enable)
			m.cluster.diskQosEnable = enable
			err = m.cluster.syncPutCluster()
		}
	}

	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("zonename [%v] not found", zoneName)))
		return
	}
	zone := value.(*Zone)
	zone.updateDataNodeQosLimit(m.cluster, qosParam)

	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

// flowRVal, flowWVal take MB as unit
func (m *Server) QosGetZoneLimit(w http.ResponseWriter, r *http.Request) {
	var (
		value interface{}
		ok    bool
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosGetZoneLimitInfo))
	defer func() {
		doStatAndMetric(proto.QosGetZoneLimitInfo, metric, nil, nil)
	}()

	var zoneName string
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}

	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("zonename [%v] not found", zoneName)))
		return
	}
	zone := value.(*Zone)

	type qosZoneStatus struct {
		Zone            string
		DiskLimitEnable bool
		IopsRVal        uint64
		IopsWVal        uint64
		FlowRVal        uint64
		FlowWVal        uint64
	}

	zoneSt := &qosZoneStatus{
		Zone:            zoneName,
		DiskLimitEnable: m.cluster.diskQosEnable,
		IopsRVal:        zone.QosIopsRLimit,
		IopsWVal:        zone.QosIopsWLimit,
		FlowRVal:        zone.QosFlowRLimit,
		FlowWVal:        zone.QosFlowWLimit,
	}
	sendOkReply(w, r, newSuccessHTTPReply(zoneSt))
}

func (m *Server) QosUpdate(w http.ResponseWriter, r *http.Request) {
	var (
		volName   string
		err       error
		vol       *Vol
		enable    bool
		value     string
		limitArgs *qosArgs
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosUpdate))
	defer func() {
		doStatAndMetric(proto.QosUpdate, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if volName, err = extractName(r); err == nil {
		if vol, err = m.cluster.getVol(volName); err != nil {
			goto RET
		}
		if value = r.FormValue(QosEnableKey); value != "" {
			if enable, err = strconv.ParseBool(value); err != nil {
				goto RET
			}

			if err = vol.volQosEnable(m.cluster, enable); err != nil {
				goto RET
			}
			log.LogInfof("action[DiskQosUpdate] update qos eanble [%v]", enable)
		}
		if limitArgs, err = parseRequestQos(r, false, false); err == nil && limitArgs.isArgsWork() {
			if err = vol.volQosUpdateLimit(m.cluster, limitArgs); err != nil {
				goto RET
			}
			log.LogInfof("action[DiskQosUpdate] update qos limit [%v] [%v] [%v] [%v] [%v]", enable,
				limitArgs.iopsRVal, limitArgs.iopsWVal, limitArgs.flowRVal, limitArgs.flowWVal)
		}
	}

RET:
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	_ = sendOkReply(w, r, newSuccessHTTPReply("success"))
	return
}

func (m *Server) createDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                     string
		volName                    string
		vol                        *Vol
		reqCreateCount             int
		lastTotalDataPartitions    int
		clusterTotalDataPartitions int
		err                        error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreateDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminCreateDataPartition, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if reqCreateCount, volName, err = parseRequestToCreateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if proto.IsCold(vol.VolType) {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("low frequency vol can't create dp")))
		return
	}

	lastTotalDataPartitions = len(vol.dataPartitions.partitions)
	clusterTotalDataPartitions = m.cluster.getDataPartitionCount()
	err = m.cluster.batchCreateDataPartition(vol, reqCreateCount, false)
	rstMsg = fmt.Sprintf(" createDataPartition succeeeds. "+
		"clusterLastTotalDataPartitions[%v],vol[%v] has %v data partitions previously and %v data partitions now",
		clusterTotalDataPartitions, volName, lastTotalDataPartitions, len(vol.dataPartitions.partitions))
	if err != nil {
		log.LogErrorf("create data partition fail: volume(%v) err(%v)", volName, err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) changeDataPartitionLeader(w http.ResponseWriter, r *http.Request) {
	var (
		dp          *DataPartition
		partitionID uint64
		err         error
		host        string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDataPartitionChangeLeader))
	defer func() {
		doStatAndMetric(proto.AdminDataPartitionChangeLeader, metric, err, nil)
	}()

	if partitionID, _, err = parseRequestToGetDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	if host = r.FormValue(addrKey); host == "" {
		err = keyNotFound(addrKey)
		return
	}
	if !checkIp(host) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
		return
	}
	if err = dp.tryToChangeLeaderByHost(host); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rstMsg := fmt.Sprintf(" changeDataPartitionLeader command sucess send to dest host but need check. ")
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) getDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		dp          *DataPartition
		partitionID uint64
		volName     string
		vol         *Vol
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminGetDataPartition, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if partitionID, volName, err = parseRequestToGetDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if volName != "" {
		if vol, err = m.cluster.getVol(volName); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
			return
		}
		if dp, err = vol.getDataPartitionByID(partitionID); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
			return
		}
	} else {
		if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
			return
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(dp.buildDpInfo(m.cluster)))
}

// Load the data partition.
func (m *Server) loadDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		dp          *DataPartition
		partitionID uint64
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminLoadDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminLoadDataPartition, metric, err, nil)
	}()

	if partitionID, err = parseRequestToLoadDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	m.cluster.loadDataPartition(dp)
	msg = fmt.Sprintf(proto.AdminLoadDataPartition+"partitionID :%v  load data partition successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		partitionID uint64
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminAddDataReplica))
	defer func() {
		doStatAndMetric(proto.AdminAddDataReplica, metric, err, nil)
	}()

	if partitionID, addr, err = parseRequestToAddDataReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	if err = m.cluster.addDataReplica(dp, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	dp.Status = proto.ReadOnly
	dp.isRecover = true
	m.cluster.putBadDataPartitionIDs(nil, addr, dp.PartitionID)

	msg = fmt.Sprintf("data partitionID :%v  add replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) deleteDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		partitionID uint64
		err         error
		force       bool // now only used in two replicas in the scenario of no leader
		raftForce   bool
		vol         *Vol
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDeleteDataReplica))
	defer func() {
		doStatAndMetric(proto.AdminDeleteDataReplica, metric, err, nil)
	}()

	if partitionID, addr, err = parseRequestToRemoveDataReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	// force only be used in scenario that dp of two replicas volume no leader caused by one replica crash
	raftForce, err = parseRaftForce(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	force, err = pareseBoolWithDefault(r, forceKey, false)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.removeDataReplica(dp, addr, !force, raftForce); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if vol, err = m.cluster.getVol(dp.VolName); err != nil {
		log.LogErrorf("action[updateVol] err[%v]", err)
		err = proto.ErrVolNotExists
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	_ = vol.tryUpdateDpReplicaNum(m.cluster, dp)

	msg = fmt.Sprintf("data partitionID :%v  delete replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addMetaReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminAddMetaReplica))
	defer func() {
		doStatAndMetric(proto.AdminAddMetaReplica, metric, err, nil)
	}()

	if partitionID, addr, err = parseRequestToAddMetaReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	if err = m.cluster.addMetaReplica(mp, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	mp.IsRecover = true
	m.cluster.putBadMetaPartitions(addr, mp.PartitionID)
	msg = fmt.Sprintf("meta partitionID :%v  add replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) deleteMetaReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		mp          *MetaPartition
		partitionID uint64
		err         error
		force       bool
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDeleteMetaReplica))
	defer func() {
		doStatAndMetric(proto.AdminDeleteMetaReplica, metric, err, nil)
	}()

	if partitionID, addr, err = parseRequestToRemoveMetaReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	var value string
	if value = r.FormValue(forceKey); value != "" {
		force, _ = strconv.ParseBool(value)
	}
	if err = m.cluster.deleteMetaReplica(mp, addr, true, force); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("meta partitionID :%v  delete replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) changeMetaPartitionLeader(w http.ResponseWriter, r *http.Request) {
	var (
		mp          *MetaPartition
		partitionID uint64
		err         error
		host        string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminChangeMetaPartitionLeader))
	defer func() {
		doStatAndMetric(proto.AdminChangeMetaPartitionLeader, metric, err, nil)
	}()

	if partitionID, _, err = parseRequestToGetDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		log.LogErrorf("changeMetaPartitionLeader.err %v", err)
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		log.LogErrorf("changeMetaPartitionLeader.err %v", proto.ErrMetaPartitionNotExists)
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	if host = r.FormValue(addrKey); host == "" {
		err = keyNotFound(addrKey)
		log.LogErrorf("changeMetaPartitionLeader.err %v", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIp(host) {
		log.LogErrorf("changeMetaPartitionLeader.err addr not legal")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
		return
	}
	if err = mp.tryToChangeLeaderByHost(host); err != nil {
		log.LogErrorf("changeMetaPartitionLeader.err %v", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rstMsg := fmt.Sprintf(" changeMetaPartitionLeader command sucess send to dest host but need check. ")
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Decommission a data partition. This usually happens when disk error has been reported.
// This function needs to be called manually by the admin.
func (m *Server) decommissionDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		dp          *DataPartition
		addr        string
		partitionID uint64
		raftForce   bool
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDecommissionDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminDecommissionDataPartition, metric, err, nil)
	}()

	if partitionID, addr, err = parseRequestToDecommissionDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	raftForce, err = parseRaftForce(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if !dp.IsDecommissionInitial() {
		rstMsg = fmt.Sprintf(" dataPartitionID :%v  status %v not support decommission",
			partitionID, dp.GetDecommissionStatus())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: rstMsg})
		return
	}
	if dp.isSpecialReplicaCnt() {
		rstMsg = fmt.Sprintf(proto.AdminDecommissionDataPartition+" dataPartitionID :%v  is special replica cnt %v on node:%v async running,need check later",
			partitionID, dp.ReplicaNum, addr)
		go m.cluster.decommissionDataPartition(addr, dp, raftForce, handleDataPartitionOfflineErr)
		sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
		return
	}
	if err = m.cluster.decommissionDataPartition(addr, dp, raftForce, handleDataPartitionOfflineErr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if !dp.isSpecialReplicaCnt() {
		rstMsg = fmt.Sprintf(proto.AdminDecommissionDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
		sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
	}
}

func (m *Server) diagnoseDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err               error
		rstMsg            *proto.DataPartitionDiagnosis
		inactiveNodes     []string
		corruptDps        []*DataPartition
		lackReplicaDps    []*DataPartition
		badReplicaDps     []*DataPartition
		corruptDpIDs      []uint64
		lackReplicaDpIDs  []uint64
		badReplicaDpIDs   []uint64
		badDataPartitions []badPartitionView
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDiagnoseDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminDiagnoseDataPartition, metric, err, nil)
	}()

	corruptDpIDs = make([]uint64, 0)
	lackReplicaDpIDs = make([]uint64, 0)
	badReplicaDpIDs = make([]uint64, 0)
	if inactiveNodes, corruptDps, err = m.cluster.checkCorruptDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if lackReplicaDps, badReplicaDps, err = m.cluster.checkReplicaOfDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	for _, dp := range corruptDps {
		corruptDpIDs = append(corruptDpIDs, dp.PartitionID)
	}
	for _, dp := range lackReplicaDps {
		lackReplicaDpIDs = append(lackReplicaDpIDs, dp.PartitionID)
	}
	for _, dp := range badReplicaDps {
		badReplicaDpIDs = append(badReplicaDpIDs, dp.PartitionID)
	}

	badDataPartitions = m.cluster.getBadDataPartitionsView()
	rstMsg = &proto.DataPartitionDiagnosis{
		InactiveDataNodes:           inactiveNodes,
		CorruptDataPartitionIDs:     corruptDpIDs,
		LackReplicaDataPartitionIDs: lackReplicaDpIDs,
		BadDataPartitionIDs:         badDataPartitions,
		BadReplicaDataPartitionIDs:  badReplicaDpIDs,
	}
	log.LogInfof("diagnose dataPartition[%v] inactiveNodes:[%v], corruptDpIDs:[%v], lackReplicaDpIDs:[%v], BadReplicaDataPartitionIDs[%v]",
		m.cluster.Name, inactiveNodes, corruptDpIDs, lackReplicaDpIDs, badReplicaDpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) resetDataPartitionDecommissionStatus(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, err = parseRequestToLoadDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	dp.ResetDecommissionStatus()
	msg = fmt.Sprintf("partitionID :%v  reset decommission status successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) queryDataPartitionDecommissionStatus(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		dp          *DataPartition
		partitionID uint64
		err         error
		replicas    []string
	)

	if partitionID, err = parseRequestToLoadDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	for _, replica := range dp.Replicas {
		replicas = append(replicas, replica.Addr)
	}
	msg = fmt.Sprintf("partitionID :%v  status[%v] retry [%v] raftForce[%v] recover [%v] "+
		"decommission src dataNode[%v] disk[%v]  dst dataNode[%v] term[%v] replicas[%v]",
		partitionID, dp.GetDecommissionStatus(), dp.DecommissionRetry, dp.DecommissionRaftForce, dp.isRecover,
		dp.DecommissionSrcAddr, dp.DecommissionSrcDiskPath, dp.DecommissionDstAddr, dp.DecommissionTerm, replicas)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Mark the volume as deleted, which will then be deleted later.
func (m *Server) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		// force   bool
		err error
		msg string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDeleteVol))
	defer func() {
		doStatAndMetric(proto.AdminDeleteVol, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, authKey, _, err = parseRequestToDeleteVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.markDeleteVol(name, authKey, false); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.user.deleteVolPolicy(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("delete vol[%v] successfully,from[%v]", name, r.RemoteAddr)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) updateVol(w http.ResponseWriter, r *http.Request) {
	var (
		req          = &updateVolReq{}
		vol          *Vol
		err          error
		replicaNum   int
		followerRead bool
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateVol))
	defer func() {
		doStatAndMetric(proto.AdminUpdateVol, metric, err, map[string]string{exporter.Vol: req.name})
	}()

	if req.name, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(req.name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	if err = parseVolUpdateReq(r, vol, req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol.dpReplicaNum == 1 && !req.followRead {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "single replica must enable follower read"})
	}
	if followerRead, req.authenticate, err = parseBoolFieldToUpdateVol(r, vol); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if (replicaNum == 1 || replicaNum == 2) && !followerRead {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "single or two replica must enable follower read"})
		return
	}

	newArgs := getVolVarargs(vol)

	newArgs.zoneName = req.zoneName
	newArgs.description = req.description
	newArgs.capacity = req.capacity
	newArgs.followerRead = req.followRead
	newArgs.authenticate = req.authenticate
	newArgs.dpSelectorName = req.dpSelectorName
	newArgs.dpSelectorParm = req.dpSelectorParm
	newArgs.enablePosixAcl = req.enablePosixAcl
	if req.coldArgs != nil {
		newArgs.coldArgs = req.coldArgs
	}

	newArgs.dpReplicaNum = uint8(replicaNum)
	newArgs.dpReadOnlyWhenVolFull = req.dpReadOnlyWhenVolFull

	log.LogInfof("[updateVolOut] name [%s], z1 [%s], z2[%s]", req.name, req.zoneName, vol.Name)
	if err = m.cluster.updateVol(req.name, req.authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("update vol[%v] successfully\n", req.name)))
}

func (m *Server) volExpand(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		authKey  string
		err      error
		msg      string
		capacity int
		vol      *Vol
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolExpand))
	defer func() {
		doStatAndMetric(proto.AdminVolExpand, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, authKey, capacity, err = parseRequestToSetVolCapacity(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	if uint64(capacity) <= vol.Capacity {
		err = fmt.Errorf("expand capacity[%v] should be larger than the old capacity[%v]", capacity, vol.Capacity)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)
	newArgs.capacity = uint64(capacity)

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) volShrink(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		authKey  string
		err      error
		msg      string
		capacity int
		vol      *Vol
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolShrink))
	defer func() {
		doStatAndMetric(proto.AdminVolShrink, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, authKey, capacity, err = parseRequestToSetVolCapacity(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	if uint64(capacity) >= vol.Capacity {
		err = fmt.Errorf("shrink capacity[%v] should be less than the old capacity[%v]", capacity, vol.Capacity)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)
	newArgs.capacity = uint64(capacity)

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) checkCreateReq(req *createVolReq) (err error) {

	if !proto.IsHot(req.volType) && !proto.IsCold(req.volType) {
		return fmt.Errorf("vol type %d is illegal", req.volType)
	}

	if req.capacity == 0 {
		return fmt.Errorf("vol capacity can't be zero, %d", req.capacity)
	}

	if req.size != 0 && req.size <= 10 {
		return fmt.Errorf("datapartition size must be bigger than 10 G")
	}

	if proto.IsHot(req.volType) {
		if req.dpReplicaNum == 0 {
			req.dpReplicaNum = defaultReplicaNum
		}

		if req.dpReplicaNum > 3 {
			return fmt.Errorf("hot vol's replicaNum should be 1 to 3, received replicaNum is[%v]", req.dpReplicaNum)
		}
		return nil
	} else if proto.IsCold(req.volType) {
		if req.dpReplicaNum > 16 {
			return fmt.Errorf("cold vol's replicaNum should less then 17, received replicaNum is[%v]", req.dpReplicaNum)
		}
	}

	if req.dpReplicaNum == 0 {
		req.dpReplicaNum = 1
	}

	req.followerRead = true
	args := req.coldArgs

	if args.objBlockSize == 0 {
		args.objBlockSize = defaultEbsBlkSize
	}

	if err = checkCacheAction(args.cacheAction); err != nil {
		return
	}

	if args.cacheTtl == 0 {
		args.cacheTtl = defaultCacheTtl
	}

	if args.cacheThreshold == 0 {
		args.cacheThreshold = defaultCacheThreshold
	}

	if args.cacheHighWater == 0 {
		args.cacheHighWater = defaultCacheHighWater
	}

	if args.cacheLowWater == 0 {
		args.cacheLowWater = defaultCacheLowWater
	}

	if args.cacheLRUInterval != 0 && args.cacheLRUInterval < 2 {
		return fmt.Errorf("cache lruInterval(%d) must bigger than 2 minutes", args.cacheLRUInterval)
	}

	if args.cacheLRUInterval == 0 {
		args.cacheLRUInterval = defaultCacheLruInterval
	}

	if args.cacheLowWater >= args.cacheHighWater {
		return fmt.Errorf("low water(%d) must be less than high water(%d)", args.cacheLowWater, args.cacheHighWater)
	}

	if args.cacheCap >= uint64(req.capacity) {
		return fmt.Errorf("cache capacity(%d) must be less than capacity(%d)", args.cacheCap, req.capacity)
	}

	if args.cacheHighWater >= 90 || args.cacheLowWater >= 90 {
		return fmt.Errorf("low(%d) or high water(%d) can't be large than 90, low than 0", args.cacheLowWater, args.cacheHighWater)
	}

	if req.dpReplicaNum > m.cluster.dataNodeCount() {
		return fmt.Errorf("dp replicaNum %d can't be large than dataNodeCnt %d", req.dpReplicaNum, m.cluster.dataNodeCount())
	}

	req.coldArgs = args
	return nil

}

func (m *Server) createVol(w http.ResponseWriter, r *http.Request) {

	req := &createVolReq{}
	vol := &Vol{}

	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreateVol))
	defer func() {
		doStatAndMetric(proto.AdminCreateVol, metric, err, map[string]string{exporter.Vol: req.name})
	}()

	if err = parseRequestToCreateVol(r, req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.checkCreateReq(req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if (req.dpReplicaNum == 1 || req.dpReplicaNum == 2) && !req.followerRead {
		err = fmt.Errorf("replicaNum be 2 and 3,followerRead must set true")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.createVol(req); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.associateVolWithUser(req.owner, req.name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg := fmt.Sprintf("create vol[%v] successfully, has allocate [%v] data partitions", req.name, len(vol.dataPartitions.partitions))
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) qosUpload(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		name  string
		vol   *Vol
		limit *proto.LimitRsp2Client
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QosUpload))
	defer func() {
		doStatAndMetric(proto.QosUpload, metric, err, map[string]string{exporter.Vol: name})
	}()

	ctx := context.Background()
	m.cluster.QosAcceptLimit.WaitN(ctx, 1)
	log.LogInfof("action[qosUpload] limit %v", m.cluster.QosAcceptLimit.Limit())
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	qosEnableStr := r.FormValue(QosEnableKey)
	if qosEnableStr == "" {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrParamError))
		return
	}
	// qos upload may called by client init,thus use qosEnable param to identify it weather need to calc by master

	if qosEnable, _ := strconv.ParseBool(qosEnableStr); qosEnable {
		if clientInfo, err := parseQosInfo(r); err == nil {
			log.LogDebugf("action[qosUpload] cliInfoMgrMap [%v],clientInfo id[%v] clientInfo.Host %v, enable %v", clientInfo.ID, clientInfo.Host, r.RemoteAddr, qosEnable)
			if clientInfo.ID == 0 {
				if limit, err = vol.qosManager.init(m.cluster, clientInfo.Host); err != nil {
					sendErrReply(w, r, newErrHTTPReply(err))
				}
			} else if limit, err = vol.qosManager.HandleClientQosReq(clientInfo, clientInfo.ID); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
			}
		} else {
			log.LogInfof("action[qosUpload] qosEnableStr:[%v] err [%v]", qosEnableStr, err)
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(limit))
}

func (m *Server) getVolSimpleInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		name string
		vol  *Vol
	)
	metric := exporter.NewTPCnt("req" + strings.Replace(proto.AdminGetVol, "/", "_", -1))
	defer func() {
		doStatAndMetric(proto.AdminGetVol, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	volView := newSimpleView(vol)

	sendOkReply(w, r, newSuccessHTTPReply(volView))
}

func newSimpleView(vol *Vol) *proto.SimpleVolView {
	var (
		volInodeCount  uint64
		volDentryCount uint64
	)
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()

	for _, mp := range vol.MetaPartitions {
		volDentryCount = volDentryCount + mp.DentryCount
		volInodeCount = volInodeCount + mp.InodeCount
	}
	maxPartitionID := vol.maxPartitionID()
	return &proto.SimpleVolView{
		ID:                    vol.ID,
		Name:                  vol.Name,
		Owner:                 vol.Owner,
		ZoneName:              vol.zoneName,
		DpReplicaNum:          vol.dpReplicaNum,
		MpReplicaNum:          vol.mpReplicaNum,
		InodeCount:            volInodeCount,
		DentryCount:           volDentryCount,
		MaxMetaPartitionID:    maxPartitionID,
		Status:                vol.Status,
		Capacity:              vol.Capacity,
		FollowerRead:          vol.FollowerRead,
		EnablePosixAcl:        vol.enablePosixAcl,
		NeedToLowerReplica:    vol.NeedToLowerReplica,
		Authenticate:          vol.authenticate,
		CrossZone:             vol.crossZone,
		DefaultPriority:       vol.defaultPriority,
		DomainOn:              vol.domainOn,
		RwDpCnt:               vol.dataPartitions.readableAndWritableCnt,
		MpCnt:                 len(vol.MetaPartitions),
		DpCnt:                 len(vol.dataPartitions.partitionMap),
		CreateTime:            time.Unix(vol.createTime, 0).Format(proto.TimeFormat),
		Description:           vol.description,
		DpSelectorName:        vol.dpSelectorName,
		DpSelectorParm:        vol.dpSelectorParm,
		DpReadOnlyWhenVolFull: vol.DpReadOnlyWhenVolFull,
		VolType:               vol.VolType,
		ObjBlockSize:          vol.EbsBlkSize,
		CacheCapacity:         vol.CacheCapacity,
		CacheAction:           vol.CacheAction,
		CacheThreshold:        vol.CacheThreshold,
		CacheLruInterval:      vol.CacheLRUInterval,
		CacheTtl:              vol.CacheTTL,
		CacheLowWater:         vol.CacheLowWater,
		CacheHighWater:        vol.CacheHighWater,
		CacheRule:             vol.CacheRule,
		PreloadCapacity:       vol.getPreloadCapacity(),
	}
}

func checkIp(addr string) bool {

	ip := strings.Trim(addr, " ")
	regStr := `^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.)(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])`
	if match, _ := regexp.MatchString(regStr, ip); match {
		return true
	}
	return false
}

func checkIpPort(addr string) bool {
	var arr []string
	if arr = strings.Split(addr, ":"); len(arr) < 2 {
		return false
	}
	if id, err := strconv.ParseUint(arr[1], 10, 64); err != nil || id > 65535 || id < 1024 {
		return false
	}
	ip := strings.Trim(addr, " ")
	regStr := `^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.)(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])`
	if match, _ := regexp.MatchString(regStr, ip); match {
		return true
	}
	return false
}

func (m *Server) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  string
		zoneName  string
		id        uint64
		err       error
		nodesetId uint64
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AddDataNode))
	defer func() {
		doStatAndMetric(proto.AddDataNode, metric, err, nil)
	}()

	if nodeAddr, zoneName, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIpPort(nodeAddr) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
		return
	}
	var value string
	if value = r.FormValue(idKey); value == "" {
		nodesetId = 0
	} else {
		if nodesetId, err = strconv.ParseUint(value, 10, 64); err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}
	if id, err = m.cluster.addDataNode(nodeAddr, zoneName, nodesetId); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr     string
		dataNode     *DataNode
		dataNodeInfo *proto.DataNodeInfo
		err          error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetDataNode))
	defer func() {
		doStatAndMetric(proto.GetDataNode, metric, err, nil)
	}()

	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	dataNode.PersistenceDataPartitions = m.cluster.getAllDataPartitionIDByDatanode(nodeAddr)
	//some dp maybe removed from this node but decommission failed
	dataNodeInfo = &proto.DataNodeInfo{
		Total:                     dataNode.Total,
		Used:                      dataNode.Used,
		AvailableSpace:            dataNode.AvailableSpace,
		ID:                        dataNode.ID,
		ZoneName:                  dataNode.ZoneName,
		Addr:                      dataNode.Addr,
		DomainAddr:                dataNode.DomainAddr,
		ReportTime:                dataNode.ReportTime,
		IsActive:                  dataNode.isActive,
		IsWriteAble:               dataNode.isWriteAble(),
		UsageRatio:                dataNode.UsageRatio,
		SelectedTimes:             dataNode.SelectedTimes,
		Carry:                     dataNode.Carry,
		DataPartitionReports:      dataNode.DataPartitionReports,
		DataPartitionCount:        dataNode.DataPartitionCount,
		NodeSetID:                 dataNode.NodeSetID,
		PersistenceDataPartitions: dataNode.PersistenceDataPartitions,
		BadDisks:                  dataNode.BadDisks,
		RdOnly:                    dataNode.RdOnly,
		MaxDpCntLimit:             dataNode.GetDpCntLimit(),
	}

	sendOkReply(w, r, newSuccessHTTPReply(dataNodeInfo))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) decommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		offLineAddr string
		raftForce   bool
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DecommissionDataNode))
	defer func() {
		doStatAndMetric(proto.DecommissionDataNode, metric, err, nil)
	}()

	if offLineAddr, err = parseDecomDataNodeReq(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	raftForce, err = parseRaftForce(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if _, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if err = m.cluster.migrateDataNode(offLineAddr, "", raftForce, 0); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg = fmt.Sprintf("decommission data node [%v] submited!need check status later!", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) migrateDataNodeHandler(w http.ResponseWriter, r *http.Request) {
	var (
		srcAddr, targetAddr string
		limit               int
		raftForce           bool
		err                 error
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.MigrateDataNode))
	defer func() {
		doStatAndMetric(proto.MigrateDataNode, metric, err, nil)
	}()

	srcAddr, targetAddr, limit, err = parseMigrateNodeParam(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	raftForce, err = parseRaftForce(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	srcNode, err := m.cluster.dataNode(srcAddr)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeDataNodeNotExists, Msg: err.Error()})
		return
	}

	targetNode, err := m.cluster.dataNode(targetAddr)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeDataNodeNotExists, Msg: err.Error()})
		return
	}

	if srcNode.NodeSetID != targetNode.NodeSetID {
		err = fmt.Errorf("src %s and target %s must exist in the same nodeSet when migrate", srcAddr, targetAddr)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if !targetNode.isWriteAble() || !targetNode.dpCntInLimit() {
		err = fmt.Errorf("[%s] is not writable, can't used as target addr for migrate", targetAddr)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.migrateDataNode(srcAddr, targetAddr, raftForce, limit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg := fmt.Sprintf("migrateDataNodeHandler from src [%v] to target[%v] has submited and run in asyn ways,need check laster!", srcAddr, targetAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) cancelDecommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		node        *DataNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.CancelDecommissionDataNode))
	defer func() {
		doStatAndMetric(proto.CancelDecommissionDataNode, metric, err, nil)
	}()

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.decommissionDataNodeCancel(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("cancel decommission data node [%v] successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) setNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetNodeInfo))
	defer func() {
		doStatAndMetric(proto.AdminSetNodeInfo, metric, err, nil)
	}()

	if params, err = parseAndExtractSetNodeInfoParams(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if batchCount, ok := params[nodeDeleteBatchCountKey]; ok {
		if bc, ok := batchCount.(uint64); ok {
			if err = m.cluster.setMetaNodeDeleteBatchCount(bc); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[clusterLoadFactorKey]; ok {
		if factor, ok := val.(float32); ok {
			if err = m.cluster.setClusterLoadFactor(factor); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[nodeMarkDeleteRateKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setDataNodeDeleteLimitRate(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[nodeAutoRepairRateKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setDataNodeAutoRepairLimitRate(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[nodeDeleteWorkerSleepMs]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setMetaNodeDeleteWorkerSleepMs(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[maxDpCntLimitKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setMaxDpCntLimit(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[clusterCreateTimeKey]; ok {
		if createTimeParam, ok := val.(string); ok {
			var createTime time.Time
			var err error
			if createTime, err = time.ParseInLocation(proto.TimeFormat, createTimeParam, time.Local); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
			if err = m.cluster.setClusterCreateTime(createTime.Unix()); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodeinfo params %v successfully", params)))

}
func (m *Server) updateDataUseRatio(ratio float64) (err error) {

	m.cluster.domainManager.dataRatioLimit = ratio
	err = m.cluster.putZoneDomain(false)
	return
}
func (m *Server) updateExcludeZoneUseRatio(ratio float64) (err error) {

	m.cluster.domainManager.excludeZoneUseRatio = ratio
	err = m.cluster.putZoneDomain(false)
	return
}
func (m *Server) updateNodesetId(zoneName string, destNodesetId uint64, nodeType uint64, addr string) (err error) {
	var (
		nsId     uint64
		dstNs    *nodeSet
		srcNs    *nodeSet
		ok       bool
		value    interface{}
		metaNode *MetaNode
		dataNode *DataNode
	)
	defer func() {
		log.LogInfof("action[updateNodesetId] step out")
	}()
	log.LogWarnf("action[updateNodesetId] zonename[%v] destNodesetId[%v] nodeType[%v] addr[%v]",
		zoneName, destNodesetId, nodeType, addr)

	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		return fmt.Errorf("zonename [%v] not found", zoneName)
	}
	zone := value.(*Zone)
	if dstNs, ok = zone.nodeSetMap[destNodesetId]; !ok {
		return fmt.Errorf("%v destNodesetId not found", destNodesetId)
	}
	if uint32(nodeType) == TypeDataPartition {
		value, ok = zone.dataNodes.Load(addr)
		if !ok {
			return fmt.Errorf("addr %v not found", addr)
		}
		nsId = value.(*DataNode).NodeSetID
	} else if uint32(nodeType) == TypeMetaPartition {
		value, ok = zone.metaNodes.Load(addr)
		if !ok {
			return fmt.Errorf("addr %v not found", addr)
		}
		nsId = value.(*MetaNode).NodeSetID
	} else {
		return fmt.Errorf("%v wrong type", nodeType)
	}
	log.LogInfof("action[updateNodesetId] zonename[%v] destNodesetId[%v] nodeType[%v] addr[%v] get destid[%v]",
		zoneName, destNodesetId, nodeType, addr, dstNs.ID)
	srcNs = zone.nodeSetMap[nsId]
	if srcNs.ID == dstNs.ID {
		return fmt.Errorf("addr belong to same nodeset")
	} else if srcNs.ID < dstNs.ID {
		// take parallel call updatenodeid and defer unlock order into consider
		srcNs.Lock()
		dstNs.Lock()
		defer srcNs.Unlock()
		defer dstNs.Unlock()
	} else {
		// take parallel call updatenodeid and defer unlock order into consider
		dstNs.Lock()
		srcNs.Lock()
		defer dstNs.Unlock()
		defer srcNs.Unlock()
	}

	// the nodeset capcity not enlarged if node be added,capacity can be adjust by
	// AdminUpdateNodeSetCapcity
	if uint32(nodeType) == TypeDataPartition {
		if value, ok = srcNs.dataNodes.Load(addr); !ok {
			return fmt.Errorf("addr not found in srcNs.dataNodes")
		}
		dataNode = value.(*DataNode)
		dataNode.NodeSetID = dstNs.ID
		dstNs.putDataNode(dataNode)
		srcNs.deleteDataNode(dataNode)
		if err = m.cluster.syncUpdateDataNode(dataNode); err != nil {
			dataNode.NodeSetID = srcNs.ID
			return
		}
	} else {
		if value, ok = srcNs.metaNodes.Load(addr); !ok {
			return fmt.Errorf("ddr not found in srcNs.metaNodes")
		}
		metaNode = value.(*MetaNode)
		metaNode.NodeSetID = dstNs.ID
		dstNs.putMetaNode(metaNode)
		srcNs.deleteMetaNode(metaNode)
		if err = m.cluster.syncUpdateMetaNode(metaNode); err != nil {
			dataNode.NodeSetID = srcNs.ID
			return
		}
	}
	if err = m.cluster.syncUpdateNodeSet(dstNs); err != nil {
		return fmt.Errorf("warn:syncUpdateNodeSet dst srcNs [%v] failed", dstNs.ID)
	}
	if err = m.cluster.syncUpdateNodeSet(srcNs); err != nil {
		return fmt.Errorf("warn:syncUpdateNodeSet src srcNs [%v] failed", srcNs.ID)
	}

	return
}

func (m *Server) setDpRdOnly(partitionID uint64, rdOnly bool) (err error) {

	var dp *DataPartition
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		return fmt.Errorf("[setPartitionRdOnly] getDataPartitionByID err(%s)", err.Error())
	}
	dp.RLock()
	dp.RdOnly = rdOnly
	m.cluster.syncUpdateDataPartition(dp)
	dp.RUnlock()

	return
}

func (m *Server) setNodeRdOnly(addr string, nodeType uint32, rdOnly bool) (err error) {
	if nodeType == TypeDataPartition {
		m.cluster.dnMutex.Lock()
		defer m.cluster.dnMutex.Unlock()
		value, ok := m.cluster.dataNodes.Load(addr)
		if !ok {
			return fmt.Errorf("[setNodeRdOnly] data node %s is not exist", addr)
		}

		dataNode := value.(*DataNode)
		oldRdOnly := dataNode.RdOnly
		dataNode.RdOnly = rdOnly

		if err = m.cluster.syncUpdateDataNode(dataNode); err != nil {
			dataNode.RdOnly = oldRdOnly
			return fmt.Errorf("[setNodeRdOnly] syncUpdateDataNode err(%s)", err.Error())
		}

		return
	}

	m.cluster.mnMutex.Lock()
	defer m.cluster.mnMutex.Unlock()

	value, ok := m.cluster.metaNodes.Load(addr)
	if !ok {
		return fmt.Errorf("[setNodeRdOnly] meta node %s is not exist", addr)
	}

	metaNode := value.(*MetaNode)
	oldRdOnly := metaNode.RdOnly
	metaNode.RdOnly = rdOnly

	if err = m.cluster.syncUpdateMetaNode(metaNode); err != nil {
		metaNode.RdOnly = oldRdOnly
		return fmt.Errorf("[setNodeRdOnly] syncUpdateMetaNode err(%s)", err.Error())
	}

	return
}

func (m *Server) updateNodesetCapcity(zoneName string, nodesetId uint64, capcity uint64) (err error) {
	var ns *nodeSet
	var ok bool
	var value interface{}

	if capcity < defaultReplicaNum || capcity > 100 {
		err = fmt.Errorf("capcity [%v] value out of scope", capcity)
		return
	}

	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		err = fmt.Errorf("zonename [%v] not found", zoneName)
		return
	}

	zone := value.(*Zone)
	if ns, ok = zone.nodeSetMap[nodesetId]; !ok {
		err = fmt.Errorf("nodesetId [%v] not found", nodesetId)
		return
	}

	ns.Capacity = int(capcity)
	m.cluster.syncUpdateNodeSet(ns)
	log.LogInfof("action[updateNodesetCapcity] zonename %v nodeset %v capcity %v", zoneName, nodesetId, capcity)
	return
}

func (m *Server) buildNodeSetGrpInfoByID(domainId, grpId uint64) (*proto.SimpleNodeSetGrpInfo, error) {
	domainIndex := m.cluster.domainManager.domainId2IndexMap[domainId]
	nsgm := m.cluster.domainManager.domainNodeSetGrpVec[domainIndex]
	var index int
	for index = 0; index < len(nsgm.nodeSetGrpMap); index++ {
		if nsgm.nodeSetGrpMap[index].ID == grpId {
			break
		}
		if nsgm.nodeSetGrpMap[index].ID > grpId {
			return nil, fmt.Errorf("id not found")
		}
	}
	if index == len(nsgm.nodeSetGrpMap) {
		return nil, fmt.Errorf("id not found")
	}
	return m.buildNodeSetGrpInfo(nsgm.nodeSetGrpMap[index]), nil
}

func (m *Server) buildNodeSetGrpInfo(nsg *nodeSetGroup) *proto.SimpleNodeSetGrpInfo {
	nsgStat := new(proto.SimpleNodeSetGrpInfo)
	nsgStat.ID = nsg.ID
	nsgStat.Status = nsg.status
	for i := 0; i < len(nsg.nodeSets); i++ {
		var nsStat proto.NodeSetInfo
		nsStat.ID = nsg.nodeSets[i].ID
		nsStat.Capacity = nsg.nodeSets[i].Capacity
		nsStat.ZoneName = nsg.nodeSets[i].zoneName
		nsg.nodeSets[i].dataNodes.Range(func(key, value interface{}) bool {
			node := value.(*DataNode)
			nsStat.DataTotal += node.Total
			if node.isWriteAble() {
				nsStat.DataUsed += node.Used
			} else {
				nsStat.DataUsed += node.Total
			}
			log.LogInfof("nodeset index[%v], datanode nodeset id[%v],zonename[%v], addr[%v] inner nodesetid[%v]",
				i, nsStat.ID, node.ZoneName, node.Addr, node.NodeSetID)

			dataNodeInfo := &proto.DataNodeInfo{
				Total:              node.Total,
				Used:               node.Used,
				AvailableSpace:     node.AvailableSpace,
				ID:                 node.ID,
				ZoneName:           node.ZoneName,
				Addr:               node.Addr,
				ReportTime:         node.ReportTime,
				IsActive:           node.isActive,
				IsWriteAble:        node.isWriteAble(),
				UsageRatio:         node.UsageRatio,
				SelectedTimes:      node.SelectedTimes,
				Carry:              node.Carry,
				DataPartitionCount: node.DataPartitionCount,
				NodeSetID:          node.NodeSetID,
			}
			nsStat.DataNodes = append(nsStat.DataNodes, dataNodeInfo)
			return true
		})

		nsStat.DataUseRatio, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(nsStat.DataUsed)/float64(nsStat.DataTotal)), 64)

		nsg.nodeSets[i].metaNodes.Range(func(key, value interface{}) bool {
			node := value.(*MetaNode)
			nsStat.MetaTotal += node.Total
			nsStat.MetaUsed += node.Used
			log.LogInfof("nodeset index[%v], metanode nodeset id[%v],zonename[%v], addr[%v] inner nodesetid[%v]",
				i, nsStat.ID, node.ZoneName, node.Addr, node.NodeSetID)

			metaNodeInfo := &proto.MetaNodeInfo{
				ID:                 node.ID,
				Addr:               node.Addr,
				IsActive:           node.IsActive,
				IsWriteAble:        node.isWritable(),
				ZoneName:           node.ZoneName,
				MaxMemAvailWeight:  node.MaxMemAvailWeight,
				Total:              node.Total,
				Used:               node.Used,
				Ratio:              node.Ratio,
				SelectCount:        node.SelectCount,
				Carry:              node.Carry,
				Threshold:          node.Threshold,
				ReportTime:         node.ReportTime,
				MetaPartitionCount: node.MetaPartitionCount,
				NodeSetID:          node.NodeSetID,
			}

			nsStat.MetaNodes = append(nsStat.MetaNodes, metaNodeInfo)

			return true
		})
		nsStat.MetaUseRatio, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(nsStat.MetaUsed)/float64(nsStat.MetaTotal)), 64)
		nsgStat.NodeSetInfo = append(nsgStat.NodeSetInfo, nsStat)
		log.LogInfof("nodeset index[%v], nodeset id[%v],capacity[%v], datatotal[%v] dataused[%v] metatotal[%v] metaused[%v], metanode[%v], datanodes[%v]",
			i, nsStat.ID, nsStat.Capacity, nsStat.DataTotal, nsStat.DataUsed, nsStat.MetaTotal, nsStat.MetaUsed, nsStat.MetaNodes, nsStat.DataNodes)
	}
	return nsgStat
}

func parseSetNodeRdOnlyParam(r *http.Request) (addr string, nodeType int, rdOnly bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if addr = r.FormValue(addrKey); addr == "" {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is empty", addrKey)
		return
	}

	if nodeType, err = parseNodeType(r); err != nil {
		return
	}

	val := r.FormValue(rdOnlyKey)
	if val == "" {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is empty", rdOnlyKey)
		return
	}

	if rdOnly, err = strconv.ParseBool(val); err != nil {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is not bool value %s", rdOnlyKey, val)
		return
	}

	return
}

func parseSetDpRdOnlyParam(r *http.Request) (dpId uint64, rdOnly bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if dpId, err = extractDataPartitionID(r); err != nil {
		err = fmt.Errorf("parseSetDpRdOnlyParam get dpid error %v", err)
		return
	}

	val := r.FormValue(rdOnlyKey)
	if val == "" {
		err = fmt.Errorf("parseSetDpRdOnlyParam %s is empty", rdOnlyKey)
		return
	}

	if rdOnly, err = strconv.ParseBool(val); err != nil {
		err = fmt.Errorf("parseSetDpRdOnlyParam %s is not bool value %s", rdOnlyKey, val)
		return
	}

	return
}

func parseNodeType(r *http.Request) (nodeType int, err error) {
	var val string
	if val = r.FormValue(nodeTypeKey); val == "" {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is empty", nodeTypeKey)
		return
	}

	if nodeType, err = strconv.Atoi(val); err != nil {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is not number, err %s", nodeTypeKey, err.Error())
		return
	}

	if nodeType != int(TypeDataPartition) && nodeType != int(TypeMetaPartition) {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is not legal, must be %d or %d", nodeTypeKey, TypeDataPartition, TypeMetaPartition)
		return
	}

	return
}

func (m *Server) setNodeRdOnlyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		addr     string
		nodeType int
		rdOnly   bool
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetNodeRdOnly))
	defer func() {
		doStatAndMetric(proto.AdminSetNodeRdOnly, metric, err, nil)
	}()

	addr, nodeType, rdOnly, err = parseSetNodeRdOnlyParam(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	log.LogInfof("[setNodeRdOnlyHandler] set node %s to rdOnly(%v)", addr, rdOnly)

	err = m.setNodeRdOnly(addr, uint32(nodeType), rdOnly)
	if err != nil {
		log.LogErrorf("[setNodeRdOnlyHandler] set node %s to rdOnly %v, err (%s)", addr, rdOnly, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("[setNodeRdOnlyHandler] set node %s to rdOnly(%v) success", addr, rdOnly)))
	return
}

func (m *Server) setDpRdOnlyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		dpId   uint64
		rdOnly bool
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetDpRdOnly))
	defer func() {
		doStatAndMetric(proto.AdminSetDpRdOnly, metric, err, nil)
	}()

	dpId, rdOnly, err = parseSetDpRdOnlyParam(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	log.LogInfof("[setNodeRdOnlyHandler] set dp %v to rdOnly(%v)", dpId, rdOnly)

	err = m.setDpRdOnly(dpId, rdOnly)
	if err != nil {
		log.LogErrorf("[setNodeRdOnlyHandler] set dp %v to rdOnly %v, err (%s)", dpId, rdOnly, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("[setNodeRdOnlyHandler] set dpid %v to rdOnly(%v) success", dpId, rdOnly)))
	return
}

func (m *Server) updateNodeSetCapacityHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateNodeSetCapcity))
	defer func() {
		doStatAndMetric(proto.AdminUpdateNodeSetCapcity, metric, err, nil)
	}()

	if params, err = parseAndExtractSetNodeSetInfoParams(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err := m.updateNodesetCapcity(params[zoneNameKey].(string), params[idKey].(uint64), params[countKey].(uint64)); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
	}
	sendOkReply(w, r, newSuccessHTTPReply("set nodesetinfo successfully"))
}

func (m *Server) updateDataUseRatioHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateDomainDataUseRatio))
	defer func() {
		doStatAndMetric(proto.AdminUpdateDomainDataUseRatio, metric, err, nil)
	}()

	var value string
	if value = r.FormValue(ratio); value == "" {
		err = keyNotFound(ratio)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var ratioVal float64
	if ratioVal, err = strconv.ParseFloat(value, 64); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if ratioVal == 0 || ratioVal > 1 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.updateDataUseRatio(ratioVal); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodesetinfo params %v successfully", params)))
}

func (m *Server) updateZoneExcludeRatioHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateZoneExcludeRatio))
	defer func() {
		doStatAndMetric(proto.AdminUpdateZoneExcludeRatio, metric, err, nil)
	}()

	var value string
	if value = r.FormValue(ratio); value == "" {
		err = keyNotFound(ratio)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var ratioVal float64
	if ratioVal, err = strconv.ParseFloat(value, 64); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.updateExcludeZoneUseRatio(ratioVal); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodesetinfo params %v successfully", params)))
}

func (m *Server) updateNodeSetIdHandler(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		zoneName string
		err      error
		nodeType uint64
		value    string
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateNodeSetId))
	defer func() {
		doStatAndMetric(proto.AdminUpdateNodeSetId, metric, err, nil)

		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}()

	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if id, err = extractNodeID(r); err != nil {
		return
	}
	if value = r.FormValue(nodeTypeKey); value == "" {
		err = fmt.Errorf("need param nodeType")
		return
	}

	if nodeType, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}

	if err = m.updateNodesetId(zoneName, id, nodeType, nodeAddr); err != nil {
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("update node setid successfully")))
}

// get metanode some interval params
func (m *Server) getNodeSetGrpInfoHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetNodeSetGrpInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetNodeSetGrpInfo, metric, err, nil)
	}()

	if err = r.ParseForm(); err != nil {
		sendOkReply(w, r, newErrHTTPReply(err))
	}
	var value string
	var id uint64
	if value = r.FormValue(idKey); value != "" {
		id, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}
	var domainId uint64
	if value = r.FormValue(domainIdKey); value != "" {
		domainId, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}

	log.LogInfof("action[getNodeSetGrpInfoHandler] id [%v]", id)
	var info *proto.SimpleNodeSetGrpInfo
	if info, err = m.buildNodeSetGrpInfoByID(domainId, id); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	sendOkReply(w, r, newSuccessHTTPReply(info))
}

func (m *Server) getIsDomainOn(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetIsDomainOn))
	defer func() {
		doStatAndMetric(proto.AdminGetIsDomainOn, metric, nil, nil)
	}()

	type SimpleDomainInfo struct {
		DomainOn bool
	}
	nsglStat := new(SimpleDomainInfo)
	nsglStat.DomainOn = m.cluster.FaultDomain
	sendOkReply(w, r, newSuccessHTTPReply(nsglStat))
}

func (m *Server) createDomainHandler(w http.ResponseWriter, r *http.Request) {
	nsgm := m.cluster.domainManager
	var (
		zoneName string
		err      error
	)

	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("zonename null").Error()})
		return
	}
	if err = nsgm.createDomain(zoneName); err != nil {
		log.LogErrorf("action[createDomainHandler] err [%v]", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("successful")))
}

// get metanode some interval params
func (m *Server) getAllNodeSetGrpInfoHandler(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetAllNodeSetGrpInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetAllNodeSetGrpInfo, metric, nil, nil)
	}()

	nsgm := m.cluster.domainManager

	nsglStat := new(proto.DomainNodeSetGrpInfoList)
	nsglStat.DomainOn = m.cluster.FaultDomain
	nsglStat.NeedDomain = m.cluster.needFaultDomain
	nsglStat.DataRatioLimit = nsgm.dataRatioLimit
	nsglStat.ZoneExcludeRatioLimit = nsgm.excludeZoneUseRatio
	nsglStat.ExcludeZones = nsgm.c.t.domainExcludeZones

	for i := 0; i < len(nsgm.domainNodeSetGrpVec); i++ {
		nodeSetGrpInfoList := &proto.SimpleNodeSetGrpInfoList{}
		nodeSetGrpInfoList.DomainId = nsgm.domainNodeSetGrpVec[i].domainId
		nodeSetGrpInfoList.Status = nsgm.domainNodeSetGrpVec[i].status
		nsglStat.DomainNodeSetGrpInfo = append(nsglStat.DomainNodeSetGrpInfo, nodeSetGrpInfoList)
		nodeSetGrpInfoList.Status = nsgm.domainNodeSetGrpVec[i].status
		log.LogInfof("action[getAllNodeSetGrpInfoHandler] start build domain id [%v]", nsgm.domainNodeSetGrpVec[i].domainId)
		for j := 0; j < len(nsgm.domainNodeSetGrpVec[i].nodeSetGrpMap); j++ {
			log.LogInfof("action[getAllNodeSetGrpInfoHandler] build domain id [%v] nodeset group index [%v] Print inner nodeset now!",
				nsgm.domainNodeSetGrpVec[i].domainId, j)

			nodeSetGrpInfoList.SimpleNodeSetGrpInfo = append(nodeSetGrpInfoList.SimpleNodeSetGrpInfo,
				m.buildNodeSetGrpInfo(nsgm.domainNodeSetGrpVec[i].nodeSetGrpMap[j]))
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(nsglStat))
}

// get metanode some interval params
func (m *Server) getNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetNodeInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetNodeInfo, metric, nil, nil)
	}()

	resp := make(map[string]string)
	resp[nodeDeleteBatchCountKey] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteBatchCount)
	resp[nodeMarkDeleteRateKey] = fmt.Sprintf("%v", m.cluster.cfg.DataNodeDeleteLimitRate)
	resp[nodeDeleteWorkerSleepMs] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)
	resp[nodeAutoRepairRateKey] = fmt.Sprintf("%v", m.cluster.cfg.DataNodeAutoRepairLimitRate)
	resp[clusterLoadFactorKey] = fmt.Sprintf("%v", m.cluster.cfg.ClusterLoadFactor)
	resp[maxDpCntLimitKey] = fmt.Sprintf("%v", m.cluster.cfg.MaxDpCntLimit)

	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) diagnoseMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err               error
		rstMsg            *proto.MetaPartitionDiagnosis
		inactiveNodes     []string
		corruptMps        []*MetaPartition
		lackReplicaMps    []*MetaPartition
		corruptMpIDs      []uint64
		lackReplicaMpIDs  []uint64
		badMetaPartitions []badPartitionView
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDiagnoseMetaPartition))
	defer func() {
		doStatAndMetric(proto.AdminDiagnoseMetaPartition, metric, err, nil)
	}()

	corruptMpIDs = make([]uint64, 0)
	lackReplicaMpIDs = make([]uint64, 0)
	if inactiveNodes, corruptMps, err = m.cluster.checkCorruptMetaPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}

	if lackReplicaMps, err = m.cluster.checkLackReplicaMetaPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	for _, mp := range corruptMps {
		corruptMpIDs = append(corruptMpIDs, mp.PartitionID)
	}
	for _, mp := range lackReplicaMps {
		lackReplicaMpIDs = append(lackReplicaMpIDs, mp.PartitionID)
	}
	badMetaPartitions = m.cluster.getBadMetaPartitionsView()
	rstMsg = &proto.MetaPartitionDiagnosis{
		InactiveMetaNodes:           inactiveNodes,
		CorruptMetaPartitionIDs:     corruptMpIDs,
		LackReplicaMetaPartitionIDs: lackReplicaMpIDs,
		BadMetaPartitionIDs:         badMetaPartitions,
	}
	log.LogInfof("diagnose metaPartition[%v] inactiveNodes:[%v], corruptMpIDs:[%v], lackReplicaMpIDs:[%v]", m.cluster.Name, inactiveNodes, corruptMpIDs, lackReplicaMpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Decommission a disk. This will decommission all the data partitions on this disk.
// If parameter diskDisable is true, creating data partitions on this disk will be not allowed.
func (m *Server) decommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                string
		offLineAddr, diskPath string
		diskDisable           bool
		err                   error
		raftForce             bool
		limit                 int
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DecommissionDisk))
	defer func() {
		doStatAndMetric(proto.DecommissionDisk, metric, err, nil)
	}()

	if offLineAddr, diskPath, diskDisable, limit, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	raftForce, err = parseRaftForce(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.migrateDisk(offLineAddr, diskPath, raftForce, limit, diskDisable); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	Warn(m.clusterName, rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Recommission a disk which is set disable when decommissioning. This will allow creating data partitions on this disk again.
func (m *Server) recommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		node                 *DataNode
		rstMsg               string
		onLineAddr, diskPath string
		err                  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RecommissionDisk))
	defer func() {
		doStatAndMetric(proto.RecommissionDisk, metric, err, nil)
	}()

	if onLineAddr, diskPath, err = parseReqToRecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(onLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if err = m.cluster.deleteAndSyncDecommissionedDisk(node, diskPath); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg = fmt.Sprintf("receive recommissionDisk node[%v] disk[%v], and recommission successfully",
		node.Addr, diskPath)

	Warn(m.clusterName, rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) queryDiskDecoProgress(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_queryDiskDecoProgress")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	value, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		ret := fmt.Sprintf("action[queryDiskDecoProgress]cannot found  decommission task for node[%v] disk[%v], "+
			"may be already offline", offLineAddr, diskPath)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	disk := value.(*DecommissionDisk)
	status, progress := disk.updateDecommissionStatus(m.cluster, true)
	progress, _ = FormatFloatFloor(progress, 4)
	resp := &proto.DecommissionProgress{
		Status:   status,
		Progress: fmt.Sprintf("%.2f%%", progress*float64(100)),
	}
	if status == DecommissionFail {
		err, dps := disk.GetDecommissionFailedDP(m.cluster)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		resp.FailedDps = dps
	}
	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) queryDecommissionDiskDecoFailedDps(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_queryDecommissionDiskDecoFailedDps")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	value, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		ret := fmt.Sprintf("action[queryDiskDecoProgress]cannot found decommission task for node[%v] disk[%v], "+
			"may be already offline", offLineAddr, diskPath)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	disk := value.(*DecommissionDisk)
	err, dps := disk.GetDecommissionFailedDP(m.cluster)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(dps))
}

func (m *Server) markDecoDiskFixed(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_markDecoDiskFixed")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	value, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		ret := fmt.Sprintf("action[queryDiskDecoProgress]cannot found decommission task for  node[%v] disk[%v], "+
			"may be already offline", offLineAddr, diskPath)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	disk := value.(*DecommissionDisk)
	err = m.cluster.syncDeleteDecommissionDisk(disk)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	m.cluster.DecommissionDisks.Delete(disk.GenerateKey())
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func (m *Server) cancelDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_cancelDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	value, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		ret := fmt.Sprintf("action[queryDiskDecoProgress]cannot found decommission task for node[%v] disk[%v], "+
			"may be already offline", offLineAddr, diskPath)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	disk := value.(*DecommissionDisk)
	err = m.cluster.decommissionDiskCancel(disk)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg := fmt.Sprintf("cancel decommission data node [%s] disk[%s] successfully",
		offLineAddr, diskPath)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// handle tasks such as heartbeat，loadDataPartition，deleteDataPartition, etc.
func (m *Server) handleDataNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		tr  *proto.AdminTask
		err error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetDataNodeTaskResponse))
	defer func() {
		doStatAndMetric(proto.GetDataNodeTaskResponse, metric, err, nil)
	}()

	tr, err = parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleDataNodeTaskResponse(tr.OperatorAddr, tr)
}

func (m *Server) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  string
		zoneName  string
		id        uint64
		err       error
		nodesetId uint64
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AddMetaNode))
	defer func() {
		doStatAndMetric(proto.AddMetaNode, metric, err, nil)
	}()

	if nodeAddr, zoneName, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIpPort(nodeAddr) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
		return
	}
	var value string
	if value = r.FormValue(idKey); value == "" {
		nodesetId = 0
	} else {
		if nodesetId, err = strconv.ParseUint(value, 10, 64); err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}
	if id, err = m.cluster.addMetaNode(nodeAddr, zoneName, nodesetId); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) checkInvalidIDNodes(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetInvalidNodes))
	defer func() {
		doStatAndMetric(proto.AdminGetInvalidNodes, metric, nil, nil)
	}()

	nodes := m.cluster.getInvalidIDNodes()
	sendOkReply(w, r, newSuccessHTTPReply(nodes))
}

func (m *Server) updateDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateDataNode))
	defer func() {
		doStatAndMetric(proto.AdminUpdateDataNode, metric, err, nil)
	}()

	if nodeAddr, id, err = parseRequestForUpdateMetaNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.updateDataNodeBaseInfo(nodeAddr, id); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) updateMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateMetaNode))
	defer func() {
		doStatAndMetric(proto.AdminUpdateMetaNode, metric, err, nil)
	}()

	if nodeAddr, id, err = parseRequestForUpdateMetaNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.updateMetaNodeBaseInfo(nodeAddr, id); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr     string
		metaNode     *MetaNode
		metaNodeInfo *proto.MetaNodeInfo
		err          error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetMetaNode))
	defer func() {
		doStatAndMetric(proto.GetMetaNode, metric, err, nil)
	}()

	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if metaNode, err = m.cluster.metaNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	metaNode.PersistenceMetaPartitions = m.cluster.getAllMetaPartitionIDByMetaNode(nodeAddr)
	metaNodeInfo = &proto.MetaNodeInfo{
		ID:                        metaNode.ID,
		Addr:                      metaNode.Addr,
		DomainAddr:                metaNode.DomainAddr,
		IsActive:                  metaNode.IsActive,
		IsWriteAble:               metaNode.isWritable(),
		ZoneName:                  metaNode.ZoneName,
		MaxMemAvailWeight:         metaNode.MaxMemAvailWeight,
		Total:                     metaNode.Total,
		Used:                      metaNode.Used,
		Ratio:                     metaNode.Ratio,
		SelectCount:               metaNode.SelectCount,
		Carry:                     metaNode.Carry,
		Threshold:                 metaNode.Threshold,
		ReportTime:                metaNode.ReportTime,
		MetaPartitionCount:        metaNode.MetaPartitionCount,
		NodeSetID:                 metaNode.NodeSetID,
		PersistenceMetaPartitions: metaNode.PersistenceMetaPartitions,
	}
	sendOkReply(w, r, newSuccessHTTPReply(metaNodeInfo))
}

func (m *Server) decommissionMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		nodeAddr    string
		mp          *MetaPartition
		msg         string
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDecommissionMetaPartition))
	defer func() {
		doStatAndMetric(proto.AdminDecommissionMetaPartition, metric, err, nil)
	}()

	if partitionID, nodeAddr, err = parseRequestToDecommissionMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	if err = m.cluster.decommissionMetaPartition(nodeAddr, mp); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf(proto.AdminDecommissionMetaPartition+" partitionID :%v  decommissionMetaPartition successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func parseMigrateNodeParam(r *http.Request) (srcAddr, targetAddr string, limit int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	srcAddr = r.FormValue(srcAddrKey)
	if srcAddr == "" {
		err = fmt.Errorf("parseMigrateNodeParam %s can't be empty", srcAddrKey)
		return
	}
	if ipAddr, ok := util.ParseAddrToIpAddr(srcAddr); ok {
		srcAddr = ipAddr
	}

	targetAddr = r.FormValue(targetAddrKey)
	if targetAddr == "" {
		err = fmt.Errorf("parseMigrateNodeParam %s can't be empty when migrate", targetAddrKey)
		return
	}
	if ipAddr, ok := util.ParseAddrToIpAddr(targetAddr); ok {
		targetAddr = ipAddr
	}

	if srcAddr == targetAddr {
		err = fmt.Errorf("parseMigrateNodeParam srcAddr %s can't be equal to targetAddr %s", srcAddr, targetAddr)
		return
	}

	limit, err = parseUintParam(r, countKey)
	if err != nil {
		return
	}

	return
}

func parseUintParam(r *http.Request, key string) (num int, err error) {
	val := r.FormValue(key)
	if val == "" {
		num = 0
		return
	}

	numVal, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		err = fmt.Errorf("parseUintParam %s-%s is not legal, err %s", key, val, err.Error())
		return
	}

	num = int(numVal)
	return
}

func (m *Server) loadMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminLoadMetaPartition))
	defer func() {
		doStatAndMetric(proto.AdminLoadMetaPartition, metric, err, nil)
	}()

	if partitionID, err = parseRequestToLoadMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	m.cluster.loadMetaPartitionAndCheckResponse(mp)
	msg = fmt.Sprintf(proto.AdminLoadMetaPartition+" partitionID :%v Load successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) migrateMetaNodeHandler(w http.ResponseWriter, r *http.Request) {
	var (
		srcAddr    string
		targetAddr string
		limit      int
		err        error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.MigrateMetaNode))
	defer func() {
		doStatAndMetric(proto.MigrateMetaNode, metric, err, nil)
	}()

	srcAddr, targetAddr, limit, err = parseMigrateNodeParam(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if limit > defaultMigrateMpCnt {
		err = fmt.Errorf("limit %d can't be bigger than %d", limit, defaultMigrateMpCnt)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	srcNode, err := m.cluster.metaNode(srcAddr)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeMetaNodeNotExists, Msg: err.Error()})
		return
	}

	targetNode, err := m.cluster.metaNode(targetAddr)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeMetaNodeNotExists, Msg: err.Error()})
		return
	}

	if srcNode.NodeSetID != targetNode.NodeSetID {
		err = fmt.Errorf("src %s and target %s must exist in the same nodeSet when migrate", srcAddr, targetAddr)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if !targetNode.isWritable() {
		err = fmt.Errorf("[%s] is not writable, can't used as target addr for migrate", targetAddr)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.migrateMetaNode(srcAddr, targetAddr, limit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg := fmt.Sprintf("migrateMetaNodeHandler from src [%v] to targaet[%s] has migrate successfully", srcAddr, targetAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) decommissionMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		offLineAddr string
		limit       int
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DecommissionMetaNode))
	defer func() {
		doStatAndMetric(proto.DecommissionMetaNode, metric, err, nil)
	}()

	if offLineAddr, limit, err = parseDecomNodeReq(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if _, err = m.cluster.metaNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	if err = m.cluster.migrateMetaNode(offLineAddr, "", limit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommissionMetaNode metaNode [%v] limit %d has offline successfully", offLineAddr, limit)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) handleMetaNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		tr  *proto.AdminTask
		err error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetMetaNodeTaskResponse))
	defer func() {
		doStatAndMetric(proto.GetMetaNodeTaskResponse, metric, err, nil)
	}()

	tr, err = parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleMetaNodeTaskResponse(tr.OperatorAddr, tr)
}

// Dynamically add a raft node (replica) for the master.
// By using this function, there is no need to stop all the master services. Adding a new raft node is performed online.
func (m *Server) addRaftNode(w http.ResponseWriter, r *http.Request) {
	var (
		id   uint64
		addr string
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AddRaftNode))
	defer func() {
		doStatAndMetric(proto.AddRaftNode, metric, err, nil)
	}()

	var msg string
	id, addr, err = parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.addRaftNode(id, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successfully \n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Dynamically remove a master node. Similar to addRaftNode, this operation is performed online.
func (m *Server) removeRaftNode(w http.ResponseWriter, r *http.Request) {
	var (
		id   uint64
		addr string
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RemoveRaftNode))
	defer func() {
		doStatAndMetric(proto.RemoveRaftNode, metric, err, nil)
	}()

	var msg string
	id, addr, err = parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	err = m.cluster.removeRaftNode(id, addr)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successfully\n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// get master's raft status
func (m *Server) getRaftStatus(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RaftStatus))
	defer func() {
		doStatAndMetric(proto.RaftStatus, metric, nil, nil)
	}()

	data := m.raftStore.RaftStatus(GroupID)
	log.LogInfof("get raft status, %s", data.String())
	sendOkReply(w, r, newSuccessHTTPReply(data))
}

func parseReqToDecoDisk(r *http.Request) (nodeAddr, diskPath string, diskDisable bool, limit int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = extractDiskPath(r)
	if err != nil {
		return
	}
	diskDisable, err = extractDiskDisable(r)
	if err != nil {
		return
	}

	limit, err = parseUintParam(r, countKey)
	return
}

func parseReqToRecoDisk(r *http.Request) (nodeAddr, diskPath string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = extractDiskPath(r)
	if err != nil {
		return
	}

	return
}

type getVolParameter struct {
	name                string
	authKey             string
	skipOwnerValidation bool
}

func pareseBoolWithDefault(r *http.Request, key string, old bool) (bool, error) {
	val := r.FormValue(key)
	if val == "" {
		return old, nil
	}

	newVal, err := strconv.ParseBool(val)
	if err != nil {
		return false, fmt.Errorf("parse %s bool val err, err %s", key, err.Error())
	}

	return newVal, nil
}

func parseRaftForce(r *http.Request) (bool, error) {
	return pareseBoolWithDefault(r, raftForceDelKey, false)
}

func extractPosixAcl(r *http.Request) (enablePosix bool, err error) {
	var value string
	if value = r.FormValue(enablePosixAclKey); value == "" {
		return
	}

	status, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("parse %s failed, val %s", enablePosixAclKey, value)
	}

	return status, nil
}

func (m *Server) getMetaPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		vol  *Vol
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientMetaPartitions))
	defer func() {
		doStatAndMetric(proto.ClientMetaPartitions, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	mpsCache := vol.getMpsCache()
	if len(mpsCache) == 0 {
		vol.updateViewCache(m.cluster)
		mpsCache = vol.getMpsCache()
	}
	send(w, r, mpsCache)
	return
}

// Obtain all the data partitions in a volume.
func (m *Server) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		name string
		vol  *Vol
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientDataPartitions))
	defer func() {
		doStatAndMetric(proto.ClientDataPartitions, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogInfof("action[getDataPartitions] tmp is leader[%v]", m.cluster.partition.IsRaftLeader())
	if !m.cluster.partition.IsRaftLeader() {
		var ok bool
		if body, ok = m.cluster.followerReadManager.getVolViewAsFollower(name); !ok {
			log.LogErrorf("action[getDataPartitions] volume [%v] not get partitions info", name)
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("follower volume info not found")))
			return
		}
		send(w, r, body)
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if body, err = vol.getDataPartitionsView(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	send(w, r, body)
}

func (m *Server) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		vol     *Vol
		message string
		jobj    proto.APIAccessReq
		ticket  cryptoutil.Ticket
		ts      int64
		param   *getVolParameter
		volName string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientVol))
	defer func() {
		doStatAndMetric(proto.ClientVol, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if param, err = parseGetVolParameter(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	volName = param.name
	if vol, err = m.cluster.getVol(param.name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if !param.skipOwnerValidation && !matchKey(vol.Owner, param.authKey) {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolAuthKeyNotMatch))
		return
	}
	viewCache := vol.getViewCache()
	if len(viewCache) == 0 {
		vol.updateViewCache(m.cluster)
		viewCache = vol.getViewCache()
	}
	if !param.skipOwnerValidation && vol.authenticate {
		if jobj, ticket, ts, err = parseAndCheckTicket(r, m.cluster.MasterSecretKey, param.name); err != nil {
			if err == proto.ErrExpiredTicket {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInvalidTicket, Msg: err.Error()})
			return
		}
		if message, err = genRespMessage(viewCache, &jobj, ts, ticket.SessionKey.Key); err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeMasterAPIGenRespError, Msg: err.Error()})
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply(message))
	} else {
		send(w, r, viewCache)
	}
}

// Obtain the volume information such as total capacity and used space, etc.
func (m *Server) getVolStatInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		ver    int
		vol    *Vol
		byMeta bool
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientVolStat))
	defer func() {
		doStatAndMetric(proto.ClientVolStat, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, ver, byMeta, err = parseVolStatReq(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if proto.IsCold(vol.VolType) && ver != proto.LFClient {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "ec-vol is supported by LF client only"})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(volStat(vol, byMeta)))
}

func volStat(vol *Vol, countByMeta bool) (stat *proto.VolStatInfo) {
	stat = new(proto.VolStatInfo)
	stat.Name = vol.Name
	stat.TotalSize = vol.Capacity * util.GB
	stat.UsedSize = vol.totalUsedSpaceByMeta(countByMeta)
	if stat.UsedSize > stat.TotalSize {
		stat.UsedSize = stat.TotalSize
	}

	stat.UsedRatio = strconv.FormatFloat(float64(stat.UsedSize)/float64(stat.TotalSize), 'f', 2, 32)
	stat.DpReadOnlyWhenVolFull = vol.DpReadOnlyWhenVolFull

	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		stat.InodeCount += mp.InodeCount
	}
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)
	if proto.IsHot(vol.VolType) {
		return
	}

	stat.CacheTotalSize = vol.CacheCapacity * util.GB
	stat.CacheUsedSize = vol.cfsUsedSpace()
	stat.CacheUsedRatio = strconv.FormatFloat(float64(stat.CacheUsedSize)/float64(stat.CacheTotalSize), 'f', 2, 32)
	log.LogDebugf("ebsTotal[%v],ebsUsedSize[%v]", stat.CacheTotalSize, stat.CacheUsedSize)

	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *proto.MetaPartitionView) {
	mpView = proto.NewMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
	mp.Lock()
	defer mp.Unlock()
	for _, host := range mp.Hosts {
		mpView.Members = append(mpView.Members, host)
	}
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return
	}
	mpView.LeaderAddr = mr.Addr
	mpView.MaxInodeID = mp.MaxInodeID
	mpView.InodeCount = mp.InodeCount
	mpView.DentryCount = mp.DentryCount
	mpView.FreeListLen = mp.FreeListLen
	mpView.IsRecover = mp.IsRecover
	return
}

func (m *Server) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		partitionID uint64
		mp          *MetaPartition
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientMetaPartition))
	defer func() {
		doStatAndMetric(proto.ClientMetaPartition, metric, err, nil)
	}()

	if partitionID, err = parseAndExtractPartitionInfo(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	var toInfo = func(mp *MetaPartition) *proto.MetaPartitionInfo {
		mp.RLock()
		defer mp.RUnlock()
		var replicas = make([]*proto.MetaReplicaInfo, len(mp.Replicas))
		zones := make([]string, len(mp.Hosts))
		for idx, host := range mp.Hosts {
			metaNode, err := m.cluster.metaNode(host)
			if err == nil {
				zones[idx] = metaNode.ZoneName
			}
		}
		for i := 0; i < len(replicas); i++ {
			replicas[i] = &proto.MetaReplicaInfo{
				Addr:       mp.Replicas[i].Addr,
				DomainAddr: mp.Replicas[i].metaNode.DomainAddr,
				MaxInodeID: mp.Replicas[i].MaxInodeID,
				ReportTime: mp.Replicas[i].ReportTime,
				Status:     mp.Replicas[i].Status,
				IsLeader:   mp.Replicas[i].IsLeader,
			}
		}
		var mpInfo = &proto.MetaPartitionInfo{
			PartitionID:   mp.PartitionID,
			Start:         mp.Start,
			End:           mp.End,
			VolName:       mp.volName,
			MaxInodeID:    mp.MaxInodeID,
			InodeCount:    mp.InodeCount,
			DentryCount:   mp.DentryCount,
			Replicas:      replicas,
			ReplicaNum:    mp.ReplicaNum,
			Status:        mp.Status,
			IsRecover:     mp.IsRecover,
			Hosts:         mp.Hosts,
			Peers:         mp.Peers,
			Zones:         zones,
			MissNodes:     mp.MissNodes,
			OfflinePeerID: mp.OfflinePeerID,
			LoadResponse:  mp.LoadResponse,
		}
		return mpInfo
	}

	sendOkReply(w, r, newSuccessHTTPReply(toInfo(mp)))
}

func (m *Server) listVols(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		keywords string
		vol      *Vol
		volsInfo []*proto.VolInfo
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminListVols))
	defer func() {
		doStatAndMetric(proto.AdminListVols, metric, err, nil)
	}()

	if keywords, err = parseKeywords(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	volsInfo = make([]*proto.VolInfo, 0)
	for _, name := range m.cluster.allVolNames() {
		if strings.Contains(name, keywords) {
			if vol, err = m.cluster.getVol(name); err != nil {
				sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
				return
			}
			stat := volStat(vol, false)
			volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize,
				stat.UsedSize, stat.DpReadOnlyWhenVolFull)
			volsInfo = append(volsInfo, volInfo)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(volsInfo))
}

func (m *Server) changeMasterLeader(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminChangeMasterLeader))
	defer func() {
		doStatAndMetric(proto.AdminChangeMasterLeader, metric, err, nil)
	}()

	if err = m.cluster.tryToChangeLeaderByHost(); err != nil {
		log.LogErrorf("changeMasterLeader.err %v", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rstMsg := fmt.Sprintf(" changeMasterLeader. command sucess send to dest host but need check. ")
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) OpFollowerPartitionsRead(w http.ResponseWriter, r *http.Request) {
	var (
		err            error
		enableFollower bool
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminOpFollowerPartitionsRead))
	defer func() {
		doStatAndMetric(proto.AdminOpFollowerPartitionsRead, metric, err, nil)
	}()

	log.LogDebugf("OpFollowerPartitionsRead.")
	if enableFollower, err = extractStatus(r); err != nil {
		log.LogErrorf("OpFollowerPartitionsRead.err %v", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
	}
	m.cluster.followerReadManager.needCheck = enableFollower

	rstMsg := fmt.Sprintf(" OpFollowerPartitionsRead. set needCheck %v command sucess. ", enableFollower)
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func genRespMessage(data []byte, req *proto.APIAccessReq, ts int64, key []byte) (message string, err error) {
	var (
		jresp []byte
		resp  proto.MasterAPIAccessResp
	)

	resp.Data = data

	resp.APIResp.Type = req.Type + 1
	resp.APIResp.ClientID = req.ClientID
	resp.APIResp.ServiceID = req.ServiceID
	resp.APIResp.Verifier = ts + 1 // increase ts by one for client verify server

	if jresp, err = json.Marshal(resp); err != nil {
		err = fmt.Errorf("json marshal for response failed %s", err.Error())
		return
	}

	if message, err = cryptoutil.EncodeMessage(jresp, key); err != nil {
		err = fmt.Errorf("encdoe message for response failed %s", err.Error())
		return
	}

	return
}

func (m *Server) associateVolWithUser(userID, volName string) error {
	var err error
	var userInfo *proto.UserInfo

	if userInfo, err = m.user.getUserInfo(userID); err != nil && err != proto.ErrUserNotExists {
		return err
	}

	if err == proto.ErrUserNotExists {

		var param = proto.UserCreateParam{
			ID:       userID,
			Password: DefaultUserPassword,
			Type:     proto.UserTypeNormal,
		}

		if userInfo, err = m.user.createKey(&param); err != nil {
			return err
		}
	}

	if _, err = m.user.addOwnVol(userInfo.UserID, volName); err != nil {
		return err
	}

	return nil
}

func (m *Server) updateDecommissionLimit(w http.ResponseWriter, r *http.Request) {
	var (
		limit uint64
		err   error
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateDecommissionLimit))
	defer func() {
		doStatAndMetric(proto.AdminUpdateDecommissionLimit, metric, err, nil)
	}()

	if limit, err = parseRequestToUpdateDecommissionLimit(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	zones := m.cluster.t.getAllZones()
	for _, zone := range zones {
		err = zone.updateDecommissionLimit(int32(limit), m.cluster)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
			return
		}
	}
	m.cluster.DecommissionLimit = limit
	if err = m.cluster.syncPutCluster(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("set master not worked %v", err)))
		return
	}
	rstMsg := fmt.Sprintf("set decommission limit to %v successfully", limit)
	log.LogDebugf("action[updateDecommissionLimit] %v", rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) queryDecommissionToken(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)

	metric := exporter.NewTPCnt("req_queryDecommissionToken")
	defer func() {
		metric.Set(err)
	}()
	var stats []nodeSetDecommissionParallelStatus
	zones := m.cluster.t.getAllZones()
	for _, zone := range zones {
		err, zoneStats := zone.queryDecommissionParallelStatus()
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
			return
		}
		stats = append(stats, zoneStats...)
	}
	log.LogDebugf("action[queryDecommissionToken] %v", stats)
	sendOkReply(w, r, newSuccessHTTPReply(stats))
}

func (m *Server) queryDecommissionLimit(w http.ResponseWriter, r *http.Request) {
	limit := m.cluster.DecommissionLimit
	rstMsg := fmt.Sprintf("decommission limit is %v", limit)
	log.LogDebugf("action[queryDecommissionLimit] %v", rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) queryDataNodeDecoProgress(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr string
		err         error
		dn          *DataNode
	)
	if offLineAddr, err = parseReqToDecoDataNodeProgress(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dn, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	status, progress := dn.updateDecommissionStatus(m.cluster, true)
	progress, _ = FormatFloatFloor(progress, 4)
	resp := &proto.DecommissionProgress{
		Status:   status,
		Progress: fmt.Sprintf("%.2f%%", progress*float64(100)),
	}
	if status == DecommissionFail {
		err, dps := dn.GetDecommissionFailedDPByTerm(m.cluster)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		resp.FailedDps = dps
	}

	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) queryDataNodeDecoFailedDps(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr string
		err         error
		dn          *DataNode
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.QueryDataNodeDecoFailedDps))
	defer func() {
		doStatAndMetric(proto.QueryDataNodeDecoFailedDps, metric, err, nil)
	}()

	if offLineAddr, err = parseReqToDecoDataNodeProgress(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dn, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	err, dps := dn.GetDecommissionFailedDP(m.cluster)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(dps))
}

func parseReqToDecoDataNodeProgress(r *http.Request) (nodeAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	return
}

func FormatFloatFloor(num float64, decimal int) (float64, error) {
	d := float64(1)
	if decimal > 0 {
		d = math.Pow10(decimal)
	}

	res := strconv.FormatFloat(math.Floor(num*d)/d, 'f', -1, 64)
	return strconv.ParseFloat(res, 64)
}

func (m *Server) setCheckDataReplicasEnable(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		enable bool
	)

	if enable, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	oldValue := m.cluster.checkDataReplicasEnable
	if oldValue != enable {
		m.cluster.checkDataReplicasEnable = enable
		if err = m.cluster.syncPutCluster(); err != nil {
			m.cluster.checkDataReplicasEnable = oldValue
			log.LogErrorf("action[setCheckDataReplicasEnable] syncPutCluster failed %v", err)
			sendErrReply(w, r, newErrHTTPReply(proto.ErrPersistenceByRaft))
			return
		}
	}

	log.LogInfof("action[setCheckDataReplicasEnable] enable be set [%v]", enable)
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf(
		"set checkDataReplicasEnable to [%v] successfully", enable)))
}

func (m *Server) setFileStats(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		enable bool
	)
	if enable, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	oldValue := m.cluster.fileStatsEnable
	m.cluster.fileStatsEnable = enable
	if err = m.cluster.syncPutCluster(); err != nil {
		m.cluster.fileStatsEnable = oldValue
		log.LogErrorf("action[setFileStats] syncPutCluster failed %v", err)
		sendErrReply(w, r, newErrHTTPReply(proto.ErrPersistenceByRaft))
		return
	}
	log.LogInfof("action[setFileStats] enable be set [%v]", enable)
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf(
		"set setFileStats to [%v] successfully", enable)))
}

func (m *Server) getFileStats(w http.ResponseWriter, r *http.Request) {
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf(
		"getFileStats enable value [%v]", m.cluster.fileStatsEnable)))
}

func (m *Server) GetClusterValue(w http.ResponseWriter, r *http.Request) {
	result, err := m.cluster.fsm.store.SeekForPrefix([]byte(clusterPrefix))
	if err != nil {
		log.LogErrorf("action[GetClusterValue],err:%v", err.Error())
		sendErrReply(w, r, newErrHTTPReply(proto.ErrInternalError))
		return
	}
	for _, value := range result {
		cv := &clusterValue{}
		if err = json.Unmarshal(value, cv); err != nil {
			log.LogErrorf("action[GetClusterValue], unmarshal err:%v", err.Error())
			sendErrReply(w, r, newErrHTTPReply(proto.ErrUnmarshalData))
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply(cv))
	}
}

func (m *Server) setClusterUuidEnable(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		enable bool
	)

	if m.cluster.clusterUuid == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "no ClusterUuid, generate it first"})
		return
	}

	if enable, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	oldValue := m.cluster.clusterUuidEnable
	m.cluster.clusterUuidEnable = enable
	if err = m.cluster.syncPutCluster(); err != nil {
		m.cluster.clusterUuidEnable = oldValue
		log.LogErrorf("action[setClusterUuidEnable] syncPutCluster failed %v", err)
		sendErrReply(w, r, newErrHTTPReply(proto.ErrPersistenceByRaft))
		return
	}

	log.LogInfof("action[setClusterUuidEnable] enable be set [%v]", enable)
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf(
		"set clusterUuIdEnable to [%v] successfully", enable)))
}

func (m *Server) generateClusterUuid(w http.ResponseWriter, r *http.Request) {
	if m.cluster.clusterUuid != "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: "The cluster already has a ClusterUuid"})
	}
	if err := m.cluster.generateClusterUuid(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrInternalError))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf(
		"generate ClusterUUID [%v] successfully", m.cluster.clusterUuid)))
}

func (m *Server) getClusterUuid(w http.ResponseWriter, r *http.Request) {
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf(
		"ClusterUUID [%v], enable value [%v]", m.cluster.clusterUuid, m.cluster.clusterUuidEnable)))
}
