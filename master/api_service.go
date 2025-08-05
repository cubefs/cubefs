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
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/compressor"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/cubefs/cubefs/util/strutil"
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
	Name                string
	Status              string
	DataNodesetSelector string
	MetaNodesetSelector string
	NodeSet             map[uint64]*NodeSetView
	DataMediaType       string
}

func newZoneView(name string) *ZoneView {
	return &ZoneView{NodeSet: make(map[uint64]*NodeSetView), Name: name}
}

type badPartitionView = proto.BadPartitionView

func (m *Server) setClusterInfo(w http.ResponseWriter, r *http.Request) {
	var (
		dirLimit uint32
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetClusterInfo))
	defer func() {
		doStatAndMetric(proto.AdminSetClusterInfo, metric, err, nil)
	}()

	if dirLimit, err = parseAndExtractDirLimit(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dirLimit < proto.MinDirChildrenNumLimit {
		dirLimit = proto.MinDirChildrenNumLimit
	}
	if err = m.cluster.setClusterInfo(dirLimit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set dir limit(min:%v, max:%v) to %v successfully",
		proto.MinDirChildrenNumLimit, math.MaxUint32, dirLimit)))
}

func (m *Server) getMonitorPushAddr(w http.ResponseWriter, r *http.Request) {
	var (
		addr string
		err  error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetMonitorPushAddr))
	defer func() {
		doStatAndMetric(proto.AdminGetMonitorPushAddr, metric, err, nil)
	}()

	addr = m.cluster.getMonitorPushAddr()
	sendOkReply(w, r, newSuccessHTTPReply(addr))
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

func (m *Server) setMasterVolDeletionDelayTime(w http.ResponseWriter, r *http.Request) {
	var (
		volDeletionDelayTimeHour int
		err                      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetMasterVolDeletionDelayTime))
	defer func() {
		doStatAndMetric(proto.AdminSetMasterVolDeletionDelayTime, metric, err, nil)
	}()

	if volDeletionDelayTimeHour, err = parseAndExtractVolDeletionDelayTime(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setMasterVolDeletionDelayTime(volDeletionDelayTimeHour); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set volDeletionDelayTime to %v h successfully", volDeletionDelayTimeHour)))
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

func (m *Server) forbidVolume(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		name   string
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolForbidden))
	defer func() {
		doStatAndMetric(proto.AdminVolForbidden, metric, err, nil)
		if err != nil {
			log.LogErrorf("set volume forbidden failed, error: %v", err)
		} else {
			log.LogInfof("set volume forbidden to (%v) success", status)
		}
	}()
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if status, err = parseAndExtractForbidden(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	if vol.Status == proto.VolStatusMarkDelete {
		err = errors.New("vol has been mark delete, freeze operation is not allowed")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolHasDeleted, Msg: err.Error()})
		return
	}
	oldForbiden := vol.Forbidden
	vol.Forbidden = status
	defer func() {
		if err != nil {
			vol.Forbidden = oldForbiden
		}
	}()
	if err = m.cluster.syncUpdateVol(vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if status {
		// set data partition status to write only
		vol.setDpForbid()
		// set meta partition status to read only
		vol.setMpForbid()
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set volume forbidden to (%v) success", status)))
}

func (m *Server) setEnableAuditLogForVolume(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		name   string
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolEnableAuditLog))
	defer func() {
		doStatAndMetric(proto.AdminVolEnableAuditLog, metric, err, nil)
		if err != nil {
			log.LogErrorf("set volume aduit log failed, error: %v", err)
		} else {
			log.LogInfof("set volume aduit log to (%v) success", status)
		}
	}()
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if status, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	oldEnable := vol.DisableAuditLog
	vol.DisableAuditLog = status
	defer func() {
		if err != nil {
			vol.DisableAuditLog = oldEnable
		}
	}()
	if err = m.cluster.syncUpdateVol(vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set volume audit log to (%v) success", status)))
}

func (m *Server) setupForbidMetaPartitionDecommission(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminClusterForbidMpDecommission))
	defer func() {
		doStatAndMetric(proto.AdminClusterForbidMpDecommission, metric, err, nil)
		if err != nil {
			log.LogErrorf("set ForbidMpDecommission failed, error: %v", err)
		} else {
			log.LogInfof("set ForbidMpDecommission to (%v) success", status)
		}
	}()

	if status, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setForbidMpDecommission(status); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set ForbidMpDecommission to %v successfully", status)))
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
		cv.DataNodesetSelector = zone.GetDataNodesetSelector()
		cv.MetaNodesetSelector = zone.GetMetaNodesetSelector()
		cv.DataMediaType = zone.GetDataMediaTypeString()
		tv.Zones = append(tv.Zones, cv)
		nsc := zone.getAllNodeSet()
		for _, ns := range nsc {
			nsView := newNodeSetView(ns.dataNodeLen(), ns.metaNodeLen())
			cv.NodeSet[ns.ID] = nsView
			ns.dataNodes.Range(func(key, value interface{}) bool {
				dataNode := value.(*DataNode)
				nsView.DataNodes = append(nsView.DataNodes, proto.NodeView{
					ID: dataNode.ID, Addr: dataNode.Addr,
					DomainAddr: dataNode.DomainAddr, Status: dataNode.isActive, IsWritable: dataNode.IsWriteAble(), MediaType: dataNode.MediaType,
				})
				return true
			})
			ns.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)
				nsView.MetaNodes = append(nsView.MetaNodes, proto.NodeView{
					ID: metaNode.ID, Addr: metaNode.Addr,
					DomainAddr: metaNode.DomainAddr, Status: metaNode.IsActive, IsWritable: metaNode.IsWriteAble(), MediaType: proto.MediaType_Unspecified,
				})
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
	dataNodesetSelector := extractDataNodesetSelector(r)
	metaNodesetSelector := extractMetaNodesetSelector(r)
	dataNodeSelector := extractDataNodeSelector(r)
	metaNodeSelector := extractMetaNodeSelector(r)
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
	err = zone.updateNodesetSelector(m.cluster, dataNodesetSelector, metaNodesetSelector)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	err = m.updateZoneNodeSelector(zone.name, dataNodeSelector, metaNodeSelector)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
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
		cv.DataNodesetSelector = zone.GetDataNodesetSelector()
		cv.MetaNodesetSelector = zone.GetMetaNodesetSelector()
		zoneViews = append(zoneViews, cv)
	}
	sendOkReply(w, r, newSuccessHTTPReply(zoneViews))
}

func (m *Server) listNodeSets(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetAllNodeSets))
	defer func() {
		doStatAndMetric(proto.GetAllNodeSets, metric, nil, nil)
	}()

	var zones []*Zone

	// if zoneName is empty, list all nodeSets, otherwise list node sets in the specified zone
	zoneName := r.FormValue(zoneNameKey)
	if zoneName == "" {
		zones = m.cluster.t.getAllZones()
	} else {
		zone, err := m.cluster.t.getZone(zoneName)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeZoneNotExists, Msg: err.Error()})
			return
		}
		zones = []*Zone{zone}
	}

	nodeSetStats := make([]*proto.NodeSetStat, 0)

	for _, zone := range zones {
		nsc := zone.getAllNodeSet()
		for _, ns := range nsc {
			nsStat := &proto.NodeSetStat{
				ID:                  ns.ID,
				Capacity:            ns.Capacity,
				Zone:                zone.name,
				CanAllocDataNodeCnt: ns.calcNodesForAlloc(ns.dataNodes),
				CanAllocMetaNodeCnt: ns.calcNodesForAlloc(ns.metaNodes),
				DataNodeNum:         ns.dataNodeLen(),
				MetaNodeNum:         ns.metaNodeLen(),
			}
			nodeSetStats = append(nodeSetStats, nsStat)
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(nodeSetStats))
}

func (m *Server) getNodeSet(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetNodeSet))
	defer func() {
		doStatAndMetric(proto.GetNodeSet, metric, nil, nil)
	}()

	nodeSetStr := r.FormValue(nodesetIdKey)
	if nodeSetStr == "" {
		err := keyNotFound(nodesetIdKey)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	nodeSetId, err := strconv.ParseUint(nodeSetStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid nodeSetId: %v", nodeSetStr)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	ns, err := m.cluster.t.getNodeSetByNodeSetId(nodeSetId)
	if err != nil {
		err := nodeSetNotFound(nodeSetId)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeNodeSetNotExists, Msg: err.Error()})
		return
	}

	nsStat := &proto.NodeSetStatInfo{
		ID:                  ns.ID,
		Capacity:            ns.Capacity,
		Zone:                ns.zoneName,
		CanAllocDataNodeCnt: ns.calcNodesForAlloc(ns.dataNodes),
		CanAllocMetaNodeCnt: ns.calcNodesForAlloc(ns.metaNodes),
		DataNodeSelector:    ns.GetDataNodeSelector(),
		MetaNodeSelector:    ns.GetMetaNodeSelector(),
	}
	ns.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		nsStat.DataNodes = append(nsStat.DataNodes, &proto.NodeStatView{
			Addr:       dn.Addr,
			Status:     dn.isActive,
			DomainAddr: dn.DomainAddr,
			ID:         dn.ID,
			IsWritable: dn.IsWriteAble(),
			Total:      dn.Total,
			Used:       dn.Used,
			Avail:      dn.Total - dn.Used,
		})
		return true
	})
	ns.metaNodes.Range(func(key, value interface{}) bool {
		mn := value.(*MetaNode)
		nsStat.MetaNodes = append(nsStat.MetaNodes, &proto.NodeStatView{
			Addr:       mn.Addr,
			Status:     mn.IsActive,
			DomainAddr: mn.DomainAddr,
			ID:         mn.ID,
			IsWritable: mn.IsWriteAble(),
			Total:      mn.Total,
			Used:       mn.Used,
			Avail:      mn.Total - mn.Used,
		})
		return true
	})

	sendOkReply(w, r, newSuccessHTTPReply(nsStat))
}

func (m *Server) updateNodeSet(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.UpdateNodeSet))
	defer func() {
		doStatAndMetric(proto.UpdateNodeSet, metric, nil, nil)
	}()
	nodeSetStr := r.FormValue(nodesetIdKey)
	if nodeSetStr == "" {
		err := keyNotFound(nodesetIdKey)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	nodeSetId, err := strconv.ParseUint(nodeSetStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid nodeSetId: %v", nodeSetStr)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	ns, err := m.cluster.t.getNodeSetByNodeSetId(nodeSetId)
	if err != nil {
		err := nodeSetNotFound(nodeSetId)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeNodeSetNotExists, Msg: err.Error()})
		return
	}
	dataNodeSelector := extractDataNodeSelector(r)
	metaNodeSelector := extractMetaNodeSelector(r)
	needSync := false
	if dataNodeSelector != "" && dataNodeSelector != ns.GetDataNodeSelector() {
		ns.SetDataNodeSelector(dataNodeSelector)
		needSync = true
	}
	if metaNodeSelector != "" && metaNodeSelector != ns.GetMetaNodeSelector() {
		ns.SetMetaNodeSelector(metaNodeSelector)
		needSync = true
	}
	if needSync {
		err = m.cluster.syncUpdateNodeSet(ns)
		if err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func (m *Server) clusterStat(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminClusterStat))
	defer func() {
		doStatAndMetric(proto.AdminClusterStat, metric, nil, nil)
	}()

	cs := &proto.ClusterStatInfo{
		DataNodeStatInfo: m.cluster.dataNodeStatInfo,
		MetaNodeStatInfo: m.cluster.metaNodeStatInfo,
		ZoneStatInfo:     make(map[string]*proto.ZoneStat),
	}
	for zoneName, zoneStat := range m.cluster.zoneStatInfos {
		cs.ZoneStatInfo[zoneName] = zoneStat
	}
	sendOkReply(w, r, newSuccessHTTPReply(cs))
}

func (m *Server) UidOperate(w http.ResponseWriter, r *http.Request) {
	var (
		uid     uint32
		err     error
		volName string
		vol     *Vol
		op      uint64
		value   string
		capSize uint64
		uidList []*proto.UidSpaceInfo
		uidInfo *proto.UidSpaceInfo
		ok      bool
	)
	if volName, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if value = r.FormValue(OperateKey); value == "" {
		err = keyNotFound(OperateKey)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	op, err = strconv.ParseUint(value, 10, 64)
	if err != nil {
		err = fmt.Errorf("parseUintParam %s-%s is not legal, err %s", OperateKey, value, err.Error())
		log.LogWarn(err)
		return
	}

	if op != util.UidLimitList {
		if uid, err = extractUint32(r, UIDKey); err != nil {
			err = keyNotFound(UIDKey)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	if op == util.UidAddLimit {
		if capSize, err = extractPositiveUint64(r, CapacityKey); err != nil {
			err = keyNotFound(CapacityKey)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	log.LogDebugf("uidOperate. name %v op %v uid %v", volName, op, uid)
	if vol, err = m.cluster.getVol(volName); err != nil {
		log.LogDebugf("aclOperate. name %v not found", volName)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	ok = true
	switch op {
	case util.UidGetLimit:
		ok, uidInfo = vol.uidSpaceManager.checkUid(uid)
		uidList = append(uidList, uidInfo)
	case util.UidAddLimit, util.UidDelLimit:
		cmd := &UidCmd{
			op:   op,
			uid:  uid,
			size: capSize,
		}
		ok = vol.uidSpaceManager.pushUidCmd(cmd)
	case util.UidLimitList:
		uidList = vol.uidSpaceManager.listAll()
	default:
		// do nothing
	}

	rsp := &proto.UidSpaceRsp{
		OK:          ok,
		UidSpaceArr: uidList,
	}

	_ = sendOkReply(w, r, newSuccessHTTPReply(rsp))
}

func (m *Server) aclOperate(w http.ResponseWriter, r *http.Request) {
	var (
		ip      string
		err     error
		volName string
		vol     *Vol
		op      uint64
		value   string
		ok, res bool
		ipList  []*proto.AclIpInfo
	)
	if volName, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if value = r.FormValue(OperateKey); value == "" {
		err = keyNotFound(OperateKey)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	op, err = strconv.ParseUint(value, 10, 64)
	if err != nil {
		err = fmt.Errorf("parseUintParam %s-%s is not legal, err %s", OperateKey, value, err.Error())
		log.LogWarn(err)
		return
	}

	if op != util.AclListIP {
		if ip = r.FormValue(IPKey); ip == "" {
			err = keyNotFound(IPKey)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	log.LogDebugf("aclOperate. name %v op %v ip %v", volName, op, ip)
	if vol, err = m.cluster.getVol(volName); err != nil {
		log.LogDebugf("aclOperate. name %v not found", volName)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	ok = true
	opAclRes := vol.aclMgr.aclOperate(op, ip)
	switch op {
	case util.AclCheckIP:
		if ipList, res = opAclRes.([]*proto.AclIpInfo); !res {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("inner error")))
			return
		}
		if len(ipList) > 0 {
			ok = false
		}
	case util.AclAddIP, util.AclDelIP:
		if opAclRes != nil {
			if _, res = opAclRes.(error); !res {
				sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("inner error")))
				return
			}
		}
	case util.AclListIP:
		if ipList, res = opAclRes.([]*proto.AclIpInfo); !res {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("inner error")))
			return
		}
	default:
		// do nothing
	}

	rsp := &proto.AclRsp{
		OK:   ok,
		List: ipList,
	}
	_ = sendOkReply(w, r, newSuccessHTTPReply(rsp))
}

func (m *Server) getOpLog(w http.ResponseWriter, r *http.Request) {
	var (
		dimension string
		volName   string
		addr      string
		dpId      string
		diskName  string
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetOpLog))
	defer func() {
		doStatAndMetric(proto.AdminGetOpLog, metric, nil, nil)
	}()

	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	dimension = r.FormValue(opLogDimensionKey)
	volName = r.FormValue(volNameKey)
	addr = r.FormValue(addrKey)
	dpId = r.FormValue(dpIdKey)
	diskName = r.FormValue(diskNameKey)

	var opv proto.OpLogView

	switch dimension {
	case proto.Dp:
		opv = m.cluster.getDpOpLog(addr, dpId)
		sort.Slice(opv.DpOpLogs, func(i, j int) bool {
			return opv.DpOpLogs[i].Count > opv.DpOpLogs[j].Count
		})

	case proto.Disk:
		opv = m.cluster.getDiskOpLog(addr, diskName)
		sort.Slice(opv.DiskOpLogs, func(i, j int) bool {
			return opv.DiskOpLogs[i].Count > opv.DiskOpLogs[j].Count
		})

	case proto.Node:
		opv = m.cluster.getClusterOpLog()
		sort.Slice(opv.ClusterOpLogs, func(i, j int) bool {
			return opv.ClusterOpLogs[i].Count > opv.ClusterOpLogs[j].Count
		})

	case proto.Vol:
		opv = m.cluster.getVolOpLog(volName)
		sort.Slice(opv.VolOpLogs, func(i, j int) bool {
			return opv.VolOpLogs[i].Count > opv.VolOpLogs[j].Count
		})

	default:
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("Invalid dimension: %s", dimension)))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(opv))
}

func (m *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	var volStorageClass bool

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetCluster))
	defer func() {
		doStatAndMetric(proto.AdminGetCluster, metric, nil, nil)
	}()

	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if value := r.FormValue(volStorageClassKey); value != "" {
		var err error
		volStorageClass, err = strconv.ParseBool(value)
		if err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	cv := &proto.ClusterView{
		Name:                         m.cluster.Name,
		CreateTime:                   time.Unix(m.cluster.CreateTime, 0).Format(proto.TimeFormat),
		LeaderAddr:                   m.leaderInfo.addr,
		DisableAutoAlloc:             m.cluster.DisableAutoAllocate,
		ForbidMpDecommission:         m.cluster.ForbidMpDecommission,
		MetaNodeThreshold:            m.cluster.cfg.MetaNodeThreshold,
		Applied:                      m.fsm.applied,
		MaxDataPartitionID:           m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:                m.cluster.idAlloc.commonID,
		MaxMetaPartitionID:           m.cluster.idAlloc.metaPartitionID,
		VolDeletionDelayTimeHour:     m.cluster.cfg.volDelayDeleteTimeHour,
		DpRepairTimeout:              m.cluster.GetDecommissionDataPartitionRecoverTimeOut().String(),
		DpBackupTimeout:              m.cluster.GetDecommissionDataPartitionBackupTimeOut().String(),
		MarkDiskBrokenThreshold:      m.cluster.getMarkDiskBrokenThreshold(),
		EnableAutoDpMetaRepair:       m.cluster.getEnableAutoDpMetaRepair(),
		AutoDpMetaRepairParallelCnt:  m.cluster.GetAutoDpMetaRepairParallelCnt(),
		EnableAutoDecommission:       m.cluster.AutoDecommissionDiskIsEnabled(),
		AutoDecommissionDiskInterval: m.cluster.GetAutoDecommissionDiskInterval().String(),
		DecommissionLimit:            atomic.LoadUint64(&m.cluster.DecommissionLimit),
		DecommissionDiskLimit:        m.cluster.GetDecommissionDiskLimit(),
		DpTimeout:                    (time.Duration(m.cluster.getDataPartitionTimeoutSec()) * time.Second).String(),
		MasterNodes:                  make([]proto.NodeView, 0),
		MetaNodes:                    make([]proto.NodeView, 0),
		DataNodes:                    make([]proto.NodeView, 0),
		VolStatInfo:                  make([]*proto.VolStatInfo, 0),
		StatOfStorageClass:           make([]*proto.StatOfStorageClass, 0),
		StatMigrateStorageClass:      make([]*proto.StatOfStorageClass, 0),
		BadPartitionIDs:              make([]proto.BadPartitionView, 0),
		BadMetaPartitionIDs:          make([]proto.BadPartitionView, 0),
		ForbidWriteOpOfProtoVer0:     m.cluster.cfg.forbidWriteOpOfProtoVer0,
		LegacyDataMediaType:          m.cluster.legacyDataMediaType,
		RaftPartitionCanUsingDifferentPortEnabled: m.cluster.RaftPartitionCanUsingDifferentPortEnabled(),
		FlashNodes: make([]proto.NodeView, 0),
	}

	vols := m.cluster.allVolNames()
	cv.MasterNodes = m.cluster.allMasterNodes()
	cv.MetaNodes = m.cluster.allMetaNodes()
	cv.DataNodes = m.cluster.allDataNodes()
	cv.FlashNodes = m.cluster.allFlashNodes()
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

	if volStorageClass {
		log.LogInfof("[getCluster] get hybrid cloud storage class stat")
		stats := make(map[uint32]*proto.StatOfStorageClass)
		migrateStats := make(map[uint32]*proto.StatOfStorageClass)
		for _, name := range vols {
			vol, err := m.cluster.getVol(name)
			if err != nil {
				log.LogErrorf("[getCluster] cannot get vol(%v) err(%v)", name, err)
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}

			volStats := vol.StatByStorageClass
			for _, stat := range volStats {
				_, ok := stats[stat.StorageClass]
				if !ok {
					// NOTE: copy a new stat
					stats[stat.StorageClass] = &proto.StatOfStorageClass{
						StorageClass:  stat.StorageClass,
						InodeCount:    stat.InodeCount,
						UsedSizeBytes: stat.UsedSizeBytes,
					}
					continue
				}
				total := stats[stat.StorageClass]
				total.InodeCount += stat.InodeCount
				total.UsedSizeBytes += stat.UsedSizeBytes
			}

			volMigrateStats := vol.StatMigrateStorageClass
			for _, migrateStat := range volMigrateStats {
				_, ok := migrateStats[migrateStat.StorageClass]
				if !ok {
					migrateStats[migrateStat.StorageClass] = &proto.StatOfStorageClass{
						StorageClass:  migrateStat.StorageClass,
						InodeCount:    migrateStat.InodeCount,
						UsedSizeBytes: migrateStat.UsedSizeBytes,
					}
					continue
				}
				migrateTotal := migrateStats[migrateStat.StorageClass]
				migrateTotal.InodeCount += migrateStat.InodeCount
				migrateTotal.UsedSizeBytes += migrateStat.UsedSizeBytes
			}
		}

		for _, stat := range stats {
			cv.StatOfStorageClass = append(cv.StatOfStorageClass, stat)
		}
		sort.Slice(cv.StatOfStorageClass, func(i, j int) bool {
			return cv.StatOfStorageClass[i].StorageClass < cv.StatOfStorageClass[j].StorageClass
		})

		for _, migStat := range migrateStats {
			cv.StatMigrateStorageClass = append(cv.StatMigrateStorageClass, migStat)
		}
		sort.Slice(cv.StatMigrateStorageClass, func(i, j int) bool {
			return cv.StatMigrateStorageClass[i].StorageClass < cv.StatMigrateStorageClass[j].StorageClass
		})

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

	// persist to rocksdb
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

	// persist to rocksdb
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
	dpMaxRepairErrCnt := atomic.LoadUint64(&m.cluster.cfg.DpMaxRepairErrCnt)

	cInfo := &proto.ClusterInfo{
		Cluster:                     m.cluster.Name,
		MetaNodeDeleteBatchCount:    batchCount,
		MetaNodeDeleteWorkerSleepMs: deleteSleepMs,
		DataNodeDeleteLimitRate:     limitRate,
		DataNodeAutoRepairLimitRate: autoRepairRate,
		DpMaxRepairErrCnt:           dpMaxRepairErrCnt,
		DirChildrenNumLimit:         dirChildrenNumLimit,
		// Ip:                          strings.Split(r.RemoteAddr, ":")[0],
		Ip:                                 iputil.RealIP(r),
		EbsAddr:                            m.bStoreAddr,
		ServicePath:                        m.servicePath,
		ClusterUuid:                        m.cluster.clusterUuid,
		ClusterUuidEnable:                  m.cluster.clusterUuidEnable,
		ClusterEnableSnapshot:              m.cluster.cfg.EnableSnapshot,
		RaftPartitionCanUsingDifferentPort: m.cluster.RaftPartitionCanUsingDifferentPortEnabled(),
	}

	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}

func (m *Server) createMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		vol     *Vol
		volName string
		count   int
		err     error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreateMetaPartition))
	defer func() {
		doStatAndMetric(proto.AdminCreateMetaPartition, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if volName, count, err = validateRequestToCreateMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if vol.status() == proto.VolStatusMarkDelete {
		log.LogErrorf("action[createMetaPartition] vol[%s] is marked delete ", vol.Name)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if vol.Forbidden {
		log.LogErrorf("action[createMetaPartition] vol[%s] is forbidden", vol.Name)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = vol.addMetaPartitions(m.cluster, count); err != nil {
		log.LogErrorf("create meta partition fail: volume(%v) err(%v)", volName, err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply("create meta partition successfully"))
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

	total := vol.CalculatePreloadCapacity() + uint64(preload.preloadCacheCapacity)
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
		parsed             uint64
		period, triggerCnt uint32
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
		if parsed, err = strconv.ParseUint(value, 10, 32); err != nil || parsed == 0 {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("wrong param of peroid")))
			return
		}
		period = uint32(parsed)
	}
	if value = r.FormValue(ClientTriggerCnt); value != "" {
		if parsed, err = strconv.ParseUint(value, 10, 32); err != nil || parsed == 0 {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("wrong param of triggerCnt")))
			return
		}
		triggerCnt = uint32(parsed)
	}
	if err = vol.updateClientParam(m.cluster, period, triggerCnt); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
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
}

func (m *Server) createDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                     string
		volName                    string
		vol                        *Vol
		reqCreateCount             int
		mediaType                  uint32
		lastTotalDataPartitions    int
		clusterTotalDataPartitions int
		err                        error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreateDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminCreateDataPartition, metric, err, map[string]string{exporter.Vol: volName})
	}()

	if reqCreateCount, volName, mediaType, err = parseRequestToCreateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogInfof("[createDataPartition] createCount(%v) volName(%v) mediaType(%v)", reqCreateCount, volName, mediaType)

	if reqCreateCount > maxInitDataPartitionCnt {
		err = fmt.Errorf("count[%d] exceeds maximum limit[%d]", reqCreateCount, maxInitDataPartitionCnt)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if proto.IsStorageClassBlobStore(vol.volStorageClass) {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("low frequency vol can't create dp")))
		return
	}

	if mediaType == proto.MediaType_Unspecified {
		mediaType = proto.GetMediaTypeByStorageClass(vol.volStorageClass)
		log.LogInfof("[createDataPartition] vol(%v) no assigned mediaType in volStorageClass, choose mediaType(%v) by volStorageClass(%v)",
			volName, proto.MediaTypeString(mediaType), proto.StorageClassString(vol.volStorageClass))
	} else if !proto.IsValidMediaType(mediaType) {
		err = fmt.Errorf("invalid param mediaType(%v)", mediaType)
		log.LogErrorf("[createDataPartition] vol(%v), err: %v", volName, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	lastTotalDataPartitions = len(vol.dataPartitions.partitions)
	clusterTotalDataPartitions = m.cluster.getDataPartitionCount()

	err = m.cluster.batchCreateDataPartition(vol, reqCreateCount, false, mediaType)
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
	rstMsg := " changeDataPartitionLeader command success send to dest host but need check. "
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

	dp.RLock()
	newHosts := append(dp.Hosts, addr)
	dp.RUnlock()
	if err = m.cluster.checkMultipleReplicasOnSameMachine(newHosts); err != nil {
		return
	}

	retry := 0
	for {
		if !dp.setRestoreReplicaForbidden() {
			retry++
			if retry > defaultDecommissionRetryLimit {
				err = errors.NewErrorf("set RestoreReplicaMetaForbidden failed")
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	if err = m.cluster.addDataReplica(dp, addr, false); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// for disk manager to check status for new replica
	dp.DecommissionDstAddr = addr
	dp.DecommissionType = ManualAddReplica
	dp.RecoverStartTime = time.Now()
	dp.SetDecommissionStatus(DecommissionRunning)

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
		allHosts    []string
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

	mp.RLock()
	allHosts = append(mp.Hosts, addr)
	mp.RUnlock()
	if err = m.cluster.checkMultipleReplicasOnSameMachine(allHosts); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
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
	rstMsg := " changeMetaPartitionLeader command success send to dest host but need check. "
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// balance the leader meta partition in metaNodes which can select all cluster some zones or noteSet
func (m *Server) balanceMetaPartitionLeader(w http.ResponseWriter, r *http.Request) {
	var (
		zonesKey     string
		nodesetIdKey string
		err          error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminBalanceMetaPartitionLeader))
	defer func() {
		doStatAndMetric(proto.AdminBalanceMetaPartitionLeader, metric, err, nil)
	}()
	if zonesKey, nodesetIdKey, err = parseRequestToBalanceMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		log.LogErrorf("balanceMetaPartitionLeader.err %v", err)
		return
	}
	log.LogInfof("zone:%v,nodesetId:%v", zonesKey, nodesetIdKey)

	zonesM := make(map[string]struct{})
	if zonesKey != "" {
		zones := strings.Split(zonesKey, commaSplit)
		for _, zone := range zones {
			zonesM[zone] = struct{}{}
		}
	}

	nodesetIdM := make(map[uint64]struct{})
	if nodesetIdKey != "" {
		nodesetIds := strings.Split(nodesetIdKey, commaSplit)
		for _, nodeSetId := range nodesetIds {
			id, err := strconv.ParseUint(nodeSetId, 10, 64)
			if err != nil {
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
				log.LogErrorf("balanceMetaPartitionLeader.err %v", err)
				return
			}
			nodesetIdM[id] = struct{}{}
		}
	}
	log.LogInfof("balanceMetaPartitionLeader zones[%v] length[%d], nodesetIds[%v] length[%d]", zonesKey, len(zonesM), nodesetIdKey, len(nodesetIdM))
	err = m.cluster.balanceMetaPartitionLeader(zonesM, nodesetIdM)
	if err != nil {
		log.LogErrorf("balanceMetaPartitionLeader.err %v", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	rstMsg := "balanceMetaPartitionLeader command sucess"
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
		c           = m.cluster
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDecommissionDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminDecommissionDataPartition, metric, err, nil)
	}()

	if partitionID, addr, err = parseRequestToDecommissionDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dp, err = c.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	raftForce, err = parseRaftForce(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	decommissionType, err := parseUintParam(r, DecommissionType)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// default is ManualDecommission
	if decommissionType == 0 {
		decommissionType = int(ManualDecommission)
	}
	node, err := c.dataNode(addr)
	if err != nil {
		rstMsg = fmt.Sprintf(" dataPartitionID :%v not find datanode for addr %v",
			partitionID, addr)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: rstMsg})
		return
	}
	err = m.cluster.markDecommissionDataPartition(dp, node, raftForce, uint32(decommissionType))
	if err != nil {
		rstMsg = err.Error()
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: rstMsg})
		return
	}
	auditlog.LogMasterOp("DataPartitionDecommission", fmt.Sprintf("decommission dp %v by manual", dp.decommissionInfo()), nil)
	rstMsg = fmt.Sprintf(proto.AdminDecommissionDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) diagnoseDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err                         error
		rstMsg                      *proto.DataPartitionDiagnosis
		inactiveNodes               []string
		corruptDps                  []*DataPartition
		lackReplicaDps              []*DataPartition
		badReplicaDps               []*DataPartition
		repFileCountDifferDps       []*DataPartition
		repUsedSizeDifferDps        []*DataPartition
		excessReplicaDPs            []*DataPartition
		corruptDpIDs                []uint64
		lackReplicaDpIDs            []uint64
		badReplicaDpIDs             []uint64
		repFileCountDifferDpIDs     []uint64
		repUsedSizeDifferDpIDs      []uint64
		excessReplicaDpIDs          []uint64
		badDataPartitionInfos       []proto.BadPartitionRepairView
		diskErrorDataPartitionInfos proto.DiskErrPartitionView
		start                       = time.Now()
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDiagnoseDataPartition))
	defer func() {
		doStatAndMetric(proto.AdminDiagnoseDataPartition, metric, err, nil)
	}()

	ignoreDiscardDp, err := pareseBoolWithDefault(r, ignoreDiscardKey, false)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	corruptDpIDs = make([]uint64, 0)
	lackReplicaDpIDs = make([]uint64, 0)
	badReplicaDpIDs = make([]uint64, 0)
	repFileCountDifferDpIDs = make([]uint64, 0)
	repUsedSizeDifferDpIDs = make([]uint64, 0)
	excessReplicaDpIDs = make([]uint64, 0)

	subStep := time.Now()
	if inactiveNodes, err = m.cluster.checkInactiveDataNodes(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	log.LogDebugf("diagnoseDataPartition checkInactiveDataNodes cost %v", time.Since(subStep).String())
	if lackReplicaDps, badReplicaDps, repFileCountDifferDps, repUsedSizeDifferDps, excessReplicaDPs,
		corruptDps, err = m.cluster.checkReplicaOfDataPartitions(ignoreDiscardDp); err != nil {
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
	for _, dp := range repFileCountDifferDps {
		repFileCountDifferDpIDs = append(repFileCountDifferDpIDs, dp.PartitionID)
	}
	for _, dp := range repUsedSizeDifferDps {
		repUsedSizeDifferDpIDs = append(repUsedSizeDifferDpIDs, dp.PartitionID)
	}
	for _, dp := range excessReplicaDPs {
		excessReplicaDpIDs = append(excessReplicaDpIDs, dp.PartitionID)
	}

	// badDataPartitions = m.cluster.getBadDataPartitionsView()
	subStep = time.Now()
	badDataPartitionInfos = m.cluster.getBadDataPartitionsRepairView()
	log.LogDebugf("diagnoseDataPartition checkInactiveDataNodes cost %v", time.Since(subStep).String())
	subStep = time.Now()
	diskErrorDataPartitionInfos = m.cluster.getDiskErrDataPartitionsView()
	log.LogDebugf("diagnoseDataPartition checkInactiveDataNodes cost %v", time.Since(subStep).String())
	rstMsg = &proto.DataPartitionDiagnosis{
		InactiveDataNodes:           inactiveNodes,
		CorruptDataPartitionIDs:     corruptDpIDs,
		LackReplicaDataPartitionIDs: lackReplicaDpIDs,
		BadDataPartitionInfos:       badDataPartitionInfos,
		BadReplicaDataPartitionIDs:  badReplicaDpIDs,
		RepFileCountDifferDpIDs:     repFileCountDifferDpIDs,
		RepUsedSizeDifferDpIDs:      repUsedSizeDifferDpIDs,
		ExcessReplicaDpIDs:          excessReplicaDpIDs,
		DiskErrorDataPartitionInfos: diskErrorDataPartitionInfos,
	}
	log.LogInfof("diagnose dataPartition[%v] inactiveNodes:[%v], corruptDpIDs:[%v], "+
		"lackReplicaDpIDs:[%v], BadReplicaDataPartitionIDs[%v], "+
		"repFileCountDifferDpIDs:[%v], RepUsedSizeDifferDpIDs[%v], excessReplicaDpIDs[%v] cost %v",
		m.cluster.Name, inactiveNodes, corruptDpIDs,
		lackReplicaDpIDs, badReplicaDpIDs,
		repFileCountDifferDpIDs, repUsedSizeDifferDpIDs, excessReplicaDpIDs, time.Since(start).String())
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
	dp.ReleaseDecommissionToken(m.cluster)
	dp.ResetDecommissionStatus()
	dp.RestoreReplica = RestoreReplicaMetaStop
	msg = fmt.Sprintf("partitionID :%v  reset decommission status successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) queryDataPartitionDecommissionStatus(w http.ResponseWriter, r *http.Request) {
	var (
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
	info := &proto.DecommissionDataPartitionInfo{
		PartitionId:        partitionID,
		ReplicaNum:         dp.ReplicaNum,
		Status:             GetDecommissionStatusMessage(dp.GetDecommissionStatus()),
		SpecialStep:        GetSpecialDecommissionStatusMessage(dp.GetSpecialReplicaDecommissionStep()),
		Retry:              dp.DecommissionRetry,
		RaftForce:          dp.DecommissionRaftForce,
		Recover:            dp.isRecover,
		SrcAddress:         dp.DecommissionSrcAddr,
		SrcDiskPath:        dp.DecommissionSrcDiskPath,
		DstAddress:         dp.DecommissionDstAddr,
		Term:               dp.DecommissionTerm,
		Replicas:           replicas,
		ErrorMessage:       dp.DecommissionErrorMessage,
		NeedRollbackTimes:  atomic.LoadUint32(&dp.DecommissionNeedRollbackTimes),
		DecommissionType:   GetDecommissionTypeMessage(dp.DecommissionType),
		RestoreReplicaType: GetRestoreReplicaMessage(dp.RestoreReplica),
		IsDiscard:          dp.IsDiscard,
		RecoverStartTime:   dp.RecoverStartTime.Format("2006-01-02 15:04:05"),
	}
	sendOkReply(w, r, newSuccessHTTPReply(info))
}

// Mark the volume as deleted, which will then be deleted later.
func (m *Server) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		status  bool
		// force   bool
		err error
		msg string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDeleteVol))
	defer func() {
		doStatAndMetric(proto.AdminDeleteVol, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, authKey, status, _, err = parseRequestToDeleteVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if enableDirectDeleteVol {
		if err = m.cluster.markDeleteVol(name, authKey, false, true); err != nil {
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
		return
	}
	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	if status {
		if vol.Status == proto.VolStatusMarkDelete {
			err = errors.New("vol has been mark delete, repeated deletions are not allowed")
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolHasDeleted, Msg: err.Error()})
			return
		}
		oldForbiden := vol.Forbidden
		vol.Forbidden = true
		vol.authKey = authKey
		vol.DeleteExecTime = time.Now().Add(time.Duration(m.config.volDelayDeleteTimeHour) * time.Hour)
		vol.user = m.user
		m.cluster.deleteVolMutex.Lock()
		m.cluster.delayDeleteVolsInfo = append(m.cluster.delayDeleteVolsInfo, &delayDeleteVolInfo{volName: name, authKey: authKey, execTime: vol.DeleteExecTime, user: m.user})
		m.cluster.deleteVolMutex.Unlock()
		defer func() {
			if err != nil {
				vol.Forbidden = oldForbiden
				vol.authKey = ""
				vol.DeleteExecTime = time.Time{}
				vol.user = nil
				var index int
				var value *delayDeleteVolInfo
				for index, value = range m.cluster.delayDeleteVolsInfo {
					if value.volName == name {
						break
					}
				}
				m.cluster.delayDeleteVolsInfo = append(m.cluster.delayDeleteVolsInfo[:index], m.cluster.delayDeleteVolsInfo[index+1:]...)
			}
		}()

		if err = m.cluster.markDeleteVol(name, authKey, false, true); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		vol.setDpForbid()
		vol.setMpForbid()

		log.LogDebugf("action[markDeleteVol] delayDeleteVolsInfo[%v]", m.cluster.delayDeleteVolsInfo)
		msg = fmt.Sprintf("delete vol: forbid vol[%v] successfully,from[%v]", name, r.RemoteAddr)
		log.LogWarn(msg)
		sendOkReply(w, r, newSuccessHTTPReply(msg))
		return
	}
	var index int
	var value *delayDeleteVolInfo
	if len(m.cluster.delayDeleteVolsInfo) == 0 {
		msg := fmt.Sprintf("vol[%v] was not previously deleted or already deleted", name)
		err = errors.New(msg)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotDelete, Msg: err.Error()})
		return
	}
	m.cluster.deleteVolMutex.RLock()
	for index, value = range m.cluster.delayDeleteVolsInfo {
		if value.volName == name {
			break
		}
	}
	m.cluster.deleteVolMutex.RUnlock()
	if index == len(m.cluster.delayDeleteVolsInfo)-1 && value.volName != name {
		msg := fmt.Sprintf("vol[%v] was not previously deleted or already deleted", name)
		err = errors.New(msg)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotDelete, Msg: err.Error()})
		return
	}

	oldForbiden := vol.Forbidden
	oldAuthKey := vol.authKey
	oldDeleteExecTime := vol.DeleteExecTime
	oldUser := vol.user
	vol.Forbidden = false
	vol.authKey = ""
	vol.DeleteExecTime = time.Time{}
	vol.user = nil
	defer func() {
		if err != nil {
			vol.Forbidden = oldForbiden
			vol.authKey = oldAuthKey
			vol.DeleteExecTime = oldDeleteExecTime
			vol.user = oldUser
		}
	}()
	if err = m.cluster.markDeleteVol(name, authKey, false, false); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	m.cluster.deleteVolMutex.Lock()
	m.cluster.delayDeleteVolsInfo = append(m.cluster.delayDeleteVolsInfo[:index], m.cluster.delayDeleteVolsInfo[index+1:]...)
	m.cluster.deleteVolMutex.Unlock()
	msg = fmt.Sprintf("undelete vol: unforbid vol[%v] successfully,from[%v]", name, r.RemoteAddr)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) checkReplicaNum(r *http.Request, vol *Vol, req *updateVolReq) (err error) {
	var (
		replicaNumInt64 int64
		replicaNum      int
	)

	if replicaNumStr := r.FormValue(replicaNumKey); replicaNumStr != "" {
		if replicaNumInt64, err = strconv.ParseInt(replicaNumStr, 10, 8); err != nil {
			err = unmatchedKey(replicaNumKey)
			return
		}
		replicaNum = int(replicaNumInt64)
	} else {
		replicaNum = int(vol.dpReplicaNum)
	}
	req.replicaNum = replicaNum
	if replicaNum != 0 && replicaNum != int(vol.dpReplicaNum) {
		if replicaNum != int(vol.dpReplicaNum)-1 {
			err = fmt.Errorf("replicaNum only need be reduced one replica one time")
			return
		}
		if !proto.IsHot(vol.VolType) {
			err = fmt.Errorf("vol type(%v) replicaNum cann't be changed", vol.VolType)
			return
		}
		if ok, dpArry := vol.isOkUpdateRepCnt(); !ok {
			err = fmt.Errorf("vol have dataPartitions[%v] with inconsistent dataPartitions cnt to volume's ", dpArry)
			return
		}
	}
	if proto.IsHot(vol.VolType) {
		if req.replicaNum == 0 ||
			((req.replicaNum == 1 || req.replicaNum == 2) && !req.followerRead) {
			err = fmt.Errorf("replica or follower read status error")
			return
		}
	} else {
		if req.replicaNum == 0 && req.coldArgs.cacheCap > 0 {
			req.replicaNum = 1
		}
		if (req.replicaNum == 0 && req.replicaNum != int(vol.dpReplicaNum)) || !req.followerRead {
			err = fmt.Errorf("replica or follower read status error")
			return
		}
	}
	return
}

func (m *Server) updateVol(w http.ResponseWriter, r *http.Request) {
	var (
		req = &updateVolReq{}
		vol *Vol
		err error
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

	if req.authenticate, err = pareseBoolWithDefault(r, authenticateKey, vol.authenticate); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.checkReplicaNum(r, vol, req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)
	if err = parseArgs(r,
		newArg("remoteCacheEnable", &newArgs.remoteCacheEnable).OmitEmpty(),
		newArg("remoteCacheEnable", &newArgs.remoteCacheEnable).OmitEmpty(),
		newArg("remoteCachePath", &newArgs.remoteCachePath).OmitEmpty(),
		newArg("remoteCacheAutoPrepare", &newArgs.remoteCacheAutoPrepare).OmitEmpty(),
		newArg("remoteCacheTTL", &newArgs.remoteCacheTTL).OmitEmpty(),
		newArg("remoteCacheReadTimeoutSec", &newArgs.remoteCacheReadTimeoutSec).OmitEmpty(),
		newArg("remoteCacheMaxFileSizeGB", &newArgs.remoteCacheMaxFileSizeGB).OmitEmpty(),
		newArg("remoteCacheOnlyForNotSSD", &newArgs.remoteCacheOnlyForNotSSD).OmitEmpty(),
		newArg("remoteCacheFollowerRead", &newArgs.remoteCacheFollowerRead).OmitEmpty(),
	); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if newArgs.remoteCacheReadTimeoutSec < proto.ReadDeadlineTime {
		err = fmt.Errorf("remoteCacheReadTimeoutSec cannot < %v", proto.ReadDeadlineTime)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if req.quotaClass != 0 {
		newArgs.quotaByClass[req.quotaClass] = req.quotaOfClass
		log.LogWarnf("updateVol: try update vol capcity, class %d, cap %d, name %s", req.quotaClass, req.quotaOfClass, req.name)
	}

	// check whether can close forbidWriteOpOfProtoVer0
	if req.forbidWriteOpOfProtoVer0 != vol.ForbidWriteOpOfProtoVer0.Load() && !req.forbidWriteOpOfProtoVer0 && len(vol.allowedStorageClass) > 1 {
		err = fmt.Errorf("can't update forbidWriteOpOfProtoVer0 to false")
		log.LogErrorf("updateVol: there are two allowd storage class, can't close forbidWriteOpOfProtoVer0. name %s, allowd %v", vol.Name, vol.allowedStorageClass)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if len(newArgs.remoteCachePath) != 0 {
		newArgs.remoteCachePath = deduplicateAndRemoveContained(newArgs.remoteCachePath)
	}
	newArgs.zoneName = req.zoneName
	newArgs.crossZone = req.crossZone
	newArgs.description = req.description
	newArgs.capacity = req.capacity
	newArgs.deleteLockTime = req.deleteLockTime
	newArgs.followerRead = req.followerRead
	newArgs.metaFollowerRead = req.metaFollowerRead
	newArgs.directRead = req.directRead
	newArgs.maximallyRead = req.maximallyRead
	newArgs.authenticate = req.authenticate
	newArgs.dpSelectorName = req.dpSelectorName
	newArgs.dpSelectorParm = req.dpSelectorParm
	newArgs.enablePosixAcl = req.enablePosixAcl
	newArgs.enableTransaction = req.enableTransaction
	newArgs.leaderRetryTimeout = req.leaderRetryTimeout
	newArgs.txTimeout = req.txTimeout
	newArgs.txConflictRetryNum = req.txConflictRetryNum
	newArgs.txConflictRetryInterval = req.txConflictRetryInterval
	newArgs.txOpLimit = req.txOpLimit
	newArgs.enableQuota = req.enableQuota
	newArgs.trashInterval = req.trashInterval
	newArgs.accessTimeValidInterval = req.accessTimeValidInterval
	newArgs.enablePersistAccessTime = req.enablePersistAccessTime
	if req.coldArgs != nil {
		newArgs.coldArgs = req.coldArgs
	}

	newArgs.dpReplicaNum = uint8(req.replicaNum)
	newArgs.dpReadOnlyWhenVolFull = req.dpReadOnlyWhenVolFull
	newArgs.enableAutoDpMetaRepair = req.enableAutoDpMetaRepair
	newArgs.volStorageClass = req.volStorageClass
	newArgs.forbidWriteOpOfProtoVer0 = req.forbidWriteOpOfProtoVer0

	log.LogWarnf("[updateVolOut] name [%s], z1 [%s], z2[%s] replicaNum[%v], FR[%v], metaFR[%v], MMR[%v]",
		req.name, req.zoneName, vol.zoneName, req.replicaNum, req.followerRead, req.metaFollowerRead, req.maximallyRead)
	if err = m.cluster.updateVol(req.name, req.authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	var response string
	if hasTxParams(r) {
		response = fmt.Sprintf("update vol[%v] successfully, txTimeout[%v] enableTransaction[%v]",
			req.name, newArgs.txTimeout, proto.GetMaskString(newArgs.enableTransaction))
	} else {
		response = fmt.Sprintf("update vol[%v] successfully", req.name)
	}
	sendOkReply(w, r, newSuccessHTTPReply(response))
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

func (m *Server) HasMultiReplicaStorageClass(allowedStorageClass []uint32) bool {
	var hasReplicaHdd bool
	var hasReplicaSSD bool

	for _, asc := range allowedStorageClass {
		if asc == proto.StorageClass_Replica_HDD {
			hasReplicaHdd = true
		}

		if asc == proto.StorageClass_Replica_SSD {
			hasReplicaSSD = true
		}
	}

	return hasReplicaHdd && hasReplicaSSD
}

func (m *Server) HasBothReplicaAndBlobstore(storageClass uint32, allowedStorageClass []uint32) bool {
	var hasReplica bool
	var hasBlob bool

	if proto.IsStorageClassReplica(storageClass) {
		hasReplica = true
	} else if proto.IsStorageClassBlobStore(storageClass) {
		hasBlob = true
	}

	for _, asc := range allowedStorageClass {
		if proto.IsStorageClassReplica(asc) {
			hasReplica = true
		} else if proto.IsStorageClassBlobStore(asc) {
			hasBlob = true
		}
	}

	return hasReplica && hasBlob
}

func (m *Server) checkStorageClassForCreateVolReq(req *createVolReq) (err error) {
	scope := "cluster"
	if req.zoneName != "" {
		scope = fmt.Sprintf("assigned zones(%v)", req.zoneName)

		notExistZones := make([]string, 0)
		reqZoneList := strings.Split(req.zoneName, ",")
		for _, reqZone := range reqZoneList {
			if _, err = m.cluster.t.getZone(reqZone); err != nil {
				notExistZones = append(notExistZones, reqZone)
			}
		}

		if len(notExistZones) != 0 {
			err = fmt.Errorf("assigned zone name not exist: %v", notExistZones)
			log.LogErrorf("[checkStorageClassForCreateVol] create vol(%v) err:%v", req.name, err.Error())
			return err
		}
	}

	resourceChecker := NewStorageClassResourceChecker(m.cluster, req.zoneName)

	if req.volStorageClass == proto.StorageClass_Unspecified {
		// when volStorageClass not specified, try to set as replica with fastest mediaType if resource can support
		req.volStorageClass = m.cluster.GetFastestReplicaStorageClassInCluster(resourceChecker, req.zoneName)
		if req.volStorageClass == proto.StorageClass_Unspecified {
			err = fmt.Errorf("volStorageClass not specified and %v has no resource to auto choose replca storageClass", scope)
			log.LogErrorf("[checkStorageClassForCreateVol] create vol(%v) err:%v", req.name, err.Error())
			return err
		}

		log.LogInfof("[checkStorageClassForCreateVol] create vol(%v), volStorageClass not specified, auto set as: %v",
			req.name, proto.StorageClassString(req.volStorageClass))
	} else if proto.IsStorageClassBlobStore(req.volStorageClass) {
		req.cacheDpStorageClass = m.cluster.GetFastestReplicaStorageClassInCluster(resourceChecker, req.zoneName)

		log.LogInfof("[checkStorageClassForCreateVol] create vol(%v) volStorageClass(%v) set cacheDpStorageClass: %v",
			req.name, proto.StorageClassString(req.volStorageClass), proto.StorageClassString(req.cacheDpStorageClass))
	}

	if !proto.IsValidStorageClass(req.volStorageClass) {
		err = fmt.Errorf("invalid volStorageClass: %v", req.volStorageClass)
		log.LogErrorf("[checkStorageClassForCreateVol] create vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if !resourceChecker.HasResourceOfStorageClass(req.volStorageClass) {
		err = fmt.Errorf("%v has no resoure to support volStorageClass(%v)", scope, proto.StorageClassString(req.volStorageClass))
		log.LogErrorf("action[checkStorageClassForCreateVol] create vol(%v) err: %v", req.name, err.Error())
		return
	}

	log.LogInfof("[checkStorageClassForCreateVol] volStorageClass: %v", proto.StorageClassString(req.volStorageClass))

	if len(req.allowedStorageClass) == 0 {
		req.allowedStorageClass = append(req.allowedStorageClass, req.volStorageClass)
		log.LogInfof("[checkStorageClassForCreateVol] create vol(%v), allowedStorageClass not specified, auto set as volStorageClass(%v)",
			req.name, req.volStorageClass)
		return
	}

	// will support both replica and blobstore later
	if m.HasBothReplicaAndBlobstore(req.volStorageClass, req.allowedStorageClass) {
		err = fmt.Errorf("vol not support both replica and blobstore")
		log.LogErrorf("action[checkStorageClassForCreateVol] create vol(%v) err: %v", req.name, err.Error())
		return
	}

	isVolStorageClassInAllowed := false
	for idx, asc := range req.allowedStorageClass {
		if !proto.IsValidStorageClass(asc) {
			err = fmt.Errorf("allowedStorageClass index(%v) invalid value(%v)", idx, asc)
			log.LogErrorf("[checkStorageClassForCreateVol] create vol(%v) err:%v", req.name, err.Error())
			return err
		}

		if !resourceChecker.HasResourceOfStorageClass(asc) {
			err = fmt.Errorf("%v has no resoure to support allowedStorageClass(%v)", scope, proto.StorageClassString(asc))
			log.LogErrorf("[checkStorageClassForCreateVol] create vol(%v) err: %v", req.name, err.Error())
			return
		}

		if proto.IsStorageClassBlobStore(asc) {
			if req.coldArgs.objBlockSize == 0 {
				req.coldArgs.objBlockSize = defaultEbsBlkSize
				log.LogInfof("[checkStorageClassForCreateVol] vol(%v) allowed %v, set objBlockSize as default(%v)",
					req.name, proto.StorageClassString(proto.StorageClass_BlobStore), defaultEbsBlkSize)
			}
		}

		// To control the complexity of the entire system at the current stage,
		// not allow normal dp and cache/preload dp both exist in a volume at the same time:
		// If volStorageClass is replica, will not create cache/preload dp even blobStore contained in allowedStorageClass
		// if volStorageClass is blobStore, replica storage class can not be supported
		if proto.IsStorageClassBlobStore(req.volStorageClass) && proto.IsStorageClassReplica(asc) {
			err = fmt.Errorf("volStorageClass is blobStore, in this case not support replica storage class")
			log.LogErrorf("[checkStorageClassForCreateVol] create vol(%v) err: %v", req.name, err.Error())
			return
		}

		if asc == req.volStorageClass {
			isVolStorageClassInAllowed = true
		}
	}

	// auto add volStorageClass to allowedStorageClass if omit
	if !isVolStorageClassInAllowed {
		log.LogInfof("[checkStorageClassForCreateVol] creating vol(%v) auto append volStorageClass(%v) to allowedStorageClass",
			req.name, req.volStorageClass)
		req.allowedStorageClass = append(req.allowedStorageClass, req.volStorageClass)
	}

	sort.Slice(req.allowedStorageClass, func(i, j int) bool {
		return req.allowedStorageClass[i] < req.allowedStorageClass[j]
	})

	if m.HasMultiReplicaStorageClass(req.allowedStorageClass) {
		if !req.crossZone {
			err = fmt.Errorf("more than one replica storageClass in request allowedStorageClass, but crossZone is false")
			log.LogErrorf("[checkStorageClassForCreateVol] vol(%v) err: %v", req.name, err.Error())
			return
		}

		if m.cluster.FaultDomain {
			err = fmt.Errorf("cluster.FaultDomain is true, can not create vol with req.allowedStorageClass has multi replica storageClass")
			log.LogErrorf("[checkStorageClassForCreateVol] vol(%v) err: %v", req.name, err.Error())
			return
		}
	}

	log.LogInfof("[checkStorageClassForCreateVol] vol(%v) volStorageClass(%v) allowedStorageClass: %v",
		req.name, req.volStorageClass, req.allowedStorageClass)
	return nil
}

func (m *Server) checkCreateVolReq(req *createVolReq) (err error) {
	if err = m.checkStorageClassForCreateVolReq(req); err != nil {
		return err
	}

	// property volType of volume is maintained for compatibility, now it's determined by volStorageClass
	if err, req.volType = proto.GetVolTypeByStorageClass(req.volStorageClass); err != nil {
		log.LogErrorf("[checkStorageClassForCreateVol] creating vol(%v) err when got volType:%v", req.name, err.Error())
		return err
	}

	if req.capacity == 0 {
		err = fmt.Errorf("vol capacity can't be zero, %d", req.capacity)
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if req.dpSize != 0 && req.dpSize <= 10 {
		err = fmt.Errorf("datapartition size must be bigger than 10 G")
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if req.dpCount > maxInitDataPartitionCnt {
		return fmt.Errorf("dpCount[%d] exceeds maximum limit[%d]", req.dpCount, maxInitDataPartitionCnt)
	}

	if req.dpCount < defaultInitDataPartitionCnt {
		req.dpCount = defaultInitDataPartitionCnt
	}

	if proto.IsStorageClassReplica(req.volStorageClass) {
		if req.dpReplicaNum == 0 {
			req.dpReplicaNum = defaultReplicaNum
			log.LogInfof("[checkCreateVolReq] creating vol(%v), req dpReplicaNum is 0, set as defaultReplicaNum(%v)",
				req.name, defaultReplicaNum)
		}

		if req.dpReplicaNum > 3 {
			err = fmt.Errorf("hot vol's replicaNum should be 1 to 3, received replicaNum is[%v]", req.dpReplicaNum)
			log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
			return err
		}

		return nil
	} else if proto.IsStorageClassBlobStore(req.volStorageClass) {
		if req.dpReplicaNum > 16 {
			err = fmt.Errorf("cold vol's replicaNum should less then 17, received replicaNum is[%v]", req.dpReplicaNum)
			log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
			return err
		}
	}

	if req.dpReplicaNum == 0 && req.coldArgs.cacheCap > 0 {
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
		err = fmt.Errorf("cache lruInterval(%d) must bigger than 2 minutes", args.cacheLRUInterval)
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if args.cacheLRUInterval == 0 {
		args.cacheLRUInterval = defaultCacheLruInterval
	}

	if args.cacheLowWater >= args.cacheHighWater {
		err = fmt.Errorf("low water(%d) must be less than high water(%d)", args.cacheLowWater, args.cacheHighWater)
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if args.cacheCap >= uint64(req.capacity) {
		err = fmt.Errorf("cache capacity(%d) must be less than capacity(%d)", args.cacheCap, req.capacity)
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if proto.IsCold(req.volType) && req.dpReplicaNum == 0 && args.cacheCap > 0 {
		return fmt.Errorf("cache capacity(%d) not zero,replicaNum should not be zero", args.cacheCap)
	}

	if args.cacheHighWater >= 90 || args.cacheLowWater >= 90 {
		err = fmt.Errorf("low(%d) or high water(%d) can't be large than 90, low than 0", args.cacheLowWater, args.cacheHighWater)
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if int(req.dpReplicaNum) > m.cluster.dataNodeCount() {
		err = fmt.Errorf("dp replicaNum %d can't be large than dataNodeCnt %d", req.dpReplicaNum, m.cluster.dataNodeCount())
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if req.accessTimeValidInterval < proto.MinAccessTimeValidInterval {
		return fmt.Errorf("accessTimeValidInterval must greater than or equal to 1800")
	}
	req.coldArgs = args

	if proto.IsHot(req.volType) && (req.dpReplicaNum == 1 || req.dpReplicaNum == 2) && !req.followerRead {
		err = fmt.Errorf("hot volume dpReplicaNum(%v) less than 3, followerRead must set true", req.dpReplicaNum)
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	if req.remoteCacheReadTimeout < proto.ReadDeadlineTime {
		err = fmt.Errorf("remoteCacheReadTimeout(%v) less than %v",
			req.remoteCacheReadTimeout, proto.ReadDeadlineTime)
		log.LogErrorf("[checkCreateVolReq] creating vol(%v) err:%v", req.name, err.Error())
		return err
	}

	return nil
}

func (m *Server) createVol(w http.ResponseWriter, r *http.Request) {
	req := &createVolReq{}
	var vol *Vol
	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreateVol))
	defer func() {
		doStatAndMetric(proto.AdminCreateVol, metric, err, map[string]string{exporter.Vol: req.name})
	}()

	if err = parseRequestToCreateVol(r, req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.checkCreateVolReq(req); err != nil {
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
	var clientInfo *proto.ClientReportLimitInfo
	if qosEnable, _ := strconv.ParseBool(qosEnableStr); qosEnable {
		if clientInfo, err = parseQosInfo(r); err == nil {
			log.LogDebugf("action[qosUpload] cliInfoMgrMap [%v],clientInfo id[%v] clientInfo.Host %v, enable %v", clientInfo.ID, clientInfo.Host, r.RemoteAddr, qosEnable)
			if clientInfo.ID == 0 {
				if limit, err = vol.qosManager.init(m.cluster, clientInfo); err != nil {
					sendErrReply(w, r, newErrHTTPReply(err))
					return
				}
				clientInfo.ID = limit.ID
			}
			if limit, err = vol.qosManager.HandleClientQosReq(clientInfo, clientInfo.ID); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
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

func newSimpleView(vol *Vol) (view *proto.SimpleVolView) {
	var (
		volInodeCount  uint64
		volDentryCount uint64
	)
	vol.mpsLock.RLock()
	for _, mp := range vol.MetaPartitions {
		volDentryCount = volDentryCount + mp.DentryCount
		volInodeCount = volInodeCount + mp.InodeCount
	}
	vol.mpsLock.RUnlock()
	maxMetaPartitionID := vol.maxMetaPartitionID()
	maxDataPartitionID := vol.dataPartitions.getMaxDataPartitionID()

	quotaOfClass := []*proto.StatOfStorageClass{}
	for t, c := range vol.getQuotaByClass() {
		quotaOfClass = append(quotaOfClass, proto.NewStatOfStorageClassEx(t, c))
	}

	view = &proto.SimpleVolView{
		ID:                 vol.ID,
		Name:               vol.Name,
		Owner:              vol.Owner,
		ZoneName:           vol.zoneName,
		DpReplicaNum:       vol.dpReplicaNum,
		MpReplicaNum:       vol.mpReplicaNum,
		InodeCount:         volInodeCount,
		DentryCount:        volDentryCount,
		MaxMetaPartitionID: maxMetaPartitionID,
		MaxDataPartitionID: maxDataPartitionID,
		Status:             vol.Status,
		Capacity:           vol.Capacity,
		FollowerRead:       vol.FollowerRead,
		MetaFollowerRead:   vol.MetaFollowerRead,
		DirectRead:         vol.DirectRead,
		MaximallyRead:      vol.MaximallyRead,
		LeaderRetryTimeOut: vol.LeaderRetryTimeout,

		EnablePosixAcl:          vol.enablePosixAcl,
		EnableQuota:             vol.enableQuota,
		EnableTransactionV1:     proto.GetMaskString(vol.enableTransaction),
		EnableTransaction:       "off",
		TxTimeout:               vol.txTimeout,
		TxConflictRetryNum:      vol.txConflictRetryNum,
		TxConflictRetryInterval: vol.txConflictRetryInterval,
		TxOpLimit:               vol.txOpLimit,
		NeedToLowerReplica:      vol.NeedToLowerReplica,
		Authenticate:            vol.authenticate,
		CrossZone:               vol.crossZone,
		DefaultPriority:         vol.defaultPriority,
		DomainOn:                vol.domainOn,
		RwDpCnt:                 vol.dataPartitions.readableAndWritableCnt,
		MpCnt:                   len(vol.MetaPartitions),
		DpCnt:                   len(vol.dataPartitions.partitionMap),
		CreateTime:              time.Unix(vol.createTime, 0).Format(proto.TimeFormat),
		DeleteLockTime:          vol.DeleteLockTime,
		Description:             vol.description,
		DpSelectorName:          vol.dpSelectorName,
		DpSelectorParm:          vol.dpSelectorParm,
		DpReadOnlyWhenVolFull:   vol.DpReadOnlyWhenVolFull,
		VolType:                 vol.VolType,
		ObjBlockSize:            vol.EbsBlkSize,
		CacheCapacity:           vol.CacheCapacity,
		CacheAction:             vol.CacheAction,
		CacheThreshold:          vol.CacheThreshold,
		CacheLruInterval:        vol.CacheLRUInterval,
		CacheTtl:                vol.CacheTTL,
		CacheLowWater:           vol.CacheLowWater,
		CacheHighWater:          vol.CacheHighWater,
		CacheRule:               vol.CacheRule,
		PreloadCapacity:         vol.getPreloadCapacity(),
		TrashInterval:           vol.TrashInterval,
		DisableAuditLog:         vol.DisableAuditLog,
		LatestVer:               vol.VersionMgr.getLatestVer(),
		Forbidden:               vol.Forbidden,
		DeleteExecTime:          vol.DeleteExecTime,
		DpRepairBlockSize:       vol.dpRepairBlockSize,
		EnableAutoDpMetaRepair:  vol.EnableAutoMetaRepair.Load(),
		AccessTimeInterval:      vol.AccessTimeValidInterval,
		EnablePersistAccessTime: vol.EnablePersistAccessTime,

		VolStorageClass:          vol.volStorageClass,
		CacheDpStorageClass:      vol.cacheDpStorageClass,
		ForbidWriteOpOfProtoVer0: vol.ForbidWriteOpOfProtoVer0.Load(),
		QuotaOfStorageClass:      quotaOfClass,

		RemoteCacheEnable:         vol.remoteCacheEnable,
		RemoteCachePath:           vol.remoteCachePath,
		RemoteCacheAutoPrepare:    vol.remoteCacheAutoPrepare,
		RemoteCacheTTL:            vol.remoteCacheTTL,
		RemoteCacheReadTimeoutSec: vol.remoteCacheReadTimeoutSec,
		RemoteCacheMaxFileSizeGB:  vol.remoteCacheMaxFileSizeGB,
		RemoteCacheOnlyForNotSSD:  vol.remoteCacheOnlyForNotSSD,
		RemoteCacheFollowerRead:   vol.remoteCacheFollowerRead,
	}
	view.AllowedStorageClass = make([]uint32, len(vol.allowedStorageClass))
	copy(view.AllowedStorageClass, vol.allowedStorageClass)

	vol.uidSpaceManager.rwMutex.RLock()
	defer vol.uidSpaceManager.rwMutex.RUnlock()
	for _, uid := range vol.uidSpaceManager.uidInfo {
		view.Uids = append(view.Uids, proto.UidSimpleInfo{
			UID:     uid.Uid,
			Limited: uid.Limited,
		})
	}
	return
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
		nodeAddr          string
		zoneName          string
		raftHeartbeatPort string
		raftReplicaPort   string
		mediaType         uint32
		id                uint64
		err               error
		nodesetId         uint64
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AddDataNode))
	defer func() {
		doStatAndMetric(proto.AddDataNode, metric, err, nil)
	}()

	if nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, mediaType, err = parseRequestForAddNode(r); err != nil {
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
			return
		}
	}
	if id, err = m.cluster.addDataNode(nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, nodesetId, mediaType); err != nil {
		log.LogErrorf("addDataNode: add failed, addr %s, zone %s, set %d, type %d, err %s",
			nodeAddr, zoneName, nodesetId, mediaType, err.Error())
		err = errors.NewErrorf("add datanode failed, err %s, hint %s", err.Error(), proto.ErrDataNodeAdd.Error())
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
	log.LogDebugf("getDataNode. addr %v Total %v used %v", nodeAddr, dataNode.Total, dataNode.Used)
	dataNode.PersistenceDataPartitions = m.cluster.getAllDataPartitionIDByDatanode(nodeAddr)
	// some dp maybe removed from this node but decommission failed
	dataNodeInfo = &proto.DataNodeInfo{
		Total:                                 dataNode.Total,
		Used:                                  dataNode.Used,
		AvailableSpace:                        dataNode.AvailableSpace,
		ID:                                    dataNode.ID,
		ZoneName:                              dataNode.ZoneName,
		Addr:                                  dataNode.Addr,
		RaftHeartbeatPort:                     dataNode.HeartbeatPort,
		RaftReplicaPort:                       dataNode.ReplicaPort,
		DomainAddr:                            dataNode.DomainAddr,
		ReportTime:                            dataNode.ReportTime,
		IsActive:                              dataNode.isActive,
		ToBeOffline:                           dataNode.ToBeOffline,
		IsWriteAble:                           dataNode.IsWriteAble(),
		UsageRatio:                            dataNode.UsageRatio,
		SelectedTimes:                         dataNode.SelectedTimes,
		DataPartitionReports:                  dataNode.DataPartitionReports,
		DataPartitionCount:                    dataNode.DataPartitionCount,
		NodeSetID:                             dataNode.NodeSetID,
		PersistenceDataPartitions:             dataNode.PersistenceDataPartitions,
		BadDisks:                              dataNode.BadDisks,
		RdOnly:                                dataNode.RdOnly,
		CanAllocPartition:                     dataNode.canAlloc() && dataNode.canAllocDp(),
		MaxDpCntLimit:                         dataNode.GetPartitionLimitCnt(),
		CpuUtil:                               dataNode.CpuUtil.Load(),
		IoUtils:                               dataNode.GetIoUtils(),
		DecommissionedDisk:                    dataNode.getDecommissionedDisks(),
		BackupDataPartitions:                  dataNode.getBackupDataPartitionIDs(),
		PersistenceDataPartitionsWithDiskPath: m.cluster.getAllDataPartitionWithDiskPathByDataNode(nodeAddr),
		MediaType:                             dataNode.MediaType,
		DiskOpLogs:                            dataNode.DiskOpLogs,
		DpOpLogs:                              dataNode.DpOpLogs,
	}

	sendOkReply(w, r, newSuccessHTTPReply(dataNodeInfo))
}

func (m *Server) setDpCntLimit(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr   string
		dpCntLimit uint64
		dataNode   *DataNode
		err        error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SetDpCntLimit))
	defer func() {
		doStatAndMetric(proto.SetDpCntLimit, metric, err, nil)
	}()

	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if dpCntLimit, err = strconv.ParseUint(r.FormValue(maxDpCntLimitKey), 10, 64); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	oldDpCntLimit := dataNode.DpCntLimit
	dataNode.DpCntLimit = dpCntLimit

	if err = m.cluster.syncUpdateDataNode(dataNode); err != nil {
		dataNode.DpCntLimit = oldDpCntLimit
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set dpCntLimit to %v successfully", dataNode.DpCntLimit)))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) decommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		offLineAddr string
		raftForce   bool
		limit       int
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DecommissionDataNode))
	defer func() {
		doStatAndMetric(proto.DecommissionDataNode, metric, err, nil)
	}()

	if offLineAddr, limit, err = parseDecomDataNodeReq(r); err != nil {
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

	if err = m.cluster.migrateDataNode(offLineAddr, "", raftForce, limit); err != nil {
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

	if !targetNode.IsWriteAble() || !targetNode.PartitionCntLimited() {
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
func (m *Server) pauseDecommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		node        *DataNode
		rstMsg      string
		offLineAddr string
		err         error
		dps         []uint64
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.PauseDecommissionDataNode))
	defer func() {
		doStatAndMetric(proto.PauseDecommissionDataNode, metric, err, nil)
	}()

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err, dps = m.cluster.decommissionDataNodePause(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("pause decommission data node [%v] with paused failed[%v]", offLineAddr, dps)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) cancelDecommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		offLineAddr string
		err         error
		// dps         []uint64
		node *DataNode
		zone *Zone
		ns   *nodeSet
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.PauseDecommissionDataNode))
	defer func() {
		doStatAndMetric(proto.PauseDecommissionDataNode, metric, err, nil)
	}()

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if zone, err = m.cluster.t.getZone(node.ZoneName); err != nil {
		ret := fmt.Sprintf("action[queryDataNodeDecoProgress] find datanode[%s] zone failed[%v]",
			node.Addr, err.Error())
		log.LogWarnf("%v", ret)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}

	if ns, err = zone.getNodeSet(node.NodeSetID); err != nil {
		ret := fmt.Sprintf("action[queryDataNodeDecoProgress] find datanode[%s] nodeset[%v] failed[%v]",
			node.Addr, node.NodeSetID, err.Error())
		log.LogWarnf("%v", ret)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	// can alloc dp
	node.ToBeOffline = false
	if err = m.cluster.syncUpdateDataNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	for _, disk := range node.DecommissionDiskList {
		key := fmt.Sprintf("%s_%s", node.Addr, disk)
		if value, ok := m.cluster.DecommissionDisks.Load(key); ok {
			dd := value.(*DecommissionDisk)
			dd.cancelDecommission(m.cluster, ns)
			// remove from decommissioned disk, do not remove bad disk until it is recovered
			// dp may allocated on bad disk otherwise
			if !node.isBadDisk(dd.DiskPath) {
				m.cluster.deleteAndSyncDecommissionedDisk(node, dd.DiskPath)
			}
		}
	}
	rstMsg = fmt.Sprintf("cancel decommission data node [%v] success", offLineAddr)
	auditlog.LogMasterOp("CancelDiskDecommission", rstMsg, nil)
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

	if val, ok := params[nodeDpRepairTimeOutKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setDataPartitionRepairTimeOut(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[nodeDpBackupKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setDataPartitionBackupTimeOut(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[nodeDpMaxRepairErrCntKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setDataPartitionMaxRepairErrCnt(v); err != nil {
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

	if val, ok := params[maxMpCntLimitKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setMaxMpCntLimit(v); err != nil {
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

	if val, ok := params[markDiskBrokenThresholdKey]; ok {
		if markDiskBrokenThreshold, ok := val.(float64); ok {
			if err = m.cluster.setMarkDiskBrokenThreshold(markDiskBrokenThreshold); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[autoDecommissionDiskKey]; ok {
		if autoDecomm, ok := val.(bool); ok {
			old := m.cluster.EnableAutoDecommissionDisk.Load()
			m.cluster.EnableAutoDecommissionDisk.Store(autoDecomm)
			if err = m.cluster.syncPutCluster(); err != nil {
				m.cluster.EnableAutoDecommissionDisk.Store(old)
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[autoDecommissionDiskIntervalKey]; ok {
		if interval, ok := val.(time.Duration); ok {
			if err = m.cluster.setAutoDecommissionDiskInterval(interval); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[autoDpMetaRepairKey]; ok {
		if autoRepair, ok := val.(bool); ok {
			if err = m.cluster.setEnableAutoDpMetaRepair(autoRepair); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[autoDpMetaRepairParallelCntKey]; ok {
		if cnt, ok := val.(int); ok {
			if err = m.cluster.setAutoDpMetaRepairParallelCnt(cnt); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[dpTimeoutKey]; ok {
		if dpTimeout, ok := val.(int64); ok {
			if err = m.cluster.setDataPartitionTimeout(dpTimeout); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[decommissionDiskLimit]; ok {
		if diskLimit, ok := val.(uint64); ok {
			if err = m.cluster.setDecommissionDiskLimit(uint32(diskLimit)); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[decommissionLimit]; ok {
		if dpLimit, ok := val.(uint64); ok {
			if err = m.cluster.setDecommissionDpLimit(dpLimit); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[dataMediaTypeKey]; ok {
		if mediaType, ok := val.(uint64); ok {
			if err = m.cluster.setClusterMediaType(uint32(mediaType)); err != nil {
				log.LogErrorf("setClusterMediaType: set mediaType failed, err %s", err.Error())
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[forbidWriteOpOfProtoVersion0]; ok {
		if forbidWriteOpOfProtoVer0, ok := val.(bool); ok {
			if err = m.cluster.setForbidWriteOpOfProtoVersion0(forbidWriteOpOfProtoVer0); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	dataNodesetSelector := extractDataNodesetSelector(r)
	metaNodesetSelector := extractMetaNodesetSelector(r)
	dataNodeSelector := extractDataNodeSelector(r)
	metaNodeSelector := extractMetaNodeSelector(r)
	if err = m.updateClusterSelector(dataNodesetSelector, metaNodesetSelector, dataNodeSelector, metaNodeSelector); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
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
		nsId           uint64
		dstNs          *nodeSet
		srcNs          *nodeSet
		ok             bool
		value          interface{}
		metaNode       *MetaNode
		dataNode       *DataNode
		nodeTypeUint32 uint32
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
	if nodeType == uint64(TypeDataPartition) {
		value, ok = zone.dataNodes.Load(addr)
		if !ok {
			return fmt.Errorf("addr %v not found", addr)
		}
		nsId = value.(*DataNode).NodeSetID
	} else if nodeType == uint64(TypeMetaPartition) {
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
	if nodeType <= math.MaxUint32 {
		nodeTypeUint32 = uint32(nodeType)
	} else {
		nodeTypeUint32 = math.MaxUint32
	}
	if nodeTypeUint32 == TypeDataPartition {
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

func (m *Server) updateZoneNodeSelector(zoneName string, dataNodeSelector string, metaNodeSelector string) (err error) {
	var ok bool
	var value interface{}

	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		err = fmt.Errorf("zonename [%v] not found", zoneName)
		return
	}

	zone := value.(*Zone)
	zone.nsLock.RLock()
	defer zone.nsLock.RUnlock()
	for _, ns := range zone.nodeSetMap {
		needSync := false
		if dataNodeSelector != "" && dataNodeSelector != ns.GetDataNodeSelector() {
			ns.SetDataNodeSelector(dataNodeSelector)
			needSync = true
		}
		if metaNodeSelector != "" && metaNodeSelector != ns.GetMetaNodeSelector() {
			ns.SetMetaNodeSelector(metaNodeSelector)
			needSync = true
		}
		if needSync {
			err = m.cluster.syncUpdateNodeSet(ns)
			if err != nil {
				return
			}
		}
	}
	return
}

func (m *Server) updateZoneNodesetNodeSelector(zoneName string, nodesetId uint64, dataNodesetSelector string, metaNodesetSelector string) (err error) {
	var ns *nodeSet
	var ok bool
	var value interface{}

	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		err = fmt.Errorf("zonename [%v] not found", zoneName)
		return
	}

	zone := value.(*Zone)
	if ns, ok = zone.nodeSetMap[nodesetId]; !ok {
		err = fmt.Errorf("nodesetId [%v] not found", nodesetId)
		return
	}
	needSync := false
	if dataNodesetSelector != "" && dataNodesetSelector != ns.GetDataNodeSelector() {
		ns.SetDataNodeSelector(dataNodesetSelector)
		needSync = true
	}
	if metaNodesetSelector != "" && metaNodesetSelector != ns.GetMetaNodeSelector() {
		ns.SetMetaNodeSelector(metaNodesetSelector)
		needSync = true
	}
	if needSync {
		err = m.cluster.syncUpdateNodeSet(ns)
		if err != nil {
			return
		}
	}
	log.LogInfof("action[updateNodesetNodeSelector] zonename %v nodeset %v dataNodeSelector %v metaNodeSelector %v", zoneName, nodesetId, dataNodesetSelector, metaNodesetSelector)
	return
}

func (m *Server) updateClusterSelector(dataNodesetSelector string, metaNodesetSelector string, dataNodeSelector string, metaNodeSelector string) (err error) {
	m.cluster.t.zoneMap.Range(func(key, value interface{}) bool {
		zone := value.(*Zone)
		err = zone.updateNodesetSelector(m.cluster, dataNodesetSelector, metaNodesetSelector)
		if err != nil {
			return false
		}
		err = m.updateZoneNodeSelector(zone.name, dataNodeSelector, metaNodeSelector)
		return err == nil
	})
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
			if node.IsWriteAble() {
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
				IsWriteAble:        node.IsWriteAble(),
				UsageRatio:         node.UsageRatio,
				SelectedTimes:      node.SelectedTimes,
				DataPartitionCount: node.DataPartitionCount,
				NodeSetID:          node.NodeSetID,
				MaxDpCntLimit:      node.GetPartitionLimitCnt(),
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
				IsWriteAble:        node.IsWriteAble(),
				ZoneName:           node.ZoneName,
				MaxMemAvailWeight:  node.MaxMemAvailWeight,
				Total:              node.Total,
				Used:               node.Used,
				Ratio:              node.Ratio,
				SelectCount:        node.SelectCount,
				Threshold:          node.Threshold,
				ReportTime:         node.ReportTime,
				MetaPartitionCount: node.MetaPartitionCount,
				NodeSetID:          node.NodeSetID,
				MaxMpCntLimit:      node.GetPartitionLimitCnt(),
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

func parseSetNodeRdOnlyParam(r *http.Request) (addr string, nodeType uint32, rdOnly bool, err error) {
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

func parseNodeType(r *http.Request) (nodeType uint32, err error) {
	var val string
	var nodeTypeUint64 uint64
	if val = r.FormValue(nodeTypeKey); val == "" {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is empty", nodeTypeKey)
		return
	}

	if nodeTypeUint64, err = strconv.ParseUint(val, 10, 32); err != nil {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is not number, err %s", nodeTypeKey, err.Error())
		return
	}
	nodeType = uint32(nodeTypeUint64)
	if nodeType != TypeDataPartition && nodeType != TypeMetaPartition {
		err = fmt.Errorf("parseSetNodeRdOnlyParam %s is not legal, must be %d or %d", nodeTypeKey, TypeDataPartition, TypeMetaPartition)
		return
	}

	return
}

func (m *Server) setNodeRdOnlyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		addr     string
		nodeType uint32
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

	err = m.setNodeRdOnly(addr, nodeType, rdOnly)
	if err != nil {
		log.LogErrorf("[setNodeRdOnlyHandler] set node %s to rdOnly %v, err (%s)", addr, rdOnly, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("[setNodeRdOnlyHandler] set node %s to rdOnly(%v) success", addr, rdOnly)))
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
		return
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
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "ratioVal is not legal"})
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

	sendOkReply(w, r, newSuccessHTTPReply("update node setid successfully"))
}

func (m *Server) updateNodeSetNodeSelector(w http.ResponseWriter, r *http.Request) {
	var (
		id       uint64
		zoneName string
		err      error
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminUpdateNodeSetNodeSelector))
	defer func() {
		doStatAndMetric(proto.AdminUpdateNodeSetNodeSelector, metric, err, nil)

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
	if id, err = extractNodesetID(r); err != nil {
		return
	}
	dataNodeSelector := extractDataNodeSelector(r)
	metaNodeSelector := r.FormValue(metaNodeSelectorKey)

	if err = m.updateZoneNodesetNodeSelector(zoneName, id, dataNodeSelector, metaNodeSelector); err != nil {
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply("update nodeset selector successfully"))
}

// get metanode some interval params
func (m *Server) getNodeSetGrpInfoHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetNodeSetGrpInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetNodeSetGrpInfo, metric, err, nil)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var value string
	var id uint64
	if value = r.FormValue(idKey); value != "" {
		id, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}
	var domainId uint64
	if value = r.FormValue(domainIdKey); value != "" {
		domainId, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}

	log.LogInfof("action[getNodeSetGrpInfoHandler] id [%v]", id)
	var info *proto.SimpleNodeSetGrpInfo
	if info, err = m.buildNodeSetGrpInfoByID(domainId, id); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
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
	resp[nodeDpMaxRepairErrCntKey] = fmt.Sprintf("%v", m.cluster.cfg.DpMaxRepairErrCnt)
	resp[clusterLoadFactorKey] = fmt.Sprintf("%v", m.cluster.cfg.ClusterLoadFactor)
	resp[maxDpCntLimitKey] = fmt.Sprintf("%v", m.cluster.getMaxDpCntLimit())
	resp[maxMpCntLimitKey] = fmt.Sprintf("%v", m.cluster.getMaxMpCntLimit())

	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) diagnoseMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err                             error
		rstMsg                          *proto.MetaPartitionDiagnosis
		inactiveNodes                   []string
		noLeaderMps                     []*MetaPartition
		lackReplicaMps                  []*MetaPartition
		badReplicaMps                   []*MetaPartition
		excessReplicaMPs                []*MetaPartition
		inodeCountNotEqualReplicaMps    []*MetaPartition
		maxInodeNotEqualMPs             []*MetaPartition
		dentryCountNotEqualReplicaMps   []*MetaPartition
		corruptMpIDs                    []uint64
		lackReplicaMpIDs                []uint64
		badReplicaMpIDs                 []uint64
		excessReplicaMpIDs              []uint64
		inodeCountNotEqualReplicaMpIDs  []uint64
		maxInodeNotEqualReplicaMpIDs    []uint64
		dentryCountNotEqualReplicaMpIDs []uint64
		badMetaPartitions               []badPartitionView
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDiagnoseMetaPartition))
	defer func() {
		doStatAndMetric(proto.AdminDiagnoseMetaPartition, metric, err, nil)
	}()

	corruptMpIDs = make([]uint64, 0)
	lackReplicaMpIDs = make([]uint64, 0)
	badReplicaMpIDs = make([]uint64, 0)
	excessReplicaMpIDs = make([]uint64, 0)

	if inactiveNodes, err = m.cluster.checkInactiveMetaNodes(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if lackReplicaMps, noLeaderMps, badReplicaMps, excessReplicaMPs,
		inodeCountNotEqualReplicaMps, maxInodeNotEqualMPs, dentryCountNotEqualReplicaMps, err = m.cluster.checkReplicaMetaPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	for _, mp := range noLeaderMps {
		corruptMpIDs = append(corruptMpIDs, mp.PartitionID)
	}
	for _, mp := range lackReplicaMps {
		lackReplicaMpIDs = append(lackReplicaMpIDs, mp.PartitionID)
	}
	for _, mp := range badReplicaMps {
		badReplicaMpIDs = append(badReplicaMpIDs, mp.PartitionID)
	}
	for _, mp := range excessReplicaMPs {
		excessReplicaMpIDs = append(excessReplicaMpIDs, mp.PartitionID)
	}

	for _, mp := range inodeCountNotEqualReplicaMps {
		inodeCountNotEqualReplicaMpIDs = append(inodeCountNotEqualReplicaMpIDs, mp.PartitionID)
	}
	for _, mp := range maxInodeNotEqualMPs {
		maxInodeNotEqualReplicaMpIDs = append(maxInodeNotEqualReplicaMpIDs, mp.PartitionID)
	}

	for _, mp := range dentryCountNotEqualReplicaMps {
		dentryCountNotEqualReplicaMpIDs = append(dentryCountNotEqualReplicaMpIDs, mp.PartitionID)
	}
	badMetaPartitions = m.cluster.getBadMetaPartitionsView()
	rstMsg = &proto.MetaPartitionDiagnosis{
		InactiveMetaNodes:                          inactiveNodes,
		CorruptMetaPartitionIDs:                    corruptMpIDs,
		LackReplicaMetaPartitionIDs:                lackReplicaMpIDs,
		BadMetaPartitionIDs:                        badMetaPartitions,
		BadReplicaMetaPartitionIDs:                 badReplicaMpIDs,
		ExcessReplicaMetaPartitionIDs:              excessReplicaMpIDs,
		InodeCountNotEqualReplicaMetaPartitionIDs:  inodeCountNotEqualReplicaMpIDs,
		MaxInodeNotEqualReplicaMetaPartitionIDs:    maxInodeNotEqualReplicaMpIDs,
		DentryCountNotEqualReplicaMetaPartitionIDs: dentryCountNotEqualReplicaMpIDs,
	}
	log.LogInfof("diagnose metaPartition cluster[%v], inactiveNodes:[%v], corruptMpIDs:[%v], "+
		"lackReplicaMpIDs:[%v], badReplicaMpIDs:[%v], excessReplicaDpIDs[%v] "+
		"inodeCountNotEqualReplicaMpIDs[%v] dentryCountNotEqualReplicaMpIDs[%v]",
		m.cluster.Name, inactiveNodes, corruptMpIDs, lackReplicaMpIDs, badReplicaMpIDs, excessReplicaMpIDs,
		inodeCountNotEqualReplicaMpIDs, dentryCountNotEqualReplicaMpIDs)
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
		decommissionType      int
		dataNode              *DataNode
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DecommissionDisk))
	defer func() {
		doStatAndMetric(proto.DecommissionDisk, metric, err, nil)
	}()
	// default diskDisable is true
	if offLineAddr, diskPath, diskDisable, limit, decommissionType, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	raftForce, err = parseRaftForce(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dataNode, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	found := false
	for _, disk := range dataNode.AllDisks {
		if disk == diskPath {
			found = true
		}
	}
	if !found {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDiskNotExists))
		return
	}
	if decommissionType == int(InitialDecommission) {
		decommissionType = int(ManualDecommission)
	}
	if err = m.cluster.migrateDisk(dataNode, diskPath, "", raftForce, limit, diskDisable, uint32(decommissionType)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission disk [%v:%v] submited!need check status later!", offLineAddr, diskPath)
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

	if onLineAddr, diskPath, err = parseNodeAddrAndDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(onLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(errors.NewErrorf("disk %v on dataNode %v is bad disk", diskPath, onLineAddr)))
		return
	}

	if node.isBadDisk(diskPath) {
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

func (m *Server) restoreStoppedAutoDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                string
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_restoreStoppedAutoDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()
	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.restoreStoppedAutoDecommissionDisk(offLineAddr, diskPath); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("restoreStoppedAutoDecommissionDisk node[%v] disk[%v] submited!need check status later!",
		offLineAddr, diskPath)
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

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
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
	status, progress := disk.updateDecommissionStatus(m.cluster, true, false)
	progress, _ = FormatFloatFloor(progress, 4)
	resp := &proto.DecommissionProgress{
		Progress:      fmt.Sprintf("%.2f%%", progress*float64(100)),
		StatusMessage: GetDecommissionStatusMessage(status),
		IgnoreDps:     disk.IgnoreDecommissionDps,
		ResidualDps:   disk.residualDecommissionDpsGetAll(),
		StartTime:     time.Unix(int64(disk.DecommissionTerm), 0).String(),
	}
	dps := disk.GetDecommissionFailedDPByTerm(m.cluster)
	resp.FailedDps = dps
	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) queryDecommissionDiskDecoFailedDps(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
		failedDps             []uint64
	)

	metric := exporter.NewTPCnt("req_queryDecommissionDiskDecoFailedDps")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	badPartitions := m.cluster.getAllDecommissionDataPartitionByDisk(offLineAddr, diskPath)
	for _, dp := range badPartitions {
		if dp.IsDecommissionFailed() {
			failedDps = append(failedDps, dp.PartitionID)
		}
	}
	log.LogWarnf("action[GetDecommissionDiskFailedDP]disk %v_%v failed dp list [%v]", offLineAddr, diskPath, failedDps)
	sendOkReply(w, r, newSuccessHTTPReply(failedDps))
}

func (m *Server) queryAllDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		err              error
		decommissoinType int
		showAll          bool
	)

	metric := exporter.NewTPCnt("req_queryAllDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()
	if decommissoinType, showAll, err = parseReqToQueryDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	resp := &proto.DecommissionDisksResponse{}
	m.cluster.DecommissionDisks.Range(func(key, value interface{}) bool {
		disk := value.(*DecommissionDisk)
		if decommissoinType == int(QueryDecommission) || (decommissoinType != int(QueryDecommission) && disk.Type == uint32(decommissoinType)) {
			status, progress := disk.updateDecommissionStatus(m.cluster, true, false)
			if !showAll && (status != DecommissionFail && status != DecommissionRunning && status != markDecommission) {
				return true
			}
			progress, _ = FormatFloatFloor(progress, 4)
			decommissionProgress := proto.DecommissionProgress{
				Progress:      fmt.Sprintf("%.2f%%", progress*float64(100)),
				StatusMessage: GetDecommissionStatusMessage(status),
				IgnoreDps:     disk.IgnoreDecommissionDps,
				ResidualDps:   disk.residualDecommissionDpsGetAll(),
				FailedDps:     disk.GetDecommissionFailedDPByTerm(m.cluster),
				StartTime:     time.Unix(int64(disk.DecommissionTerm), 0).String(),
			}
			dps := disk.GetDecommissionFailedDPByTerm(m.cluster)
			decommissionProgress.FailedDps = dps
			resp.Infos = append(resp.Infos, proto.DecommissionDiskInfo{
				SrcAddr:      disk.SrcAddr,
				DiskPath:     disk.DiskPath,
				ProgressInfo: decommissionProgress,
			})
		}
		return true
	})
	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) deleteDecommissionDiskRecord(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_deleteDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	value, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		ret := fmt.Sprintf("action[deleteDecommissionDiskRecord]cannot found decommission task for  node[%v] disk[%v], "+
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

func (m *Server) pauseDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_pauseDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
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
	err, dps := m.cluster.decommissionDiskPause(disk)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg := fmt.Sprintf("cancel decommission data node [%s] disk[%s] successfully with failed dp %v",
		offLineAddr, diskPath, dps)
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
		log.LogErrorf("master handle dataNode task response(%v), err(%v)", tr, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleDataNodeTaskResponse(tr.OperatorAddr, tr)
}

func (m *Server) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr      string
		heartbeatPort string
		replicaPort   string
		zoneName      string
		id            uint64
		err           error
		nodesetId     uint64
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AddMetaNode))
	defer func() {
		doStatAndMetric(proto.AddMetaNode, metric, err, nil)
	}()

	if nodeAddr, heartbeatPort, replicaPort, zoneName, _, err = parseRequestForAddNode(r); err != nil {
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
			return
		}
	}
	if id, err = m.cluster.addMetaNode(nodeAddr, heartbeatPort, replicaPort, zoneName, nodesetId); err != nil {
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
		RaftHeartbeatPort:         metaNode.HeartbeatPort,
		RaftReplicaPort:           metaNode.ReplicaPort,
		DomainAddr:                metaNode.DomainAddr,
		IsActive:                  metaNode.IsActive,
		IsWriteAble:               metaNode.IsWriteAble(),
		ZoneName:                  metaNode.ZoneName,
		MaxMemAvailWeight:         metaNode.MaxMemAvailWeight,
		Total:                     metaNode.Total,
		Used:                      metaNode.Used,
		Ratio:                     metaNode.Ratio,
		SelectCount:               metaNode.SelectCount,
		Threshold:                 metaNode.Threshold,
		ReportTime:                metaNode.ReportTime,
		MetaPartitionCount:        metaNode.MetaPartitionCount,
		NodeSetID:                 metaNode.NodeSetID,
		PersistenceMetaPartitions: metaNode.PersistenceMetaPartitions,
		CanAllowPartition:         metaNode.IsWriteAble() && metaNode.PartitionCntLimited(),
		MaxMpCntLimit:             metaNode.GetPartitionLimitCnt(),
		CpuUtil:                   metaNode.CpuUtil.Load(),
	}
	sendOkReply(w, r, newSuccessHTTPReply(metaNodeInfo))
}

func (m *Server) setMpCntLimit(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr   string
		mpCntLimit uint64
		metaNode   *MetaNode
		err        error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.SetMpCntLimit))
	defer func() {
		doStatAndMetric(proto.SetMpCntLimit, metric, err, nil)
	}()

	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if metaNode, err = m.cluster.metaNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}

	if mpCntLimit, err = strconv.ParseUint(r.FormValue(maxMpCntLimitKey), 10, 64); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	oldMpCntLimit := metaNode.MpCntLimit
	metaNode.MpCntLimit = mpCntLimit
	if err = m.cluster.syncUpdateMetaNode(metaNode); err != nil {
		metaNode.MpCntLimit = oldMpCntLimit
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set MpCntLimit to %v successfully", metaNode.MpCntLimit)))
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

	numVal, err := strconv.ParseInt(val, 10, 32)
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

	if limit > util.DefaultMigrateMpCnt {
		err = fmt.Errorf("limit %d can't be bigger than %d", limit, util.DefaultMigrateMpCnt)
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

	if !targetNode.IsWriteAble() || !targetNode.PartitionCntLimited() {
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

func parseReqToDecoDisk(r *http.Request) (nodeAddr, diskPath string, diskDisable bool, limit, decommissionType int, err error) {
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
	decommissionType, err = parseUintParam(r, DecommissionType)
	if err != nil {
		return
	}
	limit, err = parseUintParam(r, countKey)
	if err != nil {
		return
	}
	return
}

func parseNodeAddrAndDisk(r *http.Request) (nodeAddr, diskPath string, err error) {
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

func parseReqToQueryDecoDisk(r *http.Request) (decommissionType int, showAll bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	decommissionType, err = parseUintParam(r, DecommissionType)
	if err != nil {
		return
	}
	showAll, err = pareseBoolWithDefault(r, ShowAll, false)
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
}

func (m *Server) putDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		name string
		err  error
	)
	defer func() {
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}()

	if name, err = parseAndExtractName(r); err != nil {
		return
	}
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = io.ReadAll(r.Body); err != nil {
		return
	}
	if !m.cluster.partition.IsRaftLeader() {
		view := &proto.DataPartitionsView{}
		if err = proto.UnmarshalHTTPReply(body, view); err != nil {
			log.LogErrorf("putDataPartitions. umarshal reply.Data error volName %v", name)
			return
		}

		m.cluster.followerReadManager.updateVolViewFromLeader(name, view)
		sendOkReply(w, r, newSuccessHTTPReply("success"))
		return
	} else {
		err = fmt.Errorf("raft leader cann't be grant dps info")
		log.LogErrorf("putDataPartitions. err %v", err)
	}
}

// Obtain all the data partitions in a volume.
func (m *Server) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body     []byte
		name     string
		compress bool
		vol      *Vol
		err      error
	)
	compress = r.Header.Get(proto.HeaderAcceptEncoding) == compressor.EncodingGzip
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientDataPartitions))
	defer func() {
		doStatAndMetric(proto.ClientDataPartitions, metric, err, map[string]string{exporter.Vol: name})
	}()
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogInfof("action[getDataPartitions] current is leader[%v], compress[%v]",
		m.cluster.partition.IsRaftLeader(), compress)
	if !m.cluster.partition.IsRaftLeader() {
		var ok bool
		if body, ok = m.cluster.followerReadManager.getVolViewAsFollower(name, compress); !ok {
			log.LogErrorf("action[getDataPartitions] volume [%v] not get partitions info", name)
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("follower volume info not found")))
			return
		}
		if compress {
			w.Header().Add(proto.HeaderContentEncoding, compressor.EncodingGzip)
		}
		send(w, r, body)
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if compress {
		body, err = vol.getDataPartitionViewCompress()
	} else {
		body, err = vol.getDataPartitionsView()
	}
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if compress {
		w.Header().Add(proto.HeaderContentEncoding, compressor.EncodingGzip)
	}
	send(w, r, body)
}

// Obtain all the data partitions in a volume.
func (m *Server) getDiskDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		diskPath string
		dataNode *DataNode
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.ClientDiskDataPartitions))
	defer func() {
		doStatAndMetric(proto.ClientDiskDataPartitions, metric, err, map[string]string{exporter.Disk: diskPath})
	}()

	if nodeAddr, diskPath, err = parseNodeAddrAndDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	diskDataPartitionsView := &proto.DiskDataPartitionsView{}
	for _, dp := range dataNode.DataPartitionReports {
		if dp.DiskPath != diskPath {
			continue
		}
		diskDataPartitionsView.DataPartitions = append(diskDataPartitionsView.DataPartitions, dp)
	}

	sendOkReply(w, r, newSuccessHTTPReply(diskDataPartitionsView))
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
func (m *Server) getAllClients(w http.ResponseWriter, r *http.Request) {
	name, _ := extractName(r)
	infos := m.cliMgr.GetClients(name)
	log.LogInfof("getAllClients: get total %d client info", len(infos))
	sendOkReply(w, r, newSuccessHTTPReply(infos))
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

	remoteIp := iputil.GetRealClientIP(r)
	clientVer := r.FormValue(proto.ClientVerKey)
	hostName := r.FormValue(proto.HostKey)
	role := r.FormValue(proto.RoleKey)
	enableBcache := r.FormValue(proto.BcacheOnlyForNotSSDKey)
	enableRCache := r.FormValue(proto.EnableRemoteCache)
	log.LogInfof("getVolStatInfo: get client ver info %s, ip %s, host %s, vol %s, role %s, enableBcache %s, enableRCache %s",
		clientVer, remoteIp, hostName, name, role, enableBcache, enableRCache)

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	m.cliMgr.PutItem(remoteIp, hostName, name, clientVer, role, enableBcache, enableRCache)

	if proto.IsCold(vol.VolType) && ver != proto.LFClient {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "ec-vol is supported by LF client only"})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(volStat(vol, byMeta)))
}

func volStat(vol *Vol, countByMeta bool) (stat *proto.VolStatInfo) {
	if proto.IsVolSupportStorageClass(vol.allowedStorageClass, proto.StorageClass_BlobStore) {
		countByMeta = true
	}

	stat = new(proto.VolStatInfo)
	stat.Name = vol.Name
	stat.TotalSize = vol.Capacity * util.GB
	stat.UsedSize = vol.totalUsedSpaceByMeta(countByMeta)
	if stat.UsedSize > stat.TotalSize {
		log.LogWarnf("vol(%v) useSize(%v) is larger than capacity(%v)", vol.Name, stat.UsedSize, stat.TotalSize)
	}

	if log.EnableInfo() {
		log.LogInfof("[volStat] vol(%v) useSize(%v)/Capacity(%v)", vol.Name, strutil.FormatSize(stat.UsedSize), strutil.FormatSize(stat.TotalSize))
	}

	stat.UsedRatio = strconv.FormatFloat(float64(stat.UsedSize)/float64(stat.TotalSize), 'f', 2, 32)
	stat.DpReadOnlyWhenVolFull = vol.DpReadOnlyWhenVolFull

	vol.mpsLock.RLock()
	for _, mp := range vol.MetaPartitions {
		stat.InodeCount += mp.InodeCount
		stat.TxCnt += mp.TxCnt
		stat.TxRbInoCnt += mp.TxRbInoCnt
		stat.TxRbDenCnt += mp.TxRbDenCnt
	}
	vol.mpsLock.RUnlock()

	stat.TrashInterval = vol.TrashInterval
	stat.DefaultStorageClass = vol.volStorageClass
	stat.CacheDpStorageClass = vol.cacheDpStorageClass
	stat.StatByStorageClass = vol.StatByStorageClass
	stat.StatMigrateStorageClass = vol.StatMigrateStorageClass
	stat.StatByDpMediaType = vol.StatByDpMediaType
	stat.MetaFollowerRead = vol.MetaFollowerRead
	stat.MaximallyRead = vol.MaximallyRead
	stat.LeaderRetryTimeOut = int(vol.LeaderRetryTimeout)

	log.LogDebugf("[volStat] vol[%v] total[%v],usedSize[%v] TrashInterval[%v] DefaultStorageClass[%v]",
		vol.Name, stat.TotalSize, stat.UsedSize, stat.TrashInterval, stat.DefaultStorageClass)
	if proto.IsHot(vol.VolType) {
		return
	}

	stat.CacheTotalSize = vol.CacheCapacity * util.GB
	stat.CacheUsedSize = vol.cfsUsedSpace()
	stat.CacheUsedRatio = strconv.FormatFloat(float64(stat.CacheUsedSize)/float64(stat.CacheTotalSize), 'f', 2, 32)
	log.LogDebugf("[volStat] vol[%v] ebsTotal[%v], ebsUsedSize[%v] DefaultStorageClass[%v]",
		vol.Name, stat.CacheTotalSize, stat.CacheUsedSize, stat.DefaultStorageClass)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *proto.MetaPartitionView) {
	mpView = proto.NewMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
	mp.RLock()
	defer mp.RUnlock()
	mpView.Members = append(mpView.Members, mp.Hosts...)
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return
	}
	mpView.LeaderAddr = mr.Addr
	mpView.MaxInodeID = mp.MaxInodeID
	mpView.InodeCount = mp.InodeCount
	mpView.DentryCount = mp.DentryCount
	mpView.FreeListLen = mp.FreeListLen
	mpView.TxCnt = mp.TxCnt
	mpView.TxRbInoCnt = mp.TxRbInoCnt
	mpView.TxRbDenCnt = mp.TxRbDenCnt
	mpView.IsRecover = mp.IsRecover
	mpView.IsFreeze = mp.IsFreeze
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

	toInfo := func(mp *MetaPartition) *proto.MetaPartitionInfo {
		mp.RLock()
		defer mp.RUnlock()
		replicas := make([]*proto.MetaReplicaInfo, len(mp.Replicas))
		zones := make([]string, len(mp.Hosts))
		nodeSets := make([]uint64, len(mp.Hosts))
		for idx, host := range mp.Hosts {
			metaNode, err := m.cluster.metaNode(host)
			if err == nil {
				zones[idx] = metaNode.ZoneName
				nodeSets[idx] = metaNode.NodeSetID
			}
		}
		for i := 0; i < len(replicas); i++ {
			replicas[i] = &proto.MetaReplicaInfo{
				Addr:        mp.Replicas[i].Addr,
				DomainAddr:  mp.Replicas[i].metaNode.DomainAddr,
				MaxInodeID:  mp.Replicas[i].MaxInodeID,
				ReportTime:  mp.Replicas[i].ReportTime,
				Status:      mp.Replicas[i].Status,
				IsLeader:    mp.Replicas[i].IsLeader,
				InodeCount:  mp.Replicas[i].InodeCount,
				DentryCount: mp.Replicas[i].DentryCount,
				MaxInode:    mp.Replicas[i].MaxInodeID,
			}
		}

		forbidden := true
		vol, err := m.cluster.getVol(mp.volName)
		if err == nil {
			forbidden = vol.Forbidden
		} else {
			log.LogErrorf("action[getMetaPartition]failed to get volume %v, err %v", mp.volName, err)
		}
		mpInfo := &proto.MetaPartitionInfo{
			PartitionID:               mp.PartitionID,
			Start:                     mp.Start,
			End:                       mp.End,
			VolName:                   mp.volName,
			MaxInodeID:                mp.MaxInodeID,
			InodeCount:                mp.InodeCount,
			DentryCount:               mp.DentryCount,
			Replicas:                  replicas,
			ReplicaNum:                mp.ReplicaNum,
			Status:                    mp.Status,
			IsRecover:                 mp.IsRecover,
			Hosts:                     mp.Hosts,
			Peers:                     mp.Peers,
			Zones:                     zones,
			NodeSets:                  nodeSets,
			MissNodes:                 mp.MissNodes,
			OfflinePeerID:             mp.OfflinePeerID,
			LoadResponse:              mp.LoadResponse,
			Forbidden:                 forbidden,
			StatByStorageClass:        mp.StatByStorageClass,
			StatByMigrateStorageClass: mp.StatByMigrateStorageClass,
			ForbidWriteOpOfProtoVer0:  mp.ForbidWriteOpOfProtoVer0,
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
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminChangeMasterLeader))
	defer func() {
		doStatAndMetric(proto.AdminChangeMasterLeader, metric, err, nil)
	}()

	log.LogDebugf("require changeMasterLeader to current node")
	if !m.cluster.IsLeader() {
		if err = m.cluster.tryToChangeLeaderByHost(); err != nil {
			log.LogErrorf("changeMasterLeader.err %v", err)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}
	rstMsg := " changeMasterLeader. command success send to dest host but need check. "
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
		return
	}
	m.cluster.followerReadManager.needCheck = enableFollower

	rstMsg := fmt.Sprintf(" OpFollowerPartitionsRead. set needCheck %v command success. ", enableFollower)
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) CreateVersion(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		vol   *Vol
		name  string
		ver   *proto.VolVersionInfo
		value string
		force bool
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminCreateVersion))
	defer func() {
		doStatAndMetric(proto.AdminCreateVersion, metric, err, map[string]string{exporter.Vol: name})
	}()

	if !m.cluster.cfg.EnableSnapshot {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrSnapshotNotEnabled))
		return
	}

	log.LogInfof("action[CreateVersion]")
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrParamError))
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrParamError))
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if len(vol.allowedStorageClass) > 1 {
		err = fmt.Errorf("volume has multiple allowedStorageClass, not support snapshot featrure at the same time")
		log.LogErrorf("[CreateVersion] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if value = r.FormValue(forceKey); value != "" {
		force, _ = strconv.ParseBool(value)
	}

	if ver, err = vol.VersionMgr.createVer2PhaseTask(m.cluster, uint64(time.Now().UnixMicro()), proto.CreateVersion, force); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVersionOpError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(ver))
}

func (m *Server) DelVersion(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		vol    *Vol
		name   string
		verSeq uint64
		value  string
		force  bool
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminDelVersion))
	defer func() {
		doStatAndMetric(proto.AdminDelVersion, metric, err, map[string]string{exporter.Vol: name})
	}()

	if !m.cluster.cfg.EnableSnapshot {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrSnapshotNotEnabled))
		return
	}

	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("volName %v not exist", name)))
		return
	}
	if value = r.FormValue(verSeqKey); value == "" {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("verSeq not exist")))
		return
	}

	verSeq, _ = extractUint64(r, verSeqKey)
	log.LogDebugf("action[DelVersion] vol %v verSeq %v", name, verSeq)
	if value = r.FormValue(forceKey); value != "" {
		force, _ = strconv.ParseBool(value)
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if _, err = vol.VersionMgr.createVer2PhaseTask(m.cluster, verSeq, proto.DeleteVersion, force); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVersionOpError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply("success!"))
}

func (m *Server) GetVersionInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		vol     *Vol
		name    string
		verSeq  uint64
		verInfo *proto.VolVersionInfo
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetVersionInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetVersionInfo, metric, err, map[string]string{exporter.Vol: name})
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}

	if verSeq, err = extractUint64(r, verSeqKey); err != nil {
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if verInfo, err = vol.VersionMgr.getVersionInfo(verSeq); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVersionOpError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(verInfo))
}

func (m *Server) GetAllVersionInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		vol     *Vol
		name    string
		verList *proto.VolVersionInfoList
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetAllVersionInfo))
	defer func() {
		doStatAndMetric(proto.AdminGetAllVersionInfo, metric, err, map[string]string{exporter.Vol: name})
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	// if !proto.IsHot(vol.VolType) {
	//	sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVersionOpError, Msg: "vol need be hot one"})
	//	return
	// }

	verList = vol.VersionMgr.getVersionList()

	sendOkReply(w, r, newSuccessHTTPReply(verList))
}

func (m *Server) SetVerStrategy(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		name     string
		strategy proto.VolumeVerStrategy
		isForce  bool
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetVerStrategy))
	defer func() {
		doStatAndMetric(proto.AdminSetVerStrategy, metric, err, map[string]string{exporter.Vol: name})
	}()

	if !m.cluster.cfg.EnableSnapshot {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrSnapshotNotEnabled))
		return
	}

	if name, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if strategy, isForce, err = parseVolVerStrategy(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.SetVerStrategy(name, strategy, isForce); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func (m *Server) getVolVer(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		name string
		info *proto.VolumeVerInfo
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetVolVer))
	defer func() {
		doStatAndMetric(proto.AdminGetVolVer, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if info, err = m.cluster.getVolVer(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(info))
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

		param := proto.UserCreateParam{
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
	if err = m.cluster.setDecommissionDpLimit(limit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("set master not worked %v", err)))
		return
	}
	rstMsg := fmt.Sprintf("set decommission limit to %v successfully", limit)
	log.LogDebugf("action[updateDecommissionLimit] %v", rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) updateDecommissionDiskLimit(w http.ResponseWriter, r *http.Request) {
	var (
		limit uint32
		err   error
	)

	metric := exporter.NewTPCnt("req_updateDecommissionDiskFactor")
	defer func() {
		metric.Set(err)
	}()

	if limit, err = parseRequestToUpdateDecommissionDiskLimit(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.setDecommissionDiskLimit(limit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg := fmt.Sprintf("set decommission factor to %v successfully", limit)
	log.LogDebugf("action[updateDecommissionDiskFactor] %v", rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) queryDecommissionToken(w http.ResponseWriter, r *http.Request) {
	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminQueryDecommissionToken))
	defer func() {
		doStatAndMetric(proto.AdminQueryDecommissionToken, metric, err, nil)
	}()

	var stats []proto.DecommissionTokenStatus
	zones := m.cluster.t.getAllZones()
	for _, zone := range zones {
		err, zoneStats := zone.queryDecommissionParallelStatus(m.cluster)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
			return
		}
		for _, s := range zoneStats {
			stats = append(stats, proto.DecommissionTokenStatus{
				NodesetID:                   s.ID,
				CurTokenNum:                 s.CurTokenNum,
				MaxTokenNum:                 s.MaxTokenNum,
				RunningDp:                   s.RunningDp,
				TotalDP:                     s.TotalDP,
				ManualDecommissionDisk:      s.ManualDecommissionDisk,
				ManualDecommissionDiskTotal: s.ManualDecommissionDiskTotal,
				AutoDecommissionDisk:        s.AutoDecommissionDisk,
				AutoDecommissionDiskTotal:   s.AutoDecommissionDiskTotal,
				MaxDiskTokenNum:             m.cluster.GetDecommissionDiskLimit(),
				RunningDisk:                 s.RunningDisk,
			})
		}
	}
	log.LogDebugf("action[queryDecommissionToken] %v", stats)
	sendOkReply(w, r, newSuccessHTTPReply(stats))
}

func (m *Server) queryDecommissionLimit(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminQueryDecommissionLimit))
	defer func() {
		doStatAndMetric(proto.AdminQueryDecommissionLimit, metric, nil, nil)
	}()

	limit := atomic.LoadUint64(&m.cluster.DecommissionLimit)
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

	metric := exporter.NewTPCnt(apiToMetricsName(proto.QueryDataNodeDecoProgress))
	defer func() {
		doStatAndMetric(proto.QueryDataNodeDecoProgress, metric, err, nil)
	}()

	if offLineAddr, err = parseReqToDecoDataNodeProgress(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dn, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	status, progress := dn.updateDecommissionStatus(m.cluster, true, false)
	progress, _ = FormatFloatFloor(progress, 4)
	resp := &proto.DataDecommissionProgress{
		Status:        status,
		Progress:      fmt.Sprintf("%.2f%%", progress*float64(100)),
		StatusMessage: GetDecommissionStatusMessage(status),
	}
	dps := dn.GetDecommissionFailedDPByTerm(m.cluster)
	resp.FailedDps = dps
	resp.IgnoreDps = dn.getIgnoreDecommissionDpList(m.cluster)
	resp.ResidualDps = dn.getResidualDecommissionDpList(m.cluster)

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

func (m *Server) enableAutoDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		enable bool
		err    error
	)

	metric := exporter.NewTPCnt("req_enableAutoDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()

	if enable, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	m.cluster.SetAutoDecommissionDisk(enable)
	if err = m.cluster.syncPutCluster(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("set master not worked %v", err)))
		return
	}
	rstMsg := fmt.Sprintf("set auto decommission disk to %v successfully", enable)
	log.LogDebugf("action[enableAutoDecommissionDisk] %v", rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) queryAutoDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt("req_queryAutoDecommissionDisk")
	defer func() {
		metric.Set(nil)
	}()
	enable := m.cluster.AutoDecommissionDiskIsEnabled()
	rstMsg := fmt.Sprintf("auto decommission disk is %v ", enable)
	log.LogDebugf("action[queryAutoDecommissionDisk] %v", rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(enable))
}

func (m *Server) queryDisableDisk(w http.ResponseWriter, r *http.Request) {
	var (
		node     *DataNode
		rstMsg   string
		nodeAddr string
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QueryDisableDisk))
	defer func() {
		doStatAndMetric(proto.QueryDisableDisk, metric, err, nil)
	}()

	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	disks := node.getDecommissionedDisks()

	disksInfo := &proto.DecommissionedDisks{
		Node:  nodeAddr,
		Disks: disks,
	}
	rstMsg = fmt.Sprintf("datanode[%v] disable disk[%v]",
		nodeAddr, disks)

	Warn(m.clusterName, rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(disksInfo))
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
		return
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

func (m *Server) setConfigHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetConfig))
	defer func() {
		doStatAndMetric(proto.AdminSetConfig, metric, err, nil)
	}()

	key, value, err := parseSetConfigParam(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	log.LogInfof("[setConfigHandler] set config key[%v], value[%v]", key, value)

	err = m.setConfig(key, value)
	if err != nil {
		log.LogErrorf("[setConfigHandler] set config key[%v], value[%v], err (%s)", key, value, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set config key[%v], value[%v] success", key, value)))
}

func (m *Server) getConfigHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetConfig))
	defer func() {
		doStatAndMetric(proto.AdminGetConfig, metric, err, nil)
	}()

	key, err := parseGetConfigParam(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	log.LogInfof("[getConfigHandler] get config key[%v]", key)
	value, err := m.getConfig(key)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(value))
}

func (m *Server) setConfig(key string, value string) (err error) {
	var metaPartitionInodeIdStep uint64
	if key == cfgmetaPartitionInodeIdStep {
		if metaPartitionInodeIdStep, err = strconv.ParseUint(value, 10, 64); err != nil {
			return err
		}
		oldValue := m.config.MetaPartitionInodeIdStep
		m.config.MetaPartitionInodeIdStep = metaPartitionInodeIdStep
		if err = m.cluster.syncPutCluster(); err != nil {
			m.config.MetaPartitionInodeIdStep = oldValue
			log.LogErrorf("setConfig syncPutCluster fail err %v", err)
			return err
		}
	} else {
		err = keyNotFound("config")
	}
	return err
}

func (m *Server) getConfig(key string) (value string, err error) {
	if key == cfgmetaPartitionInodeIdStep {
		v := m.config.MetaPartitionInodeIdStep
		value = strconv.FormatUint(v, 10)
	} else {
		err = keyNotFound("config")
	}
	return value, err
}

func (m *Server) CreateQuota(w http.ResponseWriter, r *http.Request) {
	req := &proto.SetMasterQuotaReuqest{}
	var (
		err     error
		vol     *Vol
		quotaId uint32
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.QuotaCreate))
	defer func() {
		doStatAndMetric(proto.QuotaCreate, metric, err, map[string]string{exporter.Vol: req.VolName})
	}()

	if err = parserSetQuotaParam(r, req); err != nil {
		log.LogErrorf("[CreateQuota] set quota fail err [%v]", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(req.VolName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if !vol.enableQuota {
		err = errors.NewErrorf("vol %v disableQuota.", vol.Name)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if quotaId, err = vol.quotaManager.createQuota(req); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(&quotaId))
}

func (m *Server) UpdateQuota(w http.ResponseWriter, r *http.Request) {
	req := &proto.UpdateMasterQuotaReuqest{}
	var (
		err error
		vol *Vol
	)
	if err = parserUpdateQuotaParam(r, req); err != nil {
		log.LogErrorf("[SetQuota] set quota fail err [%v]", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(req.VolName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if !vol.enableQuota {
		err = errors.NewErrorf("vol %v disableQuota.", vol.Name)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = vol.quotaManager.updateQuota(req); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg := fmt.Sprintf("update quota successfully, req %v", req)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) DeleteQuota(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		vol     *Vol
		quotaId uint32
		name    string
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.QuotaDelete))
	defer func() {
		doStatAndMetric(proto.QuotaDelete, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, quotaId, err = parseDeleteQuotaParam(r); err != nil {
		log.LogErrorf("[DeleteQuota] del quota fail err [%v]", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if err = vol.quotaManager.deleteQuota(quotaId); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg := fmt.Sprintf("delete quota successfully, vol [%v] quotaId [%v]", name, quotaId)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) ListQuota(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		vol  *Vol
		resp *proto.ListMasterQuotaResponse
		name string
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.QuotaList))
	defer func() {
		doStatAndMetric(proto.QuotaList, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if vol.quotaManager == nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrParamError))
		return
	}
	resp = vol.quotaManager.listQuota()

	log.LogInfof("list quota vol [%v] resp [%v] success.", name, *resp)

	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) ListQuotaAll(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QuotaListAll))
	defer func() {
		doStatAndMetric(proto.QuotaListAll, metric, nil, nil)
	}()

	volsInfo := m.cluster.listQuotaAll()
	log.LogInfof("list all vol has quota [%v]", volsInfo)
	sendOkReply(w, r, newSuccessHTTPReply(volsInfo))
}

func (m *Server) GetQuota(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		vol       *Vol
		name      string
		quotaId   uint32
		quotaInfo *proto.QuotaInfo
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.QuotaGet))
	defer func() {
		doStatAndMetric(proto.QuotaGet, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, quotaId, err = parseGetQuotaParam(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if quotaInfo, err = vol.quotaManager.getQuota(quotaId); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	log.LogInfof("get quota vol [%v] quotaInfo [%v] success.", name, *quotaInfo)
	sendOkReply(w, r, newSuccessHTTPReply(quotaInfo))
}

// func (m *Server) BatchModifyQuotaFullPath(w http.ResponseWriter, r *http.Request) {
// 	var (
// 		name              string
// 		body              []byte
// 		changeFullPathMap map[uint32]string
// 		err               error
// 		vol               *Vol
// 	)
// 	metric := exporter.NewTPCnt(apiToMetricsName(proto.QuotaGet))
// 	defer func() {
// 		doStatAndMetric(proto.QuotaBatchModifyPath, metric, err, map[string]string{exporter.Vol: name})
// 	}()

// 	if name, err = parseAndExtractName(r); err != nil {
// 		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
// 		return
// 	}

// 	if body, err = io.ReadAll(r.Body); err != nil {
// 		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
// 		return
// 	}
// 	changeFullPathMap = make(map[uint32]string)
// 	if err = json.Unmarshal(body, &changeFullPathMap); err != nil {
// 		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
// 		return
// 	}

// 	if vol, err = m.cluster.getVol(name); err != nil {
// 		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
// 		return
// 	}

// 	vol.quotaManager.batchModifyQuotaFullPath(changeFullPathMap)

// 	log.LogInfof("BatchModifyQuotaFullPath vol [%v] changeFullPathMap [%v] success.", name, changeFullPathMap)
// 	msg := fmt.Sprintf("BatchModifyQuotaFullPath successfully, vol [%v]", name)
// 	sendOkReply(w, r, newSuccessHTTPReply(msg))
// }

func parseSetDpDiscardParam(r *http.Request) (dpId uint64, discard bool, force bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if dpId, err = extractDataPartitionID(r); err != nil {
		err = fmt.Errorf("parseSetDpDiscardParam get dpid error %v", err)
		return
	}

	val := r.FormValue(dpDiscardKey)
	if val == "" {
		err = fmt.Errorf("parseSetDpDiscardParam %s is empty", dpDiscardKey)
		return
	}

	if discard, err = strconv.ParseBool(val); err != nil {
		err = fmt.Errorf("parseSetDpDiscardParam %s is not bool value %s", dpDiscardKey, val)
		return
	}

	val = r.FormValue(forceKey)
	if val != "" {
		if force, err = strconv.ParseBool(val); err != nil {
			err = fmt.Errorf("parseSetDpDiscardParam %s is not bool value %s", forceKey, val)
			return
		}
	}

	return
}

func (m *Server) setDpDiscard(partitionID uint64, isDiscard bool, force bool) (err error) {
	var dp *DataPartition
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		return fmt.Errorf("[setDpDiacard] getDataPartitionByID err(%s)", err.Error())
	}
	dp.Lock()
	defer dp.Unlock()
	if dp.IsDiscard && !isDiscard {
		log.LogWarnf("[setDpDiscard] usnet dp discard flag may cause some junk data")
	}
	if isDiscard && !force && dp.Status != proto.Unavailable {
		err = fmt.Errorf("data partition %v is not unavailable", dp.PartitionID)
		log.LogErrorf("[setDpDiscard] dp(%v) set discard, but still available status(%v)", dp.PartitionID, dp.Status)
		return
	}
	dp.IsDiscard = isDiscard
	m.cluster.syncUpdateDataPartition(dp)
	return
}

func (m *Server) setDpDiscardHandler(w http.ResponseWriter, r *http.Request) {
	var (
		dpId    uint64
		discard bool
		force   bool
		err     error
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetDpDiscard))
	defer func() {
		doStatAndMetric(proto.AdminSetDpDiscard, metric, err, nil)
	}()

	dpId, discard, force, err = parseSetDpDiscardParam(r)
	if err != nil {
		log.LogInfof("[setDpDiscardHandler] set dp %v to discard(%v)", dpId, discard)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	err = m.setDpDiscard(dpId, discard, force)
	if err != nil {
		log.LogErrorf("[setDpDiscardHandler] set dp %v to discard %v, err (%s)", dpId, discard, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	msg := fmt.Sprintf("[setDpDiscardHandler] set dpid %v to discard(%v) success", dpId, discard)
	log.LogInfo(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) getDiscardDpHandler(w http.ResponseWriter, r *http.Request) {
	DiscardDpInfos := proto.DiscardDataPartitionInfos{}

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetDiscardDp))
	defer func() {
		doStatAndMetric(proto.AdminGetDiscardDp, metric, nil, nil)
	}()

	vols := m.cluster.copyVols()
	for _, vol := range vols {
		dps := vol.dataPartitions
		for _, dp := range dps.partitions {
			if dp.IsDiscard {
				DiscardDpInfos.DiscardDps = append(DiscardDpInfos.DiscardDps, *dp.buildDpInfo(m.cluster))
			}
		}
	}

	msg := fmt.Sprintf("[GetDiscardDpHandler] discard dp num:%v", len(DiscardDpInfos.DiscardDps))
	log.LogInfo(msg)
	sendOkReply(w, r, newSuccessHTTPReply(DiscardDpInfos))
}

func (m *Server) queryBadDisks(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		infos proto.DiskInfos
	)

	metric := exporter.NewTPCnt("req_queryBadDisks")
	defer func() {
		metric.Set(err)
	}()

	m.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}

		for _, ds := range dataNode.DiskStats {
			if ds.Status != proto.Unavailable {
				continue
			}
			info := proto.DiskInfo{
				Address:              dataNode.Addr,
				Path:                 ds.DiskPath,
				TotalPartitionCnt:    ds.TotalPartitionCnt,
				DiskErrPartitionList: ds.DiskErrPartitionList,
			}
			infos.Disks = append(infos.Disks, info)
		}
		return true
	})

	sendOkReply(w, r, newSuccessHTTPReply(infos))
}

func (m *Server) queryDisks(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		nodeAddr string
		infos    proto.DiskInfos
	)

	metric := exporter.NewTPCnt("req_queryDisks")
	defer func() {
		metric.Set(err)
	}()

	nodeAddr = r.FormValue(addrKey)
	if len(nodeAddr) > 0 {
		if !checkIp(nodeAddr) {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
			return
		}
	}

	m.cluster.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode, ok := node.(*DataNode)
		if !ok {
			return true
		}
		if len(nodeAddr) > 0 && nodeAddr != dataNode.Addr {
			return true
		}

		for _, ds := range dataNode.DiskStats {
			info := proto.DiskInfo{
				NodeId:               dataNode.ID,
				Address:              dataNode.Addr,
				Path:                 ds.DiskPath,
				Status:               proto.DiskStatusMap[ds.Status],
				TotalPartitionCnt:    ds.TotalPartitionCnt,
				DiskErrPartitionList: ds.DiskErrPartitionList,
			}
			infos.Disks = append(infos.Disks, info)
		}
		return true
	})

	sendOkReply(w, r, newSuccessHTTPReply(infos))
}

func (m *Server) queryDiskDetail(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		nodeAddr   string
		diskPath   string
		dataNode   *DataNode
		diskDetail proto.DiskInfo
	)

	metric := exporter.NewTPCnt("req_queryDiskDetail")
	defer func() {
		metric.Set(err)
	}()

	if nodeAddr, diskPath, err = parseNodeAddrAndDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	targetDisk := proto.DiskStat{}
	diskExist := false
	for _, ds := range dataNode.DiskStats {
		if ds.DiskPath == diskPath {
			targetDisk = ds
			diskExist = true
			break
		}
	}
	if !diskExist {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDiskNotExists))
		return
	}

	diskDetail = proto.DiskInfo{
		NodeId:  dataNode.ID,
		Address: dataNode.Addr,
		Path:    targetDisk.DiskPath,
		Status:  proto.DiskStatusMap[targetDisk.Status],

		Total:     targetDisk.Total,
		Used:      targetDisk.Used,
		Available: targetDisk.Available,
		IOUtil:    targetDisk.IOUtil,

		TotalPartitionCnt:    targetDisk.TotalPartitionCnt,
		DiskErrPartitionList: targetDisk.DiskErrPartitionList,
	}

	sendOkReply(w, r, newSuccessHTTPReply(diskDetail))
}

func (m *Server) volSetTrashInterval(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		interval int64
		err      error
		msg      string
		authKey  string
		vol      *Vol
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminSetTrashInterval))
	defer func() {
		doStatAndMetric(proto.AdminSetTrashInterval, metric, err, map[string]string{exporter.Vol: name})
	}()

	if name, authKey, interval, err = parseRequestToSetTrashInterval(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if interval < 0 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "interval cannot be less than 0"})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	newArgs := getVolVarargs(vol)
	newArgs.trashInterval = interval

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update vol[%v] TrashInterval to %v min successfully\n", name, interval)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addLcNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AddLcNode))
	defer func() {
		doStatAndMetric(proto.AddLcNode, metric, err, nil)
	}()

	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIp(nodeAddr) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
		return
	}
	if id, err = m.cluster.addLcNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

// handle tasks such as heartbeat，expiration scanning, etc.
func (m *Server) handleLcNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	var (
		tr  *proto.AdminTask
		err error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetLcNodeTaskResponse))
	defer func() {
		doStatAndMetric(proto.GetLcNodeTaskResponse, metric, err, nil)
	}()

	tr, err = parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleLcNodeTaskResponse(tr.OperatorAddr, tr)
}

func (m *Server) SetBucketLifecycle(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		err   error
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.SetBucketLifecycle))
	defer func() {
		doStatAndMetric(proto.SetBucketLifecycle, metric, err, nil)
	}()

	if bytes, err = io.ReadAll(r.Body); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	req := proto.LcConfiguration{}
	if err = json.Unmarshal(bytes, &req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var vol *Vol
	if vol, err = m.cluster.getVol(req.VolName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	if err = proto.ValidRules(req.Rules); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// lifecycle transition storage class must in vol allowedStorageClass
	for _, rule := range req.Rules {
		for _, t := range rule.Transitions {
			if !allowedStorageClass(t.StorageClass, vol.allowedStorageClass) || len(vol.allowedStorageClass) < 2 {
				sendErrReply(w, r, newErrHTTPReply(proto.ErrNoSupportStorageClass))
				return
			}
		}
	}

	err = m.cluster.SetBucketLifecycle(&req)
	b, _ := json.Marshal(req)
	auditlog.LogMasterOp("SetBucketLifecycle", fmt.Sprintf("LcConfiguration(%v)", string(b)), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set vol[%v] lifecycle successfully", vol.Name)))
}

func allowedStorageClass(sc string, allowed []uint32) bool {
	for _, a := range allowed {
		if proto.OpTypeToStorageType(sc) == a {
			return true
		}
	}
	return false
}

func (m *Server) GetBucketLifecycle(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		lcConf *proto.LcConfiguration
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetBucketLifecycle))
	defer func() {
		doStatAndMetric(proto.GetBucketLifecycle, metric, err, nil)
	}()

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	lcConf = m.cluster.GetBucketLifecycle(name)
	if lcConf == nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrNoSuchLifecycleConfiguration))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(lcConf))
}

func (m *Server) DelBucketLifecycle(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		name string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DeleteBucketLifecycle))
	defer func() {
		doStatAndMetric(proto.DeleteBucketLifecycle, metric, err, nil)
	}()

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	err = m.cluster.DelBucketLifecycle(name)
	auditlog.LogMasterOp("DelBucketLifecycle", fmt.Sprintf("vol(%v)", name), err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	msg := fmt.Sprintf("delete vol[%v] lifecycle successfully", name)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) adminLcNode(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminLcNode))
	defer func() {
		doStatAndMetric(proto.AdminLcNode, metric, nil, nil)
	}()

	if err := r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	switch r.FormValue("op") {
	case "info":
		if m.cluster.partition != nil && m.cluster.partition.IsRaftLeader() {
			vol := r.FormValue("vol")
			rid := r.FormValue("ruleid")
			done := r.FormValue("done")
			rsp, err := m.cluster.adminLcNodeInfo(vol, rid, done)
			auditlog.LogMasterOp("AdminLcNode", fmt.Sprintf("op(info), vol(%v), ruleid(%v), done(%v)", vol, rid, done), err)
			if err != nil {
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			} else {
				sendOkReply(w, r, newSuccessHTTPReply(rsp))
			}
		} else {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "not leader"})
		}
	case "start":
		if m.cluster.partition != nil && m.cluster.partition.IsRaftLeader() {
			vol := r.FormValue("vol")
			rid := r.FormValue("ruleid")
			success, msg := m.cluster.lcMgr.startLcScan(vol, rid)
			auditlog.LogMasterOp("AdminLcNode", fmt.Sprintf("op(start), vol(%v), ruleid(%v), msg: %v", vol, rid, msg), nil)
			if !success {
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
			} else {
				sendOkReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: msg})
			}
		} else {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "not leader"})
		}
	case "stop":
		if m.cluster.partition != nil && m.cluster.partition.IsRaftLeader() {
			vol := r.FormValue("vol")
			rid := r.FormValue("ruleid")
			success, msg := m.cluster.lcMgr.stopLcScan(vol, rid)
			auditlog.LogMasterOp("AdminLcNode", fmt.Sprintf("op(stop), vol(%v), ruleid(%v), msg: %v", vol, rid, msg), nil)
			if !success {
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
			} else {
				sendOkReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: msg})
			}
		} else {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "not leader"})
		}
	default:
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "invalid op"})
	}
}

func (m *Server) S3QosSet(w http.ResponseWriter, r *http.Request) {
	var (
		param = &proto.S3QosRequest{}
		err   error
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.S3QoSSet))
	defer func() {
		doStatAndMetric(proto.S3QoSSet, metric, err, nil)
	}()

	if err = parseS3QosReq(r, param); err != nil {
		log.LogErrorf("[S3QosSet] parse fail err [%v]", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !isS3QosConfigValid(param) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "s3 qos param err"})
		return
	}

	// set s3 qos quota
	if param.Quota != 0 {
		if strings.ToLower(param.Uid) == proto.DefaultUid {
			param.Uid = proto.DefaultUid
		}
		param.Api = strings.ToLower(param.Api)
		metadata := new(RaftCmd)
		metadata.Op = opSyncS3QosSet
		key := param.Api + keySeparator + param.Uid + keySeparator + param.Type
		metadata.K = S3QoSPrefix + key
		metadata.V = []byte(strconv.FormatUint(param.Quota, 10))

		// raft sync
		if err = m.cluster.submit(metadata); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		// memory cache
		m.cluster.S3ApiQosQuota.Store(metadata.K, param.Quota)
	}

	// set s3 node num
	if param.Nodes != 0 {
		metadata := new(RaftCmd)
		metadata.Op = opSyncS3QosSet
		key := proto.S3Nodes
		metadata.K = S3QoSPrefix + key
		metadata.V = []byte(strconv.FormatUint(param.Nodes, 10))
		// raft sync
		if err = m.cluster.submit(metadata); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		// memory cache
		m.cluster.S3ApiQosQuota.Store(metadata.K, param.Nodes)
	}

	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func (m *Server) S3QosGet(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.S3QoSGet))
	defer func() {
		doStatAndMetric(proto.S3QoSGet, metric, err, nil)
	}()

	apiLimitConf := make(map[string]*proto.UserLimitConf)
	s3QosResponse := proto.S3QoSResponse{
		ApiLimitConf: apiLimitConf,
	}
	// memory cache
	m.cluster.S3ApiQosQuota.Range(func(key, value interface{}) bool {
		k := key.(string)
		v := value.(uint64)
		api, uid, limitType, nodeNumKey, err := parseS3QoSKey(k)
		if err != nil {
			log.LogErrorf("[S3QosGet] parseS3QoSKey err [%v]", err)
			return true
		}
		if nodeNumKey != "" {
			s3QosResponse.Nodes = v
			return true
		}
		if _, ok := apiLimitConf[api]; !ok {
			bandWidthQuota := make(map[string]uint64)
			qpsQuota := make(map[string]uint64)
			concurrentQuota := make(map[string]uint64)
			userLimitConf := &proto.UserLimitConf{
				BandWidthQuota:  bandWidthQuota,
				QPSQuota:        qpsQuota,
				ConcurrentQuota: concurrentQuota,
			}
			apiLimitConf[api] = userLimitConf
		}
		switch limitType {
		case proto.FlowLimit:
			apiLimitConf[api].BandWidthQuota[uid] = v
		case proto.QPSLimit:
			apiLimitConf[api].QPSQuota[uid] = v
		case proto.ConcurrentLimit:
			apiLimitConf[api].ConcurrentQuota[uid] = v
		default:
			// do nothing
		}
		return true
	})

	log.LogDebugf("[S3QosGet] s3qosInfoMap %+v", s3QosResponse)
	sendOkReply(w, r, newSuccessHTTPReply(s3QosResponse))
}

func (m *Server) S3QosDelete(w http.ResponseWriter, r *http.Request) {
	var (
		param = &proto.S3QosRequest{}
		err   error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.S3QoSDelete))
	defer func() {
		doStatAndMetric(proto.S3QoSDelete, metric, err, nil)
	}()

	if err = parseS3QosReq(r, param); err != nil {
		log.LogErrorf("[S3QosSet] parse fail err [%v]", err)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !isS3QosConfigValid(param) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "s3 qos param err"})
		return
	}

	if strings.ToLower(param.Uid) == proto.DefaultUid {
		param.Uid = proto.DefaultUid
	}
	param.Api = strings.ToLower(param.Api)
	metadata := new(RaftCmd)
	metadata.Op = opSyncS3QosDelete
	key := param.Api + keySeparator + param.Uid + keySeparator + param.Type
	metadata.K = S3QoSPrefix + key

	// raft sync
	if err = m.cluster.submit(metadata); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	// memory cache
	m.cluster.S3ApiQosQuota.Delete(metadata.K)

	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func parseS3QoSKey(key string) (api, uid, limitType, nodes string, err error) {
	s3qosInfo := strings.TrimPrefix(key, S3QoSPrefix)
	strs := strings.Split(s3qosInfo, keySeparator)
	if len(strs) == 3 {
		return strs[0], strs[1], strs[2], "", nil
	}
	if len(strs) == 1 && strs[0] == proto.S3Nodes {
		return "", "", "", strs[0], nil
	}
	return "", "", "", "", errors.New("unexpected key")
}

func isS3QosConfigValid(param *proto.S3QosRequest) bool {
	if param.Type != proto.FlowLimit && param.Type != proto.QPSLimit && param.Type != proto.ConcurrentLimit {
		return false
	}

	if proto.IsS3PutApi(param.Api) {
		return false
	}

	return true
}

func (m *Server) setVolDpRepairBlockSize(w http.ResponseWriter, r *http.Request) {
	var (
		repairSize uint64
		name       string
		err        error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolSetDpRepairBlockSize))
	defer func() {
		doStatAndMetric(proto.AdminVolSetDpRepairBlockSize, metric, err, nil)
		if err != nil {
			log.LogErrorf("[updateVolDpRepairSize] set dp repair size failed, error: %v", err)
		} else {
			log.LogInfof("[updateVolDpRepairSize] set dp repair size to (%v) success", repairSize)
		}
	}()
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if repairSize, err = parseAndExtractDpRepairBlockSize(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if repairSize == 0 {
		repairSize = 512 * util.KB
	}
	if repairSize < 128*util.KB {
		err = errors.NewErrorf("[updateVolDpRepairSize] repair size %v too small", repairSize)
		log.LogErrorf("[updateVolDpRepairSize] cannot set repair size < 128KB, err(%v)", err)
		return
	}
	if repairSize > 5*util.MB {
		err = errors.NewErrorf("[updateVolDpRepairSize] repair size %v too large", repairSize)
		log.LogErrorf("[updateVolDpRepairSize] cannot set repair size > 5MB, err(%v)", err)
		return
	}

	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	oldRepairSize := vol.dpRepairBlockSize
	vol.dpRepairBlockSize = repairSize
	defer func() {
		if err != nil {
			vol.dpRepairBlockSize = oldRepairSize
		}
	}()
	if err = m.cluster.syncUpdateVol(vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set volume dp repair block size to (%v) success", repairSize)))
}

func (m *Server) checkReplicaMeta(w http.ResponseWriter, r *http.Request) {
	var resp proto.BadReplicaMetaResponse

	vols := m.cluster.allVols()
	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			for _, replica := range dp.Replicas {
				// check peer length first
				if !dp.checkReplicaMetaEqualToMaster(replica.LocalPeers) {
					resp.Infos = append(resp.Infos, proto.BadReplicaMetaInfo{
						PartitionId: dp.PartitionID,
						Replica:     fmt.Sprintf("%v_%v", replica.Addr, replica.DiskPath),
						BadPeer:     replica.LocalPeers,
						ExpectPeer:  dp.Peers,
					})
				}
			}
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) recoverReplicaMeta(w http.ResponseWriter, r *http.Request) {
	var (
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
	for _, replica := range dp.Replicas {
		if !dp.checkReplicaMetaEqualToMaster(replica.LocalPeers) {
			err = dp.recoverDataReplicaMeta(replica.Addr, m.cluster)
			if err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
			}
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("recover meta for dp (%v) replica success", dp.PartitionID)))
}

func (m *Server) QueryDecommissionFailedDisk(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		decommType int
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminQueryDecommissionFailedDisk))
	defer func() {
		doStatAndMetric(proto.AdminQueryDecommissionFailedDisk, metric, err, nil)
	}()

	decommType, err = parseAndExtractDecommissionType(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	disks := make([]*proto.DecommissionFailedDiskInfo, 0)
	m.cluster.DecommissionDisks.Range(func(key, value interface{}) bool {
		d := value.(*DecommissionDisk)
		if d.GetDecommissionStatus() == DecommissionFail {
			if d.Type == uint32(decommType) || decommType == int(QueryDecommission) {
				disks = append(disks, &proto.DecommissionFailedDiskInfo{
					SrcAddr:               d.SrcAddr,
					DiskPath:              d.DiskPath,
					DecommissionRaftForce: d.DecommissionRaftForce,
					DecommissionTimes:     d.DecommissionTimes,
					DecommissionDpTotal:   d.DecommissionDpTotal,
					IsAutoDecommission:    d.Type == AutoDecommission,
				})
			}
		}
		return true
	})
	sendOkReply(w, r, newSuccessHTTPReply(disks))
}

func (m *Server) abortDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		addr string
		disk string
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminAbortDecommissionDisk))
	defer func() {
		doStatAndMetric(proto.AdminAbortDecommissionDisk, metric, err, nil)
	}()

	addr, err = parseAndExtractNodeAddr(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	disk, err = extractDiskPath(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%v_%v", addr, disk)
	val, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("decommission datanode %v disk %v not found", addr, disk)})
		return
	}
	dd := val.(*DecommissionDisk)
	err = dd.Abort(m.cluster)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodePersistenceByRaft, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("cancel decommission datanode(%v) disk(%v) success", addr, disk)))
}

func (m *Server) queryDiskBrokenThreshold(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt("req_queryDiskBrokenThreshold")
	defer func() {
		metric.Set(nil)
	}()
	ratio := m.cluster.MarkDiskBrokenThreshold.Load()
	rstMsg := fmt.Sprintf("disk broken ratio is %v ", ratio)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) setDiskBrokenThreshold(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt("req_setDiskBrokenThreshold")
	defer func() {
		metric.Set(nil)
	}()
	var (
		ratio float64
		err   error
	)
	if ratio, err = parseRequestToSetDiskBrokenThreshold(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.setMarkDiskBrokenThreshold(ratio); err != nil {
		sendErrReply(w, r, newErrHTTPReply(errors.NewErrorf("set disk broken ratio failed:%v", err)))
		return
	}
	rstMsg := fmt.Sprintf("disk broken rationis set to %v ", ratio)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) cancelDecommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
		dataNode              *DataNode
		zone                  *Zone
		ns                    *nodeSet
	)

	metric := exporter.NewTPCnt("req_cancelDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	value, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		ret := fmt.Sprintf("action[cancelDecommissionDisk]cannot found decommission task for node[%v] disk[%v], "+
			"may be already offline", offLineAddr, diskPath)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	if dataNode, err = m.cluster.dataNode(offLineAddr); err != nil {
		ret := fmt.Sprintf("action[cancelDecommissionDisk]cannot find dataNode[%s]", offLineAddr)
		log.LogWarnf("%v", ret)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}

	if zone, err = m.cluster.t.getZone(dataNode.ZoneName); err != nil {
		ret := fmt.Sprintf("action[cancelDecommissionDisk] find datanode[%s] zone failed[%v]",
			dataNode.Addr, err.Error())
		log.LogWarnf("%v", ret)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
		ret := fmt.Sprintf("action[cancelDecommissionDisk] find datanode[%s] nodeset[%v] failed[%v]",
			dataNode.Addr, dataNode.NodeSetID, err.Error())
		log.LogWarnf("%v", ret)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	disk := value.(*DecommissionDisk)
	status := disk.GetDecommissionStatus()
	if status == DecommissionSuccess || status == DecommissionFail {
		ret := fmt.Sprintf("action[cancelDecommissionDisk] disk %v status %v do not support cancel",
			key, status)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	if err = disk.cancelDecommission(m.cluster, ns); err != nil {
		ret := fmt.Sprintf("action[cancelDecommissionDisk] cancel disk %v decommission failed[%v]",
			key, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	// remove from decommissioned disk, do not remove bad disk until it is recovered
	// dp may allocated on bad disk otherwise
	if !dataNode.isBadDisk(diskPath) {
		m.cluster.deleteAndSyncDecommissionedDisk(dataNode, diskPath)
	}
	rstMsg := fmt.Sprintf("cancel decommission disk[%s] successfully ", key)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) resetDataPartitionRestoreStatus(w http.ResponseWriter, r *http.Request) {
	var err error

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminAbortDecommissionDisk))
	defer func() {
		doStatAndMetric(proto.AdminResetDataPartitionRestoreStatus, metric, err, nil)
	}()

	dpId, err := parseRequestToResetDpRestoreStatus(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	dp, err := m.cluster.getDataPartitionByID(dpId)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	ok := dp.resetRestoreMeta(RestoreReplicaMetaRunning)
	log.LogInfof("[resetDataPartitionRestoreStatus] reset dp(%v) restore status ok(%v)", dpId, ok)
	sendOkReply(w, r, newSuccessHTTPReply(ok))
}

func (m *Server) getAllDataNodes(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetClusterDataNodes))
	defer func() {
		doStatAndMetric(proto.AdminGetClusterDataNodes, metric, nil, nil)
	}()
	dataNodes := m.cluster.allDataNodes()
	sendOkReply(w, r, newSuccessHTTPReply(dataNodes))
}

func (m *Server) getAllMetaNodes(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetClusterMetaNodes))
	defer func() {
		doStatAndMetric(proto.AdminGetClusterMetaNodes, metric, nil, nil)
	}()
	metaNodes := m.cluster.allMetaNodes()
	sendOkReply(w, r, newSuccessHTTPReply(metaNodes))
}

func (m *Server) recoverBackupDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		dataNode    *DataNode
		partitionID uint64
		err         error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminRecoverBackupDataReplica))
	defer func() {
		doStatAndMetric(proto.AdminRecoverBackupDataReplica, metric, err, nil)
	}()

	if partitionID, addr, err = parseRequestToAddDataReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	dataNode, err = m.cluster.dataNode(addr)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if !proto.IsNormalDp(dp.PartitionType) {
		err = fmt.Errorf("action[recoverBackupDataReplica] [%d] is not normal dp, not support add or delete replica", dp.PartitionID)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if dp.ReplicaNum == uint8(len(dp.Replicas)) {
		err = fmt.Errorf("action[recoverBackupDataReplica] [%d] already have %v replicas", dp.PartitionID, dp.ReplicaNum)
		sendErrReply(w, r, newErrHTTPReply(err))
	}

	retry := 0
	for {
		if !dp.setRestoreReplicaForbidden() {
			retry++
			if retry > defaultDecommissionRetryLimit {
				err = errors.NewErrorf("set RestoreReplicaMetaForbidden failed")
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	defer dp.setRestoreReplicaStop()
	// restore raft member first
	addPeer := proto.Peer{ID: dataNode.ID, Addr: addr, HeartbeatPort: dataNode.HeartbeatPort, ReplicaPort: dataNode.ReplicaPort}

	log.LogInfof("action[recoverBackupDataReplica] dp %v dst addr %v try add raft member, node id %v", dp.PartitionID, addr, dataNode.ID)
	if err = m.cluster.addDataPartitionRaftMember(dp, addPeer); err != nil {
		log.LogWarnf("action[recoverBackupDataReplica] dp %v addr %v try add raft member err [%v]", dp.PartitionID, addr, err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	// find replica disk path
	backupInfo, err := dataNode.getBackupDataPartitionInfo(partitionID)
	if err != nil {
		log.LogWarnf("action[recoverBackupDataReplica] cannot find backup info for dp %v on dataNode %v", partitionID, dataNode.Addr)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	err = m.cluster.syncRecoverBackupDataPartitionReplica(addr, backupInfo.Disk, dp)
	if err != nil {
		log.LogWarnf("action[recoverBackupDataReplica] dp(%v)  recover replica [%v_%v] fail %v", dp.PartitionID, addr, backupInfo.Disk, err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("action[recoverBackupDataReplica] dp(%v)  recover replica [%v_%v] successfully", dp.decommissionInfo(), backupInfo.Addr, backupInfo.Disk)
	log.LogInfof("%v", msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) resetDecommissionDiskStatus(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
	)

	metric := exporter.NewTPCnt("req_resetDecommissionDisk")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	value, ok := m.cluster.DecommissionDisks.Load(key)
	if !ok {
		ret := fmt.Sprintf("action[resetDecommissionDiskStatus]cannot found decommission task for node[%v] disk[%v], "+
			"may be already offline", offLineAddr, diskPath)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	disk := value.(*DecommissionDisk)
	disk.SetDecommissionStatus(DecommissionInitial)
	err = m.cluster.syncUpdateDecommissionDisk(disk)
	if err != nil {
		ret := fmt.Sprintf("action[resetDecommissionDiskStatus]persist disk[%v] failed %v", key, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: ret})
		return
	}
	rstMsg := fmt.Sprintf("reset decommission disk[%s] successfully ", key)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) recoverBadDisk(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
		dataNode              *DataNode
		found                 = false
	)

	metric := exporter.NewTPCnt("req_recoverBadDisk")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// check if bad disk is reported
	if dataNode, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	for _, disk := range dataNode.BadDiskStats {
		if disk.DiskPath == diskPath {
			found = true
			break
		}
	}
	if !found {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("disk %v is not bad "+
			"disk on dataNode %v, do not support recover", diskPath, offLineAddr)})
		return
	}
	err = dataNode.createTaskToRecoverBadDisk(diskPath)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	key := fmt.Sprintf("%s_%s", offLineAddr, diskPath)
	// do not delete disk from DecommissionDisks, bad disk may be auto decommissioned again if recover is not finished
	rstMsg := fmt.Sprintf("recover bad disk[%s] task is submit ", key)
	auditlog.LogMasterOp("RecoverBadDisk", rstMsg, nil)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) queryBadDiskRecoverProgress(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
		dataNode              *DataNode
		resp                  *proto.Packet
	)

	metric := exporter.NewTPCnt("req_queryBadDiskRecoverProgress")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// check if bad disk is reported
	if dataNode, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	resp, err = dataNode.createTaskToQueryBadDiskRecoverProgress(diskPath)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	progress := &proto.BadDiskRecoverProgress{}
	if err = json.Unmarshal(resp.Data, progress); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(progress))
}

func (m *Server) deleteBackupDirectories(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
		dataNode              *DataNode
	)

	metric := exporter.NewTPCnt("req_deleteBackupDirectories")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// check if bad disk is reported
	if dataNode, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	_, err = dataNode.createTaskToDeleteBackupDirectories(diskPath)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	resp := fmt.Sprintf("All backup directories in disk(%v_%v) is sumbited,please check later", offLineAddr, diskPath)
	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) queryBackupDirectories(w http.ResponseWriter, r *http.Request) {
	var (
		offLineAddr, diskPath string
		err                   error
		dataNode              *DataNode
		ids                   []uint64
	)

	metric := exporter.NewTPCnt("req_queryBackupDirectories")
	defer func() {
		metric.Set(err)
	}()

	if offLineAddr, diskPath, _, _, _, err = parseReqToDecoDisk(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// check if bad disk is reported
	if dataNode, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	for _, info := range dataNode.BackupDataPartitions {
		if info.Addr == offLineAddr && info.Disk == diskPath {
			ids = append(ids, info.PartitionID)
		}
	}
	resp := fmt.Sprintf("There are backup directoires for(%v) left on disk(%v) datanode(%v)", ids, offLineAddr, diskPath)
	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) resetDecommissionDataNodeStatus(w http.ResponseWriter, r *http.Request) {
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
	for _, disk := range dn.AllDisks {
		if err = m.cluster.deleteAndSyncDecommissionedDisk(dn, disk); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	dn.resetDecommissionStatus()
	m.cluster.syncUpdateDataNode(dn)
	msg := fmt.Sprintf("reset decommission status for datanode %v success", dn.Addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) volAddAllowedStorageClass(w http.ResponseWriter, r *http.Request) {
	var (
		name                   string
		authKey                string
		err                    error
		msg                    string
		addAllowedStorageClass uint32
		ebsBlockSize           int
		vol                    *Vol
		force                  bool
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolAddAllowedStorageClass))
	defer func() {
		doStatAndMetric(proto.AdminVolAddAllowedStorageClass, metric, err, map[string]string{exporter.Vol: name})
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if authKey, err = extractAuthKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if force, err = extractBoolWithDefault(r, forceKey, false); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if !m.cluster.dataMediaTypeVaild {
		err := fmt.Errorf("cluster media type still not set, can't add storage class")
		log.LogErrorf("volAddAllowedStorageClass: vol %s, err %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if addAllowedStorageClass, err = extractUint32(r, allowedStorageClassKey); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if ebsBlockSize, err = extractUint(r, ebsBlkSizeKey); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if !proto.IsValidStorageClass(addAllowedStorageClass) {
		err = fmt.Errorf("invalid storageClass(%v)", addAllowedStorageClass)
		log.LogErrorf("[volAddAllowedStorageClass] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	if !vol.AllPartitionForbidVer0() {
		err = fmt.Errorf("there is still some dp or mp not forbidden write")
		log.LogErrorf("[volAddAllowedStorageClass] vol(%v), err: %v", name, err.Error())
		if !force || !vol.ForbidWriteOpOfProtoVer0.Load() {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
			return
		}
	}

	if in := vol.isStorageClassInAllowed(addAllowedStorageClass); in {
		err = fmt.Errorf("storageClass(%v) already in vol allowedStorageClass(%v)",
			addAllowedStorageClass, vol.allowedStorageClass)
		log.LogErrorf("[volAddAllowedStorageClass] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if m.HasBothReplicaAndBlobstore(addAllowedStorageClass, vol.allowedStorageClass) {
		err = fmt.Errorf("vol not support both replica and blobstore")
		log.LogErrorf("[volAddAllowedStorageClass] create vol(%v) err: %v", vol.Name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if proto.IsStorageClassBlobStore(vol.volStorageClass) {
		err = fmt.Errorf("volStorageClass is %v, can not add allowedStorageClass",
			proto.StorageClassString(vol.volStorageClass))
		log.LogErrorf("[volAddAllowedStorageClass] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	resourceChecker := NewStorageClassResourceChecker(m.cluster, vol.zoneName)
	if !resourceChecker.HasResourceOfStorageClass(addAllowedStorageClass) {
		scope := "cluster"
		if vol.zoneName != "" {
			scope = fmt.Sprintf("assigned zones(%v)", vol.zoneName)
		}

		err = fmt.Errorf("%v has no resoure to support storageClass(%v)",
			scope, proto.StorageClassString(addAllowedStorageClass))
		log.LogErrorf("[volAddAllowedStorageClass] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if m.cluster.FaultDomain {
		err = fmt.Errorf("cluster fault domain is on, not support multiple allowedStorageClass at the same time")
		log.LogErrorf("[volAddAllowedStorageClass] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	verList := vol.VersionMgr.getVersionList()
	if len(verList.VerList) > 1 {
		err = fmt.Errorf("vol(%v) now has or used to have snapshot version, not support multiple allowedStorageClass, verListLen(%v)",
			name, len(verList.VerList))
		log.LogErrorf("[volAddAllowedStorageClass] err: %v", err.Error())
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	newArgs := getVolVarargs(vol)

	if proto.IsStorageClassBlobStore(addAllowedStorageClass) && ebsBlockSize == 0 {
		if vol.EbsBlkSize == 0 {
			ebsBlockSize = defaultEbsBlkSize
			log.LogInfof("[volAddAllowedStorageClass] vol(%v) allowedStorageClass(%v) use default ebsBlockSize(%v)",
				name, proto.StorageClassString(proto.StorageClass_BlobStore), ebsBlockSize)
		} else {
			ebsBlockSize = vol.EbsBlkSize
			log.LogInfof("[volAddAllowedStorageClass] vol(%v) allowedStorageClass(%v) use old ebsBlockSize(%v)",
				name, proto.StorageClassString(proto.StorageClass_BlobStore), defaultEbsBlkSize)
		}
	}
	newArgs.coldArgs.objBlockSize = ebsBlockSize

	newArgs.allowedStorageClass = append(newArgs.allowedStorageClass, addAllowedStorageClass)
	sort.Slice(newArgs.allowedStorageClass, func(i, j int) bool {
		return newArgs.allowedStorageClass[i] < newArgs.allowedStorageClass[j]
	})
	log.LogInfof("[volAddAllowedStorageClass] vol(%v) to add allowedStorageClass, old(%v), add(%v)",
		name, vol.allowedStorageClass, addAllowedStorageClass)

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg = fmt.Sprintf("add vol(%v) allowedStorageClass successfully, new allowedStorageClass: %v",
		name, newArgs.allowedStorageClass)
	log.LogInfof("[volAddAllowedStorageClass] %v, added(%v), current(%v)",
		msg, addAllowedStorageClass, vol.allowedStorageClass)
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func (m *Server) getVolListForbidWriteOpOfProtoVer0() (volsForbidWriteOpOfProtoVer0 []string) {
	volsForbidWriteOpOfProtoVer0 = make([]string, 0)

	m.cluster.volMutex.RLock()
	defer m.cluster.volMutex.RUnlock()
	for _, vol := range m.cluster.vols {
		if vol.ForbidWriteOpOfProtoVer0.Load() {
			volsForbidWriteOpOfProtoVer0 = append(volsForbidWriteOpOfProtoVer0, vol.Name)
		}
	}

	return
}

func (m *Server) getUpgradeCompatibleSettings(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminGetUpgradeCompatibleSettings))
	defer func() {
		doStatAndMetric(proto.AdminGetUpgradeCompatibleSettings, metric, nil, nil)
	}()

	volsForbidWriteOpOfProtoVer0 := m.getVolListForbidWriteOpOfProtoVer0()

	cInfo := &proto.UpgradeCompatibleSettings{
		VolsForbidWriteOpOfProtoVer0:    volsForbidWriteOpOfProtoVer0,
		ClusterForbidWriteOpOfProtoVer0: m.cluster.cfg.forbidWriteOpOfProtoVer0,
		LegacyDataMediaType:             m.cluster.legacyDataMediaType,
		DataMediaTypeVaild:              m.cluster.dataMediaTypeVaild,
	}
	log.LogInfof("[getUpgradeCompatibleSettings] cluster.legacyDataMediaType(%v), ClusterForbidWriteOpOfProtoVer0(%v), VolsForbidWriteOpOfProtoVer0(total %v): %v",
		cInfo.LegacyDataMediaType, cInfo.ClusterForbidWriteOpOfProtoVer0,
		len(cInfo.VolsForbidWriteOpOfProtoVer0), cInfo.VolsForbidWriteOpOfProtoVer0)

	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}

func (m *Server) getMetaPartitionEmptyStatus(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionEmptyStatus))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionEmptyStatus, metric, nil, nil)
	}()

	mpsStatus := make([]proto.VolEmptyMpStats, 0, len(m.cluster.vols))
	for _, name := range m.cluster.allVolNames() {
		vol, err := m.cluster.getVol(name)
		if err != nil {
			log.LogErrorf("[getMetaPartitionEmptyStatus] getVol(%s) failed: %s", name, err.Error())
			continue
		}
		// skip the deleted volume.
		if vol.Status == proto.VolStatusMarkDelete {
			continue
		}
		volStatus := proto.VolEmptyMpStats{
			Name: name,
		}
		volStatus.MetaPartitions = make([]*proto.MetaPartitionView, 0, len(vol.MetaPartitions))
		volStatus.Total = len(vol.MetaPartitions)
		mps := vol.getSortMetaPartitions()
		for _, mp := range mps {
			if mp.IsFreeze || mp.IsEmptyToBeClean() {
				volStatus.EmptyCount++
				volStatus.MetaPartitions = append(volStatus.MetaPartitions, getMetaPartitionView(mp))
			}
		}
		if volStatus.EmptyCount > RsvEmptyMetaPartitionCnt {
			mpsStatus = append(mpsStatus, volStatus)
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(mpsStatus))
}

func (m *Server) freezeEmptyMetaPartition(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionFreezeEmpty))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionFreezeEmpty, metric, nil, nil)
	}()

	var (
		name  string
		count int
		err   error
	)

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if count, err = extractUint(r, countKey); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if count < RsvEmptyMetaPartitionCnt {
		// reserve 2 empty mp at least, not include the last one.
		count = RsvEmptyMetaPartitionCnt
	}

	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol.Status == proto.VolStatusMarkDelete {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("volume (%s) is deleted already.", name)))
		return
	}

	mps := vol.getSortMetaPartitions()
	if len(mps) <= RsvEmptyMetaPartitionCnt {
		err = fmt.Errorf("the all meta partition number is less than %d", RsvEmptyMetaPartitionCnt)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	total := 0
	for _, mp := range mps {
		if mp.IsEmptyToBeClean() {
			total++
		}
	}

	cleans := total - count
	if cleans <= 0 {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "reserve mp number is larger than or equal empty number"})
		return
	}

	freezeList := make([]*MetaPartition, 0, cleans)
	i := 0
	for j := len(mps) - 1; j >= 0; j -= 1 {
		mp := mps[j]
		if !mp.IsEmptyToBeClean() {
			continue
		}

		mp.IsFreeze = true
		if mp.Status == proto.ReadWrite {
			mp.Status = proto.ReadOnly
		}
		// store the meta partition status.
		err = m.cluster.syncUpdateMetaPartition(mp)
		if err != nil {
			log.LogErrorf("volume(%s) meta partition(%d) update failed: %s", name, mp.PartitionID, err.Error())
			continue
		}
		freezeList = append(freezeList, mp)

		i++
		if i >= cleans {
			break
		}
	}
	err = m.cluster.FreezeEmptyMetaPartitionJob(name, freezeList)

	rstMsg := fmt.Sprintf("Freeze empty volume(%s) meta partitions(%d)", name, cleans)
	auditlog.LogMasterOp("freezeEmptyMetaPartition", rstMsg, err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("Master will freeze empty meta partition of volume (%s) after 10 minutes. Task id: %s", name, name)))
}

func (m *Server) cleanEmptyMetaPartition(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionCleanEmpty))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionCleanEmpty, metric, nil, nil)
	}()

	var (
		name string
		err  error
	)

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	vol, err := m.cluster.getVol(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol.Status == proto.VolStatusMarkDelete {
		sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("volume (%s) is deleted already.", name)))
		return
	}

	err = m.cluster.StartCleanEmptyMetaPartition(name)

	rstMsg := fmt.Sprintf("Clean volume(%s) empty meta partitions", name)
	auditlog.LogMasterOp("cleanEmptyMetaPartition", rstMsg, err)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("Clean frozen meta partition for volume (%s) in the background. It may takes several hours. task id: %s", name, name)))
}

func (m *Server) removeBackupMetaPartition(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionRemoveBackup))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionRemoveBackup, metric, nil, nil)
	}()

	m.cluster.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}
		task := proto.NewAdminTask(proto.OpRemoveBackupMetaPartition, metanode.Addr, nil)
		_, err := metanode.Sender.syncSendAdminTask(task)
		if err != nil {
			log.LogErrorf("failed to remove empty meta partition")
		}
		return true
	})

	auditlog.LogMasterOp("removeBackupMetaPartition", "clean all backup meta partitions", nil)

	sendOkReply(w, r, newSuccessHTTPReply("Remove all backup meta partitions successfully."))
}

func (m *Server) getCleanMetaPartitionTask(w http.ResponseWriter, r *http.Request) {
	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminMetaPartitionGetCleanTask))
	defer func() {
		doStatAndMetric(proto.AdminMetaPartitionGetCleanTask, metric, nil, nil)
	}()

	var (
		name string
		err  error
	)

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	name = r.FormValue(nameKey)

	m.cluster.mu.Lock()
	defer m.cluster.mu.Unlock()

	if name == "" {
		sendOkReply(w, r, newSuccessHTTPReply(m.cluster.cleanTask))
	} else {
		task, ok := m.cluster.cleanTask[name]
		if !ok {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Sprintf("Can't find task for volume(%s)", name)})
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply(task))
	}
}

func removeDuplicatePaths(paths []string) []string {
	uniquePaths := make(map[string]bool)
	var result []string
	for _, path := range paths {
		if _, exists := uniquePaths[path]; !exists {
			uniquePaths[path] = true
			result = append(result, path)
		}
	}
	return result
}

func removeSubPaths(paths []string) []string {
	sort.Slice(paths, func(i, j int) bool {
		return len(paths[i]) < len(paths[j])
	})

	var result []string
	for _, path := range paths {
		shouldAdd := true
		if path == "/" {
			return []string{"/"}
		}
		for i := range result {
			if strings.HasPrefix(path+"/", result[i]+"/") && path != result[i] {
				shouldAdd = false
				break
			}
		}
		if shouldAdd {
			result = append(result, path)
		}
	}
	return result
}

func deduplicateAndRemoveContained(pathStr string) string {
	paths := strings.Split(pathStr, ",")
	uniquePaths := removeDuplicatePaths(paths)
	finalPaths := removeSubPaths(uniquePaths)
	return strings.Join(finalPaths, ",")
}

func (m *Server) createBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.CreateBalanceTask))
	defer func() {
		doStatAndMetric(proto.CreateBalanceTask, metric, err, nil)
	}()

	var plan *ClusterPlan
	// search the raft storage. Only store one plan
	plan, err = m.cluster.loadBalanceTask()
	if err == nil && plan != nil {
		err = fmt.Errorf("There is a meta partition task plan already. Please remove it before create new one.")
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	plan, err = m.cluster.GetMetaNodePressureView()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	// Save into raft storage.
	err = m.cluster.syncAddBalanceTask(plan)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) getBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.GetBalanceTask))
	defer func() {
		doStatAndMetric(proto.GetBalanceTask, metric, err, nil)
	}()

	var plan *ClusterPlan
	// search the raft storage. Only store one plan
	plan, err = m.cluster.loadBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error(), Data: plan})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(plan))
}

func (m *Server) runBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.RunBalanceTask))
	defer func() {
		doStatAndMetric(proto.RunBalanceTask, metric, err, nil)
	}()

	// search the raft storage. Only store one plan
	err = m.cluster.RunMetaPartitionBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply("Start running balance task successfully."))
}

func (m *Server) stopBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.StopBalanceTask))
	defer func() {
		doStatAndMetric(proto.StopBalanceTask, metric, err, nil)
	}()

	// search the raft storage. Only store one plan
	err = m.cluster.StopMetaPartitionBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply("Stop balance task successfully."))
}

func (m *Server) deleteBalancePlan(w http.ResponseWriter, r *http.Request) {
	var err error
	metric := exporter.NewTPCnt(apiToMetricsName(proto.DeleteBalanceTask))
	defer func() {
		doStatAndMetric(proto.DeleteBalanceTask, metric, err, nil)
	}()

	m.cluster.mu.Lock()
	defer m.cluster.mu.Unlock()

	if m.cluster.PlanRun {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "Please stop the running task before deleting it."})
		return
	}

	// search the raft storage. Only store one plan
	_, err = m.cluster.syncDeleteBalanceTask()
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply("Delete balance plan task successfully."))
}
