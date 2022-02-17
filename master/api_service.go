// Copyright 2018 The Chubao Authors.
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
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/log"
)

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

//ZoneView define the view of zone
type ZoneView struct {
	Name    string
	Status  string
	NodeSet map[uint64]*NodeSetView
}

func newZoneView(name string) *ZoneView {
	return &ZoneView{NodeSet: make(map[uint64]*NodeSetView, 0), Name: name}
}

type badPartitionView = proto.BadPartitionView

// Set the threshold of the memory usage on each meta node.
// If the memory usage reaches this threshold, then all the mata partition will be marked as readOnly.
func (m *Server) setMetaNodeThreshold(w http.ResponseWriter, r *http.Request) {
	var (
		threshold float64
		err       error
	)
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
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
//
// If DisableAutoAllocate == on, then we WILL automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
func (m *Server) setupAutoAllocation(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
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
				nsView.DataNodes = append(nsView.DataNodes, proto.NodeView{ID: dataNode.ID, Addr: dataNode.Addr, Status: dataNode.isActive, IsWritable: dataNode.isWriteAble()})
				return true
			})
			ns.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)
				nsView.MetaNodes = append(nsView.MetaNodes, proto.NodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
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
	cv := &proto.ClusterView{
		Name:                m.cluster.Name,
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
			cv.VolStatInfo = append(cv.VolStatInfo, newVolStatInfo(name, 0, 0, 0, 0))
			continue
		}
		cv.VolStatInfo = append(cv.VolStatInfo, stat.(*volStatInfo))
	}
	cv.BadPartitionIDs = m.cluster.getBadDataPartitionsView()
	cv.BadMetaPartitionIDs = m.cluster.getBadMetaPartitionsView()

	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *Server) getIPAddr(w http.ResponseWriter, r *http.Request) {
	m.cluster.loadClusterValue()
	batchCount := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteBatchCount)
	limitRate := atomic.LoadUint64(&m.cluster.cfg.DataNodeDeleteLimitRate)
	deleteSleepMs := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)
	autoRepairRate := atomic.LoadUint64(&m.cluster.cfg.DataNodeAutoRepairLimitRate)

	cInfo := &proto.ClusterInfo{
		Cluster:                     m.cluster.Name,
		MetaNodeDeleteBatchCount:    batchCount,
		MetaNodeDeleteWorkerSleepMs: deleteSleepMs,
		DataNodeDeleteLimitRate:     limitRate,
		DataNodeAutoRepairLimitRate: autoRepairRate,
		Ip:                          strings.Split(r.RemoteAddr, ":")[0],
		EbsAddr:                     m.ebsAddr,
		ServicePath:                 m.servicePath,
	}

	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}

func (m *Server) createMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		start   uint64
		err     error
	)

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

	if preload.preloadReplicaNum < 1 || preload.preloadReplicaNum > 3 {
		return fmt.Errorf("preload replicaNum must be between [%d] to [%d], now[%d]", 1, 3, preload.preloadReplicaNum)
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
	err = m.cluster.batchCreateDataPartition(vol, reqCreateCount)
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

func (m *Server) getDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		dp          *DataPartition
		partitionID uint64
		volName     string
		vol         *Vol
		err         error
	)

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
	)

	if partitionID, addr, err = parseRequestToRemoveDataReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	if err = m.cluster.removeDataReplica(dp, addr, true); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

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

// Decommission a data partition. This usually happens when disk error has been reported.
// This function needs to be called manually by the admin.
func (m *Server) decommissionDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		dp          *DataPartition
		addr        string
		partitionID uint64
		err         error
	)

	if partitionID, addr, err = parseRequestToDecommissionDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	if err = m.cluster.decommissionDataPartition(addr, dp, handleDataPartitionOfflineErr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf(proto.AdminDecommissionDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) diagnoseDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err               error
		rstMsg            *proto.DataPartitionDiagnosis
		inactiveNodes     []string
		corruptDps        []*DataPartition
		lackReplicaDps    []*DataPartition
		corruptDpIDs      []uint64
		lackReplicaDpIDs  []uint64
		badDataPartitions []badPartitionView
	)
	corruptDpIDs = make([]uint64, 0)
	lackReplicaDpIDs = make([]uint64, 0)
	if inactiveNodes, corruptDps, err = m.cluster.checkCorruptDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if lackReplicaDps, err = m.cluster.checkLackReplicaDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	for _, dp := range corruptDps {
		corruptDpIDs = append(corruptDpIDs, dp.PartitionID)
	}
	for _, dp := range lackReplicaDps {
		lackReplicaDpIDs = append(lackReplicaDpIDs, dp.PartitionID)
	}
	badDataPartitions = m.cluster.getBadDataPartitionsView()
	rstMsg = &proto.DataPartitionDiagnosis{
		InactiveDataNodes:           inactiveNodes,
		CorruptDataPartitionIDs:     corruptDpIDs,
		LackReplicaDataPartitionIDs: lackReplicaDpIDs,
		BadDataPartitionIDs:         badDataPartitions,
	}
	log.LogInfof("diagnose dataPartition[%v] inactiveNodes:[%v], corruptDpIDs:[%v], lackReplicaDpIDs:[%v]", m.cluster.Name, inactiveNodes, corruptDpIDs, lackReplicaDpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Mark the volume as deleted, which will then be deleted later.
func (m *Server) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		force   bool
		err     error
		msg     string
	)

	if name, authKey, force, err = parseRequestToDeleteVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.markDeleteVol(name, authKey, force); err != nil {
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
		req = &updateVolReq{}
		vol *Vol
		err error
	)

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

	newArgs := getVolVarargs(vol)

	newArgs.zoneName = req.zoneName
	newArgs.description = req.description
	newArgs.capacity = req.capacity
	newArgs.followerRead = req.followRead
	newArgs.authenticate = req.authenticate
	newArgs.dpSelectorName = req.dpSelectorName
	newArgs.dpSelectorParm = req.dpSelectorParm
	newArgs.coldArgs = req.coldArgs

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

// func (c *Cluster) checkZoneArgs(zoneName string, crossZone bool) error {
// 	var zones []string
// 	if zoneName == "" {
// 		zoneName = DefaultZoneName
// 	}

// 	zones = strings.Split(zoneName, ",")
// 	for _, name := range zones {
// 		if _, err := c.t.getZone(name); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

func (m *Server) checkCreateReq(req *createVolReq) (err error) {

	if !proto.IsHot(req.volType) && !proto.IsCold(req.volType) {
		return fmt.Errorf("vol type %d is illegal", req.volType)
	}

	if req.capacity == 0 {
		return fmt.Errorf("vol capacity can't be zero, %d", req.capacity)
	}

	if proto.IsHot(req.volType) {
		req.dpReplicaNum = defaultReplicaNum
		return nil
	}

	if req.dpReplicaNum == 0 {
		req.dpReplicaNum = 1
	}

	if req.dpReplicaNum < 1 || req.dpReplicaNum > 3 {
		return fmt.Errorf("cold vol's replicaNum can only be  between 1-3, received replicaNum is[%v]", req.dpReplicaNum)
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

	req.coldArgs = args
	return nil

}

func (m *Server) createVol(w http.ResponseWriter, r *http.Request) {

	req := &createVolReq{}
	vol := &Vol{}

	var err error
	if err = parseRequestToCreateVol(r, req); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.checkCreateReq(req); err != nil {
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

func (m *Server) getVolSimpleInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		name    string
		vol     *Vol
		volView *proto.SimpleVolView
	)

	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	volView = newSimpleView(vol)
	sendOkReply(w, r, newSuccessHTTPReply(volView))
}

func newSimpleView(vol *Vol) *proto.SimpleVolView {
	var (
		volInodeCount  uint64
		volDentryCount uint64
	)
	for _, mp := range vol.MetaPartitions {
		volDentryCount = volDentryCount + mp.DentryCount
		volInodeCount = volInodeCount + mp.InodeCount
	}
	maxPartitionID := vol.maxPartitionID()
	return &proto.SimpleVolView{
		ID:                 vol.ID,
		Name:               vol.Name,
		Owner:              vol.Owner,
		ZoneName:           vol.zoneName,
		DpReplicaNum:       vol.dpReplicaNum,
		MpReplicaNum:       vol.mpReplicaNum,
		InodeCount:         volInodeCount,
		DentryCount:        volDentryCount,
		MaxMetaPartitionID: maxPartitionID,
		Status:             vol.Status,
		Capacity:           vol.Capacity,
		FollowerRead:       vol.FollowerRead,
		NeedToLowerReplica: vol.NeedToLowerReplica,
		Authenticate:       vol.authenticate,
		CrossZone:          vol.crossZone,
		DefaultPriority:    vol.defaultPriority,
		DomainOn:           vol.domainOn,
		RwDpCnt:            vol.dataPartitions.readableAndWritableCnt,
		MpCnt:              len(vol.MetaPartitions),
		DpCnt:              len(vol.dataPartitions.partitionMap),
		CreateTime:         time.Unix(vol.createTime, 0).Format(proto.TimeFormat),
		Description:        vol.description,
		DpSelectorName:     vol.dpSelectorName,
		DpSelectorParm:     vol.dpSelectorParm,
		DefaultZonePrior:   vol.defaultPriority,
		VolType:            vol.VolType,
		ObjBlockSize:       vol.EbsBlkSize,
		CacheCapacity:      vol.CacheCapacity,
		CacheAction:        vol.CacheAction,
		CacheThreshold:     vol.CacheThreshold,
		CacheLruInterval:   vol.CacheLRUInterval,
		CacheTtl:           vol.CacheTTL,
		CacheLowWater:      vol.CacheLowWater,
		CacheHighWater:     vol.CacheHighWater,
		CacheRule:          vol.CacheRule,
		PreloadCapacity:    vol.getPreloadCapacity(),
	}
}

func checkIp(addr string) bool {
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
	if nodeAddr, zoneName, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIp(nodeAddr) {
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
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	dataNode.PersistenceDataPartitions = m.cluster.getAllDataPartitionIDByDatanode(nodeAddr)

	dataNodeInfo = &proto.DataNodeInfo{
		Total:                     dataNode.Total,
		Used:                      dataNode.Used,
		AvailableSpace:            dataNode.AvailableSpace,
		ID:                        dataNode.ID,
		ZoneName:                  dataNode.ZoneName,
		Addr:                      dataNode.Addr,
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
	}

	sendOkReply(w, r, newSuccessHTTPReply(dataNodeInfo))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) decommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		offLineAddr string
		limit       int
		err         error
	)

	if offLineAddr, limit, err = parseDecomNodeReq(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if _, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if err = m.cluster.migrateDataNode(offLineAddr, "", limit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg = fmt.Sprintf("decommission data node [%v] limit %d successfully", offLineAddr, limit)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) migrateDataNodeHandler(w http.ResponseWriter, r *http.Request) {
	srcAddr, targetAddr, limit, err := parseMigrateNodeParam(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if limit > defaultMigrateDpCnt {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError,
			Msg: fmt.Sprintf("limit %d can't be bigger than %d", limit, defaultMigrateDpCnt)})
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
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if !targetNode.isWriteAble() {
		err = fmt.Errorf("[%s] is not writable, can't used as target addr for migrate", targetAddr)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.migrateDataNode(srcAddr, targetAddr, limit); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg := fmt.Sprintf("migrateDataNodeHandler from src [%v] to target[%v] has migrate successfully", srcAddr, targetAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) setNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)

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
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodeinfo params %v successfully", params)))

}
func (m *Server) updateDataUseRatio(ratio float64) (err error) {
	m.cluster.domainManager.Lock()
	defer m.cluster.domainManager.Unlock()

	m.cluster.domainManager.dataRatioLimit = ratio
	err = m.cluster.putZoneDomain(false)
	return
}
func (m *Server) updateExcludeZoneUseRatio(ratio float64) (err error) {
	m.cluster.domainManager.Lock()
	defer m.cluster.domainManager.Unlock()

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

func (m *Server) setNodeRdOnly(addr string, nodeType uint32, rdOnly bool) (err error) {
	if nodeType == TypeDataPartion {
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

func (m *Server) updateNodesetCapcity(zoneName string, nodesetId uint64, capcity int) (err error) {
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
	ns.Lock()
	defer ns.Unlock()

	ns.Capacity = capcity
	m.cluster.syncUpdateNodeSet(ns)
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

	addr, nodeType, rdOnly, err := parseSetNodeRdOnlyParam(r)
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

func (m *Server) updateNodeSetCapacityHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	if params, err = parseAndExtractSetNodeSetInfoParams(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err := m.updateNodesetCapcity(params[zoneNameKey].(string), params[idKey].(uint64), params[countKey].(int)); err == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodesetinfo params %v successfully", params)))
}

func (m *Server) updateDataUseRatioHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
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
	defer func() {
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
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("successful")))
}

// get metanode some interval params
func (m *Server) getAllNodeSetGrpInfoHandler(w http.ResponseWriter, r *http.Request) {
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
	resp := make(map[string]string)
	resp[nodeDeleteBatchCountKey] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteBatchCount)
	resp[nodeMarkDeleteRateKey] = fmt.Sprintf("%v", m.cluster.cfg.DataNodeDeleteLimitRate)
	resp[nodeDeleteWorkerSleepMs] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)
	resp[nodeAutoRepairRateKey] = fmt.Sprintf("%v", m.cluster.cfg.DataNodeAutoRepairLimitRate)
	resp[clusterLoadFactorKey] = fmt.Sprintf("%v", m.cluster.cfg.ClusterLoadFactor)

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
func (m *Server) decommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		node                  *DataNode
		rstMsg                string
		offLineAddr, diskPath string
		err                   error
		badPartitionIds       []uint64
		badPartitions         []*DataPartition
	)

	if offLineAddr, diskPath, err = parseRequestToDecommissionNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	badPartitions = node.badPartitions(diskPath, m.cluster)
	if len(badPartitions) == 0 {
		rstMsg = fmt.Sprintf("receive decommissionDisk node[%v] no any partitions on disk[%v],offline successfully",
			node.Addr, diskPath)
		sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
		return
	}
	for _, bdp := range badPartitions {
		badPartitionIds = append(badPartitionIds, bdp.PartitionID)
	}
	rstMsg = fmt.Sprintf("receive decommissionDisk node[%v] disk[%v], badPartitionIds[%v] has offline successfully",
		node.Addr, diskPath, badPartitionIds)
	if err = m.cluster.decommissionDisk(node, diskPath, badPartitions); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	Warn(m.clusterName, rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// handle tasks such as heartbeat，loadDataPartition，deleteDataPartition, etc.
func (m *Server) handleDataNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
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
	if nodeAddr, zoneName, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIp(nodeAddr) {
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
	nodes := m.cluster.getInvalidIDNodes()
	sendOkReply(w, r, newSuccessHTTPReply(nodes))
}

func (m *Server) updateDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
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

	targetAddr = r.FormValue(targetAddrKey)
	if targetAddr == "" {
		err = fmt.Errorf("parseMigrateNodeParam %s can't be empty when migrate", targetAddrKey)
		return
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
	srcAddr, targetAddr, limit, err := parseMigrateNodeParam(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if limit > defaultMigrateMpCnt {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError,
			Msg: fmt.Sprintf("limit %d can't be bigger than %d", limit, defaultMigrateMpCnt)})
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
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeMetaNodeNotExists, Msg: err.Error()})
		return
	}

	if !targetNode.isWritable() {
		err = fmt.Errorf("[%s] is not writable, can't used as target addr for migrate", targetAddr)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
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
		metaNode    *MetaNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if metaNode, err = m.cluster.metaNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	if err = m.cluster.decommissionMetaNode(metaNode); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommissionMetaNode metaNode [%v] has offline successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) handleMetaNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
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
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
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
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
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

type getVolParameter struct {
	name                string
	authKey             string
	skipOwnerValidation bool
}

func (m *Server) getMetaPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		vol  *Vol
		err  error
	)
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
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	log.LogInfof("action[getDataPartitions] tmp is leader[%v]", m.cluster.partition.IsRaftLeader())
	if !m.cluster.partition.IsRaftLeader() {
		var ok bool
		m.cluster.followerReadManager.rwMutex.RLock()
		if body, ok = m.cluster.followerReadManager.volDataPartitionsView[name]; !ok {
			m.cluster.followerReadManager.rwMutex.RUnlock()
			log.LogErrorf("action[getDataPartitions] volume [%v] not get partitions info", name)
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("follower volume info not found")))
			return
		}
		m.cluster.followerReadManager.rwMutex.RUnlock()
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
	)
	if param, err = parseGetVolParameter(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
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
		err  error
		name string
		ver  int
		vol  *Vol
	)

	if name, ver, err = parseVolStatReq(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if proto.IsCold(vol.VolType) && ver != proto.LFClient {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "low frequency vol is supported by LF client only"})
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(volStat(vol)))
}

func volStat(vol *Vol) (stat *proto.VolStatInfo) {
	stat = new(proto.VolStatInfo)
	stat.Name = vol.Name
	stat.TotalSize = vol.Capacity * util.GB
	stat.UsedSize = vol.totalUsedSpace()
	if stat.UsedSize > stat.TotalSize {
		stat.UsedSize = stat.TotalSize
	}

	stat.UsedRatio = strconv.FormatFloat(float64(stat.UsedSize)/float64(stat.TotalSize), 'f', 2, 32)
	for _, mp := range vol.MetaPartitions {
		stat.InodeCount += mp.InodeCount
	}
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)

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
	mpView.IsRecover = mp.IsRecover
	return
}

func (m *Server) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		partitionID uint64
		mp          *MetaPartition
	)
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
			stat := volStat(vol)
			volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize, stat.UsedSize)
			volsInfo = append(volsInfo, volInfo)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(volsInfo))
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
