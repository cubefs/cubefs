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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/iputil"

	"bytes"
	"io/ioutil"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	Status     bool
	ID         uint64
	IsWritable bool
}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	Zones   []*ZoneView
	Regions []*proto.RegionView
}

type nodeSetView struct {
	DataNodeLen int
	MetaNodeLen int
	MetaNodes   []NodeView
	DataNodes   []NodeView
}

func newNodeSetView(dataNodeLen, metaNodeLen int) *nodeSetView {
	return &nodeSetView{DataNodes: make([]NodeView, 0), MetaNodes: make([]NodeView, 0), DataNodeLen: dataNodeLen, MetaNodeLen: metaNodeLen}
}

//ZoneView define the view of zone
type ZoneView struct {
	Name       string
	Status     string
	Region     string
	MediumType string
	NodeSet    map[uint64]*nodeSetView
}

func newZoneView(name string) *ZoneView {
	return &ZoneView{NodeSet: make(map[uint64]*nodeSetView, 0), Name: name}
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
		Zones:   make([]*ZoneView, 0),
		Regions: m.cluster.t.getRegionViews(),
	}
	defaultRegionView := proto.NewRegionView("default")
	zones := m.cluster.t.getAllZones()
	for _, zone := range zones {
		cv := newZoneView(zone.name)
		cv.Region = zone.regionName
		cv.Status = zone.getStatusToString()
		tv.Zones = append(tv.Zones, cv)
		nsc := zone.getAllNodeSet()
		for _, ns := range nsc {
			nsView := newNodeSetView(ns.dataNodeLen(), ns.metaNodeLen())
			cv.NodeSet[ns.ID] = nsView
			ns.dataNodes.Range(func(key, value interface{}) bool {
				dataNode := value.(*DataNode)
				nsView.DataNodes = append(nsView.DataNodes, NodeView{ID: dataNode.ID, Addr: dataNode.Addr, Status: dataNode.isActive, IsWritable: dataNode.isWriteAble()})
				return true
			})
			ns.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)
				nsView.MetaNodes = append(nsView.MetaNodes, NodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
				return true
			})
		}
		if zone.regionName == "" {
			defaultRegionView.Zones = append(defaultRegionView.Zones, zone.name)
		}
	}
	if len(defaultRegionView.Zones) != 0 {
		tv.Regions = append(tv.Regions, defaultRegionView)
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
		cv.Region = zone.regionName
		cv.MediumType = zone.MType.String()
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
		Name:                   m.cluster.Name,
		LeaderAddr:             m.leaderInfo.addr,
		DisableAutoAlloc:       m.cluster.DisableAutoAllocate,
		AutoMergeNodeSet:       m.cluster.AutoMergeNodeSet,
		NodeSetCapacity:        m.cluster.cfg.nodeSetCapacity,
		MetaNodeThreshold:      m.cluster.cfg.MetaNodeThreshold,
		DpRecoverPool:          m.cluster.cfg.DataPartitionsRecoverPoolSize,
		MpRecoverPool:          m.cluster.cfg.MetaPartitionsRecoverPoolSize,
		ClientPkgAddr:          m.cluster.cfg.ClientPkgAddr,
		Applied:                m.fsm.applied,
		MaxDataPartitionID:     m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:          m.cluster.idAlloc.commonID,
		MaxMetaPartitionID:     m.cluster.idAlloc.metaPartitionID,
		MetaNodes:              make([]proto.NodeView, 0),
		DataNodes:              make([]proto.NodeView, 0),
		VolStatInfo:            make([]*proto.VolStatInfo, 0),
		BadPartitionIDs:        make([]proto.BadPartitionView, 0),
		BadMetaPartitionIDs:    make([]proto.BadPartitionView, 0),
		MigratedDataPartitions: make([]proto.BadPartitionView, 0),
		MigratedMetaPartitions: make([]proto.BadPartitionView, 0),
		DataNodeBadDisks:       make([]proto.DataNodeBadDisksView, 0),
	}

	vols := m.cluster.allVolNames()
	cv.MetaNodes = m.cluster.allMetaNodes()
	cv.DataNodes = m.cluster.allDataNodes()
	cv.DataNodeStatInfo = m.cluster.dataNodeStatInfo
	cv.MetaNodeStatInfo = m.cluster.metaNodeStatInfo
	for _, name := range vols {
		stat, ok := m.cluster.volStatInfo.Load(name)
		if !ok {
			cv.VolStatInfo = append(cv.VolStatInfo, newVolStatInfo(name, 0, 0, "0.0001", false))
			continue
		}
		cv.VolStatInfo = append(cv.VolStatInfo, stat.(*volStatInfo))
	}
	m.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badDataPartitionIds}
		cv.BadPartitionIDs = append(cv.BadPartitionIDs, bpv)
		return true
	})
	m.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badPartitionIds}
		cv.BadMetaPartitionIDs = append(cv.BadMetaPartitionIDs, bpv)
		return true
	})
	m.cluster.MigratedDataPartitionIds.Range(func(key, value interface{}) bool {
		badPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badPartitionIds}
		cv.MigratedDataPartitions = append(cv.MigratedDataPartitions, bpv)
		return true
	})
	m.cluster.MigratedMetaPartitionIds.Range(func(key, value interface{}) bool {
		badPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badPartitionIds}
		cv.MigratedMetaPartitions = append(cv.MigratedMetaPartitions, bpv)
		return true
	})
	cv.DataNodeBadDisks = m.cluster.getDataNodeBadDisks()

	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *Server) getIPAddr(w http.ResponseWriter, r *http.Request) {
	//m.cluster.loadClusterValue()
	cInfo := &proto.ClusterInfo{
		Cluster:              m.cluster.Name,
		Ip:                   strings.Split(r.RemoteAddr, ":")[0],
		ClientReadLimitRate:  m.cluster.cfg.ClientReadVolRateLimitMap[""],
		ClientWriteLimitRate: m.cluster.cfg.ClientWriteVolRateLimitMap[""],
	}
	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}

func (m *Server) getLimitInfo(w http.ResponseWriter, r *http.Request) {
	vol := r.FormValue(nameKey)
	//m.cluster.loadClusterValue()
	batchCount := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteBatchCount)
	deleteLimitRate := atomic.LoadUint64(&m.cluster.cfg.DataNodeDeleteLimitRate)
	repairTaskCount := atomic.LoadUint64(&m.cluster.cfg.DataNodeRepairTaskCount)
	deleteSleepMs := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)
	metaNodeReqRateLimit := atomic.LoadUint64(&m.cluster.cfg.MetaNodeReqRateLimit)
	metaNodeReadDirLimitNum := atomic.LoadUint64(&m.cluster.cfg.MetaNodeReadDirLimitNum)
	m.cluster.cfg.reqRateLimitMapMutex.Lock()
	defer m.cluster.cfg.reqRateLimitMapMutex.Unlock()
	cInfo := &proto.LimitInfo{
		Cluster:                                m.cluster.Name,
		MetaNodeDeleteBatchCount:               batchCount,
		MetaNodeDeleteWorkerSleepMs:            deleteSleepMs,
		MetaNodeReqRateLimit:                   metaNodeReqRateLimit,
		MetaNodeReadDirLimitNum:                metaNodeReadDirLimitNum,
		MetaNodeReqOpRateLimitMap:              m.cluster.cfg.MetaNodeReqOpRateLimitMap,
		DataNodeDeleteLimitRate:                deleteLimitRate,
		DataNodeRepairTaskLimitOnDisk:          repairTaskCount,
		DataNodeRepairTaskCountZoneLimit:       m.cluster.cfg.DataNodeRepairTaskCountZoneLimit,
		DataNodeReqZoneRateLimitMap:            m.cluster.cfg.DataNodeReqZoneRateLimitMap,
		DataNodeReqZoneOpRateLimitMap:          m.cluster.cfg.DataNodeReqZoneOpRateLimitMap,
		DataNodeReqZoneVolOpRateLimitMap:       m.cluster.cfg.DataNodeReqZoneVolOpRateLimitMap,
		DataNodeReqVolPartRateLimitMap:         m.cluster.cfg.DataNodeReqVolPartRateLimitMap,
		DataNodeReqVolOpPartRateLimitMap:       m.cluster.cfg.DataNodeReqVolOpPartRateLimitMap,
		ClientReadVolRateLimitMap:              m.cluster.cfg.ClientReadVolRateLimitMap,
		ClientWriteVolRateLimitMap:             m.cluster.cfg.ClientWriteVolRateLimitMap,
		ClientVolOpRateLimit:                   m.cluster.cfg.ClientVolOpRateLimitMap[vol],
		ExtentMergeIno:                         m.cluster.cfg.ExtentMergeIno,
		ExtentMergeSleepMs:                     m.cluster.cfg.ExtentMergeSleepMs,
		DataNodeFixTinyDeleteRecordLimitOnDisk: m.cluster.dnFixTinyDeleteRecordLimit,
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

func (m *Server) createDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                     string
		volName                    string
		designatedZoneName         string
		vol                        *Vol
		reqCreateCount             int
		lastTotalDataPartitions    int
		clusterTotalDataPartitions int
		err                        error
	)

	if reqCreateCount, volName, designatedZoneName, err = parseRequestToCreateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if designatedZoneName != "" {
		if IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
			if err = m.cluster.validCrossRegionHA(designatedZoneName); err != nil {
				sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
				return
			}
		}
		if err = m.cluster.validZone(designatedZoneName, int(vol.dpReplicaNum), vol.isSmart); err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	}
	lastTotalDataPartitions = len(vol.dataPartitions.partitions)
	clusterTotalDataPartitions = m.cluster.getDataPartitionCount()
	err = m.cluster.batchCreateDataPartition(vol, reqCreateCount, designatedZoneName)
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

	sendOkReply(w, r, newSuccessHTTPReply(dp.ToProto(m.cluster)))
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
		msg                        string
		addr                       string
		dp                         *DataPartition
		partitionID                uint64
		addReplicaType             proto.AddReplicaType
		vol                        *Vol
		totalReplicaNum            int
		isNeedIncreaseDPReplicaNum bool
		err                        error
	)

	if partitionID, addr, addReplicaType, err = parseRequestToAddDataReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	if isAutoChooseAddrForQuorumVol(addReplicaType) {
		if vol, err = m.cluster.getVol(dp.VolName); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
			return
		}
		if !IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("can only auto add replica for quorum vol,vol type:%s", vol.CrossRegionHAType)))
			return
		}
		totalReplicaNum = int(vol.dpReplicaNum)
		isNeedIncreaseDPReplicaNum = true
		if addr, err = dp.chooseTargetDataNodeForCrossRegionQuorumVol(m.cluster, vol.zoneName, totalReplicaNum); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	dp.offlineMutex.Lock()
	defer dp.offlineMutex.Unlock()

	if isAutoChooseAddrForQuorumVol(addReplicaType) && len(dp.Hosts) >= totalReplicaNum {
		err = fmt.Errorf("partition:%v can not add replica for type:%s, replica more than vol replica num", dp.PartitionID, addReplicaType)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.addDataReplica(dp, addr, isNeedIncreaseDPReplicaNum); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	dp.Status = proto.ReadOnly
	dp.isRecover = true
	m.cluster.putBadDataPartitionIDs(nil, addr, dp.PartitionID)
	go m.cluster.syncDataPartitionReplicasToDataNode(dp)

	msg = fmt.Sprintf("data partitionID :%v  add replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) resetDataPartitionHosts(w http.ResponseWriter, r *http.Request) {
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

	hosts := make([]string, 0)
	peers := make([]proto.Peer, 0)
	learners := make([]proto.Learner, 0)

	for _, host := range dp.Hosts {
		if host == addr {
			continue
		}
		hosts = append(hosts, host)
	}
	for _, peer := range dp.Peers {
		if peer.Addr == addr {
			continue
		}
		peers = append(peers, peer)
	}
	for _, learner := range dp.Learners {
		if learner.Addr == addr {
			continue
		}
		learners = append(learners, learner)
	}
	if err = dp.update("resetDataPartitionHosts", dp.VolName, peers, hosts, learners, m.cluster); err != nil {
		return
	}
	msg = fmt.Sprintf("data partitionID :%v  reset hosts [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) deleteDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		partitionID uint64
		isLearner   bool
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
	dp.offlineMutex.Lock()
	defer dp.offlineMutex.Unlock()
	if isLearner, _, err = m.cluster.removeDataReplica(dp, addr, true, false); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("data partitionID: %v  delete replica [%v] successfully, isLearner[%v]", partitionID, addr, isLearner)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addMetaReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg             string
		addr            string
		mp              *MetaPartition
		partitionID     uint64
		addReplicaType  proto.AddReplicaType
		totalReplicaNum int
		storeMode       int
		err             error
	)

	if partitionID, addr, addReplicaType, storeMode, err = parseRequestToAddMetaReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	if isAutoChooseAddrForQuorumVol(addReplicaType) {
		if addr, totalReplicaNum, err = m.cluster.chooseTargetMetaNodeForCrossRegionQuorumVol(mp); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	mp.offlineMutex.Lock()
	defer mp.offlineMutex.Unlock()
	if isAutoChooseAddrForQuorumVol(addReplicaType) && len(mp.Hosts) >= totalReplicaNum {
		err = fmt.Errorf("partition:%v can not add replica for type:%s, replica more than vol replica num", mp.PartitionID, addReplicaType)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if storeMode == 0 {
		storeMode = int(proto.StoreModeMem)
		vol, _ := m.cluster.getVol(mp.volName)
		if vol != nil {
			storeMode = int(vol.DefaultStoreMode)
		}
	}

	if !(storeMode == int(proto.StoreModeMem) || storeMode == int(proto.StoreModeRocksDb)) {
		err = fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, storeMode)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.addMetaReplica(mp, addr, proto.StoreMode(storeMode)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("meta partitionID[%v]  add replica [%v] successfully", partitionID, addr)
	mp.IsRecover = true
	m.cluster.putBadMetaPartitions(addr, mp.PartitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) deleteMetaReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		mp          *MetaPartition
		partitionID uint64
		isLearner   bool
		err         error
	)

	if partitionID, addr, _, err = parseRequestToRemoveMetaReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	mp.offlineMutex.Lock()
	defer mp.offlineMutex.Unlock()
	if isLearner, _, err = m.cluster.deleteMetaReplica(mp, addr, true, false); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("meta partitionID: %v  delete replica [%v] successfully, isLearner[%v]", partitionID, addr, isLearner)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addMetaReplicaLearner(w http.ResponseWriter, r *http.Request) {
	var (
		msg                        string
		addr                       string
		mp                         *MetaPartition
		partitionID                uint64
		auto                       bool
		threshold                  uint8
		addReplicaType             proto.AddReplicaType
		totalReplicaNum            int
		isNeedIncreaseMPLearnerNum bool
		err                        error
		storeMode                  int
	)

	if partitionID, addr, auto, threshold, addReplicaType, storeMode, err = parseRequestToAddMetaReplicaLearner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	if isAutoChooseAddrForQuorumVol(addReplicaType) {
		if addr, totalReplicaNum, err = m.cluster.chooseTargetMetaNodeForCrossRegionQuorumVolOfLearnerReplica(mp); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		auto = false
		threshold = 100
		isNeedIncreaseMPLearnerNum = true
	}

	if storeMode == 0 {
		storeMode = int(proto.StoreModeMem)
		vol, _ := m.cluster.getVol(mp.volName)
		if vol != nil {
			storeMode = int(vol.DefaultStoreMode)
		}
	}

	if !(storeMode == int(proto.StoreModeMem) || storeMode == int(proto.StoreModeRocksDb)) {
		err = fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, storeMode)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	mp.offlineMutex.Lock()
	defer mp.offlineMutex.Unlock()
	if isAutoChooseAddrForQuorumVol(addReplicaType) && len(mp.Hosts) >= totalReplicaNum {
		err = fmt.Errorf("partition:%v can not add replica for type:%s, replica more than vol replica num", mp.PartitionID, addReplicaType)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.addMetaReplicaLearner(mp, addr, auto, threshold, isNeedIncreaseMPLearnerNum, proto.StoreMode(storeMode)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	//mp.IsRecover = true
	//m.cluster.putBadMetaPartitions(addr, mp.PartitionID)
	msg = fmt.Sprintf("meta partitionID[%v] add replica learner[%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) promoteMetaReplicaLearner(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, addr, _, err = parseRequestToPromoteMetaReplicaLearner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	if err = m.cluster.promoteMetaReplicaLearner(mp, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("meta partitionID[%v] promote replica learner[%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addDataReplicaLearner(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		partitionID uint64
		auto        bool
		threshold   uint8
		err         error
	)
	if partitionID, addr, auto, threshold, err = parseRequestToAddDataReplicaLearner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	dp.offlineMutex.Lock()
	defer dp.offlineMutex.Unlock()

	if err = m.cluster.addDataReplicaLearner(dp, addr, auto, threshold); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	dp.Status = proto.ReadOnly
	dp.isRecover = true
	m.cluster.putBadDataPartitionIDs(nil, addr, dp.PartitionID)
	go m.cluster.syncDataPartitionReplicasToDataNode(dp)

	msg = fmt.Sprintf("data partitionID[%v] add replica learner[%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) promoteDataReplicaLearner(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, addr, err = parseRequestToPromoteDataReplicaLearner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	if err = m.cluster.promoteDataReplicaLearner(dp, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("data partitionID[%v] promote replica learner[%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Decommission a data partition. This usually happens when disk error has been reported.
// This function needs to be called manually by the admin.
func (m *Server) decommissionDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		dp          *DataPartition
		addr        string
		destAddr    string
		partitionID uint64
		err         error
		vol         *Vol
		chooseFunc  ChooseDataHostFunc
	)

	if partitionID, addr, destAddr, err = parseRequestToDecommissionDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	vol, err = m.cluster.getVol(dp.VolName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if vol.isSmart && dp.isFrozen() {
		chooseFunc = getTargetAddressForDataPartitionSmartTransfer
	} else {
		chooseFunc = getTargetAddressForDataPartitionDecommission
	}
	if err = m.cluster.decommissionDataPartition(addr, dp, chooseFunc, handleDataPartitionOfflineErr, "", destAddr, false); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf(proto.AdminDecommissionDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) transferDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		dp          *DataPartition
		addr        string
		partitionID uint64
		err         error
		vol         *Vol
	)

	if partitionID, addr, _, err = parseRequestToDecommissionDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	vol, err = m.cluster.getVol(dp.VolName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if dp.Status != proto.ReadOnly &&
		vol.getWritableDataPartitionsCount() < vol.MinWritableDPNum {
		sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("too less writable data partitions: %v", vol.getWritableDataPartitionsCount())))
		return
	}
	if err = m.cluster.decommissionDataPartition(addr, dp, getTargetAddressForDataPartitionSmartTransfer, handleDataPartitionOfflineErr, "", "", false); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf(proto.AdminTransferDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) setNodeToOfflineState(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		startID  uint64
		endID    uint64
		nodeType string
		zoneName string
		state    bool
	)
	if startID, endID, nodeType, zoneName, state, err = parseRequestToSetNodeToOfflineState(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if nodeType == nodeTypeAll {
		m.cluster.setDataNodeToOfflineState(startID, endID, state, zoneName)
		m.cluster.setMetaNodeToOfflineState(startID, endID, state, zoneName)
	} else {
		if nodeType == nodeTypeDataNode {
			m.cluster.setDataNodeToOfflineState(startID, endID, state, zoneName)
		} else {
			m.cluster.setMetaNodeToOfflineState(startID, endID, state, zoneName)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func parseRequestToSetNodeToOfflineState(r *http.Request) (startID, endID uint64, nodeType, zoneName string, state bool, err error) {
	var value string
	if value = r.FormValue(startKey); value == "" {
		err = keyNotFound(startKey)
		return
	}
	startID, err = strconv.ParseUint(value, 10, 64)
	if err != nil {
		return
	}
	if value = r.FormValue(endKey); value == "" {
		err = keyNotFound(endKey)
		return
	}
	endID, err = strconv.ParseUint(value, 10, 64)
	if err != nil {
		return
	}
	nodeType = r.FormValue(nodeTypeKey)
	if !(nodeType == nodeTypeDataNode || nodeType == nodeTypeMetaNode || nodeType == nodeTypeAll) {
		err = fmt.Errorf("nodeType must be dataNode or metaNode or all")
		return
	}
	if zoneName, err = extractZoneName(r); err != nil {
		return
	}
	state, err = strconv.ParseBool(r.FormValue(stateKey))
	return
}

func (m *Server) setupAutoMergeNodeSet(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	if status, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	m.cluster.AutoMergeNodeSet = status
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set setupAutoMergeNodeSet to %v successfully", status)))
	if m.cluster.AutoMergeNodeSet {
		m.cluster.checkMergeZoneNodeset()
	}
}

func (m *Server) mergeNodeSet(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		zoneName   string
		nodeType   string
		sourceID   uint64
		targetID   uint64
		nodeAddr   string
		count      int
		successNum int
	)
	if zoneName, nodeType, nodeAddr, sourceID, targetID, count, err = parseRequestToMergeNodeSet(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if nodeType == nodeTypeDataNode {
		for i := 0; i < count; i++ {
			if err = m.cluster.adjustNodeSetForDataNode(zoneName, nodeAddr, sourceID, targetID); err != nil {
				break
			}
			successNum++
		}
	} else if nodeType == nodeTypeMetaNode {
		for i := 0; i < count; i++ {
			if err = m.cluster.adjustNodeSetForMetaNode(zoneName, nodeAddr, sourceID, targetID); err != nil {
				break
			}
			successNum++
		}
	}
	msg := fmt.Sprintf("type[%v], sourceID[%v], targetID[%v] success num[%v]", nodeType, sourceID, targetID, successNum)
	if err != nil {
		msg = fmt.Sprintf("type[%v], sourceID[%v], targetID[%v] success num[%v] err[%v]", nodeType, sourceID, targetID, successNum, err)
	}
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func parseRequestToMergeNodeSet(r *http.Request) (zoneName, nodeType, nodeAddr string, sourceID, targetID uint64, count int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if zoneName, err = extractZoneName(r); err != nil {
		return
	}
	nodeType = r.FormValue(nodeTypeKey)
	if !(nodeType == nodeTypeDataNode || nodeType == nodeTypeMetaNode) {
		err = fmt.Errorf("nodeType must be dataNode or metaNode ")
		return
	}
	nodeAddr = r.FormValue(addrKey)

	var value string
	if value = r.FormValue(sourceKey); value == "" {
		err = keyNotFound(sourceKey)
		return
	}
	if sourceID, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}
	if value = r.FormValue(targetKey); value == "" {
		err = keyNotFound(targetKey)
		return
	}
	if targetID, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}
	if nodeAddr != "" {
		count = 1
		return
	}
	if value = r.FormValue(countKey); value == "" && nodeAddr == "" {
		err = keyNotFound(countKey)
		return
	}
	if count, err = strconv.Atoi(value); err != nil || count <= 0 {
		err = fmt.Errorf("count should more than 0 ")
		return
	}
	return
}

func (m *Server) diagnoseDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err              error
		rstMsg           *proto.DataPartitionDiagnosis
		inactiveNodes    []string
		corruptDps       []*DataPartition
		lackReplicaDps   []*DataPartition
		corruptDpIDs     []uint64
		lackReplicaDpIDs []uint64
	)
	if inactiveNodes, corruptDps, err = m.cluster.checkCorruptDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}

	if lackReplicaDps, err = m.cluster.checkLackReplicaDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	for _, dp := range corruptDps {
		corruptDpIDs = append(corruptDpIDs, dp.PartitionID)
	}
	for _, dp := range lackReplicaDps {
		lackReplicaDpIDs = append(lackReplicaDpIDs, dp.PartitionID)
	}
	rstMsg = &proto.DataPartitionDiagnosis{
		InactiveDataNodes:           inactiveNodes,
		CorruptDataPartitionIDs:     corruptDpIDs,
		LackReplicaDataPartitionIDs: lackReplicaDpIDs,
	}
	log.LogInfof("diagnose dataPartition[%v] inactiveNodes:[%v], corruptDpIDs:[%v], lackReplicaDpIDs:[%v]", m.cluster.Name, inactiveNodes, corruptDpIDs, lackReplicaDpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) resetDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		dp          *DataPartition
		partitionID uint64
		rstMsg      string
		panicHosts  []string
		err         error
	)
	if partitionID, err = parseRequestToResetDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	if panicHosts, err = m.getPanicHostsInDataPartition(dp); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.resetDataPartition(dp, panicHosts); err != nil {
		msg := fmt.Sprintf("resetDataPartition[%v] failed, err[%v]", dp.PartitionID, err)
		log.LogErrorf(msg)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg = fmt.Sprintf(proto.AdminResetDataPartition+" dataPartitionID :%v successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}
func (m *Server) getPanicHostsInDataPartition(dp *DataPartition) (panicHosts []string, err error) {
	for _, host := range dp.Hosts {
		var dataNode *DataNode
		if dataNode, err = m.cluster.dataNode(host); err != nil {
			err = proto.ErrDataNodeNotExists
			return
		}
		if !dataNode.isActive {
			panicHosts = append(panicHosts, host)
		}
	}
	//Todo: maybe replaced by actual data replica number
	if uint8(len(panicHosts)) < dp.ReplicaNum/2+dp.ReplicaNum%2 {
		err = proto.ErrBadReplicaNoMoreThanHalf
		return
	}
	if uint8(len(panicHosts)) >= dp.ReplicaNum {
		err = proto.ErrNoLiveReplicas
		return
	}
	return
}
func (m *Server) manualResetDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		dp          *DataPartition
		partitionID uint64
		rstMsg      string
		nodeAddrs   string
		panicHosts  []string
		err         error
	)
	panicHosts = make([]string, 0)
	if nodeAddrs, partitionID, err = parseRequestToManualResetDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	nodes := strings.Split(nodeAddrs, ",")
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	//validate nodeAddrs
	for _, node := range nodes {
		if _, err = m.cluster.dataNode(node); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
			return
		}
		if !contains(dp.Hosts, node) {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("host not exist in data partition")))
			return
		}
	}
	for _, host := range dp.Hosts {
		if !contains(nodes, host) {
			panicHosts = append(panicHosts, host)
		}
	}
	if err = m.cluster.resetDataPartition(dp, panicHosts); err != nil {
		msg := fmt.Sprintf("resetDataPartition[%v] failed, err[%v]", dp.PartitionID, err)
		log.LogErrorf(msg)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rstMsg = fmt.Sprintf(proto.AdminManualResetDataPartition+" dataPartitionID :%v to %v successfully", partitionID, nodeAddrs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) updateDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		vol         *Vol
		dp          *DataPartition
		volName     string
		partitionID uint64
		isManual    bool
		err         error
	)
	if partitionID, volName, isManual, err = parseUpdateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if volName != "" {
		if vol, err = m.cluster.getVol(volName); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
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

	if err = m.cluster.updateDataPartition(dp, isManual); err != nil {
		msg := fmt.Sprintf("updateDataPartition[%v] failed, err[%v]", dp.PartitionID, err)
		log.LogErrorf(msg)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg := fmt.Sprintf("updateDataPartition[%v] to isManual[%v] successfully", partitionID, isManual)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) batchUpdateDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		vol            *Vol
		volName        string
		mediumType     string
		isManual       bool
		startID        uint64
		endID          uint64
		count          int
		err            error
		dataPartitions []*DataPartition
		msg            string
	)
	if volName, mediumType, isManual, count, startID, endID, err = parseBatchUpdateDataPartitions(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if count > 0 {
		dataPartitions, err = vol.getRWDataPartitionsOfGivenCount(count, mediumType, m.cluster)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		msg = fmt.Sprintf("batchUpdateDataPartitions to isManual[%v] count[%v] mediumType[%v] ", isManual, count, mediumType)
	} else {
		dataPartitions, err = vol.getDataPartitionsFromStartIDToEndID(startID, endID, mediumType, m.cluster)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		msg = fmt.Sprintf("batchUpdateDataPartitions to isManual[%v] startID[%v], endID[%v] mediumType[%v] ", isManual, startID, endID, mediumType)
	}
	successDpIDs, err := m.cluster.batchUpdateDataPartitions(dataPartitions, isManual)
	if err != nil {
		msg += fmt.Sprintf("successDpIDs[%v] err:%v", successDpIDs, err)
		log.LogErrorf(msg)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	msg += fmt.Sprintf("successDpIDs[%v].", successDpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Mark the volume as deleted, which will then be deleted later.
func (m *Server) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		err     error
		msg     string
	)

	if name, authKey, err = parseRequestToDeleteVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.markDeleteVol(name, authKey); err != nil {
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
		name                 string
		authKey              string
		err                  error
		msg                  string
		capacity             int
		replicaNum           int
		mpReplicaNum         int
		followerRead         bool
		nearRead             bool
		forceROW             bool
		authenticate         bool
		enableToken          bool
		autoRepair           bool
		zoneName             string
		description          string
		extentCacheExpireSec int64

		vol *Vol

		dpSelectorName       string
		dpSelectorParm       string
		dpWriteableThreshold float64
		ossBucketPolicy      proto.BucketAccessPolicy
		crossRegionHAType    proto.CrossRegionHAType
		trashRemainingDays   uint32
		storeMode            int
		mpLayout             proto.MetaPartitionLayout
		isSmart              bool
		smartRules           []string
	)
	if name, authKey, replicaNum, mpReplicaNum, err = parseRequestToUpdateVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if replicaNum != 0 && !(replicaNum == 2 || replicaNum == 3 || replicaNum == 5) {
		err = fmt.Errorf("replicaNum can only be 2, 3 or 5, received replicaNum is[%v]", replicaNum)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mpReplicaNum != 0 && !(mpReplicaNum == 2 || mpReplicaNum == 3 || mpReplicaNum == 5) {
		err = fmt.Errorf("mpReplicaNum can only be 2, 3 or 5, received mpReplicaNum is[%v]", mpReplicaNum)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	if zoneName, capacity, storeMode, description, mpLayout, extentCacheExpireSec, err = parseDefaultInfoToUpdateVol(r, vol); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if replicaNum == 0 {
		replicaNum = int(vol.dpReplicaNum)
	}
	if mpReplicaNum == 0 {
		mpReplicaNum = int(vol.mpReplicaNum)
	}
	if followerRead, nearRead, authenticate, enableToken, autoRepair, forceROW, err = parseBoolFieldToUpdateVol(r, vol); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	dpSelectorName, dpSelectorParm, err = parseDefaultSelectorToUpdateVol(r, vol)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	ossBucketPolicy, err = parseOSSBucketPolicyToUpdateVol(r, vol)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	dpWriteableThreshold, err = parseDpWriteableThresholdToUpdateVol(r, vol)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	crossRegionHAType, err = parseCrossRegionHATypeToUpdateVol(r, vol)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	trashRemainingDays, err = parseDefaultTrashDaysToUpdateVol(r, vol)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if !(storeMode == int(proto.StoreModeMem) || storeMode == int(proto.StoreModeRocksDb)) {
		err = fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, storeMode)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mpLayout.PercentOfMP > 100 || mpLayout.PercentOfReplica > 100 || mpLayout.PercentOfMP < 0 || mpLayout.PercentOfReplica < 0 {
		err = fmt.Errorf("mpPercent repPercent can only be [0-100],received is[%v - %v]", mpLayout.PercentOfMP, mpLayout.PercentOfReplica)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	isSmart, smartRules, err = parseSmartToUpdateVol(r, vol)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.updateVol(name, authKey, zoneName, description, uint64(capacity), uint8(replicaNum), uint8(mpReplicaNum),
		followerRead, nearRead, authenticate, enableToken, autoRepair, forceROW, isSmart, dpSelectorName, dpSelectorParm, ossBucketPolicy,
		crossRegionHAType, dpWriteableThreshold, trashRemainingDays, proto.StoreMode(storeMode), mpLayout, extentCacheExpireSec, smartRules); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update vol[%v] successfully", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) setVolConvertTaskState(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		newState int
		name     string
		authKey  string
		msg      string
	)

	if name, authKey, newState, err = parseRequestToSetVolConvertSt(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if newState < int(proto.VolConvertStInit) || newState > int(proto.VolConvertStFinished) {
		err = fmt.Errorf("unknown state:%d", newState)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.setVolConvertTaskState(name, authKey, proto.VolConvertState(newState)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("Vol[%v] convert task state change to be [%v] successfully\n", name, newState)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) createVol(w http.ResponseWriter, r *http.Request) {
	var (
		name                 string
		owner                string
		err                  error
		msg                  string
		size                 int
		mpCount              int
		dpReplicaNum         int
		mpReplicaNum         int
		capacity             int
		vol                  *Vol
		followerRead         bool
		authenticate         bool
		enableToken          bool
		autoRepair           bool
		volWriteMutexEnable  bool
		forceROW             bool
		crossRegionHAType    proto.CrossRegionHAType
		zoneName             string
		description          string
		dpWriteableThreshold float64
		trashDays            int
		storeMode            int
		mpLayout             proto.MetaPartitionLayout
		isSmart              bool
		smartRules           []string
	)

	if name, owner, zoneName, description, mpCount, dpReplicaNum, mpReplicaNum, size, capacity, storeMode, trashDays, followerRead, authenticate,
		enableToken, autoRepair, volWriteMutexEnable, forceROW, isSmart, crossRegionHAType, dpWriteableThreshold, mpLayout, smartRules, err = parseRequestToCreateVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !(dpReplicaNum == 2 || dpReplicaNum == 3 || dpReplicaNum == 5) {
		err = fmt.Errorf("dp replicaNum can only be 2 or 3 or 5,received replicaNum is[%v]", dpReplicaNum)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if !(mpReplicaNum == 3 || mpReplicaNum == 5) {
		err = fmt.Errorf("mp replicaNum can only be 3 or 5,received mp replicaNum is[%v]", mpReplicaNum)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if !(storeMode == int(proto.StoreModeMem) || storeMode == int(proto.StoreModeRocksDb)) {
		err = fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, storeMode)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mpLayout.PercentOfMP > 100 || mpLayout.PercentOfReplica > 100 || mpLayout.PercentOfMP < 0 || mpLayout.PercentOfReplica < 0 {
		err = fmt.Errorf("mpPercent repPercent can only be [0-100],received is[%v - %v]", mpLayout.PercentOfMP, mpLayout.PercentOfReplica)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.createVol(name, owner, zoneName, description, mpCount, dpReplicaNum, mpReplicaNum, size,
		capacity, trashDays, followerRead, authenticate, enableToken, autoRepair, volWriteMutexEnable, forceROW, isSmart,
		crossRegionHAType, dpWriteableThreshold, proto.StoreMode(storeMode), mpLayout, smartRules); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.associateVolWithUser(owner, name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("create vol[%v] successfully, has allocate [%v] data partitions", name, len(vol.dataPartitions.partitions))
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
	if IsCrossRegionHATypeQuorum(vol.CrossRegionHAType) {
		if masterRegionZoneName, slaveRegionZone, err := m.cluster.getMasterAndSlaveRegionZoneName(vol.zoneName); err == nil {
			volView.MasterRegionZone = convertSliceToVolZoneName(masterRegionZoneName)
			volView.SlaveRegionZone = convertSliceToVolZoneName(slaveRegionZone)
		}
	}
	log.LogInfof("view vol convert state %v, mem vol convert st:%v", volView.ConvertState, vol.convertState)
	sendOkReply(w, r, newSuccessHTTPReply(volView))
}

func newSimpleView(vol *Vol) *proto.SimpleVolView {
	var (
		volInodeCount  uint64
		volDentryCount uint64
		usedRatio      float64
		fileAvgSize    float64
	)
	for _, mp := range vol.MetaPartitions {
		volDentryCount = volDentryCount + mp.DentryCount
		volInodeCount = volInodeCount + mp.InodeCount
	}
	stat := volStat(vol)
	if stat.TotalSize > 0 {
		usedRatio = float64(stat.UsedSize) / float64(stat.TotalSize)
	}
	if volInodeCount > 0 {
		fileAvgSize = float64(stat.UsedSize) / float64(volInodeCount)
	}
	maxPartitionID := vol.maxPartitionID()
	return &proto.SimpleVolView{
		ID:                   vol.ID,
		Name:                 vol.Name,
		Owner:                vol.Owner,
		ZoneName:             vol.zoneName,
		DpReplicaNum:         vol.dpReplicaNum,
		MpReplicaNum:         vol.mpReplicaNum,
		DpLearnerNum:         vol.dpLearnerNum,
		MpLearnerNum:         vol.mpLearnerNum,
		InodeCount:           volInodeCount,
		DentryCount:          volDentryCount,
		MaxMetaPartitionID:   maxPartitionID,
		Status:               vol.Status,
		Capacity:             vol.Capacity,
		FollowerRead:         vol.FollowerRead,
		NearRead:             vol.NearRead,
		ForceROW:             vol.ForceROW,
		CrossRegionHAType:    vol.CrossRegionHAType,
		NeedToLowerReplica:   vol.NeedToLowerReplica,
		Authenticate:         vol.authenticate,
		EnableToken:          vol.enableToken,
		CrossZone:            vol.crossZone,
		AutoRepair:           vol.autoRepair,
		VolWriteMutexEnable:  vol.volWriteMutexEnable,
		Tokens:               vol.tokens,
		RwDpCnt:              vol.dataPartitions.readableAndWritableCnt,
		MpCnt:                len(vol.MetaPartitions),
		DpCnt:                len(vol.dataPartitions.partitionMap),
		CreateTime:           time.Unix(vol.createTime, 0).Format(proto.TimeFormat),
		Description:          vol.description,
		DpSelectorName:       vol.dpSelectorName,
		DpSelectorParm:       vol.dpSelectorParm,
		OSSBucketPolicy:      vol.OSSBucketPolicy,
		DPConvertMode:        vol.DPConvertMode,
		MPConvertMode:        vol.MPConvertMode,
		Quorum:               vol.getDataPartitionQuorum(),
		DpWriteableThreshold: vol.dpWriteableThreshold,
		ExtentCacheExpireSec: vol.ExtentCacheExpireSec,
		RwMpCnt:              int(vol.getWritableMpCount()),
		MinWritableMPNum:     vol.MinWritableMPNum,
		MinWritableDPNum:     vol.MinWritableDPNum,
		TrashRemainingDays:   vol.trashRemainingDays,
		DefaultStoreMode:     vol.DefaultStoreMode,
		ConvertState:         vol.convertState,
		MpLayout:             vol.MpLayout,
		IsSmart:              vol.isSmart,
		SmartEnableTime:           time.Unix(vol.smartEnableTime, 0).Format(proto.TimeFormat),
		SmartRules:           vol.smartRules,
		TotalSize:            stat.TotalSize,
		UsedSize:             stat.UsedSize,
		UsedRatio:            usedRatio,
		FileAvgSize:          fileAvgSize,
		CreateStatus:         vol.CreateStatus,
	}
}

func (m *Server) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		zoneName string
		id       uint64
		version  string
		err      error
	)
	if nodeAddr, zoneName, version, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if id, err = m.cluster.addDataNode(nodeAddr, zoneName, version); err != nil {
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
		UsageRatio:                dataNode.UsageRatio,
		SelectedTimes:             dataNode.SelectedTimes,
		Carry:                     dataNode.Carry,
		DataPartitionReports:      dataNode.DataPartitionReports,
		DataPartitionCount:        dataNode.DataPartitionCount,
		NodeSetID:                 dataNode.NodeSetID,
		PersistenceDataPartitions: dataNode.PersistenceDataPartitions,
		BadDisks:                  dataNode.BadDisks,
		ToBeOffline:               dataNode.ToBeOffline,
		ToBeMigrated:              dataNode.ToBeMigrated,
		Version:                   dataNode.Version,
	}

	sendOkReply(w, r, newSuccessHTTPReply(dataNodeInfo))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) decommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		node         *DataNode
		rstMsg       string
		offLineAddr  string
		destZoneName string
		strictFlag   bool
		err          error
	)

	if offLineAddr, destZoneName, err = parseRequestForDecommissionDataNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}

	if strictFlag, err = extractStrictFlag(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.decommissionDataNode(node, destZoneName, strictFlag); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission data node [%v] successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) resetCorruptDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg         string
		err            error
		resetAddr      string
		node           *DataNode
		corruptDps     []*DataPartition
		panicHostsList [][]string
	)

	if resetAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if node, err = m.cluster.dataNode(resetAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if node.isActive {
		err = errors.NewErrorf("can not reset active node")
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if corruptDps, panicHostsList, err = m.cluster.checkCorruptDataNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	for i, dp := range corruptDps {
		if err = m.cluster.resetDataPartition(dp, panicHostsList[i]); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	rstMsg = fmt.Sprintf(proto.AdminResetCorruptDataNode+"successfully, node:[%v] count:[%v]", node.Addr, len(corruptDps))
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

	err = m.cluster.setClusterConfig(params)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	var (
		zone string
		vol  string
		op   uint8
	)
	if val, ok := params[zoneNameKey]; ok {
		zone = val.(string)
		if zone != "" {
			err = proto.ErrZoneNotExists
			for _, z := range m.cluster.t.getAllZones() {
				if zone == z.name {
					err = nil
					break
				}
			}
			if err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[volumeKey]; ok {
		vol = val.(string)
		if vol != "" {
			if _, err = m.cluster.getVol(vol); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[opcodeKey]; ok {
		op = uint8(val.(uint64))
	}

	if val, ok := params[dataNodeRepairTaskCntZoneKey]; ok {
		v := val.(uint64)
		if err = m.cluster.setDataNodeRepairTaskCountZoneLimit(v, zone); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[dataNodeReqRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", dataNodeReqRateKey, minRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setDataNodeReqRateLimit(v, zone); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[dataNodeReqVolOpRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", dataNodeReqVolOpRateKey, minRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setDataNodeReqVolOpRateLimit(v, zone, vol, op); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[dataNodeReqOpRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", dataNodeReqOpRateKey, minRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setDataNodeReqOpRateLimit(v, zone, op); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[dataNodeReqVolPartRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minPartRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", dataNodeReqVolPartRateKey, minPartRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setDataNodeReqVolPartRateLimit(v, vol); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[dataNodeReqVolOpPartRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minPartRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", dataNodeReqVolOpPartRateKey, minPartRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setDataNodeReqVolOpPartRateLimit(v, vol, op); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[metaNodeReqOpRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", metaNodeReqOpRateKey, minRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setMetaNodeReqOpRateLimit(v, op); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[clientReadVolRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", clientReadVolRateKey, minRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setClientReadVolRateLimit(v, vol); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[clientWriteVolRateKey]; ok {
		v := val.(uint64)
		if v > 0 && v < minRateLimit {
			err = errors.NewErrorf("parameter %s can't be less than %d", clientWriteVolRateKey, minRateLimit)
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
		if err = m.cluster.setClientWriteVolRateLimit(v, vol); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[clientVolOpRateKey]; ok {
		v := val.(int64)
		if op <= 0 || op > 255 {
			err = errors.NewErrorf("value range of parameter %v is 0~255", opcodeKey)
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		if err = m.cluster.setClientVolOpRateLimit(v, vol, uint8(op)); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	if val, ok := params[extentMergeInoKey]; ok {
		if err = m.cluster.setExtentMergeIno(val.(string), vol); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	if val, ok := params[fixTinyDeleteRecordKey]; ok {
		if err = m.cluster.setFixTinyDeleteRecord(val.(uint64)); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodeinfo params %v successfully", params)))
}

// get metanode some interval params
func (m *Server) getNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	resp := make(map[string]string)
	resp[nodeDeleteBatchCountKey] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteBatchCount)
	resp[nodeMarkDeleteRateKey] = fmt.Sprintf("%v", m.cluster.cfg.DataNodeDeleteLimitRate)
	resp[nodeDeleteWorkerSleepMs] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", resp)))
}

func (m *Server) diagnoseMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err              error
		rstMsg           *proto.MetaPartitionDiagnosis
		inactiveNodes    []string
		corruptMps       []*MetaPartition
		lackReplicaMps   []*MetaPartition
		corruptMpIDs     []uint64
		lackReplicaMpIDs []uint64
	)
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
	rstMsg = &proto.MetaPartitionDiagnosis{
		InactiveMetaNodes:           inactiveNodes,
		CorruptMetaPartitionIDs:     corruptMpIDs,
		LackReplicaMetaPartitionIDs: lackReplicaMpIDs,
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
		auto                  bool
		err                   error
		badPartitionIds       []uint64
		badPartitions         []*DataPartition
	)

	if offLineAddr, diskPath, err = parseRequestToDecommissionNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	auto = extractAuto(r)
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
	if auto {
		go m.cluster.checkDecommissionBadDiskDataPartitions(node, diskPath)
		rstMsg = fmt.Sprintf("receive decommissionDisk node[%v] disk[%v], badPartitionIds[%v] will be offline in background",
			node.Addr, diskPath, badPartitionIds)
		sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
		return
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
		nodeAddr string
		zoneName string
		version  string
		id       uint64
		err      error
	)
	if nodeAddr, zoneName, version, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if id, err = m.cluster.addMetaNode(nodeAddr, zoneName, version); err != nil {
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
		ToBeOffline:               metaNode.ToBeOffline,
		ToBeMigrated:              metaNode.ToBeMigrated,
		ProfPort:                  metaNode.ProfPort,
		Version:                   metaNode.Version,
	}
	sendOkReply(w, r, newSuccessHTTPReply(metaNodeInfo))
}

func (m *Server) decommissionMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		nodeAddr    string
		destAddr    string
		mp          *MetaPartition
		msg         string
		storeMode   int
		err         error
	)
	if partitionID, nodeAddr, destAddr, storeMode, err = parseRequestToDecommissionMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	if !(storeMode == int(proto.StoreModeMem) || storeMode == int(proto.StoreModeRocksDb) || storeMode == int(proto.StoreModeDef)) {
		err = fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, storeMode)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.decommissionMetaPartition(nodeAddr, mp, getTargetAddressForMetaPartitionDecommission, destAddr, false, proto.StoreMode(storeMode)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf(proto.AdminDecommissionMetaPartition+" partitionID :%v  decommissionMetaPartition successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) selectMetaReplaceNodeAddr(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		nodeAddr    string
		destAddr    string
		mp          *MetaPartition
		storeMode   int
		err         error
	)

	if partitionID, nodeAddr, storeMode, err = parseRequestToSelectMetaReplace(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	if !(storeMode == int(proto.StoreModeMem) || storeMode == int(proto.StoreModeRocksDb) || storeMode == int(proto.StoreModeDef)) {
		err = fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, storeMode)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if destAddr, err = m.cluster.selectMetaReplaceAddr(nodeAddr, mp, proto.StoreMode(storeMode)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	rsp := proto.SelectMetaNodeInfo{
		PartitionID: partitionID,
		OldNodeAddr: nodeAddr,
		NewNodeAddr: destAddr,
	}

	sendOkReply(w, r, newSuccessHTTPReply(rsp))
	return
}

func (m *Server) resetMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		mp          *MetaPartition
		partitionID uint64
		rstMsg      string
		panicHosts  []string
		err         error
	)
	if partitionID, err = parseRequestToReplicateMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	if panicHosts, err = m.getPanicHostsInMetaPartition(mp); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.cluster.resetMetaPartition(mp, panicHosts); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf(proto.AdminResetMetaPartition+" metaPartitionID :%v successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) getPanicHostsInMetaPartition(mp *MetaPartition) (panicHosts []string, err error) {
	learnerHosts := mp.getLearnerHosts()
	for _, host := range mp.Hosts {
		if contains(learnerHosts, host) {
			continue
		}
		var metaNode *MetaNode
		if metaNode, err = m.cluster.metaNode(host); err != nil {
			err = proto.ErrMetaNodeNotExists
			return
		}
		if !metaNode.IsActive {
			panicHosts = append(panicHosts, host)
		}
	}
	if uint8(len(panicHosts)) <= mp.ReplicaNum/2 {
		err = proto.ErrBadReplicaNoMoreThanHalf
		return
	}
	if uint8(len(panicHosts)) >= mp.ReplicaNum {
		err = proto.ErrNoLiveReplicas
		return
	}
	return
}
func (m *Server) manualResetMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		mp          *MetaPartition
		partitionID uint64
		rstMsg      string
		nodeAddrs   string
		panicHosts  []string
		err         error
	)
	panicHosts = make([]string, 0)
	if nodeAddrs, partitionID, err = parseRequestToManualResetMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	nodes := strings.Split(nodeAddrs, ",")

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	//validate nodeAddrs
	for _, node := range nodes {
		if _, err = m.cluster.metaNode(node); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
			return
		}
		if !contains(mp.Hosts, node) {
			sendErrReply(w, r, newErrHTTPReply(fmt.Errorf("host not exist in meta partition")))
			return
		}
	}
	for _, host := range mp.Hosts {
		if !contains(nodes, host) {
			panicHosts = append(panicHosts, host)
		}
	}
	if err = m.cluster.resetMetaPartition(mp, panicHosts); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf(proto.AdminResetMetaPartition+" metaPartitionID :%v successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
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

func (m *Server) decommissionMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode    *MetaNode
		rstMsg      string
		offLineAddr string
		strictFlag  bool
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

	if strictFlag, err = extractStrictFlag(r); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.cluster.decommissionMetaNode(metaNode, strictFlag); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommissionMetaNode metaNode [%v] has offline successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) resetCorruptMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		node           *MetaNode
		addr           string
		rstMsg         string
		corruptMps     []*MetaPartition
		panicHostsList [][]string
		err            error
	)
	if addr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.metaNode(addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	if node.IsActive {
		err = errors.NewErrorf("can not reset active node")
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if corruptMps, panicHostsList, err = m.cluster.checkCorruptMetaNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	for i, mp := range corruptMps {
		if err = m.cluster.resetMetaPartition(mp, panicHostsList[i]); err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	rstMsg = fmt.Sprintf(proto.AdminResetCorruptMetaNode+" node :%v successfully", node.Addr)
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

// Parse the request that adds/deletes a raft node.
func parseRequestForRaftNode(r *http.Request) (id uint64, host string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var idStr string
	if idStr = r.FormValue(idKey); idStr == "" {
		err = keyNotFound(idKey)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue(addrKey); host == "" {
		err = keyNotFound(addrKey)
		return
	}

	if arr := strings.Split(host, colonSplit); len(arr) < 2 {
		err = unmatchedKey(addrKey)
		return
	}
	return
}

func parseRequestForUpdateMetaNode(r *http.Request) (nodeAddr string, id uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if id, err = extractNodeID(r); err != nil {
		return
	}
	return
}

func parseRequestForAddNode(r *http.Request) (nodeAddr, zoneName, version string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}

	if versionStr := r.FormValue(versionKey); versionStr == "" {
		version = defaultMetaNodeVersion
	}
	return
}

func parseRequestForDecommissionDataNode(r *http.Request) (nodeAddr, zoneName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	zoneName = r.FormValue(zoneNameKey)
	return
}

func parseAndExtractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractNodeAddr(r)
}

func extractStrictFlag(r *http.Request) (strict bool, err error) {
	var strictStr string
	if strictStr = r.FormValue(strictFlagKey); strictStr == "" {
		strictStr = "false"
		return
	}
	return strconv.ParseBool(strictStr)
}

func parseRequestToDecommissionNode(r *http.Request) (nodeAddr, diskPath string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = extractDiskPath(r)
	return
}

func parseRequestToGetTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(body)))
	decoder.UseNumber()
	err = decoder.Decode(tr)
	return
}

func parseVolName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	return
}

type getVolParameter struct {
	name                string
	authKey             string
	skipOwnerValidation bool
}

func parseGetVolParameter(r *http.Request) (p *getVolParameter, err error) {
	p = &getVolParameter{}
	skipOwnerValidationVal := r.Header.Get(proto.SkipOwnerValidation)
	if len(skipOwnerValidationVal) > 0 {
		if p.skipOwnerValidation, err = strconv.ParseBool(skipOwnerValidationVal); err != nil {
			return
		}
	}
	if p.name = r.FormValue(nameKey); p.name == "" {
		err = keyNotFound(nameKey)
		return
	}
	if !volNameRegexp.MatchString(p.name) {
		err = errors.New("name can only be number and letters")
		return
	}
	if p.authKey = r.FormValue(volAuthKey); !p.skipOwnerValidation && len(p.authKey) == 0 {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func parseVolNameAndAuthKey(r *http.Request) (name, authKey string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	return

}

func parseRequestToDeleteVol(r *http.Request) (name, authKey string, err error) {
	return parseVolNameAndAuthKey(r)

}

func parseRequestToUpdateVol(r *http.Request) (name, authKey string, replicaNum, mpReplicaNum int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	if replicaNumStr := r.FormValue(replicaNumKey); replicaNumStr != "" {
		if replicaNum, err = strconv.Atoi(replicaNumStr); err != nil {
			err = unmatchedKey(replicaNumKey)
			return
		}
	}

	if mpReplicaNumStr := r.FormValue(mpReplicaNumKey); mpReplicaNumStr != "" {
		if mpReplicaNum, err = strconv.Atoi(mpReplicaNumStr); err != nil {
			err = unmatchedKey(mpReplicaNumKey)
			return
		}
	}
	return
}

func parseRequestToSetVolConvertSt(r *http.Request) (name, authKey string, newState int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	if stateStr := r.FormValue(stateKey); stateStr != "" {
		if newState, err = strconv.Atoi(stateStr); err != nil {
			err = unmatchedKey(stateKey)
			return
		}
	}
	return
}

func parseDefaultInfoToUpdateVol(r *http.Request, vol *Vol) (zoneName string, capacity, storeMode int, description string,
	layout proto.MetaPartitionLayout, extentCacheExpireSec int64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = vol.zoneName
	}
	if capacityStr := r.FormValue(volCapacityKey); capacityStr != "" {
		if capacity, err = strconv.Atoi(capacityStr); err != nil {
			err = unmatchedKey(volCapacityKey)
			return
		}
	} else {
		capacity = int(vol.Capacity)
	}
	if description = r.FormValue(descriptionKey); description == "" {
		description = vol.description
	}
	if ekExpireSecStr := r.FormValue(extentExpirationKey); ekExpireSecStr != "" {
		if extentCacheExpireSec, err = strconv.ParseInt(ekExpireSecStr, 10, 64); err != nil {
			err = unmatchedKey(extentExpirationKey)
			return
		}
	} else {
		extentCacheExpireSec = vol.ExtentCacheExpireSec
	}

	if storeModeStr := r.FormValue(StoreModeKey); storeModeStr != "" {
		if storeMode, err = strconv.Atoi(storeModeStr); err != nil {
			err = unmatchedKey(StoreModeKey)
			return
		}
	} else {
		storeMode = int(vol.DefaultStoreMode)
	}

	if mpLayoutStr := r.FormValue(volMetaLayoutKey); mpLayoutStr != "" {
		num, tmpErr := fmt.Sscanf(mpLayoutStr, "%d,%d", &layout.PercentOfMP, &layout.PercentOfReplica)
		if tmpErr != nil || num != 2 {
			err = unmatchedKey(volMetaLayoutKey)
			return
		}
	} else {
		layout = vol.MpLayout
	}

	return
}

func parseBoolFieldToUpdateVol(r *http.Request, vol *Vol) (followerRead, nearRead, authenticate, enableToken, autoRepair, forceROW bool, err error) {
	if followerReadStr := r.FormValue(followerReadKey); followerReadStr != "" {
		if followerRead, err = strconv.ParseBool(followerReadStr); err != nil {
			err = unmatchedKey(followerReadKey)
			return
		}
	} else {
		followerRead = vol.FollowerRead
	}
	if nearReadStr := r.FormValue(nearReadKey); nearReadStr != "" {
		if nearRead, err = strconv.ParseBool(nearReadStr); err != nil {
			err = unmatchedKey(nearReadKey)
			return
		}
	} else {
		nearRead = vol.NearRead
	}
	if authenticateStr := r.FormValue(authenticateKey); authenticateStr != "" {
		if authenticate, err = strconv.ParseBool(authenticateStr); err != nil {
			err = unmatchedKey(authenticateKey)
			return
		}
	} else {
		authenticate = vol.authenticate
	}
	if enableTokenStr := r.FormValue(enableTokenKey); enableTokenStr != "" {
		if enableToken, err = strconv.ParseBool(enableTokenStr); err != nil {
			err = unmatchedKey(enableTokenKey)
			return
		}
	} else {
		enableToken = vol.enableToken
	}
	if autoRepairStr := r.FormValue(autoRepairKey); autoRepairStr != "" {
		if autoRepair, err = strconv.ParseBool(autoRepairStr); err != nil {
			err = unmatchedKey(autoRepairKey)
			return
		}
	} else {
		autoRepair = vol.autoRepair
	}
	if forceROWStr := r.FormValue(forceROWKey); forceROWStr != "" {
		if forceROW, err = strconv.ParseBool(forceROWStr); err != nil {
			err = unmatchedKey(forceROWKey)
			return
		}
	} else {
		forceROW = vol.ForceROW
	}
	return
}

func parseDefaultSelectorToUpdateVol(r *http.Request, vol *Vol) (dpSelectorName, dpSelectorParm string, err error) {
	err = r.ParseForm()
	if err != nil {
		return
	}
	dpSelectorName = r.FormValue(dpSelectorNameKey)
	dpSelectorParm = r.FormValue(dpSelectorParmKey)
	if (dpSelectorName == "") || (dpSelectorParm == "") {
		if (dpSelectorName != "") || (dpSelectorParm != "") {
			err = keyNotFound(dpSelectorNameKey + " or " + dpSelectorParmKey)
			return
		}
		dpSelectorName = vol.dpSelectorName
		dpSelectorParm = vol.dpSelectorParm
	}

	return
}

func parseDpWriteableThresholdToUpdateVol(r *http.Request, vol *Vol) (dpWriteableThreshold float64, err error) {
	var dpWriteableThresholdStr string
	if dpWriteableThresholdStr = r.FormValue(dpWritableThresholdKey); dpWriteableThresholdStr == "" {
		dpWriteableThreshold = vol.dpWriteableThreshold
	} else if dpWriteableThreshold, err = strconv.ParseFloat(dpWriteableThresholdStr, 64); err != nil {
		err = unmatchedKey(dpWritableThresholdKey)
		return
	}
	if dpWriteableThreshold > 0 && dpWriteableThreshold < defaultMinDpWriteableThreshold {
		err = fmt.Errorf("dpWriteableThreshold must be larger than 0.5")
		return
	}
	return
}

func parseCrossRegionHATypeToUpdateVol(r *http.Request, vol *Vol) (crossRegionHAType proto.CrossRegionHAType, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if crossRegionHAStr := r.FormValue(crossRegionHAKey); crossRegionHAStr != "" {
		crossRegionHA, err1 := strconv.ParseUint(crossRegionHAStr, 10, 64)
		if err1 != nil {
			err = unmatchedKey(crossRegionHAKey)
			return
		}
		crossRegionHAType = proto.CrossRegionHAType(crossRegionHA)
		if crossRegionHAType != proto.DefaultCrossRegionHAType && crossRegionHAType != proto.CrossRegionHATypeQuorum {
			err = fmt.Errorf("parameter %s should be %d(%s) or %d(%s)", crossRegionHAKey,
				proto.DefaultCrossRegionHAType, proto.DefaultCrossRegionHAType, proto.CrossRegionHATypeQuorum, proto.CrossRegionHATypeQuorum)
			return
		}
	} else {
		crossRegionHAType = vol.CrossRegionHAType
	}
	return
}

func parseSmartToUpdateVol(r *http.Request, vol *Vol) (isSmart bool, smartRules []string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	isSmartStr := r.FormValue(smartKey)
	if isSmartStr == "" {
		isSmart = vol.isSmart
	} else {
		isSmart, err = strconv.ParseBool(isSmartStr)
		if err != nil {
			return
		}
	}
	rules := r.FormValue(smartRulesKey)
	if rules != "" {
		smartRules = strings.Split(rules, ",")
	} else {
		smartRules = vol.smartRules
	}
	return
}

func parseOSSBucketPolicyToUpdateVol(r *http.Request, vol *Vol) (ossBucketPolicy proto.BucketAccessPolicy, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if bucketPolicyStr := r.FormValue(bucketPolicyKey); bucketPolicyStr != "" {
		bucketPolicy, err1 := strconv.ParseUint(bucketPolicyStr, 10, 64)
		if err1 != nil {
			err = unmatchedKey(bucketPolicyKey)
			return
		}
		ossBucketPolicy = proto.BucketAccessPolicy(bucketPolicy)
		if ossBucketPolicy != proto.OSSBucketPolicyPrivate && ossBucketPolicy != proto.OSSBucketPolicyPublicRead {
			err = fmt.Errorf("parameter %s should be %v or %v", bucketPolicyKey, proto.OSSBucketPolicyPrivate, proto.OSSBucketPolicyPublicRead)
			return
		}
	} else {
		ossBucketPolicy = vol.OSSBucketPolicy
	}
	return
}

func parseDefaultTrashDaysToUpdateVol(r *http.Request, vol *Vol) (remaining uint32, err error) {
	err = r.ParseForm()
	if err != nil {
		return
	}

	val := r.FormValue(trashRemainingDaysKey)
	if val == "" {
		remaining = vol.trashRemainingDays
		return
	}

	var valTemp int
	valTemp, err = strconv.Atoi(val)
	if err != nil {
		return
	}

	remaining = uint32(valTemp)
	return
}

func parseRequestToCreateVol(r *http.Request) (name, owner, zoneName, description string,
	mpCount, dpReplicaNum, mpReplicaNum, size, capacity, storeMode, trashDays int,
	followerRead, authenticate, enableToken, autoRepair, volWriteMutexEnable, forceROW, isSmart bool,
	crossRegionHAType proto.CrossRegionHAType, dpWritableThreshold float64,
	layout proto.MetaPartitionLayout, smartRules []string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if owner, err = extractOwner(r); err != nil {
		return
	}

	if mpCountStr := r.FormValue(metaPartitionCountKey); mpCountStr != "" {
		if mpCount, err = strconv.Atoi(mpCountStr); err != nil {
			mpCount = defaultInitMetaPartitionCount
		}
	}

	if replicaStr := r.FormValue(replicaNumKey); replicaStr == "" {
		dpReplicaNum = defaultReplicaNum
	} else if dpReplicaNum, err = strconv.Atoi(replicaStr); err != nil {
		err = unmatchedKey(replicaNumKey)
		return
	}

	if replicaStr := r.FormValue(mpReplicaNumKey); replicaStr == "" {
		mpReplicaNum = defaultReplicaNum
	} else if mpReplicaNum, err = strconv.Atoi(replicaStr); err != nil {
		err = unmatchedKey(mpReplicaNumKey)
		return
	}

	if sizeStr := r.FormValue(dataPartitionSizeKey); sizeStr != "" {
		if size, err = strconv.Atoi(sizeStr); err != nil {
			err = unmatchedKey(dataPartitionSizeKey)
			return
		}
	}

	if capacityStr := r.FormValue(volCapacityKey); capacityStr == "" {
		err = keyNotFound(volCapacityKey)
		return
	} else if capacity, err = strconv.Atoi(capacityStr); err != nil {
		err = unmatchedKey(volCapacityKey)
		return
	}
	var dpWriteableThresholdStr string
	if dpWriteableThresholdStr = r.FormValue(dpWritableThresholdKey); dpWriteableThresholdStr == "" {
		dpWritableThreshold = 0.0
	} else if dpWritableThreshold, err = strconv.ParseFloat(dpWriteableThresholdStr, 64); err != nil {
		err = unmatchedKey(dpWritableThresholdKey)
		return
	}

	if dpWritableThreshold > 0 && dpWritableThreshold < defaultMinDpWriteableThreshold {
		err = fmt.Errorf("dpWritableThreshold must be larger than 0.5")
		return
	}

	if followerRead, err = extractFollowerRead(r); err != nil {
		return
	}
	if forceROW, err = extractForceROW(r); err != nil {
		return
	}
	if crossRegionHAType, err = extractCrossRegionHA(r); err != nil {
		return
	}

	if authenticate, err = extractAuthenticate(r); err != nil {
		return
	}
	if autoRepair, err = extractAutoRepair(r); err != nil {
		return
	}
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	enableToken = extractEnableToken(r)
	volWriteMutexEnable = extractVolWriteMutex(r)
	description = r.FormValue(descriptionKey)

	if trashDaysStr := r.FormValue(trashRemainingDaysKey); trashDaysStr == "" {
		trashDays = 0
	} else if trashDays, err = strconv.Atoi(trashDaysStr); err != nil {
		err = unmatchedKey(trashRemainingDaysKey)
		return
	}

	storeMode = int(proto.StoreModeMem)
	if storeModeStr := r.FormValue(StoreModeKey); storeModeStr != "" {
		if storeMode, err = strconv.Atoi(storeModeStr); err != nil {
			err = unmatchedKey(StoreModeKey)
			return
		}
	}

	layout.PercentOfReplica = 0
	layout.PercentOfMP = 0
	if mpLayoutStr := r.FormValue(volMetaLayoutKey); mpLayoutStr != "" {
		num, tmpErr := fmt.Sscanf(mpLayoutStr, "%d,%d", &layout.PercentOfMP, &layout.PercentOfReplica)
		if tmpErr != nil || num != 2 {
			err = unmatchedKey(StoreModeKey)
			return
		}
	}

	isSmartStr := r.FormValue(smartKey)
	if isSmartStr == "" {
		isSmart = false
	} else {
		isSmart, err = strconv.ParseBool(isSmartStr)
		if err != nil {
			return
		}
	}

	rules := r.FormValue(smartRulesKey)
	if rules != "" {
		smartRules = strings.Split(rules, ",")
	}
	return
}

func extractEnableToken(r *http.Request) (enableToken bool) {
	enableToken, err := strconv.ParseBool(r.FormValue(enableTokenKey))
	if err != nil {
		enableToken = false
	}
	return
}

func extractVolWriteMutex(r *http.Request) bool {
	volWriteMutex, err := strconv.ParseBool(r.FormValue(volWriteMutexKey))
	if err != nil {
		volWriteMutex = false
	}
	return volWriteMutex
}

func parseRequestToCreateDataPartition(r *http.Request) (count int, name, designatedZoneName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if countStr := r.FormValue(countKey); countStr == "" {
		err = keyNotFound(countKey)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = unmatchedKey(countKey)
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	designatedZoneName = r.FormValue(zoneNameKey)
	return
}

func parseRequestToGetDataPartition(r *http.Request) (ID uint64, volName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	volName = r.FormValue(nameKey)
	return
}

func parseRequestToResetDataPartition(r *http.Request) (ID uint64, err error) {
	return extractDataPartitionID(r)
}
func parseRequestToManualResetDataPartition(r *http.Request) (nodeAddrs string, ID uint64, err error) {
	if nodeAddrs, err = extractNodeAddr(r); err != nil {
		return
	}
	ID, err = extractDataPartitionID(r)
	return
}
func parseRequestToLoadDataPartition(r *http.Request) (ID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	return
}

func parseUpdateDataPartition(r *http.Request) (ID uint64, volName string, isManual bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	if isManual, err = extractIsManual(r); err != nil {
		return
	}
	volName = r.FormValue(nameKey)
	return
}

func parseBatchUpdateDataPartitions(r *http.Request) (volName, medium string, isManual bool, count int, startID, endID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if volName = r.FormValue(nameKey); volName == "" {
		err = keyNotFound(nameKey)
		return
	}
	if medium, err = extractMedium(r); err != nil {
		return
	}
	if isManual, err = extractIsManual(r); err != nil {
		return
	}
	if count, err = extractCount(r); err != nil {
		return
	}
	if count > 0 {
		return
	}
	if startID, err = extractStart(r); err != nil {
		return
	}
	if endID, err = extractEnd(r); err != nil {
		return
	}
	if startID > endID {
		err = fmt.Errorf("startID:%v should not more than endID:%v", startID, endID)
	}
	return
}

func extractMedium(r *http.Request) (medium string, err error) {
	if medium = r.FormValue(mediumKey); medium == "" {
		err = keyNotFound(mediumKey)
		return
	}
	if !(medium == mediumAll || medium == mediumSSD || medium == mediumHDD) {
		err = fmt.Errorf("medium must be %v, %v or %v ", mediumAll, mediumSSD, mediumHDD)
		return
	}
	return
}

func extractIsManual(r *http.Request) (isManual bool, err error) {
	var value string
	if value = r.FormValue(isManualKey); value == "" {
		err = keyNotFound(isManualKey)
		return
	}
	return strconv.ParseBool(value)
}

func extractStart(r *http.Request) (startID uint64, err error) {
	var value string
	if value = r.FormValue(startKey); value == "" {
		err = keyNotFound(startKey)
		return
	}
	if startID, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}
	return
}

func extractEnd(r *http.Request) (endID uint64, err error) {
	var value string
	if value = r.FormValue(endKey); value == "" {
		err = keyNotFound(endKey)
		return
	}
	if endID, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}
	return
}

func extractCount(r *http.Request) (count int, err error) {
	var value string
	if value = r.FormValue(countKey); value == "" {
		return
	}
	if count, err = strconv.Atoi(value); err != nil {
		err = unmatchedKey(countKey)
		return
	}
	return
}

func parseRequestToAddMetaReplica(r *http.Request) (ID uint64, addr string, addReplicaType proto.AddReplicaType,
	storeMode int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	addr, _ = extractNodeAddr(r)
	if addReplicaType, err = extractAddReplicaType(r); err != nil {
		return
	}
	if addReplicaType == proto.DefaultAddReplicaType && addr == "" {
		err = keyNotFound(addrKey)
		return
	}
	if storeMode, err = extractStoreMode(r); err != nil {
		return
	}
	return
}

func parseRequestToRemoveMetaReplica(r *http.Request) (ID uint64, addr string, storeMode int, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseRequestToAddMetaReplicaLearner(r *http.Request) (ID uint64, addr string, auto bool, threshold uint8,
	addReplicaType proto.AddReplicaType, storeMode int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	addr, _ = extractNodeAddr(r)
	auto = extractAuto(r)
	threshold = extractLearnerThreshold(r)
	storeMode, err = extractStoreMode(r)
	if addReplicaType, err = extractAddReplicaType(r); err != nil {
		return
	}
	if addReplicaType == proto.DefaultAddReplicaType && addr == "" {
		err = keyNotFound(addrKey)
		return
	}
	return
}

func parseRequestToPromoteMetaReplicaLearner(r *http.Request) (ID uint64, addr string, storeMode int, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseRequestToAddDataReplicaLearner(r *http.Request) (ID uint64, addr string, auto bool, threshold uint8, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	auto = extractAuto(r)
	threshold = extractLearnerThreshold(r)
	return
}

func extractMetaPartitionIDAndAddr(r *http.Request) (ID uint64, addr string, storeMode int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}

	if storeMode, err = extractStoreMode(r); err != nil {
		return
	}
	return
}

func extractMetaPartitionIDAddrAndDestAddr(r *http.Request) (ID uint64, addr string, destAddr string, storeMode int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	destAddr, _ = extractDestNodeAddr(r)
	storeMode, err = extractStoreMode(r)
	return
}

func parseRequestToAddDataReplica(r *http.Request) (ID uint64, addr string, addReplicaType proto.AddReplicaType, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	addr, _ = extractNodeAddr(r)
	if addReplicaType, err = extractAddReplicaType(r); err != nil {
		return
	}
	if addReplicaType == proto.DefaultAddReplicaType && addr == "" {
		err = keyNotFound(addrKey)
		return
	}
	return
}

func parseRequestToRemoveDataReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func parseRequestToPromoteDataReplicaLearner(r *http.Request) (ID uint64, addr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func extractDataPartitionIDAndAddr(r *http.Request) (ID uint64, addr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func extractDataPartitionID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseRequestToDecommissionDataPartition(r *http.Request) (ID uint64, nodeAddr string, destAddr string, err error) {
	return extractDataPartitionIDAddrAndDestAddr(r)
}

func extractDataPartitionIDAddrAndDestAddr(r *http.Request) (ID uint64, addr string, destAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	destAddr, _ = extractDestNodeAddr(r)
	return
}

func extractDestNodeAddr(r *http.Request) (destAddr string, err error) {
	if destAddr = r.FormValue(destAddrKey); destAddr == "" {
		err = keyNotFound(destAddrKey)
		return
	}
	return
}

func extractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue(addrKey); nodeAddr == "" {
		err = keyNotFound(addrKey)
		return
	}
	return
}

func extractStoreMode(r *http.Request) (storeMode int, err error) {
	storeModeStr := r.FormValue(StoreModeKey)
	if storeModeStr == "" {
		return
	}

	storeMode, err = strconv.Atoi(storeModeStr)
	if err != nil {
		err = fmt.Errorf("convert storeMode[%v] to num failed; err:%v", storeModeStr, err.Error())
	}
	return
}

func extractAuto(r *http.Request) (auto bool) {
	if value := r.FormValue(autoKey); value != "" {
		auto, _ = strconv.ParseBool(value)
	}
	return auto
}

func extractLearnerThreshold(r *http.Request) (threshold uint8) {
	var value string
	if value = r.FormValue(thresholdKey); value != "" {
		num, _ := strconv.ParseUint(value, 10, 32)
		threshold = uint8(num)
	}
	if threshold <= 0 || threshold > 100 {
		threshold = defaultLearnerPromThreshold
	}
	return
}

func extractNodeID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractDiskPath(r *http.Request) (diskPath string, err error) {
	if diskPath = r.FormValue(diskPathKey); diskPath == "" {
		err = keyNotFound(diskPathKey)
		return
	}
	return
}

func parseRequestToLoadMetaPartition(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func parseRequestToDecommissionMetaPartition(r *http.Request) (partitionID uint64, nodeAddr string, destAddr string, storeMode int, err error) {
	return extractMetaPartitionIDAddrAndDestAddr(r)
}

func parseRequestToSelectMetaReplace(r *http.Request) (partitionID uint64, nodeAddr string, storeMode int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	storeMode, err = extractStoreMode(r)
	return
}

func parseRequestToReplicateMetaPartition(r *http.Request) (partitionID uint64, err error) {
	return extractMetaPartitionID(r)
}

func parseRequestToManualResetMetaPartition(r *http.Request) (nodeAddrs string, ID uint64, err error) {
	if nodeAddrs, err = extractNodeAddr(r); err != nil {
		return
	}
	ID, err = extractMetaPartitionID(r)
	return
}

func parseAndExtractStatus(r *http.Request) (status bool, err error) {

	if err = r.ParseForm(); err != nil {
		return
	}
	return extractStatus(r)
}

func extractStatus(r *http.Request) (status bool, err error) {
	var value string
	if value = r.FormValue(enableKey); value == "" {
		err = keyNotFound(enableKey)
		return
	}
	if status, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractFollowerRead(r *http.Request) (followerRead bool, err error) {
	var value string
	if value = r.FormValue(followerReadKey); value == "" {
		followerRead = false
		return
	}
	if followerRead, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractForceROW(r *http.Request) (forceROW bool, err error) {
	var value string
	if value = r.FormValue(forceROWKey); value == "" {
		forceROW = false
		return
	}
	if forceROW, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractAuthenticate(r *http.Request) (authenticate bool, err error) {
	var value string
	if value = r.FormValue(authenticateKey); value == "" {
		authenticate = false
		return
	}
	if authenticate, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractAutoRepair(r *http.Request) (autoRepair bool, err error) {
	var value string
	if value = r.FormValue(autoRepairKey); value == "" {
		autoRepair = false
		return
	}
	if autoRepair, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractCrossRegionHA(r *http.Request) (crossRegionHAType proto.CrossRegionHAType, err error) {
	crossRegionHAStr := r.FormValue(crossRegionHAKey)
	if crossRegionHAStr != "" {
		crossRegionHA, err1 := strconv.ParseUint(crossRegionHAStr, 10, 64)
		if err1 != nil {
			err = unmatchedKey(crossRegionHAKey)
			return
		}
		crossRegionHAType = proto.CrossRegionHAType(crossRegionHA)
		if crossRegionHAType != proto.DefaultCrossRegionHAType && crossRegionHAType != proto.CrossRegionHATypeQuorum {
			err = fmt.Errorf("parameter %s should be %d(%s) or %d(%s)", crossRegionHAKey,
				proto.DefaultCrossRegionHAType, proto.DefaultCrossRegionHAType, proto.CrossRegionHATypeQuorum, proto.CrossRegionHATypeQuorum)
			return
		}
	} else {
		crossRegionHAType = proto.DefaultCrossRegionHAType
	}
	return
}

func parseAndExtractThreshold(r *http.Request) (threshold float64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(thresholdKey); value == "" {
		err = keyNotFound(thresholdKey)
		return
	}
	if threshold, err = strconv.ParseFloat(value, 64); err != nil {
		return
	}
	return
}

func parseAndExtractSetNodeInfoParams(r *http.Request) (params map[string]interface{}, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	params = make(map[string]interface{})
	if val := r.FormValue(zoneNameKey); val != "" {
		params[zoneNameKey] = val
	}
	if val := r.FormValue(volumeKey); val != "" {
		params[volumeKey] = val
	}
	if val := r.FormValue(extentMergeInoKey); val != "" {
		params[extentMergeInoKey] = val
	}

	uintKeys := []string{nodeDeleteBatchCountKey, nodeMarkDeleteRateKey, dataNodeRepairTaskCountKey, nodeDeleteWorkerSleepMs, metaNodeReqRateKey, metaNodeReqOpRateKey,
		dataNodeReqRateKey, dataNodeReqVolOpRateKey, dataNodeReqOpRateKey, dataNodeReqVolPartRateKey, dataNodeReqVolOpPartRateKey, opcodeKey, clientReadVolRateKey, clientWriteVolRateKey,
		extentMergeSleepMsKey, fixTinyDeleteRecordKey, metaNodeReadDirLimitKey, dataNodeRepairTaskCntZoneKey}
	for _, key := range uintKeys {
		if err = parseUintKey(params, key, r); err != nil {
			return
		}
	}
	intKeys := []string{dpRecoverPoolSizeKey, mpRecoverPoolSizeKey, clientVolOpRateKey}
	for _, key := range intKeys {
		if err = parseIntKey(params, key, r); err != nil {
			return
		}
	}
	if len(params) == 0 {
		err = errors.NewErrorf("no valid parameters")
		return
	}
	return
}

func parseUintKey(params map[string]interface{}, key string, r *http.Request) (err error) {
	if value := r.FormValue(key); value != "" {
		var val = uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(key)
			return
		}
		params[key] = val
	}
	return
}

func parseIntKey(params map[string]interface{}, key string, r *http.Request) (err error) {
	if value := r.FormValue(key); value != "" {
		var val = int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(key)
			return
		}
		params[key] = val
	}
	return
}

func validateRequestToCreateMetaPartition(r *http.Request) (volName string, start uint64, err error) {
	if volName, err = extractName(r); err != nil {
		return
	}

	var value string
	if value = r.FormValue(startKey); value == "" {
		err = keyNotFound(startKey)
		return
	}
	start, err = strconv.ParseUint(value, 10, 64)
	return
}

func newSuccessHTTPReply(data interface{}) *proto.HTTPReply {
	return &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func newErrHTTPReply(err error) *proto.HTTPReply {
	if err == nil {
		return newSuccessHTTPReply("")
	}
	code, ok := proto.Err2CodeMap[err]
	if ok {
		return &proto.HTTPReply{Code: code, Msg: err.Error()}
	}
	return &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()}
}

func sendOkReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	switch httpReply.Data.(type) {
	case *DataPartition:
		dp := httpReply.Data.(*DataPartition)
		dp.RLock()
		defer dp.RUnlock()
	case *MetaPartition:
		mp := httpReply.Data.(*MetaPartition)
		mp.RLock()
		defer mp.RUnlock()
	case *MetaNode:
		mn := httpReply.Data.(*MetaNode)
		mn.RLock()
		defer mn.RUnlock()
	case *DataNode:
		dn := httpReply.Data.(*DataNode)
		dn.RLock()
		defer dn.RUnlock()
	}
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	send(w, r, reply)
	return
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
		return
	}
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
	return
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
		vol  *Vol
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
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
	stat.EnableToken = vol.enableToken
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *proto.MetaPartitionView) {
	mpView = proto.NewMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
	mp.Lock()
	defer mp.Unlock()
	for _, host := range mp.Hosts {
		mpView.Members = append(mpView.Members, host)
	}
	mpView.MaxInodeID = mp.MaxInodeID
	mpView.InodeCount = mp.InodeCount
	mpView.DentryCount = mp.DentryCount
	mpView.IsRecover = mp.IsRecover
	mpView.MaxExistIno = mp.MaxExistIno
	for _, learner := range mp.Learners {
		mpView.Learners = append(mpView.Learners, learner.Addr)
	}
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return
	}
	mpView.LeaderAddr = mr.Addr
	if len(mp.Replicas) <= 0 {
		return
	}

	mpView.StoreMode = mp.Replicas[0].StoreMode
	for _, replica := range mp.Replicas {
		if mpView.StoreMode != replica.StoreMode {
			mpView.StoreMode = proto.StoreModeMem | proto.StoreModeRocksDb
		}
		switch replica.StoreMode {
		case proto.StoreModeMem:
			mpView.MemCount++
		case proto.StoreModeRocksDb:
			mpView.RocksCount++
		default:
			mpView.MemCount++
		}
	}
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
		memCnt := uint8(0)
		rocksCnt := uint8(0)
		for i := 0; i < len(replicas); i++ {
			replicas[i] = &proto.MetaReplicaInfo{
				Addr:        mp.Replicas[i].Addr,
				ReportTime:  mp.Replicas[i].ReportTime,
				Status:      mp.Replicas[i].Status,
				IsLeader:    mp.Replicas[i].IsLeader,
				DentryCount: mp.Replicas[i].DentryCount,
				InodeCount:  mp.Replicas[i].InodeCount,
				IsLearner:   mp.Replicas[i].IsLearner,
				StoreMode:   mp.Replicas[i].StoreMode,
				ApplyId:     mp.Replicas[i].ApplyId,
			}

			if mp.Replicas[i].StoreMode == proto.StoreModeMem {
				memCnt++
			}

			if mp.Replicas[i].StoreMode == proto.StoreModeRocksDb {
				rocksCnt++
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
			MaxExistIno:   mp.MaxExistIno,
			Replicas:      replicas,
			ReplicaNum:    mp.ReplicaNum,
			LearnerNum:    mp.LearnerNum,
			Status:        mp.Status,
			IsRecover:     mp.IsRecover,
			Hosts:         mp.Hosts,
			Peers:         mp.Peers,
			Learners:      mp.Learners,
			Zones:         zones,
			OfflinePeerID: mp.OfflinePeerID,
			MissNodes:     mp.MissNodes,
			LoadResponse:  mp.LoadResponse,
			MemStoreCnt:   memCnt,
			RcokStoreCnt:  rocksCnt,
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

			volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize, stat.UsedSize, vol.trashRemainingDays, vol.isSmart, vol.smartRules)
			volsInfo = append(volsInfo, volInfo)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(volsInfo))
}

func (m *Server) listSmartVols(w http.ResponseWriter, r *http.Request) {
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
			if !vol.isSmart {
				continue
			}
			stat := volStat(vol)
			volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize, stat.UsedSize, vol.trashRemainingDays, vol.isSmart, vol.smartRules)
			volsInfo = append(volsInfo, volInfo)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(volsInfo))
}

func (m *Server) applyVolWriteMutex(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		vol     *Vol
		err     error
	)
	if volName, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	clientIP := iputil.RealIP(r)
	err = vol.applyVolMutex(clientIP)

	if err != nil && err != proto.ErrVolWriteMutexUnable {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err != nil && err == proto.ErrVolWriteMutexUnable {
		sendOkReply(w, r, newSuccessHTTPReply(err.Error()))
		return
	}
	log.LogInfof("apply volume mutex success, volume(%v), clientIP(%v)", volName, clientIP)
	sendOkReply(w, r, newSuccessHTTPReply("apply volume mutex success"))
}

func (m *Server) releaseVolWriteMutex(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		vol     *Vol
		err     error
	)
	if volName, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	err = vol.releaseVolMutex()
	if err != nil && err != proto.ErrVolWriteMutexUnable {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err != nil && err == proto.ErrVolWriteMutexUnable {
		sendOkReply(w, r, newSuccessHTTPReply(err.Error()))
		return
	}
	log.LogInfof("release volume mutex success, volume(%v)", volName)
	sendOkReply(w, r, newSuccessHTTPReply("release volume mutex success"))
}

func (m *Server) getVolWriteMutexInfo(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		vol     *Vol
		err     error
	)
	if volName, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	var clientInfo *VolWriteMutexClient

	err, clientInfo = vol.getVolMutexClientInfo()
	if err != nil && err != proto.ErrVolWriteMutexUnable {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err != nil && err == proto.ErrVolWriteMutexUnable {
		sendOkReply(w, r, newSuccessHTTPReply(err.Error()))
		return
	}
	if clientInfo == nil {
		sendOkReply(w, r, newSuccessHTTPReply("no client info"))
	} else {
		sendOkReply(w, r, newSuccessHTTPReply(clientInfo))
	}
}

func parseAndExtractPartitionInfo(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func extractMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractAuthKey(r *http.Request) (authKey string, err error) {
	if authKey = r.FormValue(volAuthKey); authKey == "" {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func parseAndExtractName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractName(r)
}

func extractZoneName(r *http.Request) (name string, err error) {
	if name = r.FormValue(zoneNameKey); name == "" {
		err = keyNotFound(zoneNameKey)
		return
	}
	return
}

func extractName(r *http.Request) (name string, err error) {
	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(nameKey)
		return
	}
	if !volNameRegexp.MatchString(name) {
		return "", errors.New("name can only be number and letters")
	}

	return
}

func extractOwner(r *http.Request) (owner string, err error) {
	if owner = r.FormValue(volOwnerKey); owner == "" {
		err = keyNotFound(volOwnerKey)
		return
	}
	if !ownerRegexp.MatchString(owner) {
		return "", errors.New("owner can only be number and letters")
	}

	return
}

func parseAndCheckTicket(r *http.Request, key []byte, volName string) (jobj proto.APIAccessReq, ticket cryptoutil.Ticket, ts int64, err error) {
	var (
		plaintext []byte
	)

	if err = r.ParseForm(); err != nil {
		return
	}

	if plaintext, err = extractClientReqInfo(r); err != nil {
		return
	}

	if err = json.Unmarshal([]byte(plaintext), &jobj); err != nil {
		return
	}

	if err = proto.VerifyAPIAccessReqIDs(&jobj); err != nil {
		return
	}

	ticket, ts, err = extractTicketMess(&jobj, key, volName)

	return
}

func extractClientReqInfo(r *http.Request) (plaintext []byte, err error) {
	var (
		message string
	)
	if err = r.ParseForm(); err != nil {
		return
	}

	if message = r.FormValue(proto.ClientMessage); message == "" {
		err = keyNotFound(proto.ClientMessage)
		return
	}

	if plaintext, err = cryptoutil.Base64Decode(message); err != nil {
		return
	}

	return
}

func extractTicketMess(req *proto.APIAccessReq, key []byte, volName string) (ticket cryptoutil.Ticket, ts int64, err error) {
	if ticket, err = proto.ExtractTicket(req.Ticket, key); err != nil {
		err = fmt.Errorf("extractTicket failed: %s", err.Error())
		return
	}
	if time.Now().Unix() >= ticket.Exp {
		err = proto.ErrExpiredTicket
		return
	}
	if ts, err = proto.ParseVerifier(req.Verifier, ticket.SessionKey.Key); err != nil {
		err = fmt.Errorf("parseVerifier failed: %s", err.Error())
		return
	}
	if err = proto.CheckAPIAccessCaps(&ticket, proto.APIRsc, req.Type, proto.APIAccess); err != nil {
		err = fmt.Errorf("CheckAPIAccessCaps failed: %s", err.Error())
		return
	}
	if err = proto.CheckVOLAccessCaps(&ticket, volName, proto.VOLAccess, proto.MasterNode); err != nil {
		err = fmt.Errorf("CheckVOLAccessCaps failed: %s", err.Error())
		return
	}
	return
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

func (m *Server) handleDataNodeValidateCRCReport(w http.ResponseWriter, r *http.Request) {
	dpCrcInfo, err := parseRequestToDataNodeValidateCRCReport(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleDataNodeValidateCRCReport(dpCrcInfo)
}

func parseRequestToDataNodeValidateCRCReport(r *http.Request) (dpCrcInfo *proto.DataPartitionExtentCrcInfo, err error) {
	var body []byte
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	dpCrcInfo = &proto.DataPartitionExtentCrcInfo{}
	err = json.Unmarshal(body, dpCrcInfo)
	return
}

func (m *Server) setZoneRegion(w http.ResponseWriter, r *http.Request) {
	var (
		zoneName   string
		regionName string
		err        error
	)
	if zoneName, regionName, err = parseRequestToSetZoneRegion(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if strings.TrimSpace(regionName) == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
	}
	if err = m.cluster.setZoneRegion(zoneName, regionName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set zone[%v] regionName to [%v] successfully", zoneName, regionName)))
}

func (m *Server) updateRegion(w http.ResponseWriter, r *http.Request) {
	var (
		regionName string
		regionType proto.RegionType
		err        error
	)
	if regionName, regionType, err = parseRequestToAddRegion(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.updateRegion(regionName, regionType); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set region[%v], regionType to [%d(%s)] successfully", regionName, regionType, regionType)))
}

func (m *Server) getRegion(w http.ResponseWriter, r *http.Request) {
	var (
		regionName string
		err        error
	)
	if regionName, err = extractRegionNameKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	region, err := m.cluster.t.getRegion(regionName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	regionView := proto.RegionView{
		Name:       region.Name,
		RegionType: region.RegionType,
		Zones:      region.getZones(),
	}
	sendOkReply(w, r, newSuccessHTTPReply(regionView))
}

func (m *Server) regionList(w http.ResponseWriter, r *http.Request) {
	regionViews := m.cluster.t.getRegionViews()
	sendOkReply(w, r, newSuccessHTTPReply(regionViews))
}

func (m *Server) addRegion(w http.ResponseWriter, r *http.Request) {
	var (
		regionName string
		regionType proto.RegionType
		err        error
	)
	if regionName, regionType, err = parseRequestToAddRegion(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.t.createRegion(regionName, regionType, m.cluster); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("add region[%v] successfully", regionName)))
}

func parseRequestToSetZoneRegion(r *http.Request) (zoneName, regionName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if zoneName, err = extractZoneName(r); err != nil {
		return
	}
	if regionName, err = extractRegionNameKey(r); err != nil {
		return
	}
	return
}

func extractRegionNameKey(r *http.Request) (regionName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if regionName = r.FormValue(regionNameKey); regionName == "" {
		err = keyNotFound(regionNameKey)
		return
	}
	return
}

func extractRegionType(r *http.Request) (regionType proto.RegionType, err error) {
	regionTypeStr := r.FormValue(regionTypeKey)
	if regionTypeStr == "" {
		err = keyNotFound(regionTypeKey)
		return
	}
	regionTypeUint, err := strconv.ParseUint(regionTypeStr, 10, 64)
	if err != nil {
		err = unmatchedKey(regionTypeKey)
		return
	}
	regionType = proto.RegionType(regionTypeUint)
	if regionType != proto.SlaveRegion && regionType != proto.MasterRegion {
		err = fmt.Errorf("parameter %s should be %d(%s) or %d(%s)", regionTypeKey,
			proto.MasterRegion, proto.MasterRegion, proto.SlaveRegion, proto.SlaveRegion)
		return
	}
	return
}

func parseRequestToAddRegion(r *http.Request) (regionName string, regionType proto.RegionType, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if regionName, err = extractRegionNameKey(r); err != nil {
		return
	}
	if regionType, err = extractRegionType(r); err != nil {
		return
	}
	return
}

func extractAddReplicaType(r *http.Request) (addReplicaType proto.AddReplicaType, err error) {
	addReplicaTypeStr := r.FormValue(addReplicaTypeKey)
	if addReplicaTypeStr == "" {
		addReplicaType = proto.DefaultAddReplicaType
		return
	}
	addReplicaTypeUint, err := strconv.ParseUint(addReplicaTypeStr, 10, 64)
	if err != nil {
		err = unmatchedKey(addReplicaTypeKey)
		return
	}
	addReplicaType = proto.AddReplicaType(addReplicaTypeUint)
	if addReplicaType != proto.DefaultAddReplicaType && addReplicaType != proto.AutoChooseAddrForQuorumVol {
		err = fmt.Errorf("parameter %s should be %d(%s) or %d(%s)", addReplicaTypeKey,
			proto.DefaultAddReplicaType, proto.DefaultAddReplicaType, proto.AutoChooseAddrForQuorumVol, proto.AutoChooseAddrForQuorumVol)
		return
	}
	return
}

func (m *Server) setVolConvertMode(w http.ResponseWriter, r *http.Request) {
	var (
		volName       string
		partitionType string
		convertMode   proto.ConvertMode
		err           error
	)
	volName, partitionType, convertMode, err = parseRequestToSetVolConvertMode(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if partitionType == partitionTypeDataPartition {
		err = m.cluster.updateVolDataPartitionConvertMode(volName, convertMode)
	} else if partitionType == partitionTypeMetaPartition {
		err = m.cluster.updateVolMetaPartitionConvertMode(volName, convertMode)
	}
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set vol[%v] %s convert mode to %d(%s) successfully",
		volName, partitionType, convertMode, convertMode)))
}

func parseRequestToSetVolConvertMode(r *http.Request) (volName, partitionType string, convertMode proto.ConvertMode, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if volName, err = extractName(r); err != nil {
		return
	}
	if partitionType, err = extractPartitionType(r); err != nil {
		return
	}
	if convertMode, err = extractConvertMode(r); err != nil {
		return
	}
	return
}

func extractPartitionType(r *http.Request) (partitionType string, err error) {
	partitionType = r.FormValue(partitionTypeKey)
	if partitionType == "" {
		err = keyNotFound(partitionTypeKey)
		return
	}
	if partitionType != partitionTypeDataPartition && partitionType != partitionTypeMetaPartition {
		err = fmt.Errorf("partitionType must be dataPartition or metaPartition ")
		return
	}
	return
}

func extractConvertMode(r *http.Request) (convertMode proto.ConvertMode, err error) {
	convertModeStr := r.FormValue(convertModeKey)
	if convertModeStr == "" {
		err = keyNotFound(convertModeKey)
		return
	}
	convertModeUint, err := strconv.ParseUint(convertModeStr, 10, 64)
	if err != nil {
		err = unmatchedKey(convertModeKey)
		return
	}
	convertMode = proto.ConvertMode(convertModeUint)
	if convertMode != proto.DefaultConvertMode && convertMode != proto.IncreaseReplicaNum {
		err = fmt.Errorf("parameter %s should be %d(%s) or %d(%s)", convertModeKey,
			proto.DefaultConvertMode, proto.DefaultConvertMode, proto.IncreaseReplicaNum, proto.IncreaseReplicaNum)
		return
	}
	return
}

func (m *Server) setVolMinRWPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName    string
		minRwMPNum int
		minRwDPNum int
		vol        *Vol
		err        error
	)
	if volName, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	minRwMPNum, minRwDPNum, err = parseMinRwMPAndDPNumToSetVolMinRWPartition(r, vol.MinWritableMPNum, vol.MinWritableDPNum)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	err = m.cluster.updateVolMinWritableMPAndDPNum(volName, minRwMPNum, minRwDPNum)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set vol[%v] minRwMPNum, minRwDPNum to %v,%v  successfully",
		volName, minRwMPNum, minRwDPNum)))
}

func parseMinRwMPAndDPNumToSetVolMinRWPartition(r *http.Request, volMinRwMPNum, volMinRwDPNum int) (minRwMPNum, minRwDPNum int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if minRwMPNum, err = extractMinWritableMPNum(r, volMinRwMPNum); err != nil {
		return
	}
	if minRwDPNum, err = extractMinWritableDPNum(r, volMinRwDPNum); err != nil {
		return
	}
	return
}

func extractMinWritableMPNum(r *http.Request, volMinRwMPNum int) (minRwMPNum int, err error) {
	if minWritableMPNumStr := r.FormValue(volMinWritableMPNum); minWritableMPNumStr != "" {
		minWritableMPNum, err1 := strconv.Atoi(minWritableMPNumStr)
		if err1 != nil || minWritableMPNum < 0 {
			err = unmatchedKey(volMinWritableMPNum)
			return
		}
		minRwMPNum = minWritableMPNum
	} else {
		minRwMPNum = volMinRwMPNum
	}
	return
}

func extractMinWritableDPNum(r *http.Request, volMinRwDPNum int) (minRwDPNum int, err error) {
	if minWritableDPNumStr := r.FormValue(volMinWritableDPNum); minWritableDPNumStr != "" {
		minWritableDPNum, err1 := strconv.Atoi(minWritableDPNumStr)
		if err1 != nil || minWritableDPNum < 0 {
			err = unmatchedKey(volMinWritableDPNum)
			return
		}
		minRwDPNum = minWritableDPNum
	} else {
		minRwDPNum = volMinRwDPNum
	}
	return
}

func (m *Server) addIDC(w http.ResponseWriter, r *http.Request) {
	var (
		idcName string
		err     error
	)
	idcName, err = parseRequestToAddIDC(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	_, err = m.cluster.t.createIDC(idcName, m.cluster)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("add idc[%v] successfully", idcName)))
}

func (m *Server) deleteIDC(w http.ResponseWriter, r *http.Request) {
	var (
		idcName string
		err     error
	)
	idcName, err = parseRequestToAddIDC(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	err = m.cluster.t.deleteIDC(idcName, m.cluster)
	if err != nil {
		sendOkReply(w, r, newSuccessHTTPReply(err.Error()))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("delete idc[%v] successfully", idcName)))
}

func parseRequestToAddIDC(r *http.Request) (idcName string, err error) {
	err = r.ParseForm()
	if err != nil {
		return
	}
	idcName, err = extractIDCNameKey(r)
	if err != nil {
		return
	}
	return
}

func extractIDCNameKey(r *http.Request) (idcName string, err error) {
	err = r.ParseForm()
	if err != nil {
		return
	}
	idcName = r.FormValue(nameKey)
	if idcName == "" {
		err = keyNotFound(nameKey)
		return
	}
	return
}

func (m *Server) setZoneIDC(w http.ResponseWriter, r *http.Request) {
	var (
		zoneName, idcName string
		mType             proto.MediumType
		err               error
	)
	zoneName, idcName, mType, err = parseRequestToSetZone(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.setZoneIDC(zoneName, idcName, mType); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set idc to: %v, medium type to: %v  for zone: %v successfully", idcName, mType, zoneName)))
}

func parseRequestToSetZone(r *http.Request) (zoneName, idcName string, mType proto.MediumType, err error) {
	err = r.ParseForm()
	if err != nil {
		return
	}
	zoneName, err = extractZoneName(r)
	if err != nil {
		return
	}
	idcName = r.FormValue(idcNameKey)
	mTypeStr := r.FormValue(mediumTypeKey)
	if mTypeStr == "" {
		mType = proto.MediumInit
		return
	}
	mType, err = proto.StrToMediumType(mTypeStr)
	return
}

func (m *Server) getIDC(w http.ResponseWriter, r *http.Request) {
	var (
		idcName string
		err     error
	)
	idcName, err = extractIDCNameKey(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	idc, err := m.cluster.t.getIDCView(idcName)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(idc))
}

func (m *Server) idcList(w http.ResponseWriter, r *http.Request) {
	views := m.cluster.t.getIDCViews()
	sendOkReply(w, r, newSuccessHTTPReply(views))
}

func (m *Server) freezeDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		partitionID uint64
		err         error
	)
	volName, partitionID, err = extractFreezeDataPartitionPara(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	err = m.cluster.freezeDataPartition(volName, partitionID)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func (m *Server) unfreezeDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName     string
		partitionID uint64
		err         error
	)
	volName, partitionID, err = extractFreezeDataPartitionPara(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	err = m.cluster.unfreezeDataPartition(volName, partitionID)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func extractFreezeDataPartitionPara(r *http.Request) (volName string, partitionID uint64, err error) {
	err = r.ParseForm()
	if err != nil {
		return
	}
	volName = r.FormValue(nameKey)
	if volName == "" {
		err = keyNotFound(nameKey)
		return
	}
	idStr := r.FormValue(idKey)
	if idStr == "" {
		err = keyNotFound(idKey)
		return
	}
	partitionID, err = strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return
	}
	return
}

func (m *Server) setClientPkgAddr(w http.ResponseWriter, r *http.Request) {
	var (
		addr string
		err  error
	)
	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if addr = r.FormValue(addrKey); addr == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: keyNotFound(addrKey).Error()})
		return
	}
	if err = m.cluster.setClientPkgAddr(addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set clientPkgAddr to %s successfully", addr)))
}

func (m *Server) getClientPkgAddr(w http.ResponseWriter, r *http.Request) {
	sendOkReply(w, r, newSuccessHTTPReply(m.cluster.cfg.ClientPkgAddr))
}
