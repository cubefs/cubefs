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

package wrapper

import (
	"fmt"
	syslog "log"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ump"
)

var (
	LocalIP                            string
	DefaultMinWritableDataPartitionCnt = 10
)

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

type SimpleClientInfo interface {
	GetFlowInfo() (*proto.ClientReportLimitInfo, bool)
	UpdateFlowInfo(limit *proto.LimitRsp2Client)
	SetClientID(id uint64) error
	UpdateLatestVer(verList *proto.VolVersionInfoList) error
	GetReadVer() uint64
	GetLatestVer() uint64
	GetVerMgr() *proto.VolVersionInfoList
	UpdateRemoteCacheConfig(view *proto.SimpleVolView)
}

// Wrapper TODO rename. This name does not reflect what it is doing.
type Wrapper struct {
	Lock                   sync.RWMutex
	ClusterName            string
	VolName                string
	volType                int
	EnablePosixAcl         bool
	masters                []string
	partitions             map[uint64]*DataPartition
	followerRead           bool
	followerReadClientCfg  bool
	nearRead               bool
	maximallyRead          bool
	maximallyReadClientCfg bool
	innerReq               bool
	dpSelectorChanged      bool
	dpSelectorName         string
	dpSelectorParm         string
	mc                     *masterSDK.MasterClient
	stopOnce               sync.Once
	stopC                  chan struct{}

	dpSelector DataPartitionSelector

	HostsStatus map[string]bool
	Uids        map[uint32]*proto.UidSimpleInfo
	UidLock     sync.RWMutex
	preload     bool
	LocalIp     string

	verConfReadSeq    uint64
	verReadSeq        uint64
	SimpleClient      SimpleClientInfo
	IsSnapshotEnabled bool

	volStorageClass        uint32
	volAllowedStorageClass []uint32
	volStatByClass         map[uint32]*proto.StatOfStorageClass
	HostsDelay             sync.Map
}

// NewDataPartitionWrapper returns a new data partition wrapper.
func NewDataPartitionWrapper(client SimpleClientInfo, volName string, masters []string, preload bool,
	minWriteAbleDataPartitionCnt int, verReadSeq uint64, volStorageClass uint32, volAllowedStorageClass []uint32, innerReq bool,
) (w *Wrapper, err error) {
	log.LogInfof("action[NewDataPartitionWrapper] verReadSeq %v", verReadSeq)

	w = new(Wrapper)
	w.stopC = make(chan struct{})
	w.masters = masters
	w.mc = masterSDK.NewMasterClient(masters, false)
	w.VolName = volName
	w.partitions = make(map[uint64]*DataPartition)
	w.HostsStatus = make(map[string]bool)
	w.preload = preload
	w.volStorageClass = volStorageClass
	w.volAllowedStorageClass = volAllowedStorageClass

	if w.LocalIp, err = ump.GetLocalIpAddr(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}

	if err = w.updateClusterInfo(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.GetSimpleVolView(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}

	w.UploadFlowInfo(client, true)

	if err = w.initDpSelector(); err != nil {
		log.LogErrorf("NewDataPartitionWrapper: init initDpSelector failed, [%v]", err)
	}
	if err = w.updateDataPartition(true); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.updateDataNodeStatus(); err != nil {
		log.LogErrorf("NewDataPartitionWrapper: init DataNodeStatus failed, [%v]", err)
	}
	w.verConfReadSeq = verReadSeq
	if verReadSeq > 0 {
		var verList *proto.VolVersionInfoList
		if verList, err = w.mc.AdminAPI().GetVerList(volName); err != nil {
			return
		}
		if verReadSeq, err = w.CheckReadVerSeq(volName, verReadSeq, verList); err != nil {
			log.LogErrorf("NewDataPartitionWrapper: init Read with ver [%v] error [%v]", verReadSeq, err)
			return
		}
	}
	w.verReadSeq = verReadSeq
	w.SimpleClient = client
	w.innerReq = innerReq
	go w.uploadFlowInfoByTick(client)
	go w.update(client)
	return
}

func (w *Wrapper) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopC)
	})
}

func (w *Wrapper) InitFollowerRead(clientConfig bool) {
	w.followerReadClientCfg = clientConfig
	w.followerRead = w.followerReadClientCfg || w.followerRead
}

func (w *Wrapper) FollowerRead() bool {
	return w.followerRead
}

func (w *Wrapper) SetMaximallyRead(maximallyRead bool) {
	w.maximallyReadClientCfg = maximallyRead
	w.maximallyRead = w.maximallyReadClientCfg || w.maximallyRead
	log.LogInfof("SetMaximallyRead: set maximallyRead to %v", w.maximallyRead)
}

func (w *Wrapper) MaximallyRead() bool {
	return w.maximallyRead
}

func (w *Wrapper) InitInnerReq(innerReq bool) {
	w.innerReq = innerReq
}

func (w *Wrapper) InnerReq() bool {
	return w.innerReq
}

func (w *Wrapper) TryGetPartition(index uint64) (partition *DataPartition, ok bool) {
	w.Lock.RLock()
	defer w.Lock.RUnlock()
	partition, ok = w.partitions[index]
	return
}

func (w *Wrapper) updateClusterInfo() (err error) {
	var info *proto.ClusterInfo
	if info, err = w.mc.AdminAPI().GetClusterInfo(); err != nil {
		log.LogWarnf("UpdateClusterInfo: get cluster info fail: err(%v)", err)
		return
	}
	log.LogInfof("UpdateClusterInfo: get cluster info: cluster(%v) localIP(%v)", info.Cluster, info.Ip)
	w.ClusterName = info.Cluster
	LocalIP = info.Ip
	w.IsSnapshotEnabled = info.ClusterEnableSnapshot
	return
}

func (w *Wrapper) UpdateUidsView(view *proto.SimpleVolView) {
	w.UidLock.Lock()
	defer w.UidLock.Unlock()
	w.Uids = make(map[uint32]*proto.UidSimpleInfo)
	for _, uid := range view.Uids {
		if !uid.Limited {
			continue
		}
		w.Uids[uid.UID] = &uid
	}
	log.LogDebugf("uid info be updated to %v", view.Uids)
}

func (w *Wrapper) GetSimpleVolView() (err error) {
	var view *proto.SimpleVolView

	if view, err = w.mc.AdminAPI().GetVolumeSimpleInfo(w.VolName); err != nil {
		log.LogWarnf("GetSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.VolName, err)
		return
	}

	if view.Status == 1 {
		log.LogWarnf("GetSimpleVolView: volume has been marked for deletion: volume(%v) status(%v - 0:normal/1:markDelete)",
			w.VolName, view.Status)
		return proto.ErrVolNotExists
	}

	w.followerRead = view.FollowerRead
	w.maximallyRead = view.MaximallyRead
	w.dpSelectorName = view.DpSelectorName
	w.dpSelectorParm = view.DpSelectorParm
	w.volType = view.VolType
	w.EnablePosixAcl = view.EnablePosixAcl

	w.UpdateUidsView(view)

	log.LogDebugf("GetSimpleVolView: get volume simple info: ID(%v) name(%v) owner(%v) status(%v) capacity(%v) "+
		"metaReplicas(%v) dataReplicas(%v) mpCnt(%v) dpCnt(%v) followerRead(%v) createTime(%v) dpSelectorName(%v) "+
		"dpSelectorParm(%v) uids(%v)",
		view.ID, view.Name, view.Owner, view.Status, view.Capacity, view.MpReplicaNum, view.DpReplicaNum, view.MpCnt,
		view.DpCnt, view.FollowerRead, view.CreateTime, view.DpSelectorName, view.DpSelectorParm, view.Uids)

	return
}

func (w *Wrapper) uploadFlowInfoByTick(clientInfo SimpleClientInfo) {
	ticker := time.NewTicker(5 * time.Second)
	lWork := true
	for {
		select {
		case <-ticker.C:
			if bWork, err := w.UploadFlowInfo(clientInfo, false); err == nil {
				if bWork != lWork {
					if bWork {
						ticker.Reset(5 * time.Second)
					} else {
						ticker.Reset(time.Minute)
					}
					lWork = bWork
				}
			}
		case <-w.stopC:
			return
		}
	}
}

func (w *Wrapper) update(clientInfo SimpleClientInfo) {
	ticker := time.NewTicker(time.Minute)
	taskFunc := func() {
		w.updateSimpleVolView(clientInfo)
		w.updateDataPartition(false)
		w.updateDataNodeStatus()
		w.CheckPermission()
		w.updateVerlist(clientInfo)
	}
	taskFunc()
	for {
		select {
		case <-ticker.C:
			taskFunc()
		case <-w.stopC:
			return
		}
	}
}

func (w *Wrapper) UploadFlowInfo(clientInfo SimpleClientInfo, init bool) (work bool, err error) {
	var (
		limitRsp *proto.LimitRsp2Client
		flowInfo *proto.ClientReportLimitInfo
	)

	flowInfo, work = clientInfo.GetFlowInfo()
	if limitRsp, err = w.mc.AdminAPI().UploadFlowInfo(w.VolName, flowInfo); err != nil {
		log.LogWarnf("UpdateSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.VolName, err)
		return
	}

	if init {
		if limitRsp.ID == 0 {
			err = fmt.Errorf("init client get id 0")
			log.LogInfof("action[UploadFlowInfo] err %v", err.Error())
			return
		}
		log.LogInfof("action[UploadFlowInfo] get id %v", limitRsp.ID)
		clientInfo.SetClientID(limitRsp.ID)
	}
	clientInfo.UpdateFlowInfo(limitRsp)
	return
}

func (w *Wrapper) CheckPermission() {
	if info, err := w.mc.UserAPI().AclOperation(w.VolName, w.LocalIp, util.AclCheckIP); err != nil {
		syslog.Println(err)
	} else if !info.OK {
		syslog.Println(err)
		log.LogFatal("Client Addr not allowed to access CubeFS Cluster!")
	}
}

func (w *Wrapper) updateVerlist(client SimpleClientInfo) (err error) {
	if !w.IsSnapshotEnabled {
		return
	}
	verList, err := w.mc.AdminAPI().GetVerList(w.VolName)
	if err != nil {
		log.LogErrorf("CheckReadVerSeq: get cluster fail: err(%v)", err)
		return err
	}

	if verList == nil {
		msg := fmt.Sprintf("get verList nil, vol [%v] reqd seq [%v]", w.VolName, w.verReadSeq)
		log.LogErrorf("action[CheckReadVerSeq] %v", msg)
		return fmt.Errorf("%v", msg)
	}

	if w.verReadSeq > 0 {
		if _, err = w.CheckReadVerSeq(w.VolName, w.verConfReadSeq, verList); err != nil {
			log.LogFatalf("updateSimpleVolView: readSeq abnormal %v", err)
		}
		return
	}

	log.LogDebugf("updateSimpleVolView.UpdateLatestVer.try update to verlist[%v]", verList)
	if err = client.UpdateLatestVer(verList); err != nil {
		log.LogWarnf("updateSimpleVolView: UpdateLatestVer ver %v faile err %v", verList.GetLastVer(), err)
		return
	}
	return
}

func (w *Wrapper) updateSimpleVolView(clientInfo SimpleClientInfo) (err error) {
	var view *proto.SimpleVolView
	if view, err = w.mc.AdminAPI().GetVolumeSimpleInfo(w.VolName); err != nil {
		log.LogWarnf("updateSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.VolName, err)
		return
	}

	w.UpdateUidsView(view)

	if w.followerRead != view.FollowerRead && !w.followerReadClientCfg {
		log.LogDebugf("UpdateSimpleVolView: update followerRead from old(%v) to new(%v)",
			w.followerRead, view.FollowerRead)
		w.followerRead = view.FollowerRead
	}

	if w.maximallyRead != view.MaximallyRead && !w.maximallyReadClientCfg {
		log.LogDebugf("UpdateSimpleVolView: update maximallyRead from old(%v) to new(%v)",
			w.maximallyRead, view.MaximallyRead)
		w.maximallyRead = view.MaximallyRead
	}

	if w.dpSelectorName != view.DpSelectorName || w.dpSelectorParm != view.DpSelectorParm {
		log.LogDebugf("UpdateSimpleVolView: update dpSelector from old(%v %v) to new(%v %v)",
			w.dpSelectorName, w.dpSelectorParm, view.DpSelectorName, view.DpSelectorParm)
		w.Lock.Lock()
		w.dpSelectorName = view.DpSelectorName
		w.dpSelectorParm = view.DpSelectorParm
		w.dpSelectorChanged = true
		w.Lock.Unlock()
	}
	clientInfo.UpdateRemoteCacheConfig(view)
	return nil
}

func (w *Wrapper) updateDataPartitionByRsp(forceUpdate bool, refreshPolicy RefreshDpPolicy, DataPartitions []*proto.DataPartitionResponse) (err error) {
	convert := func(response *proto.DataPartitionResponse) *DataPartition {
		return &DataPartition{
			DataPartitionResponse: *response,
			ClientWrapper:         w,
		}
	}
	if proto.IsCold(w.volType) || proto.IsStorageClassBlobStore(w.volStorageClass) { // avoid stuck on read from deleted cache-dp
		w.clearPartitions()
	}

	ssdDpCount := 0
	ssdDpWritableCount := 0
	hddDpCount := 0
	hddDpWritableCount := 0
	rwPartitionGroups := make([]*DataPartition, 0)
	for index, partition := range DataPartitions {
		if partition == nil {
			log.LogErrorf("[updateDataPartitionByRsp] index [%v] is nil", index)
			continue
		}
		dp := convert(partition)
		if w.followerRead && w.nearRead {
			dp.NearHosts = w.sortHostsByDistance(dp.Hosts)
		}
		log.LogInfof("[updateDataPartitionByRsp]: dp(%v)", dp)
		w.replaceOrInsertPartition(dp)

		if dp.MediaType == proto.MediaType_SSD {
			ssdDpCount += 1
		} else if dp.MediaType == proto.MediaType_HDD {
			hddDpCount += 1
		}

		// do not insert preload dp in cold vol
		if (proto.IsCold(w.volType) || proto.IsStorageClassBlobStore(w.volStorageClass)) && proto.IsPreLoadDp(dp.PartitionType) {
			continue
		}
		if dp.Status == proto.ReadWrite {
			dp.MetricsRefresh()
			rwPartitionGroups = append(rwPartitionGroups, dp)
			log.LogInfof("updateDataPartition: dpId(%v) mediaType(%v) address(%p) insert to rwPartitionGroups",
				dp.PartitionID, proto.MediaTypeString(dp.MediaType), dp)
			if dp.MediaType == proto.MediaType_SSD {
				ssdDpWritableCount += 1
			} else if dp.MediaType == proto.MediaType_HDD {
				hddDpWritableCount += 1
			}
		}
	}

	// if not forceUpdate, at least keep 1 rw dp in the selector to avoid can't do write
	if forceUpdate || len(rwPartitionGroups) >= 1 {
		log.LogInfof("updateDataPartition: volume(%v) refresh dpSelector, forceUpdate(%v) policy(%v), "+
			"allDp(%v) allWritableDp(%v), SsdDp(%v) SsdWritableDp(%v), hddDp(%v) hddWritableDp(%v)",
			w.VolName, forceUpdate, refreshPolicy, len(DataPartitions), len(rwPartitionGroups),
			ssdDpCount, ssdDpWritableCount, hddDpCount, hddDpWritableCount)
		w.refreshDpSelector(refreshPolicy, rwPartitionGroups)
	} else {
		if len(DataPartitions) == 0 && (proto.IsCold(w.volType) || proto.IsStorageClassBlobStore(w.volStorageClass)) {
			log.LogInfof("updateDataPartition: cold volume with no data partitions")
		} else {
			err = errors.New("updateDataPartition: no writable data partition")
			log.LogWarnf("updateDataPartition: no enough writable data partitions, volume(%v) with %v rw partitions(%v all), forceUpdate(%v)",
				w.VolName, len(rwPartitionGroups), len(DataPartitions), forceUpdate)
		}
	}

	log.LogInfof("updateDataPartition: finish")
	return err
}

func (w *Wrapper) updateDataPartition(isInit bool) (err error) {
	if w.preload {
		return
	}
	var dpv *proto.DataPartitionsView
	if dpv, err = w.mc.ClientAPI().EncodingGzip().GetDataPartitions(w.VolName); err != nil {
		log.LogErrorf("updateDataPartition: get data partitions fail: volume(%v) err(%v)", w.VolName, err)
		return
	}
	log.LogInfof("updateDataPartition: get data partitions: volume(%v) partitions(%v) VolReadOnly(%v)",
		w.VolName, len(dpv.DataPartitions), dpv.VolReadOnly)

	forceUpdate := false
	if isInit || dpv.VolReadOnly {
		forceUpdate = true
	}

	w.Lock.Lock()
	m := make(map[uint32]*proto.StatOfStorageClass)
	for _, st := range dpv.StatByClass {
		m[st.StorageClass] = st
		log.LogInfof("updateDataPartition: get storage class stat info: volume(%v) stat(%s) VolReadOnly(%v)",
			w.VolName, st.String(), dpv.VolReadOnly)
	}
	w.volStatByClass = m
	w.Lock.Unlock()

	return w.updateDataPartitionByRsp(forceUpdate, UpdateDpPolicy, dpv.DataPartitions)
}

func (w *Wrapper) CanWriteByClass(class uint32) bool {
	w.Lock.RLock()
	defer w.Lock.RUnlock()

	if len(w.volStatByClass) == 0 {
		return true
	}

	st := w.volStatByClass[class]
	return !st.Full()
}

func (w *Wrapper) UpdateDataPartition() (err error) {
	return w.updateDataPartition(false)
}

// getDataPartitionFromMaster will call master to get data partition info which not include in  cache updated by
// updateDataPartition which may not take effect if nginx be placed for reduce the pressure of master
func (w *Wrapper) getDataPartitionFromMaster(dpId uint64) (err error) {
	var dpInfo *proto.DataPartitionInfo
	if dpInfo, err = w.mc.AdminAPI().GetDataPartition(w.VolName, dpId); err != nil {
		log.LogErrorf("getDataPartitionFromMaster: get data partitions fail: volume(%v) dpId(%v) err(%v)",
			w.VolName, dpId, err)
		return
	}

	log.LogInfof("getDataPartitionFromMaster: get data partitions: volume(%v), dpId(%v) mediaType(%v)",
		w.VolName, dpId, proto.MediaTypeString(dpInfo.MediaType))
	var leaderAddr string
	for _, replica := range dpInfo.Replicas {
		if replica.IsLeader {
			leaderAddr = replica.Addr
		}
	}

	dpr := new(proto.DataPartitionResponse)
	dpr.PartitionID = dpId
	dpr.Status = dpInfo.Status
	dpr.ReplicaNum = dpInfo.ReplicaNum
	dpr.Hosts = make([]string, len(dpInfo.Hosts))
	copy(dpr.Hosts, dpInfo.Hosts)
	dpr.LeaderAddr = leaderAddr
	dpr.IsRecover = dpInfo.IsRecover
	dpr.IsDiscard = dpInfo.IsDiscard
	dpr.MediaType = dpInfo.MediaType

	DataPartitions := make([]*proto.DataPartitionResponse, 0)
	DataPartitions = append(DataPartitions, dpr)
	return w.updateDataPartitionByRsp(false, MergeDpPolicy, DataPartitions)
}

func (w *Wrapper) clearPartitions() {
	w.Lock.Lock()
	defer w.Lock.Unlock()
	w.partitions = make(map[uint64]*DataPartition)
}

func (w *Wrapper) AllocatePreLoadDataPartition(volName string, count int, capacity, ttl uint64, zones string) (err error) {
	var dpv *proto.DataPartitionsView

	if dpv, err = w.mc.AdminAPI().CreatePreLoadDataPartition(volName, count, capacity, ttl, zones); err != nil {
		log.LogWarnf("CreatePreLoadDataPartition fail: err(%v)", err)
		return
	}
	convert := func(response *proto.DataPartitionResponse) *DataPartition {
		return &DataPartition{
			DataPartitionResponse: *response,
			ClientWrapper:         w,
		}
	}
	rwPartitionGroups := make([]*DataPartition, 0)
	for _, partition := range dpv.DataPartitions {
		dp := convert(partition)
		if (proto.IsCold(w.volType) || proto.IsStorageClassBlobStore(w.volStorageClass)) && !proto.IsPreLoadDp(dp.PartitionType) {
			continue
		}
		log.LogInfof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(dp)
		dp.MetricsRefresh()
		rwPartitionGroups = append(rwPartitionGroups, dp)
	}

	w.refreshDpSelector(MergeDpPolicy, rwPartitionGroups)
	return nil
}

func (w *Wrapper) replaceOrInsertPartition(dp *DataPartition) {
	var oldstatus int8
	w.Lock.Lock()
	old, ok := w.partitions[dp.PartitionID]
	if ok {
		oldstatus = old.Status

		old.Status = dp.Status
		old.ReplicaNum = dp.ReplicaNum
		old.Hosts = dp.Hosts
		old.IsDiscard = dp.IsDiscard
		old.NearHosts = dp.NearHosts

		dp.Metrics = old.Metrics
	} else {
		dp.Metrics = NewDataPartitionMetrics()
	}

	w.partitions[dp.PartitionID] = dp

	w.Lock.Unlock()

	if ok && oldstatus != dp.Status {
		log.LogInfof("partition:dp[%v] address %p status change (%v) -> (%v)", dp.PartitionID, &old, oldstatus, dp.Status)
	}
}

func (w *Wrapper) GetDataPartitionFromMaster(partitionID uint64) (*DataPartition, error) {
	err := w.getDataPartitionFromMaster(partitionID)
	if err == nil {
		dp, ok := w.TryGetPartition(partitionID)
		if !ok {
			return nil, fmt.Errorf("after get from master, partition[%v] not exsit", partitionID)
		}
		return dp, nil
	}
	return nil, fmt.Errorf("get from master failed, partition[%v] not exsit", partitionID)
}

// GetDataPartition returns the data partition based on the given partition ID.
func (w *Wrapper) GetDataPartition(partitionID uint64) (*DataPartition, error) {
	dp, ok := w.TryGetPartition(partitionID)
	if dp.LeaderAddr == "" || (!ok && (!proto.IsCold(w.volType) || proto.IsStorageClassReplica(w.volStorageClass))) { // leaderAddr miss || (cache miss && hot volume)
		return w.GetDataPartitionFromMaster(partitionID)
	}
	if !ok {
		return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
	}
	return dp, nil
}

func (w *Wrapper) GetReadVerSeq() uint64 {
	return w.verReadSeq
}

func (w *Wrapper) CheckReadVerSeq(volName string, verReadSeq uint64, verList *proto.VolVersionInfoList) (readReadVer uint64, err error) {
	w.Lock.RLock()
	defer w.Lock.RUnlock()

	log.LogInfof("action[CheckReadVerSeq] vol [%v] req seq [%v]", volName, verReadSeq)

	readReadVer = verReadSeq
	// Whether it is version 0 or any other version, there may be uncommitted versions between the requested version
	// and the next official version. In this case, the data needs to be read.
	if verReadSeq == math.MaxUint64 {
		verReadSeq = 0
	}

	var (
		id     int
		ver    *proto.VolVersionInfo
		verLen = len(verList.VerList)
	)
	for id, ver = range verList.VerList {
		if id == verLen-1 {
			err = fmt.Errorf("action[CheckReadVerSeq] readReadVer %v not found", readReadVer)
			break
		}
		log.LogInfof("action[CheckReadVerSeq] ver %v,%v", ver.Ver, ver.Status)
		if ver.Ver == verReadSeq {
			if ver.Status != proto.VersionNormal {
				err = fmt.Errorf("action[CheckReadVerSeq] status %v not right", ver.Status)
				return
			}
			readReadVer = verList.VerList[id+1].Ver - 1
			log.LogInfof("action[CheckReadVerSeq] get read ver %v", readReadVer)
			return
		}
	}

	err = fmt.Errorf("not found read ver %v", verReadSeq)
	return
}

// WarningMsg returns the warning message that contains the cluster name.
func (w *Wrapper) WarningMsg() string {
	return fmt.Sprintf("%s_client_warning", w.ClusterName)
}

func (w *Wrapper) updateDataNodeStatus() (err error) {
	var cv *proto.ClusterView
	cv, err = w.mc.AdminAPI().GetCluster(false)
	if err != nil {
		log.LogErrorf("updateDataNodeStatus: get cluster fail: err(%v)", err)
		return
	}

	newHostsStatus := make(map[string]bool)
	for _, node := range cv.DataNodes {
		newHostsStatus[node.Addr] = node.Status
	}
	log.LogInfof("updateDataNodeStatus: update %d hosts status", len(newHostsStatus))

	w.HostsStatus = newHostsStatus

	return
}

func (w *Wrapper) SetNearRead(nearRead bool) {
	w.nearRead = nearRead
	log.LogInfof("SetNearRead: set nearRead to %v", w.nearRead)
}

func (w *Wrapper) NearRead() bool {
	return w.nearRead
}

// Sort hosts by distance form local
func (w *Wrapper) sortHostsByDistance(srcHosts []string) []string {
	hosts := make([]string, len(srcHosts))
	copy(hosts, srcHosts)

	for i := 0; i < len(hosts); i++ {
		for j := i + 1; j < len(hosts); j++ {
			if distanceFromLocal(hosts[i]) > distanceFromLocal(hosts[j]) {
				hosts[i], hosts[j] = hosts[j], hosts[i]
			}
		}
	}
	return hosts
}

func distanceFromLocal(b string) int {
	remote := strings.Split(b, ":")[0]

	return iputil.GetDistance(net.ParseIP(LocalIP), net.ParseIP(remote))
}
