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
	"context"
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
	"github.com/cubefs/cubefs/util/ump"
)

var (
	LocalIP                             string
	DefaultMinWriteAbleDataPartitionCnt = 10
)

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

type SimpleClientInfo interface {
	GetFlowInfo(ctx context.Context) (*proto.ClientReportLimitInfo, bool)
	UpdateFlowInfo(ctx context.Context, limit *proto.LimitRsp2Client)
	SetClientID(id uint64) error
	UpdateLatestVer(ctx context.Context, verList *proto.VolVersionInfoList) error
	GetReadVer() uint64
	GetLatestVer() uint64
	GetVerMgr() *proto.VolVersionInfoList
}

// Wrapper TODO rename. This name does not reflect what it is doing.
type Wrapper struct {
	Lock                  sync.RWMutex
	clusterName           string
	volName               string
	volType               int
	EnablePosixAcl        bool
	masters               []string
	partitions            map[uint64]*DataPartition
	followerRead          bool
	followerReadClientCfg bool
	nearRead              bool
	dpSelectorChanged     bool
	dpSelectorName        string
	dpSelectorParm        string
	mc                    *masterSDK.MasterClient
	stopOnce              sync.Once
	stopC                 chan struct{}

	dpSelector DataPartitionSelector

	HostsStatus map[string]bool
	Uids        map[uint32]*proto.UidSimpleInfo
	UidLock     sync.RWMutex
	preload     bool
	LocalIp     string

	minWriteAbleDataPartitionCnt int
	verConfReadSeq               uint64
	verReadSeq                   uint64
	SimpleClient                 SimpleClientInfo
}

func (w *Wrapper) GetMasterClient() *masterSDK.MasterClient {
	return w.mc
}

// NewDataPartitionWrapper returns a new data partition wrapper.
func NewDataPartitionWrapper(ctx context.Context, client SimpleClientInfo, volName string, masters []string, preload bool, minWriteAbleDataPartitionCnt int, verReadSeq uint64) (w *Wrapper, err error) {
	span := proto.SpanFromContext(ctx)
	span.Infof("action[NewDataPartitionWrapper] verReadSeq %v", verReadSeq)

	w = new(Wrapper)
	w.stopC = make(chan struct{})
	w.masters = masters
	w.mc = masterSDK.NewMasterClient(masters, false)
	w.volName = volName
	w.partitions = make(map[uint64]*DataPartition)
	w.HostsStatus = make(map[string]bool)
	w.preload = preload

	w.minWriteAbleDataPartitionCnt = minWriteAbleDataPartitionCnt
	if w.minWriteAbleDataPartitionCnt < 0 {
		w.minWriteAbleDataPartitionCnt = DefaultMinWriteAbleDataPartitionCnt
	}

	if w.LocalIp, err = ump.GetLocalIpAddr(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}

	if err = w.updateClusterInfo(ctx); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}

	if err = w.GetSimpleVolView(ctx); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}

	w.UploadFlowInfo(ctx, client, true)

	if err = w.initDpSelector(ctx); err != nil {
		span.Errorf("NewDataPartitionWrapper: init initDpSelector failed, [%v]", err)
	}
	if err = w.updateDataPartition(ctx, true); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.updateDataNodeStatus(ctx); err != nil {
		span.Errorf("NewDataPartitionWrapper: init DataNodeStatus failed, [%v]", err)
	}
	w.verConfReadSeq = verReadSeq
	if verReadSeq > 0 {
		var verList *proto.VolVersionInfoList
		if verList, err = w.mc.AdminAPI().GetVerList(ctx, volName); err != nil {
			return
		}
		if verReadSeq, err = w.CheckReadVerSeq(ctx, volName, verReadSeq, verList); err != nil {
			span.Errorf("NewDataPartitionWrapper: init Read with ver [%v] error [%v]", verReadSeq, err)
			return
		}
	}
	w.verReadSeq = verReadSeq
	w.SimpleClient = client
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

func (w *Wrapper) tryGetPartition(index uint64) (partition *DataPartition, ok bool) {
	w.Lock.RLock()
	defer w.Lock.RUnlock()
	partition, ok = w.partitions[index]
	return
}

func (w *Wrapper) updateClusterInfo(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	var info *proto.ClusterInfo
	if info, err = w.mc.AdminAPI().GetClusterInfo(ctx); err != nil {
		span.Warnf("UpdateClusterInfo: get cluster info fail: err(%v)", err)
		return
	}
	span.Infof("UpdateClusterInfo: get cluster info: cluster(%v) localIP(%v)", info.Cluster, info.Ip)
	w.clusterName = info.Cluster
	LocalIP = info.Ip
	return
}

func (w *Wrapper) UpdateUidsView(ctx context.Context, view *proto.SimpleVolView) {
	span := proto.SpanFromContext(ctx)
	w.UidLock.Lock()
	defer w.UidLock.Unlock()
	w.Uids = make(map[uint32]*proto.UidSimpleInfo)
	for _, uid := range view.Uids {
		if !uid.Limited {
			continue
		}
		w.Uids[uid.UID] = &uid
	}
	span.Debugf("uid info be updated to %v", view.Uids)
}

func (w *Wrapper) GetSimpleVolView(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	var view *proto.SimpleVolView

	if view, err = w.mc.AdminAPI().GetVolumeSimpleInfo(ctx, w.volName); err != nil {
		span.Warnf("GetSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.volName, err)
		return
	}

	if view.Status == 1 {
		span.Warnf("GetSimpleVolView: volume has been marked for deletion: volume(%v) status(%v - 0:normal/1:markDelete)",
			w.volName, view.Status)
		return proto.ErrVolNotExists
	}

	w.followerRead = view.FollowerRead
	w.dpSelectorName = view.DpSelectorName
	w.dpSelectorParm = view.DpSelectorParm
	w.volType = view.VolType
	w.EnablePosixAcl = view.EnablePosixAcl
	w.UpdateUidsView(ctx, view)

	span.Debugf("GetSimpleVolView: get volume simple info: ID(%v) name(%v) owner(%v) status(%v) capacity(%v) "+
		"metaReplicas(%v) dataReplicas(%v) mpCnt(%v) dpCnt(%v) followerRead(%v) createTime(%v) dpSelectorName(%v) "+
		"dpSelectorParm(%v) uids(%v)",
		view.ID, view.Name, view.Owner, view.Status, view.Capacity, view.MpReplicaNum, view.DpReplicaNum, view.MpCnt,
		view.DpCnt, view.FollowerRead, view.CreateTime, view.DpSelectorName, view.DpSelectorParm, view.Uids)

	return
}

func (w *Wrapper) uploadFlowInfoByTick(clientInfo SimpleClientInfo) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	roundCtx := proto.RoundContext("warpper-flowinfo")
	for {
		select {
		case <-ticker.C:
			w.UploadFlowInfo(roundCtx(), clientInfo, false)
		case <-w.stopC:
			return
		}
	}
}

func (w *Wrapper) update(clientInfo SimpleClientInfo) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	taskFunc := func(ctx context.Context) {
		w.updateSimpleVolView(ctx)
		w.updateDataPartition(ctx, false)
		w.updateDataNodeStatus(ctx)
		w.CheckPermission(ctx)
		w.updateVerlist(ctx, clientInfo)
	}
	roundCtx := proto.RoundContext("warpper-update")
	for {
		taskFunc(roundCtx())
		select {
		case <-ticker.C:
		case <-w.stopC:
			return
		}
	}
}

func (w *Wrapper) UploadFlowInfo(ctx context.Context, clientInfo SimpleClientInfo, init bool) (err error) {
	span := proto.SpanFromContext(ctx)
	var limitRsp *proto.LimitRsp2Client

	flowInfo, isNeedReport := clientInfo.GetFlowInfo(ctx)
	if !isNeedReport {
		span.Debugf("action[UploadFlowInfo] no need report!")
		return nil
	}

	if limitRsp, err = w.mc.AdminAPI().UploadFlowInfo(ctx, w.volName, flowInfo); err != nil {
		span.Warnf("UpdateSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.volName, err)
		return
	}

	if init {
		if limitRsp.ID == 0 {
			err = fmt.Errorf("init client get id 0")
			span.Infof("action[UploadFlowInfo] err %v", err.Error())
			return
		}
		span.Infof("action[UploadFlowInfo] get id %v", limitRsp.ID)
		clientInfo.SetClientID(limitRsp.ID)
	}
	clientInfo.UpdateFlowInfo(ctx, limitRsp)
	return
}

func (w *Wrapper) CheckPermission(ctx context.Context) {
	span := proto.SpanFromContext(ctx)
	if info, err := w.mc.UserAPI().AclOperation(ctx, w.volName, w.LocalIp, util.AclCheckIP); err != nil {
		syslog.Println(err)
	} else if !info.OK {
		syslog.Println(err)
		span.Fatalf("Client Addr not allowed to access CubeFS Cluster!")
	}
}

func (w *Wrapper) updateVerlist(ctx context.Context, client SimpleClientInfo) (err error) {
	span := proto.SpanFromContext(ctx)
	verList, err := w.mc.AdminAPI().GetVerList(ctx, w.volName)
	if err != nil {
		span.Errorf("CheckReadVerSeq: get cluster fail: err(%v)", err)
		return err
	}

	if verList == nil {
		msg := fmt.Sprintf("get verList nil, vol [%v] reqd seq [%v]", w.volName, w.verReadSeq)
		span.Errorf("action[CheckReadVerSeq] %v", msg)
		return fmt.Errorf("%v", msg)
	}

	if w.verReadSeq > 0 {
		if _, err = w.CheckReadVerSeq(ctx, w.volName, w.verConfReadSeq, verList); err != nil {
			span.Fatalf("updateSimpleVolView: readSeq abnormal %v", err)
		}
		return
	}

	span.Debugf("updateSimpleVolView.UpdateLatestVer.try update to verlist[%v]", verList)
	if err = client.UpdateLatestVer(ctx, verList); err != nil {
		span.Warnf("updateSimpleVolView: UpdateLatestVer ver %v faile err %v", verList.GetLastVer(), err)
		return
	}
	return
}

func (w *Wrapper) updateSimpleVolView(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	var view *proto.SimpleVolView
	if view, err = w.mc.AdminAPI().GetVolumeSimpleInfo(ctx, w.volName); err != nil {
		span.Warnf("updateSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.volName, err)
		return
	}

	w.UpdateUidsView(ctx, view)

	if w.followerRead != view.FollowerRead && !w.followerReadClientCfg {
		span.Debugf("UpdateSimpleVolView: update followerRead from old(%v) to new(%v)",
			w.followerRead, view.FollowerRead)
		w.followerRead = view.FollowerRead
	}

	if w.dpSelectorName != view.DpSelectorName || w.dpSelectorParm != view.DpSelectorParm {
		span.Debugf("UpdateSimpleVolView: update dpSelector from old(%v %v) to new(%v %v)",
			w.dpSelectorName, w.dpSelectorParm, view.DpSelectorName, view.DpSelectorParm)
		w.Lock.Lock()
		w.dpSelectorName = view.DpSelectorName
		w.dpSelectorParm = view.DpSelectorParm
		w.dpSelectorChanged = true
		w.Lock.Unlock()
	}

	return nil
}

func (w *Wrapper) updateDataPartitionByRsp(ctx context.Context, isInit bool, DataPartitions []*proto.DataPartitionResponse) (err error) {
	span := proto.SpanFromContext(ctx)
	convert := func(response *proto.DataPartitionResponse) *DataPartition {
		return &DataPartition{
			DataPartitionResponse: *response,
			ClientWrapper:         w,
		}
	}

	if proto.IsCold(w.volType) {
		w.clearPartitions()
	}
	rwPartitionGroups := make([]*DataPartition, 0)
	for index, partition := range DataPartitions {
		if partition == nil {
			span.Errorf("action[updateDataPartitionByRsp] index [%v] is nil", index)
			continue
		}
		dp := convert(partition)
		if w.followerRead && w.nearRead {
			dp.NearHosts = w.sortHostsByDistance(dp.Hosts)
		}
		span.Infof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(ctx, dp)
		// do not insert preload dp in cold vol
		if proto.IsCold(w.volType) && proto.IsPreLoadDp(dp.PartitionType) {
			continue
		}
		if dp.Status == proto.ReadWrite {
			dp.MetricsRefresh()
			rwPartitionGroups = append(rwPartitionGroups, dp)
			span.Infof("updateDataPartition: dp(%v) address(%p) insert to rwPartitionGroups", dp.PartitionID, dp)
		}
	}

	// isInit used to identify whether this call is caused by mount action
	if isInit || len(rwPartitionGroups) >= w.minWriteAbleDataPartitionCnt || (proto.IsCold(w.volType) && (len(rwPartitionGroups) >= 1)) {
		span.Infof("updateDataPartition: refresh dpSelector of volume(%v) with %v rw partitions(%v all), isInit(%v), minWriteAbleDataPartitionCnt(%v)",
			w.volName, len(rwPartitionGroups), len(DataPartitions), isInit, w.minWriteAbleDataPartitionCnt)
		w.refreshDpSelector(ctx, rwPartitionGroups)
	} else {
		err = errors.New("updateDataPartition: no writable data partition")
		span.Warnf("updateDataPartition: no enough writable data partitions, volume(%v) with %v rw partitions(%v all), isInit(%v), minWriteAbleDataPartitionCnt(%v)",
			w.volName, len(rwPartitionGroups), len(DataPartitions), isInit, w.minWriteAbleDataPartitionCnt)
	}

	span.Infof("updateDataPartition: finish")
	return err
}

func (w *Wrapper) updateDataPartition(ctx context.Context, isInit bool) (err error) {
	span := proto.SpanFromContext(ctx)
	if w.preload {
		return
	}
	var dpv *proto.DataPartitionsView
	if dpv, err = w.mc.ClientAPI().EncodingGzip().GetDataPartitions(ctx, w.volName); err != nil {
		span.Errorf("updateDataPartition: get data partitions fail: volume(%v) err(%v)", w.volName, err)
		return
	}
	span.Infof("updateDataPartition: get data partitions: volume(%v) partitions(%v)", w.volName, len(dpv.DataPartitions))
	return w.updateDataPartitionByRsp(ctx, isInit, dpv.DataPartitions)
}

func (w *Wrapper) UpdateDataPartition(ctx context.Context) (err error) {
	return w.updateDataPartition(ctx, false)
}

// getDataPartitionFromMaster will call master to get data partition info which not include in  cache updated by
// updateDataPartition which may not take effect if nginx be placed for reduce the pressure of master
func (w *Wrapper) getDataPartitionFromMaster(ctx context.Context, isInit bool, dpId uint64) (err error) {
	span := proto.SpanFromContext(ctx)
	var dpInfo *proto.DataPartitionInfo
	if dpInfo, err = w.mc.AdminAPI().GetDataPartition(ctx, w.volName, dpId); err != nil {
		span.Errorf("getDataPartitionFromMaster: get data partitions fail: volume(%v) dpId(%v) err(%v)",
			w.volName, dpId, err)
		return
	}

	span.Infof("getDataPartitionFromMaster: get data partitions: volume(%v), dpId(%v)", w.volName, dpId)
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

	DataPartitions := make([]*proto.DataPartitionResponse, 1)
	DataPartitions = append(DataPartitions, dpr)
	return w.updateDataPartitionByRsp(ctx, isInit, DataPartitions)
}

func (w *Wrapper) clearPartitions() {
	w.Lock.Lock()
	defer w.Lock.Unlock()
	w.partitions = make(map[uint64]*DataPartition)
}

func (w *Wrapper) AllocatePreLoadDataPartition(ctx context.Context, volName string, count int, capacity, ttl uint64, zones string) (err error) {
	span := proto.SpanFromContext(ctx)
	var dpv *proto.DataPartitionsView

	if dpv, err = w.mc.AdminAPI().CreatePreLoadDataPartition(ctx, volName, count, capacity, ttl, zones); err != nil {
		span.Warnf("CreatePreLoadDataPartition fail: err(%v)", err)
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
		if proto.IsCold(w.volType) && !proto.IsPreLoadDp(dp.PartitionType) {
			continue
		}
		span.Infof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(ctx, dp)
		dp.MetricsRefresh()
		rwPartitionGroups = append(rwPartitionGroups, dp)
	}

	w.refreshDpSelector(ctx, rwPartitionGroups)
	return nil
}

func (w *Wrapper) replaceOrInsertPartition(ctx context.Context, dp *DataPartition) {
	span := proto.SpanFromContext(ctx)
	var oldstatus int8
	w.Lock.Lock()
	old, ok := w.partitions[dp.PartitionID]
	if ok {
		oldstatus = old.Status

		old.Status = dp.Status
		old.ReplicaNum = dp.ReplicaNum
		old.Hosts = dp.Hosts
		old.IsDiscard = dp.IsDiscard
		old.NearHosts = dp.Hosts

		dp.Metrics = old.Metrics
	} else {
		dp.Metrics = NewDataPartitionMetrics()
		w.partitions[dp.PartitionID] = dp
	}

	w.Lock.Unlock()

	if ok && oldstatus != dp.Status {
		span.Infof("partition:dp[%v] address %p status change (%v) -> (%v)", dp.PartitionID, &old, oldstatus, dp.Status)
	}
}

// GetDataPartition returns the data partition based on the given partition ID.
func (w *Wrapper) GetDataPartition(ctx context.Context, partitionID uint64) (*DataPartition, error) {
	dp, ok := w.tryGetPartition(partitionID)
	if !ok && !proto.IsCold(w.volType) { // cache miss && hot volume
		err := w.getDataPartitionFromMaster(ctx, false, partitionID)
		if err == nil {
			dp, ok = w.tryGetPartition(partitionID)
			if !ok {
				return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
			}
			return dp, nil
		}
		return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
	}
	if !ok {
		return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
	}
	return dp, nil
}

func (w *Wrapper) GetReadVerSeq() uint64 {
	return w.verReadSeq
}

func (w *Wrapper) CheckReadVerSeq(ctx context.Context, volName string, verReadSeq uint64, verList *proto.VolVersionInfoList) (readReadVer uint64, err error) {
	span := proto.SpanFromContext(ctx)
	w.Lock.RLock()
	defer w.Lock.RUnlock()

	span.Infof("action[CheckReadVerSeq] vol [%v] req seq [%v]", volName, verReadSeq)

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
		span.Infof("action[CheckReadVerSeq] ver %v,%v", ver.Ver, ver.Status)
		if ver.Ver == verReadSeq {
			if ver.Status != proto.VersionNormal {
				err = fmt.Errorf("action[CheckReadVerSeq] status %v not right", ver.Status)
				return
			}
			readReadVer = verList.VerList[id+1].Ver - 1
			span.Infof("action[CheckReadVerSeq] get read ver %v", readReadVer)
			return
		}
	}

	err = fmt.Errorf("not found read ver %v", verReadSeq)
	return
}

// WarningMsg returns the warning message that contains the cluster name.
func (w *Wrapper) WarningMsg() string {
	return fmt.Sprintf("%s_client_warning", w.clusterName)
}

func (w *Wrapper) updateDataNodeStatus(ctx context.Context) (err error) {
	span := proto.SpanFromContext(ctx)
	var cv *proto.ClusterView
	cv, err = w.mc.AdminAPI().GetCluster(ctx)
	if err != nil {
		span.Errorf("updateDataNodeStatus: get cluster fail: err(%v)", err)
		return
	}

	newHostsStatus := make(map[string]bool)
	for _, node := range cv.DataNodes {
		newHostsStatus[node.Addr] = node.IsActive
	}
	span.Infof("updateDataNodeStatus: update %d hosts status", len(newHostsStatus))

	w.HostsStatus = newHostsStatus

	return
}

func (w *Wrapper) SetNearRead(ctx context.Context, nearRead bool) {
	span := proto.SpanFromContext(ctx)
	w.nearRead = nearRead
	span.Infof("SetNearRead: set nearRead to %v", w.nearRead)
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
