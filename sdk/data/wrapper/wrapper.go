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

package wrapper

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
)

var (
	LocalIP                      string
	MinWriteAbleDataPartitionCnt = 10
)

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

// Wrapper TODO rename. This name does not reflect what it is doing.
type Wrapper struct {
	sync.RWMutex
	clusterName           string
	volName               string
	volType               int
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
	preload     bool
}

// NewDataPartitionWrapper returns a new data partition wrapper.
func NewDataPartitionWrapper(volName string, masters []string, preload bool) (w *Wrapper, err error) {
	w = new(Wrapper)
	w.stopC = make(chan struct{})
	w.masters = masters
	w.mc = masterSDK.NewMasterClient(masters, false)
	w.volName = volName
	w.partitions = make(map[uint64]*DataPartition)
	w.HostsStatus = make(map[string]bool)
	w.preload = preload
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
	if err = w.updateDataNodeStatus(); err != nil {
		log.LogErrorf("NewDataPartitionWrapper: init DataNodeStatus failed, [%v]", err)
	}
	go w.update()
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

func (w *Wrapper) updateClusterInfo() (err error) {
	var info *proto.ClusterInfo
	if info, err = w.mc.AdminAPI().GetClusterInfo(); err != nil {
		log.LogWarnf("UpdateClusterInfo: get cluster info fail: err(%v)", err)
		return
	}
	log.LogInfof("UpdateClusterInfo: get cluster info: cluster(%v) localIP(%v)", info.Cluster, info.Ip)
	w.clusterName = info.Cluster
	LocalIP = info.Ip
	return
}

func (w *Wrapper) getSimpleVolView() (err error) {
	var view *proto.SimpleVolView

	if view, err = w.mc.AdminAPI().GetVolumeSimpleInfo(w.volName); err != nil {
		log.LogWarnf("getSimpleVolView: get volume simple info fail: volume(%v) err(%v)", w.volName, err)
		return
	}
	w.followerRead = view.FollowerRead
	w.dpSelectorName = view.DpSelectorName
	w.dpSelectorParm = view.DpSelectorParm
	w.volType = view.VolType
	log.LogInfof("getSimpleVolView: get volume simple info: ID(%v) name(%v) owner(%v) status(%v) capacity(%v) "+
		"metaReplicas(%v) dataReplicas(%v) mpCnt(%v) dpCnt(%v) followerRead(%v) createTime(%v) dpSelectorName(%v) "+
		"dpSelectorParm(%v)",
		view.ID, view.Name, view.Owner, view.Status, view.Capacity, view.MpReplicaNum, view.DpReplicaNum, view.MpCnt,
		view.DpCnt, view.FollowerRead, view.CreateTime, view.DpSelectorName, view.DpSelectorParm)
	return nil
}

func (w *Wrapper) update() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			w.updateSimpleVolView()
			w.updateDataPartition(false)
			w.updateDataNodeStatus()
		case <-w.stopC:
			return
		}
	}
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

	if w.dpSelectorName != view.DpSelectorName || w.dpSelectorParm != view.DpSelectorParm {
		log.LogInfof("updateSimpleVolView: update dpSelector from old(%v %v) to new(%v %v)",
			w.dpSelectorName, w.dpSelectorParm, view.DpSelectorName, view.DpSelectorParm)
		w.Lock()
		w.dpSelectorName = view.DpSelectorName
		w.dpSelectorParm = view.DpSelectorParm
		w.dpSelectorChanged = true
		w.Unlock()
	}

	return nil
}
func (w *Wrapper) updateDataPartitionByRsp(isInit bool, DataPartitions []*proto.DataPartitionResponse) (err error) {

	var convert = func(response *proto.DataPartitionResponse) *DataPartition {
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
			log.LogErrorf("action[updateDataPartitionByRsp] index [%v] is nil", index)
			continue
		}
		dp := convert(partition)
		if w.followerRead && w.nearRead {
			dp.NearHosts = w.sortHostsByDistance(dp.Hosts)
		}
		log.LogInfof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(dp)
		if dp.Status == proto.ReadWrite {
			dp.MetricsRefresh()
			rwPartitionGroups = append(rwPartitionGroups, dp)
		}
	}

	// isInit used to identify whether this call is caused by mount action
	if isInit || (len(rwPartitionGroups) >= MinWriteAbleDataPartitionCnt || (proto.IsCold(w.volType) && (len(rwPartitionGroups) >= 1))){
		w.refreshDpSelector(rwPartitionGroups)
	} else {
		err = errors.New("updateDataPartition: no writable data partition")
	}

	log.LogInfof("updateDataPartition: finish")
	return err
}

func (w *Wrapper) updateDataPartition(isInit bool) (err error) {
	if w.preload {
		return
	}
	var dpv *proto.DataPartitionsView
	if dpv, err = w.mc.ClientAPI().GetDataPartitions(w.volName); err != nil {
		log.LogErrorf("updateDataPartition: get data partitions fail: volume(%v) err(%v)", w.volName, err)
		return
	}
	log.LogInfof("updateDataPartition: get data partitions: volume(%v) partitions(%v)", w.volName, len(dpv.DataPartitions))
	return w.updateDataPartitionByRsp(isInit, dpv.DataPartitions)
}

func (w *Wrapper) UpdateDataPartition() (err error) {
	return w.updateDataPartition(false)
}

// getDataPartition will call master to get data partition info which not include in  cache updated by
// updateDataPartition which may not take effect if nginx be placed for reduce the pressure of master
func (w *Wrapper) getDataPartition(isInit bool, dpId uint64) (err error) {

	var dpInfo *proto.DataPartitionInfo
	if dpInfo, err = w.mc.AdminAPI().GetDataPartition(w.volName, dpId); err != nil {
		log.LogErrorf("getDataPartition: get data partitions fail: volume(%v) err(%v)", w.volName, err)
		return
	}

	log.LogInfof("getDataPartition: get data partitions: volume(%v)", w.volName)
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

	DataPartitions := make([]*proto.DataPartitionResponse, 1)
	DataPartitions = append(DataPartitions, dpr)
	return w.updateDataPartitionByRsp(isInit, DataPartitions)
}

func (w *Wrapper) clearPartitions() {
	w.Lock()
	defer w.Unlock()
	w.partitions = make(map[uint64]*DataPartition)
}

func (w *Wrapper) AllocatePreLoadDataPartition(volName string, count int, capacity, ttl uint64, zones string) (err error) {
	var dpv *proto.DataPartitionsView

	if dpv, err = w.mc.AdminAPI().CreatePreLoadDataPartition(volName, count, capacity, ttl, zones); err != nil {
		log.LogWarnf("CreatePreLoadDataPartition fail: err(%v)", err)
		return
	}
	var convert = func(response *proto.DataPartitionResponse) *DataPartition {
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
		log.LogInfof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(dp)
		dp.MetricsRefresh()
		rwPartitionGroups = append(rwPartitionGroups, dp)
	}

	w.refreshDpSelector(rwPartitionGroups)
	return nil
}

func (w *Wrapper) replaceOrInsertPartition(dp *DataPartition) {
	var (
		oldstatus int8
	)
	w.Lock()
	old, ok := w.partitions[dp.PartitionID]
	if ok {
		oldstatus = old.Status
		old.Status = dp.Status
		old.ReplicaNum = dp.ReplicaNum
		old.Hosts = dp.Hosts
		old.NearHosts = dp.Hosts
		dp.Metrics = old.Metrics
	} else {
		dp.Metrics = NewDataPartitionMetrics()
		w.partitions[dp.PartitionID] = dp
	}

	w.Unlock()

	if ok && oldstatus != dp.Status {
		log.LogInfof("partition: status change (%v) -> (%v)", old, dp)
	}
}

// GetDataPartition returns the data partition based on the given partition ID.
func (w *Wrapper) GetDataPartition(partitionID uint64) (*DataPartition, error) {
	w.RLock()
	defer w.RUnlock()
	dp, ok := w.partitions[partitionID]
	if !ok && !proto.IsCold(w.volType) { // cache miss && hot volume
		err := w.getDataPartition(false, partitionID)
		if err == nil {
			dp, ok = w.partitions[partitionID]
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

// WarningMsg returns the warning message that contains the cluster name.
func (w *Wrapper) WarningMsg() string {
	return fmt.Sprintf("%s_client_warning", w.clusterName)
}

func (w *Wrapper) updateDataNodeStatus() (err error) {
	var cv *proto.ClusterView
	cv, err = w.mc.AdminAPI().GetCluster()
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
