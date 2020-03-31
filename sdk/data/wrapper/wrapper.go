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
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
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
	masters               []string
	partitions            map[uint64]*DataPartition
	rwPartition           []*DataPartition
	localLeaderPartitions []*DataPartition
	followerRead          bool
	mc                    *masterSDK.MasterClient
	stopOnce              sync.Once
	stopC                 chan struct{}
}

// NewDataPartitionWrapper returns a new data partition wrapper.
func NewDataPartitionWrapper(volName string, masters []string) (w *Wrapper, err error) {
	w = new(Wrapper)
	w.stopC = make(chan struct{})
	w.masters = masters
	w.mc = masterSDK.NewMasterClient(masters, false)
	w.volName = volName
	w.rwPartition = make([]*DataPartition, 0)
	w.partitions = make(map[uint64]*DataPartition)
	if err = w.updateClusterInfo(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.getSimpleVolView(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.updateDataPartition(true); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	go w.update()
	return
}

func (w *Wrapper) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopC)
	})
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

	log.LogInfof("getSimpleVolView: get volume simple info: ID(%v) name(%v) owner(%v) status(%v) capacity(%v) "+
		"metaReplicas(%v) dataReplicas(%v) mpCnt(%v) dpCnt(%v) followerRead(%v) createTime(%v)",
		view.ID, view.Name, view.Owner, view.Status, view.Capacity, view.MpReplicaNum, view.DpReplicaNum, view.MpCnt,
		view.DpCnt, view.FollowerRead, view.CreateTime)
	return nil
}

func (w *Wrapper) update() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			w.updateDataPartition(false)
		case <-w.stopC:
			return
		}
	}
}

func (w *Wrapper) updateDataPartition(isInit bool) (err error) {

	var dpv *proto.DataPartitionsView
	if dpv, err = w.mc.ClientAPI().GetDataPartitions(w.volName); err != nil {
		log.LogErrorf("updateDataPartition: get data partitions fail: volume(%v) err(%v)", w.volName, err)
		return
	}
	log.LogInfof("updateDataPartition: get data partitions: volume(%v) partitions(%v)", w.volName, len(dpv.DataPartitions))

	var convert = func(response *proto.DataPartitionResponse) *DataPartition {
		return &DataPartition{DataPartitionResponse: *response}
	}

	rwPartitionGroups := make([]*DataPartition, 0)
	localLeaderPartitionGroups := make([]*DataPartition, 0)
	for _, partition := range dpv.DataPartitions {
		dp := convert(partition)
		log.LogInfof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(dp)
		if dp.Status == proto.ReadWrite {
			rwPartitionGroups = append(rwPartitionGroups, dp)
			if strings.Split(dp.Hosts[0], ":")[0] == LocalIP {
				localLeaderPartitionGroups = append(localLeaderPartitionGroups, dp)
			}
		}
	}

	// isInit used to identify whether this call is caused by mount action
	if isInit || (len(rwPartitionGroups) >= MinWriteAbleDataPartitionCnt) {
		w.rwPartition = rwPartitionGroups
		w.localLeaderPartitions = localLeaderPartitionGroups
	} else {
		err = errors.New("updateDataPartition: no writable data partition")
	}

	log.LogInfof("updateDataPartition: finish")
	return err
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
	} else {
		dp.Metrics = NewDataPartitionMetrics()
		w.partitions[dp.PartitionID] = dp
	}

	w.Unlock()

	if ok && oldstatus != dp.Status {
		log.LogInfof("partition: status change (%v) -> (%v)", old, dp)
	}
}

func (w *Wrapper) getRandomDataPartition(partitions []*DataPartition, exclude map[string]struct{}) *DataPartition {
	var dp *DataPartition

	if len(partitions) == 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(partitions))
	dp = partitions[index]
	if !isExcluded(dp, exclude) {
		return dp
	}

	for _, dp = range partitions {
		if !isExcluded(dp, exclude) {
			return dp
		}
	}
	return nil
}

func (w *Wrapper) getLocalLeaderDataPartition(exclude map[string]struct{}) *DataPartition {
	w.RLock()
	localLeaderPartitions := w.localLeaderPartitions
	w.RUnlock()
	return w.getRandomDataPartition(localLeaderPartitions, exclude)
}

// GetDataPartitionForWrite returns an available data partition for write.
func (w *Wrapper) GetDataPartitionForWrite(exclude map[string]struct{}) (*DataPartition, error) {
	dp := w.getLocalLeaderDataPartition(exclude)
	if dp != nil {
		return dp, nil
	}

	w.RLock()
	rwPartitionGroups := w.rwPartition
	w.RUnlock()

	dp = w.getRandomDataPartition(rwPartitionGroups, exclude)
	if dp != nil {
		return dp, nil
	}

	return nil, fmt.Errorf("no writable data partition")
}

// GetDataPartition returns the data partition based on the given partition ID.
func (w *Wrapper) GetDataPartition(partitionID uint64) (*DataPartition, error) {
	w.RLock()
	defer w.RUnlock()
	dp, ok := w.partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
	}
	return dp, nil
}

// WarningMsg returns the warning message that contains the cluster name.
func (w *Wrapper) WarningMsg() string {
	return fmt.Sprintf("%s_client_warning", w.clusterName)
}
