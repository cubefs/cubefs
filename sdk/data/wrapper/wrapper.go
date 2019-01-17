// Copyright 2018 The Container File System Authors.
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
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

var (
	MasterHelper = util.NewMasterHelper()
)

var (
	LocalIP string
)

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

type Wrapper struct {
	sync.RWMutex
	clusterName           string
	volName               string
	masters               []string
	partitions            map[uint64]*DataPartition
	rwPartition           []*DataPartition
	localLeaderPartitions []*DataPartition
}

func NewDataPartitionWrapper(volName, masterHosts string) (w *Wrapper, err error) {
	masters := strings.Split(masterHosts, ",")
	w = new(Wrapper)
	w.masters = masters
	for _, m := range w.masters {
		MasterHelper.AddNode(m)
	}
	w.volName = volName
	w.rwPartition = make([]*DataPartition, 0)
	w.partitions = make(map[uint64]*DataPartition)
	if err = w.updateClusterInfo(); err != nil {
		err = errors.Annotate(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.updateDataPartition(); err != nil {
		err = errors.Annotate(err, "NewDataPartitionWrapper:")
		return
	}
	go w.update()
	return
}

func (w *Wrapper) GetClusterName() string {
	return w.clusterName
}
func (w *Wrapper) updateClusterInfo() error {
	masterHelper := util.NewMasterHelper()
	for _, ip := range w.masters {
		masterHelper.AddNode(ip)
	}
	body, err := masterHelper.Request(http.MethodPost, proto.AdminGetIP, nil, nil)
	if err != nil {
		log.LogWarnf("UpdateClusterInfo request: err(%v)", err)
		return err
	}

	info := new(proto.ClusterInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogWarnf("UpdateClusterInfo unmarshal: err(%v)", err)
		return err
	}
	log.LogInfof("ClusterInfo: %v", *info)
	w.clusterName = info.Cluster
	LocalIP = info.Ip
	return nil
}

func (w *Wrapper) update() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			w.updateDataPartition()
		}
	}
}

func (w *Wrapper) updateDataPartition() error {
	paras := make(map[string]string, 0)
	paras["name"] = w.volName
	msg, err := MasterHelper.Request(http.MethodGet, proto.ClientDataPartitions, paras, nil)
	if err != nil {
		return errors.Annotate(err, "updateDataPartition: request to master failed!")
	}

	log.LogInfof("updateDataPartition: msg(%v)", string(msg))

	view := &DataPartitionView{}
	if err = json.Unmarshal(msg, view); err != nil {
		return errors.Annotatef(err, "updateDataPartition: unmarshal failed, msg(%v)", msg)
	}

	rwPartitionGroups := make([]*DataPartition, 0)
	localLeaderPartitionGroups := make([]*DataPartition, 0)
	for _, dp := range view.DataPartitions {
		if dp.Status == proto.ReadWrite {
			rwPartitionGroups = append(rwPartitionGroups, dp)
		}
	}

	for _, dp := range view.DataPartitions {
		w.replaceOrInsertPartition(dp)
	}
	partitions := make([]*DataPartition, 0)
	w.RLock()
	for _, p := range w.partitions {
		partitions = append(partitions, p)
	}
	w.RUnlock()

	rwPartitionGroups = make([]*DataPartition, 0)
	localLeaderPartitionGroups = make([]*DataPartition, 0)
	for _, dp := range partitions {
		if dp.Status == proto.ReadWrite {
			rwPartitionGroups = append(rwPartitionGroups, dp)
			if strings.Split(dp.Hosts[0], ":")[0] == LocalIP {
				localLeaderPartitionGroups = append(localLeaderPartitionGroups, dp)
			}
		}
	}
	w.rwPartition = rwPartitionGroups
	w.localLeaderPartitions = localLeaderPartitionGroups

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
	} else {
		dp.Metrics = NewDataPartitionMetrics()
		w.partitions[dp.PartitionID] = dp
	}

	w.Unlock()

	if ok && oldstatus != dp.Status {
		log.LogInfof("partition: status change (%v) -> (%v)", old, dp)
	}
}

func (w *Wrapper) getLocalLeaderDataPartition(exclude []uint64) (*DataPartition, error) {
	rwPartitionGroups := w.localLeaderPartitions
	if len(rwPartitionGroups) == 0 {
		return nil, fmt.Errorf("no writable data partition")
	}
	var (
		partition *DataPartition
	)

	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(rwPartitionGroups))
	partition = rwPartitionGroups[index]
	if !isExcluded(partition.PartitionID, exclude) {
		return partition, nil
	}

	for _, partition = range rwPartitionGroups {
		if !isExcluded(partition.PartitionID, exclude) {
			return partition, nil
		}
	}
	return nil, fmt.Errorf("no writable data partition")
}

func (w *Wrapper) GetWriteDataPartition(exclude []uint64) (*DataPartition, error) {
	dp, err := w.getLocalLeaderDataPartition(exclude)
	if err == nil {
		return dp, nil
	}
	rwPartitionGroups := w.rwPartition
	if len(rwPartitionGroups) == 0 {
		return nil, fmt.Errorf("no writable data partition")
	}
	var (
		partition *DataPartition
	)

	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(rwPartitionGroups))
	partition = rwPartitionGroups[index]

	if !isExcluded(partition.PartitionID, exclude) {
		return partition, nil
	}

	for _, partition = range rwPartitionGroups {
		if !isExcluded(partition.PartitionID, exclude) {
			return partition, nil
		}
	}
	return nil, fmt.Errorf("no writable data partition")
}

func (w *Wrapper) GetDataPartition(partitionID uint64) (*DataPartition, error) {
	w.RLock()
	defer w.RUnlock()
	dp, ok := w.partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
	}
	return dp, nil
}

func (w *Wrapper) WarningKey() string {
	return fmt.Sprintf("%s_client_warning", w.clusterName)
}
