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
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/errors"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
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

// Wrapper TODO rename. This name does not reflect what it is doing.
type Wrapper struct {
	sync.RWMutex
	clusterName           string
	volName               string
	masters               []string
	partitions            map[uint64]*DataPartition
	rwPartition           []*DataPartition
	localLeaderPartitions []*DataPartition
}

// NewDataPartitionWrapper returns a new data partition wrapper.
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
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	if err = w.updateDataPartition(); err != nil {
		err = errors.Trace(err, "NewDataPartitionWrapper:")
		return
	}
	go w.update()
	return
}

// GetClusterName returns the cluster name of the wrapper.
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
		return errors.Trace(err, "updateDataPartition: request to master failed!")
	}

	log.LogInfof("updateDataPartition: start!")

	view := &DataPartitionView{}
	if err = json.Unmarshal(msg, view); err != nil {
		return errors.Trace(err, "updateDataPartition: unmarshal failed, msg(%v)", msg)
	}

	rwPartitionGroups := make([]*DataPartition, 0)
	localLeaderPartitionGroups := make([]*DataPartition, 0)
	for _, dp := range view.DataPartitions {
		log.LogInfof("updateDataPartition: dp(%v)", dp)
		w.replaceOrInsertPartition(dp)
		if dp.Status == proto.ReadWrite {
			rwPartitionGroups = append(rwPartitionGroups, dp)
			if strings.Split(dp.Hosts[0], ":")[0] == LocalIP {
				localLeaderPartitionGroups = append(localLeaderPartitionGroups, dp)
			}
		}
	}

	if len(rwPartitionGroups) > 0 {
		w.rwPartition = rwPartitionGroups
		w.localLeaderPartitions = localLeaderPartitionGroups
	}

	log.LogInfof("updateDataPartition: end!")
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
