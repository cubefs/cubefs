// Copyright 2018 The ChuBao Authors.
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
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/chubaoio/cbfs/util/pool"
	"sort"
)

const (
	DataPartitionViewUrl        = "/client/dataPartitions"
	ActionGetDataPartitionView  = "ActionGetDataPartitionView"
	MinWritableDataPartitionNum = 10
)

var (
	MasterHelper = util.NewMasterHelper()
	LocalIP, _   = util.GetLocalIP()
	GconnPool    = pool.NewConnPool()
)

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

type Wrapper struct {
	sync.RWMutex
	volName               string
	masters               []string
	partitions            map[uint32]*DataPartition
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
	w.partitions = make(map[uint32]*DataPartition)
	if err = w.updateDataPartition(); err != nil {
		return
	}
	go w.update()
	return
}

func (w *Wrapper) update() {
	ticker := time.NewTicker(time.Minute)
	sortTicker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ticker.C:
			w.updateDataPartition()
		case <-sortTicker.C:
			w.SortDataPartition()
		}
	}
}

func (w *Wrapper) updateDataPartition() error {
	paras := make(map[string]string, 0)
	paras["name"] = w.volName
	msg, err := MasterHelper.Request(http.MethodGet, DataPartitionViewUrl, paras, nil)
	if err != nil {
		return err
	}

	view := &DataPartitionView{}
	if err = json.Unmarshal(msg, view); err != nil {
		return err
	}

	rwPartitionGroups := make([]*DataPartition, 0)
	localLeaderPartitionGroups := make([]*DataPartition, 0)
	for _, dp := range view.DataPartitions {
		if dp.Status == proto.ReadWrite {
			rwPartitionGroups = append(rwPartitionGroups, dp)
		}
	}
	if len(rwPartitionGroups) < MinWritableDataPartitionNum {
		err = fmt.Errorf("action[Wrapper.updateDataPartition] RW partitions[%v] Minimum[%v]",
			len(rwPartitionGroups), MinWritableDataPartitionNum)
		log.LogErrorf(err.Error())
		return err
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
		log.LogInfof("DataPartition: status change (%v) -> (%v)", old, dp)
	}
}

func (w *Wrapper) getLocalLeaderDataPartition(exclude []uint32) (*DataPartition, error) {
	rwPartitionGroups := w.localLeaderPartitions
	if len(rwPartitionGroups) == 0 {
		return nil, fmt.Errorf("no writable data partition")
	}
	var (
		partition *DataPartition
	)

	rand.Seed(time.Now().UnixNano())
	choose := rand.Float64()
	if choose < 0.8 {
		index:=rand.Intn(util.Min(10,len(rwPartitionGroups)))
		partition = rwPartitionGroups[index]
		if isExcluded(partition.PartitionID, exclude) {
			index := rand.Intn(len(rwPartitionGroups))
			partition = rwPartitionGroups[index]
		}
	} else {
		index := rand.Intn(len(rwPartitionGroups))
		partition = rwPartitionGroups[index]
	}
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

func (w *Wrapper) GetWriteDataPartition(exclude []uint32) (*DataPartition, error) {
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
	choose := rand.Float64()
	if choose < 0.8 {
		index:=rand.Intn(util.Min(4,len(rwPartitionGroups)))
		partition = rwPartitionGroups[index]
		if isExcluded(partition.PartitionID, exclude) {
			index := rand.Intn(len(rwPartitionGroups))
			partition = rwPartitionGroups[index]
		}
	} else {
		index := rand.Intn(len(rwPartitionGroups))
		partition = rwPartitionGroups[index]
	}

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

func (w *Wrapper) GetDataPartition(partitionID uint32) (*DataPartition, error) {
	w.RLock()
	defer w.RUnlock()
	dp, ok := w.partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("DataPartition[%v] not exsit", partitionID)
	}
	return dp, nil
}

func (w *Wrapper) GetConnect(addr string) (*net.TCPConn, error) {
	return GconnPool.Get(addr)
}

func (w *Wrapper) PutConnect(conn *net.TCPConn, forceClose bool) {
	GconnPool.Put(conn, forceClose)
}

func (w *Wrapper) SortDataPartition() {
	paritions := make([]*DataPartition, 0)
	w.RLock()
	for _, p := range w.partitions {
		paritions = append(paritions, p)
	}
	w.RUnlock()
	var wg sync.WaitGroup
	for _, p := range paritions {
		wg.Add(1)
		go func(dp *DataPartition) {
			dp.updateMetrics()
			wg.Done()
		}(p)
	}
	wg.Wait()
	w.Lock()
	sort.Sort((DataPartitionSlice)(w.localLeaderPartitions))
	sort.Sort((DataPartitionSlice)(w.rwPartition))
	w.Unlock()
}
