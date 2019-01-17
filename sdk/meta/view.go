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

package meta

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/juju/errors"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
)

const (
	MaxSendToMaster = 3
)

var (
	NotLeader = errors.New("NotLeader")
)

type VolumeView struct {
	VolName        string
	MetaPartitions []*MetaPartition
}

type VolStatInfo struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

// VolName view managements
//
func (mw *MetaWrapper) fetchVolumeView() (*VolumeView, error) {
	params := make(map[string]string)
	params["name"] = mw.volname
	body, err := mw.master.Request(http.MethodPost, proto.ClientVol, params, nil)
	if err != nil {
		log.LogWarnf("fetchVolumeView request: err(%v)", err)
		return nil, err
	}

	view := new(VolumeView)
	if err = json.Unmarshal(body, view); err != nil {
		log.LogWarnf("fetchVolumeView unmarshal: err(%v) body(%v)", err, string(body))
		return nil, err
	}
	return view, nil
}

// fetch and update cluster info if successful
func (mw *MetaWrapper) updateClusterInfo() error {
	body, err := mw.master.Request(http.MethodPost, proto.AdminGetIP, nil, nil)
	if err != nil {
		log.LogWarnf("updateClusterInfo request: err(%v)", err)
		return err
	}

	info := new(proto.ClusterInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogWarnf("updateClusterInfo unmarshal: err(%v)", err)
		return err
	}
	log.LogInfof("ClusterInfo: %v", *info)
	mw.cluster = info.Cluster
	return nil
}

func (mw *MetaWrapper) updateVolStatInfo() error {
	params := make(map[string]string)
	params["name"] = mw.volname
	body, err := mw.master.Request(http.MethodPost, proto.ClientVolStat, params, nil)
	if err != nil {
		log.LogWarnf("updateVolStatInfo request: err(%v)", err)
		return err
	}

	info := new(VolStatInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogWarnf("updateVolStatInfo unmarshal: err(%v)", err)
		return err
	}
	atomic.StoreUint64(&mw.totalSize, info.TotalSize)
	atomic.StoreUint64(&mw.usedSize, info.UsedSize)
	log.LogInfof("VolStatInfo: info(%v)", *info)
	return nil
}

func (mw *MetaWrapper) updateMetaPartitions() error {
	view, err := mw.fetchVolumeView()
	if err != nil {
		return err
	}

	rwPartitions := make([]*MetaPartition, 0)
	for _, mp := range view.MetaPartitions {
		mw.replaceOrInsertPartition(mp)
		log.LogInfof("updateMetaPartition: mp(%v)", mp)
		if mp.Status == proto.ReadWrite {
			rwPartitions = append(rwPartitions, mp)
		}
	}

	if len(rwPartitions) == 0 {
		log.LogInfof("updateMetaPartition: no rw partitions")
		return nil
	}

	mw.Lock()
	mw.rwPartitions = rwPartitions
	mw.Unlock()
	return nil
}

func (mw *MetaWrapper) refresh() {
	t := time.NewTicker(RefreshMetaPartitionsInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			mw.updateMetaPartitions()
			mw.updateVolStatInfo()
		}
	}
}
