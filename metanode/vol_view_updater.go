// Copyright 2023 The CubeFS Authors.
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

package metanode

import (
	"errors"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const UpdateVolTicket = 2 * time.Minute

var (
	ErrMpExistsInVolViewUpdater   = errors.New("mp already exists in updater")
	ErrMpNotFoundInVolViewUpdater = errors.New("mp not found in updater")
)

type VolViewUpdater interface {
	Stop()
	Register(mp MetaPartition) (err error)
	Unregister(mp MetaPartition) (err error)
}

type volViewUpdater struct {
	metaPartitions *sync.Map // NOTE: sync.Map[vol name](sync.Map[uint64]MetaPartition)
	timer          time.Ticker
	forceUpdateC   chan string
	wg             sync.WaitGroup
}

var _ VolViewUpdater = &volViewUpdater{}

func (v *volViewUpdater) getDataPartitions(volName string) (view *proto.DataPartitionsView, err error) {
	view, err = masterClient.ClientAPI().GetDataPartitions(volName)
	if err != nil {
		log.LogErrorf("action[getDataPartitions]: failed to get data partitions for volume %v", volName)
	}
	return
}

func (v *volViewUpdater) getVolumeView(volName string) (view *proto.SimpleVolView, err error) {
	view, err = masterClient.AdminAPI().GetVolumeSimpleInfo(volName)
	if err != nil {
		log.LogErrorf("action[getVolumeView]: failed to get view of volume %v", volName)
	}
	return
}

func (v *volViewUpdater) getVol(volName string) (view *proto.SimpleVolView, dpView *proto.DataPartitionsView, err error) {
	view, err = v.getVolumeView(volName)
	if err != nil {
		return
	}
	dpView, err = v.getDataPartitions(volName)
	if err != nil {
		return
	}
	return
}

func (v *volViewUpdater) updateVol(volName string) (err error) {
	log.LogDebugf("[updateVol] update volume(%v)", volName)
	value, ok := v.metaPartitions.Load(volName)
	if !ok {
		log.LogErrorf("[updateVol] volume(%v) is unregistered", volName)
		return
	}
	mps := value.(*sync.Map)
	empty := true
	mps.Range(func(key, value interface{}) bool {
		empty = false
		return false
	})
	if empty {
		log.LogDebugf("[updateVol] volume(%v) mp count is 0", volName)
		return
	}
	volView, dpView, err := v.getVol(volName)
	if err != nil {
		log.LogErrorf("[updateVol] failed to get vol(%v), err(%v)", volName, err)
		return
	}
	convert := func(view *proto.DataPartitionsView) *DataPartitionsView {
		newView := &DataPartitionsView{
			DataPartitions: make([]*DataPartition, len(view.DataPartitions)),
		}
		for i := 0; i < len(view.DataPartitions); i++ {
			if len(view.DataPartitions[i].Hosts) < 1 {
				log.LogErrorf("[updateVol] dp id(%v) is invalid, DataPartitionResponse detail[%v]",
					view.DataPartitions[i].PartitionID, view.DataPartitions[i])
				continue
			}
			newView.DataPartitions[i] = &DataPartition{
				PartitionID: view.DataPartitions[i].PartitionID,
				Status:      view.DataPartitions[i].Status,
				Hosts:       view.DataPartitions[i].Hosts,
				ReplicaNum:  view.DataPartitions[i].ReplicaNum,
			}
		}
		return newView
	}
	mps.Range(func(key, value interface{}) bool {
		mp := value.(MetaPartition)
		log.LogDebugf("[updateVol] update volume(%v) for mp(%v)", volName, mp.GetBaseConfig().PartitionId)
		mp.UpdateVolView(volView, convert(dpView))
		return true
	})
	return
}

func (v *volViewUpdater) updater() {
	defer log.LogDebugf("[updater] updater exit!")
	defer v.wg.Done()
	for {
		select {
		case _, ok := <-v.timer.C:
			if !ok {
				return
			}
			v.metaPartitions.Range(func(key, value interface{}) bool {
				volName := key.(string)
				err := v.updateVol(volName)
				if err != nil {
					log.LogErrorf("[updater] failed to update vol(%v), err(%v)", volName, err)
					return true
				}
				return true
			})
		case volName, ok := <-v.forceUpdateC:
			if !ok {
				return
			}
			err := v.updateVol(volName)
			if err != nil {
				log.LogErrorf("[updater] failed to update vol(%v), err(%v)", volName, err)
			}
		}
	}
}

func (v *volViewUpdater) Register(mp MetaPartition) (err error) {
	mpMap := &sync.Map{}
	value, _ := v.metaPartitions.LoadOrStore(mp.GetBaseConfig().VolName, mpMap)
	mpMap = value.(*sync.Map)
	_, load := mpMap.LoadOrStore(mp.GetBaseConfig().PartitionId, mp)
	if load {
		err = ErrMpExistsInVolViewUpdater
		log.LogErrorf("[Register] mp(%v) exists in vol updater", mp.GetBaseConfig().PartitionId)
		return
	}
	// NOTE: we need to update volume
	log.LogDebugf("[Register] force update volume(%v)", mp.GetBaseConfig().VolName)
	v.forceUpdateC <- mp.GetBaseConfig().VolName
	return
}

func (v *volViewUpdater) Unregister(mp MetaPartition) (err error) {
	value, ok := v.metaPartitions.Load(mp.GetBaseConfig().VolName)
	if !ok {
		return
	}
	mpMap := value.(*sync.Map)
	_, load := mpMap.LoadAndDelete(mp.GetBaseConfig().PartitionId)
	if !load {
		err = ErrMpNotFoundInVolViewUpdater
		return
	}
	return
}

func (v *volViewUpdater) start() (err error) {
	v.wg.Add(1)
	go v.updater()
	return
}

func (v *volViewUpdater) Stop() {
	close(v.forceUpdateC)
	v.timer.Stop()
	v.wg.Wait()
}

func NewVolViewUpdater() VolViewUpdater {
	updater := &volViewUpdater{
		metaPartitions: &sync.Map{},
		timer:          *time.NewTicker(UpdateVolTicket),
		forceUpdateC:   make(chan string, 1000),
		wg:             sync.WaitGroup{},
	}
	updater.start()
	return updater
}
