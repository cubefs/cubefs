// Copyright 2022 The CubeFS Authors.
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

package volumemgr

import (
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	volumeStateExist = uint8(1)
)

var defaultVolumeStatusStat *volumeStatusStat

// global volume status stat
func init() {
	initialVolumeStatusStat()
}

func initialVolumeStatusStat() {
	defaultVolumeStatusStat = &volumeStatusStat{
		stats: map[proto.VolumeStatus]statusVolumesMap{
			proto.VolumeStatusLock:      make(statusVolumesMap),
			proto.VolumeStatusIdle:      make(statusVolumesMap),
			proto.VolumeStatusActive:    make(statusVolumesMap),
			proto.VolumeStatusUnlocking: make(statusVolumesMap),
		},
	}
}

// statusVolumesMap hold a map that indicate the vid exist or not
type statusVolumesMap map[proto.Vid]uint8

// volumeStatusStat keep the different status volume statistic info
// also, it will call change status event function every Add func calls
type volumeStatusStat struct {
	stats map[proto.VolumeStatus]statusVolumesMap
	sync.RWMutex
}

// Add will trigger a volume status change action, it will do the internal statistic and call changeStatusFunc
func (v *volumeStatusStat) Add(vol *volume, status proto.VolumeStatus) {
	if !status.IsValid() {
		return
	}
	v.Lock()
	for st := range v.stats {
		if st == status {
			v.stats[st][vol.vid] = volumeStateExist
			continue
		}
		delete(v.stats[st], vol.vid)
	}
	v.Unlock()
}

// StatStatusNum return all kinds of status volumes num
func (v *volumeStatusStat) StatStatusNum() map[proto.VolumeStatus]int {
	ret := make(map[proto.VolumeStatus]int)
	v.RLock()
	defer v.RUnlock()
	for status := range v.stats {
		ret[status] = len(v.stats[status])
	}
	return ret
}

// StatTotal return total num of volumes
func (v *volumeStatusStat) StatTotal() int {
	v.RLock()
	defer v.RUnlock()
	total := 0
	for i := range v.stats {
		total += len(v.stats[i])
	}
	return total
}

// GetVidsByStatus return specified status vids
func (v *volumeStatusStat) GetVidsByStatus(status proto.VolumeStatus) []proto.Vid {
	if !status.IsValid() {
		return nil
	}
	v.RLock()
	defer v.RUnlock()
	ret := make([]proto.Vid, 0, len(v.stats[status]))
	for vid := range v.stats[status] {
		ret = append(ret, vid)
	}
	return ret
}
