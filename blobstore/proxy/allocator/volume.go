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

package allocator

import (
	"sort"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type volume struct {
	clustermgr.AllocVolumeInfo
	deleted bool
	mu      sync.RWMutex
}

type volumes struct {
	vols []*volume
	sync.RWMutex
}

func (s *volumes) Get(vid proto.Vid) (*volume, bool) {
	s.RLock()
	defer s.RUnlock()
	i, ok := search(s.vols, vid)
	if !ok {
		return nil, false
	}
	return s.vols[i], true
}

func (s *volumes) Put(vol *volume) {
	s.Lock()
	defer s.Unlock()
	_, ok := search(s.vols, vol.Vid)
	if !ok {
		s.vols = append(s.vols, vol)
		sort.Slice(s.vols, func(i, j int) bool {
			return s.vols[i].Vid < s.vols[j].Vid
		})
	}
}

func (s *volumes) Delete(vid proto.Vid) {
	s.Lock()
	defer s.Unlock()
	i, ok := search(s.vols, vid)
	if ok {
		vols := make([]*volume, len(s.vols)-1)
		copy(vols, s.vols[:i])
		copy(vols[i:], s.vols[i+1:])
		s.vols = vols
	}
}

func (s *volumes) List() (vols []*volume) {
	s.RLock()
	defer s.RUnlock()
	vols = s.vols
	return vols
}

func search(vols []*volume, vid proto.Vid) (int, bool) {
	idx := sort.Search(len(vols), func(i int) bool {
		return vols[i].Vid >= vid
	})
	if idx == len(vols) || vols[idx].Vid != vid {
		return idx, false
	}
	return idx, true
}
