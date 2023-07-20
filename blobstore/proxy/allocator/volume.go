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
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type volume struct {
	clustermgr.AllocVolumeInfo
	deleted bool
	mu      sync.RWMutex
}

type volumes struct {
	vols      []*volume
	totalFree int64

	sync.RWMutex
}

func (s *volumes) Get(vid proto.Vid) (res *volume, ok bool) {
	s.RLock()
	i, ok := search(s.vols, vid)
	if !ok {
		s.RUnlock()
		return nil, false
	}
	res = s.vols[i]
	s.RUnlock()
	return res, true
}

func (s *volumes) UpdateTotalFree(fsize int64) int64 {
	return atomic.AddInt64(&s.totalFree, fsize)
}

func (s *volumes) TotalFree() int64 {
	return atomic.LoadInt64(&s.totalFree)
}

func (s *volumes) Put(vol *volume) {
	s.Lock()
	idx, ok := search(s.vols, vol.Vid)
	if !ok {
		atomic.AddInt64(&s.totalFree, int64(vol.Free))
		s.vols = append(s.vols, vol)
		if idx == len(s.vols)-1 {
			s.Unlock()
			return
		}
		copy(s.vols[idx+1:], s.vols[idx:len(s.vols)-1])
		s.vols[idx] = vol
	}
	s.Unlock()
}

func (s *volumes) Delete(vid proto.Vid) bool {
	s.Lock()
	i, ok := search(s.vols, vid)
	if ok {
		atomic.AddInt64(&s.totalFree, -int64(s.vols[i].Free))
		copy(s.vols[i:], s.vols[i+1:])
		s.vols = s.vols[:len(s.vols)-1]
	}
	s.Unlock()
	return ok
}

func (s *volumes) List() (vols []*volume) {
	s.RLock()
	vols = s.vols[:]
	s.RUnlock()
	return vols
}

func (s *volumes) Len() int {
	s.RLock()
	length := len(s.vols)
	s.RUnlock()
	return length
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
