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

package fs

import (
	"sync"
	"sync/atomic"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/jacobsa/fuse/fuseops"
)

type HandleCache struct {
	sync.RWMutex
	handles      map[fuseops.HandleID]*Handle
	currHandleID uint64
}

type Handle struct {
	// Imutable
	ino uint64

	// For directory handles only
	lock    sync.Mutex
	entries []proto.Dentry
}

func NewHandleCache() *HandleCache {
	hc := &HandleCache{
		handles: make(map[fuseops.HandleID]*Handle),
	}
	return hc
}

func (hc *HandleCache) Assign(ino uint64) fuseops.HandleID {
	hid := fuseops.HandleID(atomic.AddUint64(&hc.currHandleID, 1))
	handle := &Handle{ino: ino}
	hc.Lock()
	defer hc.Unlock()
	hc.handles[hid] = handle
	return hid
}

func (hc *HandleCache) Get(hid fuseops.HandleID) *Handle {
	hc.Lock()
	defer hc.Unlock()
	return hc.handles[hid]
}

func (hc *HandleCache) Release(hid fuseops.HandleID) *Handle {
	hc.Lock()
	defer hc.Unlock()
	handle, ok := hc.handles[hid]
	if !ok {
		return nil
	}
	delete(hc.handles, hid)
	return handle
}
