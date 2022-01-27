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

package data

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	se "github.com/chubaofs/chubaofs/util/sortedextent"
)

// ExtentRequest defines the struct for the request of read or write an extent.
type ExtentRequest struct {
	FileOffset int
	Size       int
	Data       []byte
	ExtentKey  *proto.ExtentKey
}

// String returns the string format of the extent request.
func (er *ExtentRequest) String() string {
	if er == nil {
		return ""
	}
	return fmt.Sprintf("FileOffset(%v) Size(%v) ExtentKey(%v)", er.FileOffset, er.Size, er.ExtentKey)
}

// NewExtentRequest returns a new extent request.
func NewExtentRequest(offset, size int, data []byte, ek *proto.ExtentKey) *ExtentRequest {
	return &ExtentRequest{
		FileOffset: offset,
		Size:       size,
		Data:       data,
		ExtentKey:  ek,
	}
}

// ExtentCache defines the struct of the extent cache.
type ExtentCache struct {
	sync.RWMutex
	inode       uint64
	gen         uint64 // generation number
	size        uint64 // size of the cache
	root        *se.SortedExtents
	refreshTime time.Time        // the last time to update extent cache
	ek          *proto.ExtentKey // temporary ek
}

// NewExtentCache returns a new extent cache.
func NewExtentCache(inode uint64) *ExtentCache {
	return &ExtentCache{
		inode: inode,
		root:  se.NewSortedExtents(),
	}
}

// Refresh refreshes the extent cache.
func (cache *ExtentCache) Refresh(ctx context.Context, inode uint64, getExtents GetExtentsFunc) error {
	cache.UpdateRefreshTime()

	gen, size, extents, err := getExtents(ctx, inode)
	if err != nil {
		return err
	}
	//log.LogDebugf("Local ExtentCache before update: gen(%v) size(%v) extents(%v)", cache.gen, cache.size, cache.List())
	cache.update(gen, size, extents)
	//log.LogDebugf("Local ExtentCache after update: gen(%v) size(%v) extents(%v)", cache.gen, cache.size, cache.List())
	return nil
}

func (cache *ExtentCache) update(gen, size uint64, eks []proto.ExtentKey) {
	cache.Lock()
	defer cache.Unlock()

	log.LogDebugf("ExtentCache update: ino(%v) cache.gen(%v) cache.size(%v) cache.ek(%v) gen(%v) size(%v)", cache.inode, cache.gen, cache.size, cache.ek, gen, size)

	//	cache.root.Ascend(func(bi btree.Item) bool {
	//		ek := bi.(*proto.ExtentKey)
	//		log.LogDebugf("ExtentCache update: local ino(%v) ek(%v)", cache.inode, ek)
	//		return true
	//	})
	//
	//	for _, ek := range eks {
	//		log.LogDebugf("ExtentCache update: remote ino(%v) ek(%v)", cache.inode, ek)
	//	}

	if cache.gen != 0 && cache.gen >= gen {
		log.LogDebugf("ExtentCache update: no need to update, ino(%v) gen(%v) size(%v)", cache.inode, gen, size)
		return
	}

	cache.gen = gen
	cache.size = size
	cache.root.Update(eks)
	// append local temporary ek to prevent read unconsistency
	if cache.ek != nil {
		cache.insert(cache.ek, false)
	}
}

func (cache *ExtentCache) UpdateRefreshTime() {
	cache.Lock()
	defer cache.Unlock()

	cache.refreshTime = time.Now()
	return
}

func (cache *ExtentCache) IsExpired(expireSecond int64) bool {
	cache.RLock()
	defer cache.RUnlock()

	return time.Since(cache.refreshTime) > time.Duration(expireSecond)*time.Second
}

func (cache *ExtentCache) Insert(ek *proto.ExtentKey, sync bool) {
	cache.Lock()
	defer cache.Unlock()
	cache.insert(ek, sync)
}

func (cache *ExtentCache) insert(ek *proto.ExtentKey, sync bool) {
	ekEnd := ek.FileOffset + uint64(ek.Size)
	deleteExtents := cache.root.Insert(nil, *ek)

	// todo gen
	if sync {
		cache.gen++
		cache.ek = nil
	} else {
		cache.ek = ek
	}
	if ekEnd > cache.size {
		cache.size = ekEnd
	}

	log.LogDebugf("ExtentCache Insert: ino(%v) ek(%v) deleteEks(%v)", cache.inode, ek, deleteExtents)
}

func (cache *ExtentCache) Pre(offset uint64) (pre *proto.ExtentKey) {
	cache.RLock()
	defer cache.RUnlock()

	cache.root.Range(func(ek proto.ExtentKey) bool {
		if ek.FileOffset >= offset {
			return false
		}
		pre = &ek
		return true
	})
	return
}

// Size returns the size of the cache.
func (cache *ExtentCache) Size() (size int, gen uint64) {
	cache.RLock()
	defer cache.RUnlock()
	return int(cache.size), cache.gen
}

// SetSize set the size of the cache.
func (cache *ExtentCache) SetSize(size uint64, sync bool) {
	cache.Lock()
	defer cache.Unlock()
	cache.size = size
	if sync {
		cache.gen++
	}
}

// List returns a list of the extents in the cache.
func (cache *ExtentCache) List() []proto.ExtentKey {
	cache.RLock()
	extents := cache.root.CopyExtents()
	cache.RUnlock()

	return extents
}

// Get returns the extent key based on the given offset.
func (cache *ExtentCache) Get(offset uint64) (ret *proto.ExtentKey) {
	cache.RLock()
	defer cache.RUnlock()

	cache.root.Range(func(ek proto.ExtentKey) bool {

		log.LogDebugf("ExtentCache GetConnect: ino(%v) ek(%v) offset(%v)", cache.inode, ek, offset)
		if offset >= ek.FileOffset && offset < ek.FileOffset+uint64(ek.Size) {
			ret = &ek
			return false
		}
		return true

	})

	return ret
}

// PrepareRequests classifies the incoming request.
func (cache *ExtentCache) PrepareRequests(offset, size int, data []byte) (requests []*ExtentRequest, fileSize int) {
	requests = make([]*ExtentRequest, 0)
	pivot := proto.GetExtentKeyFromPool()
	pivot.FileOffset = uint64(offset)
	upper := proto.GetExtentKeyFromPool()
	upper.FileOffset = uint64(offset + size)
	lower := proto.GetExtentKeyFromPool()

	defer func() {
		proto.PutExtentKeyToPool(pivot)
		proto.PutExtentKeyToPool(upper)
		proto.PutExtentKeyToPool(lower)
	}()
	start := offset
	end := offset + size

	cache.RLock()
	defer cache.RUnlock()

	fileSize = int(cache.size)
	cache.root.Range(func(ek proto.ExtentKey) bool {
		ekStart := int(ek.FileOffset)
		ekEnd := int(ek.FileOffset) + int(ek.Size)

		log.LogDebugf("PrepareRequests: ino(%v) start(%v) end(%v) ekStart(%v) ekEnd(%v)", cache.inode, start, end, ekStart, ekEnd)

		if end <= ekStart {
			return false
		}

		if start < ekStart {
			if end <= ekStart {
				return false
			} else if end < ekEnd {
				// add hole (start, ekStart)
				req := NewExtentRequest(start, ekStart-start, data[start-offset:ekStart-offset], nil)
				requests = append(requests, req)
				// add non-hole (ekStart, end)
				req = NewExtentRequest(ekStart, end-ekStart, data[ekStart-offset:end-offset], &ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				// add hole (start, ekStart)
				req := NewExtentRequest(start, ekStart-start, data[start-offset:ekStart-offset], nil)
				requests = append(requests, req)

				// add non-hole (ekStart, ekEnd)
				req = NewExtentRequest(ekStart, ekEnd-ekStart, data[ekStart-offset:ekEnd-offset], &ek)
				requests = append(requests, req)

				start = ekEnd
				return true
			}
		} else if start < ekEnd {
			if end <= ekEnd {
				// add non-hole (start, end)
				req := NewExtentRequest(start, end-start, data[start-offset:end-offset], &ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				// add non-hole (start, ekEnd), start = ekEnd
				req := NewExtentRequest(start, ekEnd-start, data[start-offset:ekEnd-offset], &ek)
				requests = append(requests, req)
				start = ekEnd
				return true
			}
		} else {
			return true
		}
	})

	if log.IsDebugEnabled() {
		log.LogDebugf("PrepareRequests: ino(%v) start(%v) end(%v)", cache.inode, start, end)
	}
	if start < end {
		// add hole (start, end)
		req := NewExtentRequest(start, end-start, data[start-offset:end-offset], nil)
		requests = append(requests, req)
	}

	return
}

func (cache *ExtentCache) prepareMergeRequests() (readRequests []*ExtentRequest, writeRequest *ExtentRequest, err error) {
	var (
		mergedToSize = uint32(1024 * 1024)
		data         = make([]byte, mergedToSize, mergedToSize)
		preEk        *proto.ExtentKey
		total        uint32
		req          *ExtentRequest
	)

	cache.RLock()
	defer cache.RUnlock()
	cache.root.Range(func(ek proto.ExtentKey) bool {
		if total+ek.Size > mergedToSize || (preEk != nil && ek.FileOffset != preEk.FileOffset+uint64(preEk.Size)) {
			if len(readRequests) > 1 {
				writeRequest = NewExtentRequest(readRequests[0].FileOffset, int(total), data[:total], nil)
				return false
			} else if len(readRequests) == 1 {
				readRequests = readRequests[:0]
				total = 0
			}
			if ek.Size >= mergedToSize {
				preEk = &ek
				return true
			}
		}

		req = NewExtentRequest(int(ek.FileOffset), int(ek.Size), data[total:total+ek.Size], &ek)
		readRequests = append(readRequests, req)
		total += ek.Size
		preEk = &ek
		return true
	})
	if writeRequest == nil {
		if len(readRequests) > 1 {
			writeRequest = NewExtentRequest(readRequests[0].FileOffset, int(total), data[:total], nil)
		} else {
			readRequests = readRequests[:0]
		}
	}
	return
}
