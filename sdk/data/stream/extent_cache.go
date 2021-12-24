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

package stream

import (
	"fmt"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/btree"
	"github.com/chubaofs/chubaofs/util/log"
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
	inode   uint64
	gen     uint64 // generation number
	size    uint64 // size of the cache
	root    *btree.BTree
	discard *btree.BTree
}

// NewExtentCache returns a new extent cache.
func NewExtentCache(inode uint64) *ExtentCache {
	return &ExtentCache{
		inode:   inode,
		root:    btree.New(32),
		discard: btree.New(32),
	}
}

// Refresh refreshes the extent cache.
func (cache *ExtentCache) Refresh(inode uint64, getExtents GetExtentsFunc) error {
	gen, size, extents, err := getExtents(inode)
	if err != nil {
		return err
	}
	//log.LogDebugf("Local ExtentCache before update: ino(%v) gen(%v) size(%v) extents(%v)", inode, cache.gen, cache.size, cache.List())
	cache.update(gen, size, extents)
	//log.LogDebugf("Local ExtentCache after update: ino(%v) gen(%v) size(%v) extents(%v)", inode, cache.gen, cache.size, cache.List())
	return nil
}

func (cache *ExtentCache) update(gen, size uint64, eks []proto.ExtentKey) {
	cache.Lock()
	defer cache.Unlock()

	log.LogDebugf("ExtentCache update: ino(%v) cache.gen(%v) cache.size(%v) gen(%v) size(%v)", cache.inode, cache.gen, cache.size, gen, size)

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
	cache.root.Clear(false)
	for _, ek := range eks {
		extent := ek
		cache.root.ReplaceOrInsert(&extent)
	}
}

// Append appends an extent key.
func (cache *ExtentCache) Append(ek *proto.ExtentKey, sync bool) (discardExtents []proto.ExtentKey) {
	ekEnd := ek.FileOffset + uint64(ek.Size)
	lower := &proto.ExtentKey{FileOffset: ek.FileOffset}
	upper := &proto.ExtentKey{FileOffset: ekEnd}
	discard := make([]*proto.ExtentKey, 0)

	cache.Lock()
	defer cache.Unlock()

	// When doing the append, we do not care about the data after the file offset.
	// Those data will be overwritten by the current extent anyway.
	cache.root.AscendRange(lower, upper, func(i btree.Item) bool {
		found := i.(*proto.ExtentKey)
		discard = append(discard, found)
		return true
	})

	// After deleting the data between lower and upper, we will do the append
	for _, key := range discard {
		cache.root.Delete(key)
		if key.PartitionId != 0 && key.ExtentId != 0 && (key.PartitionId != ek.PartitionId || key.ExtentId != ek.ExtentId || ek.ExtentOffset != key.ExtentOffset) {
			if sync || (ek.PartitionId == 0 && ek.ExtentId == 0) {
				cache.discard.ReplaceOrInsert(key)
				//log.LogDebugf("ExtentCache Append add to discard: ino(%v) ek(%v) discard(%v)", cache.inode, ek, key)
			}
		}
	}

	cache.root.ReplaceOrInsert(ek)
	if sync {
		cache.gen++
		discardExtents = make([]proto.ExtentKey, 0, cache.discard.Len())
		cache.discard.AscendRange(lower, upper, func(i btree.Item) bool {
			found := i.(*proto.ExtentKey)
			if found.PartitionId != ek.PartitionId || found.ExtentId != ek.ExtentId || found.ExtentOffset != ek.ExtentOffset {
				discardExtents = append(discardExtents, *found)
			}
			return true
		})
	}
	if ekEnd > cache.size {
		cache.size = ekEnd
	}

	log.LogDebugf("ExtentCache Append: ino(%v) sync(%v) ek(%v) local discard(%v) discardExtents(%v)", cache.inode, sync, ek, discard, discardExtents)
	return
}

func (cache *ExtentCache) RemoveDiscard(discardExtents []proto.ExtentKey) {
	cache.Lock()
	defer cache.Unlock()
	for _, ek := range discardExtents {
		cache.discard.Delete(&ek)
		//log.LogDebugf("ExtentCache ClearDiscard: ino(%v) discard(%v)", cache.inode, ek)
	}
}

func (cache *ExtentCache) TruncDiscard(size uint64) {
	cache.Lock()
	defer cache.Unlock()
	if size >= cache.size {
		return
	}
	pivot := &proto.ExtentKey{FileOffset: size}
	discardExtents := make([]proto.ExtentKey, 0, cache.discard.Len())
	cache.discard.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		found := i.(*proto.ExtentKey)
		discardExtents = append(discardExtents, *found)
		return true
	})
	for _, key := range discardExtents {
		cache.discard.Delete(&key)
	}
	log.LogDebugf("truncate ExtentCache discard: ino(%v) size(%v) discard(%v)", cache.inode, size, discardExtents)
}

// Max returns the max extent key in the cache.
func (cache *ExtentCache) Max() *proto.ExtentKey {
	cache.RLock()
	defer cache.RUnlock()
	ek := cache.root.Max().(*proto.ExtentKey)
	return ek
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
func (cache *ExtentCache) List() []*proto.ExtentKey {
	cache.RLock()
	root := cache.root.Clone()
	cache.RUnlock()

	extents := make([]*proto.ExtentKey, 0, root.Len())
	root.Ascend(func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		extents = append(extents, ek)
		return true
	})
	return extents
}

// Get returns the extent key based on the given offset.
func (cache *ExtentCache) Get(offset uint64) (ret *proto.ExtentKey) {
	pivot := &proto.ExtentKey{FileOffset: offset}
	cache.RLock()
	defer cache.RUnlock()

	cache.root.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		//log.LogDebugf("ExtentCache GetConnect: ino(%v) ek(%v) offset(%v)", cache.inode, ek, offset)
		if offset >= ek.FileOffset && offset < ek.FileOffset+uint64(ek.Size) {
			ret = ek
		}
		return false
	})
	return ret
}

// GetEnd returns the extent key whose end offset equals the given offset.
func (cache *ExtentCache) GetEnd(offset uint64) (ret *proto.ExtentKey) {
	pivot := &proto.ExtentKey{FileOffset: offset}
	cache.RLock()
	defer cache.RUnlock()

	cache.root.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		// skip if the start offset matches with the given offset
		if offset == ek.FileOffset {
			return true
		}
		if offset == ek.FileOffset+uint64(ek.Size) {
			ret = ek
		}
		return false
	})
	return ret
}

// PrepareReadRequests classifies the incoming request.
func (cache *ExtentCache) PrepareReadRequests(offset, size int, data []byte) []*ExtentRequest {
	requests := make([]*ExtentRequest, 0)
	pivot := &proto.ExtentKey{FileOffset: uint64(offset)}
	upper := &proto.ExtentKey{FileOffset: uint64(offset + size)}
	start := offset
	end := offset + size

	cache.RLock()
	defer cache.RUnlock()

	lower := &proto.ExtentKey{}
	cache.root.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		lower.FileOffset = ek.FileOffset
		return false
	})

	cache.root.AscendRange(lower, upper, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		ekStart := int(ek.FileOffset)
		ekEnd := int(ek.FileOffset) + int(ek.Size)

		log.LogDebugf("PrepareReadRequests: ino(%v) start(%v) end(%v) ekStart(%v) ekEnd(%v)", cache.inode, start, end, ekStart, ekEnd)

		if start < ekStart {
			if end <= ekStart {
				return false
			} else if end < ekEnd {
				// add hole (start, ekStart)
				req := NewExtentRequest(start, ekStart-start, data[start-offset:ekStart-offset], nil)
				requests = append(requests, req)
				// add non-hole (ekStart, end)
				req = NewExtentRequest(ekStart, end-ekStart, data[ekStart-offset:end-offset], ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				// add hole (start, ekStart)
				req := NewExtentRequest(start, ekStart-start, data[start-offset:ekStart-offset], nil)
				requests = append(requests, req)

				// add non-hole (ekStart, ekEnd)
				req = NewExtentRequest(ekStart, ekEnd-ekStart, data[ekStart-offset:ekEnd-offset], ek)
				requests = append(requests, req)

				start = ekEnd
				return true
			}
		} else if start < ekEnd {
			if end <= ekEnd {
				// add non-hole (start, end)
				req := NewExtentRequest(start, end-start, data[start-offset:end-offset], ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				// add non-hole (start, ekEnd), start = ekEnd
				req := NewExtentRequest(start, ekEnd-start, data[start-offset:ekEnd-offset], ek)
				requests = append(requests, req)
				start = ekEnd
				return true
			}
		} else {
			return true
		}
	})

	log.LogDebugf("PrepareReadRequests: ino(%v) start(%v) end(%v)", cache.inode, start, end)
	if start < end {
		// add hole (start, end)
		req := NewExtentRequest(start, end-start, data[start-offset:end-offset], nil)
		requests = append(requests, req)
	}

	return requests
}

// PrepareWriteRequests TODO explain
func (cache *ExtentCache) PrepareWriteRequests(offset, size int, data []byte) []*ExtentRequest {
	requests := make([]*ExtentRequest, 0)
	pivot := &proto.ExtentKey{FileOffset: uint64(offset)}
	upper := &proto.ExtentKey{FileOffset: uint64(offset + size)}
	start := offset
	end := offset + size

	cache.RLock()
	defer cache.RUnlock()

	lower := &proto.ExtentKey{}
	cache.root.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		lower.FileOffset = ek.FileOffset
		return false
	})

	cache.root.AscendRange(lower, upper, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		ekStart := int(ek.FileOffset)
		ekEnd := int(ek.FileOffset) + int(ek.Size)

		log.LogDebugf("PrepareWriteRequests: ino(%v) start(%v) end(%v) ekStart(%v) ekEnd(%v)", cache.inode, start, end, ekStart, ekEnd)

		if start <= ekStart {
			if end <= ekStart {
				return false
			} else if end < ekEnd {
				var req *ExtentRequest
				if start < ekStart {
					// add hole (start, ekStart)
					req = NewExtentRequest(start, ekStart-start, data[start-offset:ekStart-offset], nil)
					requests = append(requests, req)
				}
				// add non-hole (ekStart, end)
				req = NewExtentRequest(ekStart, end-ekStart, data[ekStart-offset:end-offset], ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				return true
			}
		} else if start < ekEnd {
			if end <= ekEnd {
				// add non-hole (start, end)
				req := NewExtentRequest(start, end-start, data[start-offset:end-offset], ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				// add non-hole (start, ekEnd), start = ekEnd
				req := NewExtentRequest(start, ekEnd-start, data[start-offset:ekEnd-offset], ek)
				requests = append(requests, req)
				start = ekEnd
				return true
			}
		} else {
			return true
		}
	})

	log.LogDebugf("PrepareWriteRequests: ino(%v) start(%v) end(%v)", cache.inode, start, end)
	if start < end {
		// add hole (start, end)
		req := NewExtentRequest(start, end-start, data[start-offset:end-offset], nil)
		requests = append(requests, req)
	}

	return requests
}
