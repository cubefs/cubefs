// Copyright 2019 The CubeFS Authors.
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

package objectnode

import (
	"container/list"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	BackGroundEvictDurFactor    = 2
	BackGroundEvictMaxNumFactor = 5
)

type AccessStat struct {
	accessNum uint64
	miss      uint64
	validHit  uint64
}

type AttrItem struct {
	proto.XAttrInfo
	expiredTime int64
}

func (attr *AttrItem) IsExpired() bool {
	if attr.expiredTime < time.Now().Unix() {
		return true
	}
	return false
}

// VolumeInodeAttrsCache caches Attrs for Inodes
type VolumeInodeAttrsCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	maxElements int64
	accessStat  AccessStat
}

func NewVolumeInodeAttrsCache(maxElements int64) *VolumeInodeAttrsCache {
	vac := &VolumeInodeAttrsCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		maxElements: maxElements,
	}
	return vac
}

func (vac *VolumeInodeAttrsCache) SetMaxElements(maxElements int64) {
	vac.Lock()
	defer vac.Unlock()
	vac.maxElements = maxElements
}

func (vac *VolumeInodeAttrsCache) putAttr(attr *AttrItem) {
	vac.Lock()
	defer vac.Unlock()

	old, ok := vac.cache[attr.Inode]
	if ok {
		oldAttr := old.Value.(*AttrItem)
		log.LogDebugf("replace old attr: inode(%v) attr(%v) with new attr(%v)", attr.Inode, oldAttr, attr)

		vac.lruList.Remove(old)
		delete(vac.cache, attr.Inode)
	}

	if int64(vac.lruList.Len()) > vac.maxElements {
		vac.evictAttr(int64(vac.lruList.Len())-vac.maxElements, false)
	}

	element := vac.lruList.PushFront(attr)
	vac.cache[attr.Inode] = element
}

func (vac *VolumeInodeAttrsCache) mergeAttr(attr *AttrItem) {
	vac.RLock()
	old, ok := vac.cache[attr.Inode]
	if ok {
		oldAttrs := old.Value.(*AttrItem)
		for key, val := range oldAttrs.XAttrs {
			_, exist := attr.XAttrs[key]
			if !exist {
				attr.XAttrs[key] = val
			}
		}
	}
	vac.RUnlock()

	vac.putAttr(attr)
}

func (vac *VolumeInodeAttrsCache) getAttr(inode uint64) *AttrItem {
	var miss bool
	defer func() {
		vac.Lock()
		vac.accessStat.accessNum++
		if miss {
			vac.accessStat.miss++
		} else {
			vac.accessStat.validHit++
		}
		vac.Unlock()
	}()

	vac.RLock()
	element, ok := vac.cache[inode]
	if !ok {
		log.LogDebugf("cache Get inode(%v) attr not found", inode)
		miss = true
		vac.RUnlock()
		return nil
	}
	attrs := element.Value.(*AttrItem)
	vac.RUnlock()

	vac.Lock()
	vac.lruList.MoveToFront(element)
	vac.Unlock()

	log.LogDebugf("cache Get inode(%v) attrs(%v)", inode, attrs)
	return attrs
}

func (vac *VolumeInodeAttrsCache) deleteAttr(inode uint64) {
	vac.Lock()
	defer vac.Unlock()

	element, ok := vac.cache[inode]
	if ok {
		attr := element.Value.(*AttrItem)
		log.LogDebugf("delete attr in cache: inode(%v) attr(%v) ", inode, attr)
		vac.lruList.Remove(element)
		delete(vac.cache, inode)
	}
}

func (vac *VolumeInodeAttrsCache) deleteAttrWithKey(inode uint64, key string) {
	vac.Lock()
	var attr *AttrItem
	element, ok := vac.cache[inode]
	if ok {
		attr = element.Value.(*AttrItem)
		log.LogDebugf("delete key: %v in attrs of inode(%v) ", key, inode)
		delete(attr.XAttrs, key)
	}
	vac.Unlock()

	if attr != nil {
		vac.putAttr(attr)
	}
}

func (vac *VolumeInodeAttrsCache) evictAttr(evictNum int64, backGround bool) {
	if evictNum <= 0 {
		return
	}

	for i := int64(0); i < evictNum; i++ {
		element := vac.lruList.Back()
		attr := element.Value.(*AttrItem)
		if !backGround || (backGround && attr.IsExpired()) {
			vac.lruList.Remove(element)
			delete(vac.cache, attr.Inode)
		}
	}
}

func (vac *VolumeInodeAttrsCache) TotalNum() int {
	vac.RLock()
	defer vac.RUnlock()
	return len(vac.cache)
}

func (vac *VolumeInodeAttrsCache) GetAccessStat() (accssNum, validHit, miss uint64) {
	return vac.accessStat.accessNum, vac.accessStat.validHit, vac.accessStat.miss
}

// type DentryItem metanode.Dentry
type DentryItem struct {
	metanode.Dentry
	expiredTime int64
}

func (di *DentryItem) Key() string {
	return strconv.FormatUint(di.ParentId, 10) + pathSep + di.Name
}

func (di *DentryItem) IsExpired() bool {
	if di.expiredTime < time.Now().Unix() {
		return true
	}
	return false
}

// VolumeDentryCache accelerates translating S3 path to posix-compatible file system metadata within a volume
type VolumeDentryCache struct {
	sync.RWMutex
	cache       map[string]*list.Element
	lruList     *list.List
	maxElements int64
	aStat       AccessStat
}

func NewVolumeDentryCache(maxElements int64) *VolumeDentryCache {
	vdc := &VolumeDentryCache{
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		maxElements: maxElements,
	}
	return vdc
}

func (vdc *VolumeDentryCache) setMaxElements(maxElements int64) {
	vdc.Lock()
	defer vdc.Unlock()
	vdc.maxElements = maxElements
}

func (vdc *VolumeDentryCache) putDentry(dentry *DentryItem) {
	vdc.Lock()
	defer vdc.Unlock()
	key := dentry.Key()
	old, ok := vdc.cache[key]
	if ok {
		oldDentry := old.Value.(*DentryItem)
		log.LogDebugf("replace old dentry: parentID(%v) inode(%v) name(%v) mode(%v) "+
			"with new one: parentID(%v) inode(%v) name(%v) mode(%v)",
			oldDentry.ParentId, oldDentry.Inode, oldDentry.Name, os.FileMode(oldDentry.Type),
			dentry.ParentId, dentry.Inode, dentry.Name, os.FileMode(dentry.Type))

		vdc.lruList.Remove(old)
		delete(vdc.cache, key)
	}

	if int64(vdc.lruList.Len()) > vdc.maxElements {
		vdc.evictDentry(int64(vdc.lruList.Len())-vdc.maxElements, false)
	}

	element := vdc.lruList.PushFront(dentry)
	vdc.cache[key] = element
}

func (vdc *VolumeDentryCache) getDentry(key string) *DentryItem {
	var miss bool
	defer func() {
		vdc.Lock()
		vdc.aStat.accessNum++
		if miss {
			vdc.aStat.miss++
		} else {
			vdc.aStat.validHit++
		}
		vdc.Unlock()
	}()

	vdc.RLock()
	element, ok := vdc.cache[key]
	if !ok {
		miss = true
		vdc.RUnlock()
		return nil
	}
	item := element.Value.(*DentryItem)
	vdc.RUnlock()

	vdc.Lock()
	vdc.lruList.MoveToFront(element)
	vdc.Unlock()
	return item
}

func (vdc *VolumeDentryCache) deleteDentry(key string) {
	vdc.Lock()
	defer vdc.Unlock()

	element, ok := vdc.cache[key]
	if ok {
		dentry := element.Value.(*DentryItem)
		log.LogDebugf("delete dentry in cache: key(%v) dentry(%v) ", key, dentry)
		vdc.lruList.Remove(element)
		delete(vdc.cache, key)
	}
}

func (vdc *VolumeDentryCache) evictDentry(evictNum int64, backGround bool) {
	if evictNum <= 0 {
		return
	}
	for i := int64(0); i < evictNum; i++ {
		element := vdc.lruList.Back()
		dentry := element.Value.(*DentryItem)
		if !backGround || (backGround && dentry.IsExpired()) {
			vdc.lruList.Remove(element)
			delete(vdc.cache, dentry.Key())
		}
	}
}

func (vdc *VolumeDentryCache) TotalNum() int {
	vdc.RLock()
	defer vdc.RUnlock()
	return len(vdc.cache)
}

func (vdc *VolumeDentryCache) GetAccessStat() (accssNum, validHit, miss uint64) {
	return vdc.aStat.accessNum, vdc.aStat.validHit, vdc.aStat.miss
}

type ObjMetaCache struct {
	sync.RWMutex
	volumeDentryCache     map[string]*VolumeDentryCache     // volume --> VolumeDentryCache
	volumeInodeAttrsCache map[string]*VolumeInodeAttrsCache // volume --> VolumeInodeAttrsCache
	maxDentryNum          int64                             // maxDentryNum that all volume share
	maxInodeAttrNum       int64                             // maxInodeAttrNum that all volume share
	refreshIntervalSec    uint64                            // dentry/attr cache expiration time
}

func NewObjMetaCache(maxDentryNum, maxInodeAttrNum int64, refreshInterval uint64) *ObjMetaCache {
	omc := &ObjMetaCache{
		volumeDentryCache:     make(map[string]*VolumeDentryCache),
		volumeInodeAttrsCache: make(map[string]*VolumeInodeAttrsCache),
		maxDentryNum:          maxDentryNum,
		maxInodeAttrNum:       maxInodeAttrNum,
		refreshIntervalSec:    refreshInterval,
	}

	go omc.backGroundEvictItem()
	return omc
}

func (omc *ObjMetaCache) backGroundEvictItem() {
	t := time.NewTicker(time.Duration(BackGroundEvictDurFactor*omc.refreshIntervalSec) * time.Second)
	defer t.Stop()

	for range t.C {
		log.LogDebugf("ObjMetaCache: start backGround evict")
		start := time.Now()
		omc.RLock()
		for _, vac := range omc.volumeInodeAttrsCache {
			vac.Lock()
			evictNum := len(vac.cache) / BackGroundEvictMaxNumFactor
			vac.evictAttr(int64(evictNum), true)
			vac.Unlock()
		}

		for _, vdc := range omc.volumeDentryCache {
			vdc.Lock()
			evictNum := len(vdc.cache) / BackGroundEvictMaxNumFactor
			vdc.evictDentry(int64(evictNum), true)
			vdc.Unlock()
		}

		elapsed := time.Since(start)
		log.LogDebugf("ObjMetaCache: finish backGround evict, dentryCache, cost(%d)ms", elapsed.Milliseconds())
		omc.RUnlock()
	}
}

func (omc *ObjMetaCache) PutAttr(volume string, item *AttrItem) {
	omc.Lock()
	vac, exist := omc.volumeInodeAttrsCache[volume]
	if !exist {
		volumeNum := len(omc.volumeInodeAttrsCache)
		avgMaxNum := omc.maxInodeAttrNum / (int64(volumeNum) + 1)
		vac = NewVolumeInodeAttrsCache(avgMaxNum)

		omc.volumeInodeAttrsCache[volume] = vac
		for _, v := range omc.volumeInodeAttrsCache {
			v.SetMaxElements(avgMaxNum)
		}
		log.LogDebugf("NewVolumeInodeAttrsCache: volume(%v), volumeNum(%v), avgMaxNum(%v)", volume, volumeNum, avgMaxNum)
	}
	omc.Unlock()

	item.expiredTime = time.Now().Unix() + int64(omc.refreshIntervalSec)
	vac.putAttr(item)
	log.LogDebugf("ObjMetaCache PutAttr: volume(%v) attr(%v)", volume, item)
}

func (omc *ObjMetaCache) MergeAttr(volume string, item *AttrItem) {
	omc.Lock()
	vac, exist := omc.volumeInodeAttrsCache[volume]
	if !exist {
		volumeNum := len(omc.volumeInodeAttrsCache)
		avgMaxNum := omc.maxInodeAttrNum / (int64(volumeNum) + 1)
		vac = NewVolumeInodeAttrsCache(avgMaxNum)

		omc.volumeInodeAttrsCache[volume] = vac
		for _, v := range omc.volumeInodeAttrsCache {
			v.SetMaxElements(avgMaxNum)
		}
		log.LogDebugf("NewVolumeInodeAttrsCache: volume(%v), volumeNum(%v), avgMaxNum(%v)", volume, volumeNum, avgMaxNum)
	}
	omc.Unlock()
	log.LogDebugf("ObjMetaCache MergeAttr: volume(%v) attr(%v)", volume, item)
	vac.mergeAttr(item)
}

func (omc *ObjMetaCache) GetAttr(volume string, inode uint64) (attr *AttrItem, needRefresh bool) {
	omc.RLock()
	vac, exist := omc.volumeInodeAttrsCache[volume]
	omc.RUnlock()
	if !exist {
		log.LogDebugf("ObjMetaCache GetAttr: fail, volume(%v) inode(%v) ", volume, inode)
		return nil, false
	}

	attr = vac.getAttr(inode)
	if attr == nil {
		return nil, false
	}
	log.LogDebugf("ObjMetaCache GetAttr: volume(%v) inode(%v) attr(%v)", volume, inode, attr)
	return attr, attr.IsExpired()
}

func (omc *ObjMetaCache) DeleteAttr(volume string, inode uint64) {
	omc.RLock()
	vac, exist := omc.volumeInodeAttrsCache[volume]
	omc.RUnlock()
	if !exist {
		return
	}

	log.LogDebugf("ObjMetaCache DeleteAttr: volume(%v), inode(%v)", volume, inode)
	vac.deleteAttr(inode)
}

func (omc *ObjMetaCache) DeleteAttrWithKey(volume string, inode uint64, key string) {
	omc.RLock()
	vac, exist := omc.volumeInodeAttrsCache[volume]
	omc.RUnlock()
	if !exist {
		return
	}

	log.LogDebugf("ObjMetaCache DeleteAttr: volume(%v), inode(%v)", volume, inode)
	vac.deleteAttrWithKey(inode, key)
}

func (omc *ObjMetaCache) TotalAttrNum() int {
	var total int
	var vacs []*VolumeInodeAttrsCache

	omc.RLock()
	for _, vac := range omc.volumeInodeAttrsCache {
		vacs = append(vacs, vac)
	}
	omc.RUnlock()

	for _, vac := range vacs {
		total += vac.TotalNum()
	}

	return total
}

func (omc *ObjMetaCache) PutDentry(volume string, item *DentryItem) {
	omc.Lock()
	vdc, exist := omc.volumeDentryCache[volume]
	if !exist {
		volumeNum := len(omc.volumeDentryCache)
		avgMaxNum := omc.maxDentryNum / (int64(volumeNum) + 1)
		vdc = NewVolumeDentryCache(avgMaxNum)

		omc.volumeDentryCache[volume] = vdc
		for _, v := range omc.volumeDentryCache {
			v.setMaxElements(avgMaxNum)
		}
		log.LogDebugf("NewVolumeDentryCache: volume(%v), volumeNum(%v), avgMaxNum(%v)", volume, volumeNum, avgMaxNum)
	}
	omc.Unlock()

	item.expiredTime = time.Now().Unix() + int64(omc.refreshIntervalSec)
	vdc.putDentry(item)
	log.LogDebugf("ObjMetaCache PutDentry: volume(%v), DentryItem(%v)", volume, item)
}

func (omc *ObjMetaCache) GetDentry(volume string, key string) (dentry *DentryItem, needRefresh bool) {
	omc.RLock()
	vdc, exist := omc.volumeDentryCache[volume]
	omc.RUnlock()
	if !exist {
		return nil, false
	}

	dentry = vdc.getDentry(key)
	if dentry == nil {
		return nil, false
	}
	log.LogDebugf("ObjMetaCache GetDentry: volume(%v), key(%v), dentry:(%v)", volume, key, dentry)
	return dentry, dentry.IsExpired()
}

func (omc *ObjMetaCache) DeleteDentry(volume string, key string) {
	omc.RLock()
	vdc, exist := omc.volumeDentryCache[volume]
	omc.RUnlock()
	if !exist {
		return
	}

	log.LogDebugf("ObjMetaCache DeleteDentry: volume(%v), key(%v)", volume, key)
	vdc.deleteDentry(key)
}

func (omc *ObjMetaCache) TotalDentryNum() int {
	var total int
	var vdcs []*VolumeDentryCache

	omc.RLock()
	for _, vdc := range omc.volumeDentryCache {
		vdcs = append(vdcs, vdc)
	}
	omc.RUnlock()

	for _, vdc := range vdcs {
		total += vdc.TotalNum()
	}

	return total
}
