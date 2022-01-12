// Copyright 2019 The ChubaoFS Authors.
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
	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"os"
	"strconv"
	"sync"
	"time"
)

type AccessStat struct {
	accssNum uint64
	miss     uint64
	validHit uint64
}

type AttrItem struct {
	proto.XAttrInfo
	UpdateTime int64
}

func (attr *AttrItem) GetUpdateTime() int64 {
	return attr.UpdateTime
}

//VolumeInodeAttrsCache caches Attrs for Inodes
type VolumeInodeAttrsCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	maxElements int64
	aStat       AccessStat
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

func (vac *VolumeInodeAttrsCache) Put(attr *AttrItem) {
	vac.Lock()
	defer vac.Unlock()

	old, ok := vac.cache[attr.Inode]
	if ok {
		oldAttrs := old.Value.(*AttrItem)
		log.LogDebugf("replace old attrs: inode(%v) attr(%v)"+
			"with new ones: attr(%v)", attr.Inode, oldAttrs, attr)

		vac.lruList.Remove(old)
		delete(vac.cache, attr.Inode)
	}

	if int64(vac.lruList.Len()) > vac.maxElements {
		vac.evict(vac.maxElements - int64(vac.lruList.Len()))
	}

	element := vac.lruList.PushFront(attr)
	attr.UpdateTime = time.Now().Unix()
	vac.cache[attr.Inode] = element
}

func (vac *VolumeInodeAttrsCache) Merge(attr *AttrItem) {
	vac.RLock()
	old, ok := vac.cache[attr.Inode]
	vac.RUnlock()
	if ok {
		oldAttrs := old.Value.(*AttrItem)
		for key, val := range oldAttrs.XAttrs {
			_, exist := attr.XAttrs[key]
			if !exist {
				attr.XAttrs[key] = val
			}
		}
	}

	vac.Put(attr)

}

func (vac *VolumeInodeAttrsCache) Get(inode uint64) *AttrItem {
	var miss bool
	defer func() {
		vac.Lock()
		vac.aStat.accssNum++
		if miss {
			vac.aStat.miss++
		} else {
			vac.aStat.validHit++
		}
		vac.Unlock()
	}()

	vac.RLock()
	element, ok := vac.cache[inode]
	if !ok {
		log.LogDebugf("VolumeInodeAttrsCache Get failed: inode(%v)", inode)
		miss = true
		vac.RUnlock()
		return nil
	}

	item := element.Value.(*AttrItem)
	vac.RUnlock()
	vac.Lock()
	vac.lruList.MoveToFront(element)
	vac.Unlock()
	log.LogDebugf("VolumeInodeAttrsCache Get: inode(%v) attrs(%v)", inode, item)
	return item
}

func (vac *VolumeInodeAttrsCache) Delete(inode uint64) {
	vac.Lock()
	defer vac.Unlock()

	element, ok := vac.cache[inode]
	attr := element.Value.(*AttrItem)
	if ok {
		log.LogDebugf("delete attr in cache: inode(%v) attr(%v) ", inode, attr)
		vac.lruList.Remove(element)
		delete(vac.cache, inode)
	}
}

func (vac *VolumeInodeAttrsCache) evict(evictNum int64) {
	if evictNum <= 0 {
		return
	}

	for i := int64(0); i < evictNum; i++ {
		element := vac.lruList.Back()
		attr := element.Value.(*AttrItem)
		vac.lruList.Remove(element)
		delete(vac.cache, attr.Inode)
	}
}

func (vac *VolumeInodeAttrsCache) TotalNum() int {
	vac.RLock()
	defer vac.RUnlock()
	return len(vac.cache)
}

func (vac *VolumeInodeAttrsCache) GetAccessStat() (accssNum, validHit, miss uint64) {
	return vac.aStat.accssNum, vac.aStat.validHit, vac.aStat.miss
}

//type DentryItem metanode.Dentry

type DentryItem struct {
	metanode.Dentry
	UpdateTime int64
}

func (di *DentryItem) Key() string {
	return strconv.FormatUint(di.ParentId, 10) + pathSep + di.Name
}

func (di *DentryItem) GetUpdateTime() int64 {
	return di.UpdateTime
}

//VolumeDentryCache accelerates translating from Amazon S3 path to posix-compatible file system metadata within a volume
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

func (vdc *VolumeDentryCache) SetMaxElements(maxElements int64) {
	vdc.Lock()
	defer vdc.Unlock()
	vdc.maxElements = maxElements
}

func (vdc *VolumeDentryCache) Put(dentry *DentryItem) {
	vdc.Lock()
	defer vdc.Unlock()
	key := dentry.Key()
	old, ok := vdc.cache[key]
	if ok {
		oldDentry := old.Value.(*DentryItem)
		log.LogDebugf("replace old dentry: parentID(%v) inode(%v) "+
			"name(%v) mode(%v) with new ones: parentID(%v) inode(%v) name(%v) mode(%v)",
			oldDentry.ParentId, oldDentry.Inode, oldDentry.Name, os.FileMode(oldDentry.Type),
			dentry.ParentId, dentry.Inode, dentry.Name, os.FileMode(dentry.Type))

		vdc.lruList.Remove(old)
		delete(vdc.cache, key)
	}

	if int64(vdc.lruList.Len()) > vdc.maxElements {
		vdc.evict(vdc.maxElements - int64(vdc.lruList.Len()))
	}

	element := vdc.lruList.PushFront(dentry)
	dentry.UpdateTime = time.Now().Unix()
	vdc.cache[key] = element
}

func (vdc *VolumeDentryCache) Get(key string) *DentryItem {
	var miss bool
	defer func() {
		vdc.Lock()
		vdc.aStat.accssNum++
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

func (vdc *VolumeDentryCache) Delete(key string) {
	vdc.Lock()
	defer vdc.Unlock()

	element, ok := vdc.cache[key]
	dentry := element.Value.(*DentryItem)
	if ok {
		log.LogDebugf("delete dentry in cache: key(%v) dentry(%v) ", key, dentry)
		vdc.lruList.Remove(element)
		delete(vdc.cache, key)
	}
}

func (vdc *VolumeDentryCache) evict(evictNum int64) {
	if evictNum <= 0 {
		return
	}

	for i := int64(0); i < evictNum; i++ {
		element := vdc.lruList.Back()
		dentry := element.Value.(*DentryItem)
		vdc.lruList.Remove(element)
		delete(vdc.cache, dentry.Key())
	}
}

func (vdc *VolumeDentryCache) TotalNum() int {
	vdc.RLock()
	defer vdc.RUnlock()
	return len(vdc.cache)
}

func (vdc *VolumeDentryCache) GetAccessStat() (accssNum, validHit, miss uint64) {
	return vdc.aStat.accssNum, vdc.aStat.validHit, vdc.aStat.miss
}

type ObjMetaCache struct {
	sync.RWMutex
	vdCache         map[string]*VolumeDentryCache
	vaCache         map[string]*VolumeInodeAttrsCache
	maxDentryNum    int64
	maxInodeAttrNum int64
	refreshInterval uint64 //seconds
}

func NewObjMetaCache(maxDentryNum, maxInodeAttrNum int64, refreshInterval uint64) *ObjMetaCache {
	omCache := &ObjMetaCache{
		vdCache:         make(map[string]*VolumeDentryCache),
		vaCache:         make(map[string]*VolumeInodeAttrsCache),
		maxDentryNum:    maxDentryNum,
		maxInodeAttrNum: maxInodeAttrNum,
		refreshInterval: refreshInterval,
	}
	return omCache
}

func (omc *ObjMetaCache) PutAttr(volume string, item *AttrItem) {
	omc.Lock()
	vac, exist := omc.vaCache[volume]
	if !exist {
		volumeNum := len(omc.vaCache)
		avgMaxNum := omc.maxInodeAttrNum / (int64(volumeNum) + 1)
		vac = NewVolumeInodeAttrsCache(avgMaxNum)

		omc.vaCache[volume] = vac
		for _, v := range omc.vaCache {
			v.SetMaxElements(avgMaxNum)
		}
		log.LogDebugf("NewVolumeInodeAttrsCache: volume(%v), volumeNum(%v), avgMaxNum(%v)",
			volume, volumeNum, avgMaxNum)
	}
	omc.Unlock()

	vac.Put(item)
	log.LogDebugf("ObjMetaCache PutAttr: volume(%v) attr(%v)", volume, item)
}

func (omc *ObjMetaCache) MergeAttr(volume string, item *AttrItem) {
	omc.Lock()
	vac, exist := omc.vaCache[volume]
	if !exist {
		volumeNum := len(omc.vaCache)
		avgMaxNum := omc.maxInodeAttrNum / (int64(volumeNum) + 1)
		vac = NewVolumeInodeAttrsCache(avgMaxNum)

		omc.vaCache[volume] = vac
		for _, v := range omc.vaCache {
			v.SetMaxElements(avgMaxNum)
		}
		log.LogDebugf("NewVolumeInodeAttrsCache: volume(%v), volumeNum(%v), avgMaxNum(%v)",
			volume, volumeNum, avgMaxNum)
	}
	omc.Unlock()
	log.LogDebugf("ObjMetaCache MergeAttr: volume(%v) attr(%v)",
		volume, item)
	vac.Merge(item)
}

func (omc *ObjMetaCache) GetAttr(volume string, inode uint64) (attr *AttrItem, needRefresh bool) {
	omc.RLock()
	vac, exist := omc.vaCache[volume]
	omc.RUnlock()
	if !exist {
		log.LogDebugf("ObjMetaCache GetAttr: fail, volume(%v) inode(%v) ",
			volume, inode)
		return nil, false
	}

	attr = vac.Get(inode)
	if attr == nil {
		return nil, false
	} else {
		log.LogDebugf("ObjMetaCache GetAttr: volume(%v) inode(%v) attr(%v)",
			volume, inode, attr)
		return attr, attr.UpdateTime+int64(omc.refreshInterval) <= time.Now().Unix()
	}
}

func (omc *ObjMetaCache) DeleteAttr(volume string, inode uint64) {
	omc.RLock()
	vac, exist := omc.vaCache[volume]
	omc.RUnlock()
	if !exist {
		return
	}

	log.LogDebugf("ObjMetaCache DeleteAttr: volume(%v), inode(%v)", volume, inode)
	vac.Delete(inode)
}

func (omc *ObjMetaCache) TotalAttrNum() int {
	var total int
	var vacs []*VolumeInodeAttrsCache

	omc.RLock()
	for _, vac := range omc.vaCache {
		vacs = append(vacs, vac)
	}
	omc.RUnlock()

	for _, vac := range vacs {
		total += vac.TotalNum()
	}

	return total
}

/*
func (omc *ObjMetaCache) RecursiveLookupTarget(volume, path string) (dentry *DentryItem) {
	parent := rootIno
	var pathIterator = NewPathIterator(path)
	if !pathIterator.HasNext() {
		return nil
	}
	for pathIterator.HasNext() {
		var pathItem = pathIterator.Next()

		dentry = &DentryItem{
			Dentry: metanode.Dentry{
				ParentId: parent,
				Name:     pathItem.Name,
			},
		}

		dentry = omc.GetDentry(volume, dentry.Key())
		if dentry == nil {
			log.LogDebugf("cache recursiveLookupPath: lookup fail, parentID(%v) name(%v) fail",
				parent, pathItem.Name)
			return
		}

		log.LogDebugf("cache recursiveLookupPath: lookup item: parentID(%v) inode(%v) name(%v) mode(%v)",
			dentry.ParentId, dentry.Inode, dentry.Name, os.FileMode(dentry.Type))
		// Check file mode
		if os.FileMode(dentry.Type).IsDir() != pathItem.IsDirectory {
			return nil
		}
		if pathIterator.HasNext() {
			parent = dentry.Inode
			continue
		}
		break
	}
	return
}*/

func (omc *ObjMetaCache) PutDentry(volume string, item *DentryItem) {
	omc.Lock()
	vdc, exist := omc.vdCache[volume]
	if !exist {
		volumeNum := len(omc.vdCache)
		avgMaxNum := omc.maxDentryNum / (int64(volumeNum) + 1)
		vdc = NewVolumeDentryCache(avgMaxNum)

		omc.vdCache[volume] = vdc
		for _, v := range omc.vdCache {
			v.SetMaxElements(avgMaxNum)
		}
		log.LogDebugf("NewVolumeDentryCache: volume(%v), volumeNum(%v), avgMaxNum(%v)",
			volume, volumeNum, avgMaxNum)
	}
	omc.Unlock()

	vdc.Put(item)
	log.LogDebugf("ObjMetaCache PutDentry: volume(%v), DentryItem(%v)", volume, item)
}

func (omc *ObjMetaCache) GetDentry(volume string, key string) (dentry *DentryItem, needRefresh bool) {
	omc.RLock()
	vdc, exist := omc.vdCache[volume]
	omc.RUnlock()
	if !exist {
		return nil, false
	}

	dentry = vdc.Get(key)
	if dentry == nil {
		return nil, false
	} else {
		log.LogDebugf("ObjMetaCache GetDentry: volume(%v), key(%v), dentry:(%v)", volume, key, dentry)
		return dentry, dentry.UpdateTime+int64(omc.refreshInterval) <= time.Now().Unix()
	}

}

func (omc *ObjMetaCache) DeleteDentry(volume string, key string) {
	omc.RLock()
	vdc, exist := omc.vdCache[volume]
	omc.RUnlock()
	if !exist {
		return
	}

	log.LogDebugf("ObjMetaCache DeleteDentry: volume(%v), key(%v)", volume, key)
	vdc.Delete(key)
}

func (omc *ObjMetaCache) TotalDentryNum() int {
	var total int
	var vdcs []*VolumeDentryCache

	omc.RLock()
	for _, vdc := range omc.vdCache {
		vdcs = append(vdcs, vdc)
	}
	omc.RUnlock()

	for _, vdc := range vdcs {
		total += vdc.TotalNum()
	}

	return total
}
