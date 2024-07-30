// Copyright 2024 The CubeFS Authors.
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

package catalog

import (
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type shardInfoBase struct {
	clustermgr.Shard
}

type shardItem struct {
	shardID proto.ShardID
	units   []*shardUnit
	info    shardInfoBase
	lock    sync.RWMutex
}

func (s *shardItem) withRLocked(f func() error) error {
	s.lock.RLock()
	err := f()
	s.lock.RUnlock()
	return err
}

func (s *shardItem) withLocked(f func() error) error {
	s.lock.Lock()
	err := f()
	s.lock.Unlock()
	return err
}

func (s *shardItem) isValid() bool {
	if s.shardID == proto.InvalidShardID {
		return false
	}
	for _, unit := range s.units {
		if !unit.isValid() {
			return false
		}
	}
	return true
}

func (s *shardItem) toShardRecord() *catalogdb.ShardInfoRecord {
	suidPrefixs := make([]proto.SuidPrefix, 0, len(s.units))
	for _, unit := range s.units {
		suidPrefixs = append(suidPrefixs, unit.suidPrefix)
	}
	return &catalogdb.ShardInfoRecord{
		Version:      catalogdb.ShardInfoVersionNormal,
		ShardID:      s.shardID,
		SuidPrefixes: suidPrefixs,
		LeaderDiskID: s.info.LeaderDiskID,
		Range:        s.info.Range,
		RouteVersion: s.info.RouteVersion,
	}
}

func (s *shardItem) toShardInfo() clustermgr.Shard {
	return s.info.Shard
}

type shardUnit struct {
	suidPrefix proto.SuidPrefix
	epoch      uint32
	nextEpoch  uint32
	info       *clustermgr.ShardUnitInfo
}

func (u *shardUnit) isValid() bool {
	suid := proto.EncodeSuid(u.suidPrefix.ShardID(), u.suidPrefix.Index(), u.epoch)
	return u.info.Suid.IsValid() && suid.IsValid() && u.epoch <= u.nextEpoch
}

func (u *shardUnit) toShardUnitRecord() (ret *catalogdb.ShardUnitInfoRecord) {
	return &catalogdb.ShardUnitInfoRecord{
		Version:    catalogdb.ShardUnitInfoVersionNormal,
		SuidPrefix: u.suidPrefix,
		Epoch:      u.epoch,
		NextEpoch:  u.nextEpoch,
		DiskID:     u.info.DiskID,
		Status:     u.info.Status,
		Learner:    u.info.Learner,
	}
}

type spaceItem struct {
	spaceID proto.SpaceID
	info    *clustermgr.Space

	lock sync.RWMutex
}

func (d *spaceItem) withRLocked(f func() error) error {
	d.lock.RLock()
	err := f()
	d.lock.RUnlock()
	return err
}

func newConcurrentShards(sliceMapNum uint32) *concurrentShards {
	m := &concurrentShards{
		num:   sliceMapNum,
		m:     make(map[uint32]map[proto.ShardID]*shardItem),
		locks: make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < sliceMapNum; i++ {
		m.locks[i] = &sync.RWMutex{}
		m.m[i] = make(map[proto.ShardID]*shardItem)
	}

	return m
}

// concurrentShards is an effective data struct (concurrent map implements)
type concurrentShards struct {
	num   uint32
	m     map[uint32]map[proto.ShardID]*shardItem
	locks map[uint32]*sync.RWMutex
}

// get shard from concurrentShards
func (s *concurrentShards) getShard(shardID proto.ShardID) *shardItem {
	idx := uint32(shardID) % s.num
	s.locks[idx].RLock()
	shard := s.m[idx][shardID]
	s.locks[idx].RUnlock()
	return shard
}

// put new shard into concurrentShards
func (s *concurrentShards) putShard(d *shardItem) {
	idx := uint32(d.shardID) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	_, ok := s.m[idx][d.shardID]
	// shard already exist
	if ok {
		return
	}
	s.m[idx][d.shardID] = d
}

// range concurrentShards, it only used in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *concurrentShards) rangeShard(f func(d *shardItem) error) {
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		for _, v := range s.m[i] {
			err := f(v)
			if err != nil {
				l.RUnlock()
				return
			}
		}
		l.RUnlock()
	}
}

// getShardNum get all shard num
func (s *concurrentShards) getShardNum() int {
	length := 0
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		length += len(s.m[i])
		l.RUnlock()
	}
	return length
}

func (s *concurrentShards) list() []*shardItem {
	ret := make([]*shardItem, 0, s.getShardNum()*2)
	s.rangeShard(func(v *shardItem) error {
		ret = append(ret, v)
		return nil
	})
	return ret
}

func newConcurrentSpaces(splitMapNum uint32) *concurrentSpaces {
	spaces := &concurrentSpaces{
		num:     splitMapNum,
		idMap:   make(map[uint32]map[proto.SpaceID]*spaceItem),
		nameMap: make(map[uint32]map[string]*spaceItem),
		locks:   make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < splitMapNum; i++ {
		spaces.locks[i] = &sync.RWMutex{}
		spaces.idMap[i] = make(map[proto.SpaceID]*spaceItem)
		spaces.nameMap[i] = make(map[string]*spaceItem)
	}
	return spaces
}

// concurrentSpaces is an effective data struct (concurrent map implements)
type concurrentSpaces struct {
	num     uint32
	idMap   map[uint32]map[proto.SpaceID]*spaceItem
	nameMap map[uint32]map[string]*spaceItem
	locks   map[uint32]*sync.RWMutex
}

// getSpaceByName space from concurrentSpaces
func (s *concurrentSpaces) getSpaceByName(name string) *spaceItem {
	idx := s.nameCharSum(name) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.nameMap[idx][name]
}

// getSpaceByID space from concurrentSpaces
func (s *concurrentSpaces) getSpaceByID(id proto.SpaceID) *spaceItem {
	idx := uint32(id) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.idMap[idx][id]
}

// put new space into shardedSpace
func (s *concurrentSpaces) putSpace(v *spaceItem) {
	idx := s.nameCharSum(v.info.Name) % s.num
	s.locks[idx].Lock()
	s.nameMap[idx][v.info.Name] = v
	s.locks[idx].Unlock()

	id := v.spaceID
	idx = uint32(id) % s.num
	s.locks[idx].Lock()
	s.idMap[idx][id] = v
	s.locks[idx].Unlock()
}

func (s *concurrentSpaces) nameCharSum(name string) (ret uint32) {
	for i := range name {
		ret += uint32(name[i])
	}
	return
}

func shardRecordToShardInfo(shardRecord *catalogdb.ShardInfoRecord, units []clustermgr.ShardUnit) shardInfoBase {
	return shardInfoBase{
		Shard: clustermgr.Shard{
			ShardID:      shardRecord.ShardID,
			LeaderDiskID: shardRecord.LeaderDiskID,
			Range:        shardRecord.Range,
			Units:        units,
			RouteVersion: shardRecord.RouteVersion,
		},
	}
}

func shardUnitRecordToShardUnit(shardRecord *catalogdb.ShardInfoRecord, unitRecord *catalogdb.ShardUnitInfoRecord) (ret *shardUnit) {
	return &shardUnit{
		suidPrefix: unitRecord.SuidPrefix,
		epoch:      unitRecord.Epoch,
		nextEpoch:  unitRecord.NextEpoch,
		info: &clustermgr.ShardUnitInfo{
			Suid:         proto.EncodeSuid(unitRecord.SuidPrefix.ShardID(), unitRecord.SuidPrefix.Index(), unitRecord.Epoch),
			DiskID:       unitRecord.DiskID,
			LeaderDiskID: shardRecord.LeaderDiskID,
			Range:        shardRecord.Range,
			Learner:      unitRecord.Learner,
			Status:       unitRecord.Status,
			RouteVersion: shardRecord.RouteVersion,
		},
	}
}

func shardUnitsToShardUnitRecords(units []*shardUnit) []*catalogdb.ShardUnitInfoRecord {
	ret := make([]*catalogdb.ShardUnitInfoRecord, len(units))
	for i, unit := range units {
		ret[i] = unit.toShardUnitRecord()
	}
	return ret
}

type routeItem struct {
	RouteVersion proto.RouteVersion
	Type         proto.CatalogChangeItemType
	ItemDetail   interface{}
}

type routeItemShardAdd struct {
	ShardID proto.ShardID
}

type routeItemShardUpdate struct {
	SuidPrefix proto.SuidPrefix
}

func routeRecordToRouteItem(info *catalogdb.RouteInfoRecord) *routeItem {
	item := &routeItem{
		RouteVersion: info.RouteVersion,
		Type:         info.Type,
	}
	switch info.Type {
	case proto.CatalogChangeItemAddShard:
		itemDetail := info.ItemDetail.(*catalogdb.RouteInfoShardAdd)
		item.ItemDetail = &routeItemShardAdd{ShardID: itemDetail.ShardID}
	case proto.CatalogChangeItemUpdateShard:
		itemDetail := info.ItemDetail.(*catalogdb.RouteInfoShardUpdate)
		item.ItemDetail = &routeItemShardUpdate{SuidPrefix: itemDetail.SuidPrefix}
	default:
	}

	return item
}

func routeItemToRouteRecord(item *routeItem) *catalogdb.RouteInfoRecord {
	record := &catalogdb.RouteInfoRecord{
		Version:      catalogdb.RouteInfoVersionNormal,
		RouteVersion: item.RouteVersion,
		Type:         item.Type,
	}
	switch item.Type {
	case proto.CatalogChangeItemAddShard:
		itemDetail := item.ItemDetail.(*routeItemShardAdd)
		record.ItemDetail = &catalogdb.RouteInfoShardAdd{ShardID: itemDetail.ShardID}
	case proto.CatalogChangeItemUpdateShard:
		itemDetail := item.ItemDetail.(*routeItemShardUpdate)
		record.ItemDetail = &catalogdb.RouteInfoShardUpdate{SuidPrefix: itemDetail.SuidPrefix}
	default:
	}

	return record
}
