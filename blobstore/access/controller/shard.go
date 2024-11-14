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

package controller

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/btree"
	"golang.org/x/sync/singleflight"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	defaultBTreeDegree     = 16
	defaultShardReloadSecs = 120
)

type IShardController interface {
	GetShard(ctx context.Context, shardKeys [][]byte) (Shard, error)
	GetShardByID(ctx context.Context, shardID proto.ShardID) (Shard, error)
	GetShardByRange(ctx context.Context, shardRange sharding.Range) (Shard, error)
	GetFisrtShard(ctx context.Context) (Shard, error)
	GetNextShard(ctx context.Context, shardRange sharding.Range) (Shard, error)
	GetSpaceID() proto.SpaceID
	UpdateRoute(ctx context.Context) error
	UpdateShard(ctx context.Context, ss shardnode.ShardStats) error
}

type shardCtrlConf struct {
	clusterID  proto.ClusterID
	reloadSecs int
	space      SpaceConf
}

type SpaceConf struct {
	Name string `json:"name"`
	AK   string `json:"ak"`
	SK   string `json:"sk"`
}

func (c *SpaceConf) IsValid() bool {
	return c.Name != "" && c.AK != "" && c.SK != ""
}

func NewShardController(conf shardCtrlConf, cmCli clustermgr.ClientAPI, punishCtrl ServiceController, stopCh <-chan struct{}) (IShardController, error) {
	defaulter.Equal(&conf.reloadSecs, defaultShardReloadSecs)

	s := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shard),
		ranges: btree.New(defaultBTreeDegree),
		conf:   conf,
		cmCli:  cmCli,
		stopCh: stopCh,

		punishCtrl: punishCtrl,
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("start new shard controller, conf:%+v", conf)

	err := s.initSpace(ctx)
	if err != nil {
		return nil, err
	}

	err = s.initRoute(ctx)
	if err != nil {
		return nil, err
	}

	go s.incrementalRoute()

	span.Debugf("success to new shard controller, clusterID:%d, space:%s", conf.clusterID, conf.space.Name)
	return s, nil
}

type shardControllerImpl struct {
	shards       map[proto.ShardID]*shard
	ranges       *btree.BTree
	version      proto.RouteVersion
	spaceID      proto.SpaceID
	groupRun     singleflight.Group
	sync.RWMutex // todo: I will optimize locker in the next version

	conf       shardCtrlConf
	cmCli      clustermgr.ClientAPI
	punishCtrl ServiceController
	stopCh     <-chan struct{}
}

func (s *shardControllerImpl) GetShard(ctx context.Context, shardKeys [][]byte) (Shard, error) {
	// shard_1 ranges [1, 100) , shard 2: [100, 200), shard 3: [200, 300) ...
	// if compare shard keys=20, it belong to shard 1 ; if keys=100, it belong to shard 2 ; keys=220, belong to shard 3
	// if keys=120, will walk [shard 2, shard end]
	s.RLock()
	defer s.RUnlock()

	span := trace.SpanFromContextSafe(ctx)
	ci := sharding.NewCompareItem(sharding.RangeType_RangeTypeHash, shardKeys)
	var si *shard
	pivot := &compareItem{ci: *ci}

	s.ranges.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		si = i.(*shard)
		// span.Debugf("shardID=%d, max boundary=%d, compare=%d, shard=%+v", si.shardID, si.rangeExt.MaxBoundary(), ci.GetBoundary(), *si)
		if si.belong(ci) {
			return false
		}
		si = nil
		return true
	})

	if si == nil { // not found expect shard
		span.Errorf("not find shard. name:%s, shard len:%d", shardKeys, s.ranges.Len())
		return nil, errcode.ErrAccessNotFoundShard
	}
	return si, nil
}

func (s *shardControllerImpl) GetShardByID(ctx context.Context, shardID proto.ShardID) (Shard, error) {
	sd, ok := s.getShardByID(shardID)
	if ok {
		return sd, nil
	}

	// not found expect shard
	return nil, errcode.ErrAccessNotFoundShard
}

func (s *shardControllerImpl) GetFisrtShard(ctx context.Context) (Shard, error) {
	s.RLock()
	defer s.RUnlock()

	span := trace.SpanFromContextSafe(ctx)
	min := s.ranges.Min()
	if min == nil { // not found expect shard
		span.Errorf("not find shard. name:%s, shard len:%d", s.ranges.Len())
		return nil, errcode.ErrAccessNotFoundShard
	}

	return min.(*shard), nil
}

func (s *shardControllerImpl) GetShardByRange(ctx context.Context, shardRange sharding.Range) (Shard, error) {
	s.RLock()
	defer s.RUnlock()

	span := trace.SpanFromContextSafe(ctx)
	var si *shard
	pivot := &shard{rangeExt: shardRange}

	// todo: shard ranges split, old shard range cant find
	s.ranges.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		si = i.(*shard)
		if si.contain(&shardRange) {
			return false
		}
		si = nil
		return true
	})

	if si == nil { // not found expect shard
		span.Errorf("not find shard. range:%s, shard len:%d", common.RawString(shardRange), s.ranges.Len())
		return nil, errcode.ErrAccessNotFoundShard
	}
	return si, nil
}

func (s *shardControllerImpl) GetNextShard(ctx context.Context, shardRange sharding.Range) (Shard, error) {
	s.RLock()
	defer s.RUnlock()

	span := trace.SpanFromContextSafe(ctx)
	// range is the end, the last one
	if s.ranges.Max().(*shard).contain(&shardRange) {
		return nil, nil
	}

	var si *shard
	pivot := &shard{rangeExt: shardRange}
	// todo: If two shard merge, it is possible that the shard queried contains the shard range
	s.ranges.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		si = i.(*shard)
		if !si.contain(&shardRange) {
			return false
		}
		si = nil
		return true
	})

	if si == nil { // not found expect shard
		span.Errorf("not find shard. range:%s, shard len:%d", common.RawString(shardRange), s.ranges.Len())
		return nil, errcode.ErrAccessNotFoundShard
	}
	return si, nil
}

func (s *shardControllerImpl) GetSpaceID() proto.SpaceID {
	return s.spaceID
}

func (s *shardControllerImpl) UpdateRoute(ctx context.Context) error {
	// Aggregation blob operations which comes from upper-layer
	_, err, _ := s.groupRun.Do("updateRoute", func() (interface{}, error) {
		// there is only one updateRoute, the same time. no concurrence
		err1 := s.updateRoute(ctx)
		return nil, err1
	})
	return err
}

func (s *shardControllerImpl) UpdateShard(ctx context.Context, sd shardnode.ShardStats) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("will update shard=%+v", sd)

	s.groupRun.Do("shardID-"+sd.Suid.ShardID().ToString(), func() (interface{}, error) {
		if isInvalidShardStat(sd) {
			panic(fmt.Sprintf("invalid shard get from shard node. shard info:%+v", sd))
		}
		// todo: optimize lock at next version; will split smaller Lock or do a copy update
		s.Lock()
		defer s.Unlock()

		// skip old route version
		oldShard, exist := s.getShardNoLock(sd.Suid.ShardID())
		if !exist || oldShard.version >= sd.RouteVersion {
			span.Warnf("dont need update shard, exist:%t, current shard:%v, replace shard:%v", exist, oldShard, sd)
			return nil, nil
		}

		newShard := &shard{
			shardID:      sd.Suid.ShardID(),
			version:      sd.RouteVersion,
			leaderDiskID: sd.LeaderDiskID,
			rangeExt:     sd.Range,
			units:        sd.Units,
			punishCtrl:   s.punishCtrl,
		}
		s.replaceShard(newShard)

		return nil, nil
	})
	return nil
}

func (s *shardControllerImpl) initSpace(ctx context.Context) error {
	token, err := clustermgr.EncodeAuthInfo(&clustermgr.AuthInfo{
		AccessKey: s.conf.space.AK,
		SecretKey: s.conf.space.SK,
	})
	if err != nil {
		return err
	}

	err = s.cmCli.AuthSpace(ctx, &clustermgr.AuthSpaceArgs{
		Name:  s.conf.space.Name,
		Token: token,
	})
	if err != nil {
		return err
	}

	ret, err := s.cmCli.GetSpaceByName(ctx, &clustermgr.GetSpaceByNameArgs{
		Name: s.conf.space.Name,
	})
	if err != nil {
		return err
	}

	s.spaceID = ret.SpaceID
	return nil
}

func (s *shardControllerImpl) initRoute(ctx context.Context) error {
	return s.updateRoute(ctx)
}

func (s *shardControllerImpl) incrementalRoute() {
	tk := time.NewTicker(time.Second * time.Duration(s.conf.reloadSecs))
	defer tk.Stop()

	for {
		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		select {
		case <-tk.C:
			// there is only one updateRoute, the same time. no concurrence
			err := s.UpdateRoute(ctx)
			span.Debugf("loop update catalog route, err:%+v", err)

		case <-s.stopCh:
			span.Info("exit shard controller")
			return
		}
	}
}

// called by period task, or read/write fail
func (s *shardControllerImpl) updateRoute(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	s.RLock()
	version := s.version
	s.RUnlock()

	ret, err := s.cmCli.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{
		RouteVersion: version,
	})
	if err != nil {
		span.Errorf("fail to get catalog from clusterMgr. err:%+v", err)
		return err
	}

	// skip
	if version >= ret.RouteVersion || len(ret.Items) == 0 {
		span.Debugf("skip get catalog changes, catalog=%+v", *ret)
		return nil
	}

	// todo: optimize lock at next version; will split smaller Lock or do a copy update
	s.Lock()
	defer s.Unlock()

	for _, item := range ret.Items {
		switch item.Type {
		case proto.CatalogChangeItemAddShard:
			err = s.handleShardAdd(ctx, item)
		case proto.CatalogChangeItemUpdateShard:
			err = s.handleShardUpdate(ctx, item)
		default:
			span.Warnf("not expected catalog. type=%d, version=%d", item.Type, item.RouteVersion)
		}
		if err != nil {
			span.Errorf("update shard error:%+v, item:%+v", err, item)
			panic(err)
		}
	}

	s.version = ret.RouteVersion
	span.Debugf("success to update catalog route version %d", ret.RouteVersion)
	return nil
}

func (s *shardControllerImpl) handleShardAdd(ctx context.Context, item clustermgr.CatalogChangeItem) error {
	span := trace.SpanFromContextSafe(ctx)

	val := clustermgr.CatalogChangeShardAdd{}
	err := val.Unmarshal(item.Item.Value)
	if err != nil {
		span.Warnf("json unmarshal failed. type=%d, version=%d, err=%+v", item.Type, item.RouteVersion, err)
		return err
	}
	// span.Debugf("----debug---- catalog add item, typeUrl:%s, byte:%v", item.Item.TypeUrl, item.Item.Value)

	// skip invalid item
	leaderIdx := -1
	for i, unit := range val.Units {
		if unit.LeaderDiskID != 0 {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		span.Warnf("skip invalid item add, leader disk id is zero. item:%+v", val)
		return nil
	}

	sh := &shard{
		shardID:      val.ShardID,
		version:      val.RouteVersion,
		leaderDiskID: val.Units[leaderIdx].LeaderDiskID,
		rangeExt:     val.Units[leaderIdx].Range,
		units:        convertShardUnitInfo(val.Units),
		punishCtrl:   s.punishCtrl,
	}
	s.addShardNoLock(sh)

	span.Debugf("handle one catalog item add :%+v", val)
	return nil
}

func (s *shardControllerImpl) handleShardUpdate(ctx context.Context, item clustermgr.CatalogChangeItem) error {
	span := trace.SpanFromContextSafe(ctx)

	val := clustermgr.CatalogChangeShardUpdate{}
	err := val.Unmarshal(item.Item.Value)
	if err != nil {
		span.Warnf("json unmarshal failed. type=%d, version=%d, err=%+v", item.Type, item.RouteVersion, err)
		return err
	}

	// skip invalid item
	if val.Unit.LeaderDiskID == 0 {
		span.Warnf("skip invalid item update, leader disk id is zero. item:%+v", val)
		return nil
	}

	// update
	ok := s.setShardByID(val.ShardID, &val)
	if !ok {
		span.Warnf("update shard failed. type=%d, version=%d, shardID=%d", item.Type, item.RouteVersion, val.ShardID)
	}

	span.Debugf("handle one catalog item update:%+v", val)
	return nil
}

func (s *shardControllerImpl) replaceShard(si *shard) {
	s.delShardNoLock(si)
	s.addShardNoLock(si)
}

func (s *shardControllerImpl) delShardNoLock(si *shard) {
	sd, ok := s.shards[si.shardID]
	if ok {
		s.ranges.Delete(sd)
		delete(s.shards, si.shardID)
	}
}

func (s *shardControllerImpl) addShardNoLock(si *shard) {
	s.shards[si.shardID] = si
	s.ranges.ReplaceOrInsert(si)
}

func (s *shardControllerImpl) getShardNoLock(id proto.ShardID) (*shard, bool) {
	sd, ok := s.shards[id]
	return sd, ok
}

func (s *shardControllerImpl) getShardByID(shardID proto.ShardID) (*shard, bool) {
	s.RLock()
	defer s.RUnlock()

	info, ok := s.shards[shardID]
	return info, ok
}

func (s *shardControllerImpl) setShardByID(shardID proto.ShardID, val *clustermgr.CatalogChangeShardUpdate) bool {
	info, ok := s.shards[shardID]
	if !ok {
		return false
	}

	info.version = val.RouteVersion
	info.leaderDiskID = val.Unit.LeaderDiskID
	// info.rangeExt = val.Unit.Range  // todo: will update range next version
	idx := val.Unit.Suid.Index()
	info.units[idx] = clustermgr.ShardUnit{
		Suid:    val.Unit.Suid,
		DiskID:  val.Unit.DiskID,
		Learner: val.Unit.Learner,
	}
	return true
}

// ShardOpInfo for upper level(stream) use, get ShardOpHeader information
type ShardOpInfo struct {
	DiskID       proto.DiskID
	Suid         proto.Suid
	RouteVersion proto.RouteVersion
}

type Shard interface {
	GetShardID() proto.ShardID
	GetRange() sharding.Range
	GetMember(mode acapi.GetShardMode, exclude proto.DiskID) ShardOpInfo
}

// shard implement btree.Item interface, shard route information
type shard struct {
	shardID      proto.ShardID
	leaderDiskID proto.DiskID
	version      proto.RouteVersion
	rangeExt     sharding.Range
	units        []clustermgr.ShardUnit
	punishCtrl   ServiceController
}

func (i *shard) Less(item btree.Item) bool {
	switch than := item.(type) {
	case *shard:
		return i.rangeExt.MaxBoundary().Less(than.rangeExt.MaxBoundary())
	case *compareItem:
		return i.rangeExt.MaxBoundary().Less(than.ci.GetBoundary())
	default:
		return false
	}
}

func (i *shard) String() string {
	return i.rangeExt.String()
}

func (i *shard) GetShardID() proto.ShardID {
	return i.shardID
}

func (i *shard) GetRange() sharding.Range {
	return i.rangeExt
}

func (i *shard) GetMember(mode acapi.GetShardMode, exclude proto.DiskID) ShardOpInfo {
	// 1. get member exclude disk id
	if exclude != 0 {
		return i.getMemberExcluded(exclude)
	}

	// 2. get member by mode
	if mode == acapi.GetShardModeLeader {
		return i.getMemberLeader()
	}
	return i.getMemberRandom(0)
}

func (i *shard) getMemberExcluded(diskID proto.DiskID) ShardOpInfo {
	return i.getMemberRandom(diskID)
}

func (i *shard) getMemberLeader() ShardOpInfo {
	for idx, unit := range i.units {
		if unit.DiskID == i.leaderDiskID {
			return i.getShardOpInfo(idx)
		}
	}

	panic(fmt.Sprintf("can not find leader disk. shard:%+v", *i))
}

func (i *shard) getMemberRandom(exclude proto.DiskID) ShardOpInfo {
	n := len(i.units)
	initIdx := rand.Intn(n)
	idx := initIdx

	for {
		if i.units[idx].DiskID != exclude && !i.punishCtrl.IsPunishShardnode(i.units[idx].DiskID) {
			return i.getShardOpInfo(idx)
		}

		idx = (idx + 1) % n
		if idx == initIdx {
			break
		}
	}

	panic(fmt.Sprintf("can not find random disk. exclude disk=%d, shard:%+v", exclude, *i))
}

func (i *shard) getShardOpInfo(idx int) ShardOpInfo {
	return ShardOpInfo{
		DiskID:       i.units[idx].DiskID,
		Suid:         i.units[idx].Suid,
		RouteVersion: i.version,
	}
}

func (i *shard) belong(ci *sharding.CompareItem) bool {
	return i.rangeExt.Belong(ci)
}

func (i *shard) contain(rg *sharding.Range) bool {
	return i.rangeExt.Contain(rg)
}

type compareItem struct {
	ci sharding.CompareItem
}

func (i *compareItem) Less(item btree.Item) bool {
	than := item.(*shard)
	return i.ci.GetBoundary().Less(than.rangeExt.MaxBoundary())
}

func (i *compareItem) String() string {
	return i.ci.String()
}

func convertShardUnitInfo(units []clustermgr.ShardUnitInfo) []clustermgr.ShardUnit {
	ret := make([]clustermgr.ShardUnit, len(units))

	for i, unit := range units {
		ret[i] = clustermgr.ShardUnit{
			Suid:    unit.Suid,
			DiskID:  unit.DiskID,
			Learner: unit.Learner,
		}
	}

	return ret
}

func isInvalidShardStat(sd shardnode.ShardStats) bool {
	if sd.Suid == 0 || sd.RouteVersion == 0 || sd.LeaderDiskID == 0 {
		return true
	}

	for _, unit := range sd.Units {
		if unit.DiskID == 0 || unit.Suid == 0 {
			return true
		}
	}

	if sd.Range.Type == sharding.RangeType_RangeTypeUNKNOWN || sd.Range.IsEmpty() {
		return true
	}

	return false
}
