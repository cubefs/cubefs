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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	defaultBTreeDegree     = 16
	defaultShardReloadSecs = 60
)

type IShardController interface {
	GetShard(ctx context.Context, blobName []byte) (Shard, error)
	GetSpaceID() proto.SpaceID
	Update(ctx context.Context) error
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

func NewShardController(conf shardCtrlConf, cmCli clustermgr.ClientAPI, stopCh <-chan struct{}) (IShardController, error) {
	defaulter.Equal(&conf.reloadSecs, defaultShardReloadSecs)

	s := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shard),
		ranges: btree.New(defaultBTreeDegree),
		conf:   conf,
		cmCli:  cmCli,
		stopCh: stopCh,
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("start new shard controller, conf:%v", conf)

	err := s.initSpace(ctx)
	if err != nil {
		return nil, err
	}

	err = s.initShard(ctx)
	if err != nil {
		return nil, err
	}

	go s.incrementalShard()

	return s, nil
}

type shardControllerImpl struct {
	shards       map[proto.ShardID]*shard
	ranges       *btree.BTree
	version      proto.RouteVersion
	spaceID      proto.SpaceID
	sync.RWMutex // todo: I will optimize locker in the next MR

	conf   shardCtrlConf
	cmCli  clustermgr.ClientAPI
	stopCh <-chan struct{}
}

func (s *shardControllerImpl) GetShard(ctx context.Context, blobName []byte) (Shard, error) {
	s.RLock()
	defer s.RUnlock()

	span := trace.SpanFromContextSafe(ctx)
	keys := [][]byte{blobName} // todo: support two specified shard keys

	ci := sharding.NewCompareItem(sharding.RangeType_RangeTypeHash, keys)
	var si *shard
	pivot := s.ranges.Max()

	s.ranges.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		si = i.(*shard)

		if si.belong(ci) {
			return false
		}

		si = nil
		return true
	})

	if si == nil { // not found
		span.Errorf("not find shard. blobName:%s, shard len:%d", blobName, s.ranges.Len())
		return nil, fmt.Errorf("can not find expect shard")
	}

	return si, nil
}

func (s *shardControllerImpl) GetSpaceID() proto.SpaceID {
	return s.spaceID
}

func (s *shardControllerImpl) Update(ctx context.Context) error {
	return s.updateShard(ctx)
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

func (s *shardControllerImpl) initShard(ctx context.Context) error {
	return s.updateShard(ctx)
}

func (s *shardControllerImpl) incrementalShard() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	tk := time.NewTicker(time.Second * time.Duration(s.conf.reloadSecs))
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			s.updateShard(ctx)
		case <-s.stopCh:
			span.Info("exit shard controller")
			return
		}
	}
}

// called by period task, or read/write fail
func (s *shardControllerImpl) updateShard(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	version := s.getVersion() // todo: optimize locker

	ret, err := s.cmCli.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{
		RouteVersion: version,
	})
	if err != nil {
		span.Errorf("fail to get catalog from clusterMgr. err:%+v", err)
		return err
	}

	// skip
	if version == ret.RouteVersion {
		return nil
	}

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
			span.Errorf("update shard error:%+v, item:%v", err, item)
			panic(err)
		}
	}

	s.setVersion(ret.RouteVersion)
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

	sh := &shard{
		shardID:      val.ShardID,
		version:      val.RouteVersion,
		leaderDiskID: val.Units[0].LeaderDiskID,
		rangeExt:     val.Units[0].Range,
		units:        convertShardUnitInfo(val.Units),
	}
	s.addShard(sh)
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

	// update
	ok := s.setShardByID(val.ShardID, &val)
	if !ok {
		span.Warnf("update shard failed. type=%d, version=%d, shardID=%d", item.Type, item.RouteVersion, val.ShardID)
	}
	return nil
}

func (s *shardControllerImpl) getVersion() proto.RouteVersion {
	s.RLock()
	defer s.RUnlock()
	return s.version
}

func (s *shardControllerImpl) setVersion(version proto.RouteVersion) {
	s.Lock()
	defer s.Unlock()
	s.version = version
}

func (s *shardControllerImpl) addShard(si *shard) {
	// todo: I will optimize locker in the next MR
	s.Lock()
	defer s.Unlock()

	s.shards[si.shardID] = si
	s.ranges.ReplaceOrInsert(si)
}

func (s *shardControllerImpl) delShard(si *shard) {
	s.Lock()
	defer s.Unlock()

	shard, ok := s.shards[si.shardID]
	if ok {
		s.ranges.Delete(shard)
		delete(s.shards, si.shardID)
	}
}

func (s *shardControllerImpl) getShardByID(shardID proto.ShardID) (*shard, bool) {
	s.RLock()
	defer s.RUnlock()

	info, ok := s.shards[shardID]
	return info, ok
}

func (s *shardControllerImpl) setShardByID(shardID proto.ShardID, val *clustermgr.CatalogChangeShardUpdate) bool {
	s.Lock()
	defer s.Unlock()

	info, ok := s.shards[shardID]
	if !ok {
		return false
	}

	info.version = val.RouteVersion
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
	GetShardLeader() ShardOpInfo
	GetShardRandom() ShardOpInfo
}

// shard implement btree.Item interface, shard route information
type shard struct {
	shardID      proto.ShardID
	leaderDiskID proto.DiskID
	version      proto.RouteVersion
	rangeExt     sharding.Range
	units        []clustermgr.ShardUnit
}

func (i *shard) Less(item btree.Item) bool {
	than := item.(*shard)

	return i.rangeExt.MaxBoundary().Less(than.rangeExt.MaxBoundary())
}

func (i *shard) Copy() btree.Item {
	return i
}

func (i *shard) String() string {
	return i.rangeExt.String()
}

func (i *shard) GetShardLeader() ShardOpInfo {
	idx := i.getShardLeaderIdx()
	return *i.getShardOpInfo(idx)
}

func (i *shard) GetShardRandom() ShardOpInfo {
	idx := rand.Intn(len(i.units))
	return *i.getShardOpInfo(idx)
}

func (i *shard) getShardOpInfo(idx int) *ShardOpInfo {
	return &ShardOpInfo{
		DiskID:       i.units[idx].DiskID,
		Suid:         i.units[idx].Suid,
		RouteVersion: i.version,
	}
}

func (i *shard) getShardLeaderIdx() int {
	for idx, unit := range i.units {
		if unit.DiskID == i.leaderDiskID {
			return idx
		}
	}
	return 0
}

func (i *shard) belong(ci *sharding.CompareItem) bool {
	return i.rangeExt.Belong(ci)
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
