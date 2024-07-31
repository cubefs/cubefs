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
	"encoding/json"
	"errors"
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
	GetShard(ctx context.Context, blobName []byte) (*shardInfo, error)
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
		shards: make(map[proto.ShardID]*shardInfo),
		ranges: btree.New(defaultBTreeDegree),
		conf:   conf,
		cmCli:  cmCli,
		stopCh: stopCh,
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("start new shard controller, conf:%v", conf)

	err := s.auth(ctx)
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
	shards  map[proto.ShardID]*shardInfo
	ranges  *btree.BTree
	version proto.RouteVersion
	sync.RWMutex

	conf   shardCtrlConf
	cmCli  clustermgr.ClientAPI
	stopCh <-chan struct{}
}

func (s *shardControllerImpl) GetShard(ctx context.Context, blobName []byte) (*shardInfo, error) {
	s.RLock()
	defer s.RUnlock()

	span := trace.SpanFromContextSafe(ctx)
	keys, err := splitTwoKeys(blobName)
	if err != nil {
		return nil, err
	}

	ci := sharding.NewCompareItem(sharding.RangeType_RangeTypeHash, keys)
	var si *shardInfo
	pivot := s.ranges.Max()

	s.ranges.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		si = i.(*shardInfo)

		if si.Contain(ci) {
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

func (s *shardControllerImpl) auth(ctx context.Context) error {
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
	return nil
}

func (s *shardControllerImpl) initShard(ctx context.Context) error {
	return s.updateShard(ctx, true)
}

func (s *shardControllerImpl) incrementalShard() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	tk := time.NewTicker(time.Second * time.Duration(s.conf.reloadSecs))
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			s.updateShard(ctx, false)
		case <-s.stopCh:
			span.Info("exit shard controller")
			return
		}
	}
}

// called by period task, or read/write fail
func (s *shardControllerImpl) updateShard(ctx context.Context, init bool) error {
	span := trace.SpanFromContextSafe(ctx)

	version := proto.RouteVersion(0)
	if !init {
		version = s.getVersion()
	}

	ret, err := s.cmCli.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{
		RouteVersion: version,
	})
	if err != nil {
		span.Errorf("fail to get catalog from clusterMgr. err:%+v", err)
		return err
	}

	if version == ret.RouteVersion {
		return nil // skip
	}

	s.setVersion(ret.RouteVersion)
	for _, item := range ret.Items {
		if shard, err := s.decodeShard(ctx, item); err == nil {
			s.addShard(shard)
		}
	}
	return nil
}

func (s *shardControllerImpl) decodeShard(ctx context.Context, item clustermgr.CatalogChangeItem) (*shardInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	var shard *shardInfo

	switch item.Type {
	case proto.CatalogChangeItemAddShard:
		val := clustermgr.CatalogChangeShardAdd{}
		err := json.Unmarshal(item.Item.Value, &val)
		if err != nil {
			span.Warnf("json unmarshal failed. type=%d, version=%d err=%+v", item.Type, item.RouteVersion, err)
			return nil, err
		}
		shard = &shardInfo{
			ShardID: val.ShardID,
			Epoch:   val.Epoch,
			Range:   val.Units[0].Range,
			Units:   val.Units,
		}
		return shard, nil
	case proto.CatalogChangeItemUpdateShard:
		val := clustermgr.CatalogChangeShardUpdate{}
		err := json.Unmarshal(item.Item.Value, &val)
		if err != nil {
			span.Warnf("json unmarshal failed. type=%d, version=%d err=%+v", item.Type, item.RouteVersion, err)
			return nil, err
		}

		shard, err := s.setShardByID(val.ShardID, &val)
		if err != nil {
			return nil, err
		}
		return shard, nil
	default:
		return nil, errors.New("not expected catalog type")
	}
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

func (s *shardControllerImpl) addShard(si *shardInfo) {
	// todo optimize lock
	s.Lock()
	defer s.Unlock()

	s.shards[si.ShardID] = si
	s.ranges.ReplaceOrInsert(si)
}

func (s *shardControllerImpl) delShard(si *shardInfo) {
	s.Lock()
	defer s.Unlock()

	shard, ok := s.shards[si.ShardID]
	if ok {
		s.ranges.Delete(shard)
		delete(s.shards, si.ShardID)
	}
}

func (s *shardControllerImpl) getShardByID(shardID proto.ShardID) (*shardInfo, bool) {
	s.RLock()
	defer s.RUnlock()

	info, ok := s.shards[shardID]
	return info, ok
}

func (s *shardControllerImpl) setShardByID(shardID proto.ShardID, val *clustermgr.CatalogChangeShardUpdate) (*shardInfo, error) {
	s.Lock()
	defer s.Unlock()

	info, ok := s.shards[shardID]
	if !ok {
		return nil, errors.New("not found shardID when update")
	}

	info.Epoch = val.Epoch
	idx := val.Unit.Suid.Index()
	info.Units[idx] = val.Unit
	return info, nil
}

func splitTwoKeys(blobName []byte) ([][]byte, error) {
	ret := make([][]byte, 0)
	ret = append(ret, blobName)
	return ret, nil
}

type shardInfo struct {
	ShardID proto.ShardID
	Epoch   uint64
	Range   sharding.Range
	Units   []clustermgr.ShardUnitInfo
	// xx        clustermgr.ShardUnit // ? need leaderIdx
	// leaderIdx int
}

func (i *shardInfo) Less(item btree.Item) bool {
	than := item.(*shardInfo)

	return i.Range.MaxBoundary().Less(than.Range.MaxBoundary())
}

func (i *shardInfo) Copy() btree.Item {
	return i
}

func (i *shardInfo) String() string {
	return i.Range.String()
}

func (i *shardInfo) Contain(ci *sharding.CompareItem) bool {
	return i.Range.Belong(ci)
}

func (i *shardInfo) GetHostLeader() (proto.ShardID, string) {
	idx := i.Units[0].LeaderIdx
	return i.ShardID, i.Units[idx].Host
}

func (i *shardInfo) GetHostRandom() (proto.ShardID, string) {
	idx := rand.Intn(len(i.Units))
	return i.ShardID, i.Units[idx].Host
}
