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
	"time"

	"github.com/google/btree"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
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

	conf   shardCtrlConf
	cmCli  clustermgr.ClientAPI
	stopCh <-chan struct{}
}

func (s *shardControllerImpl) GetShard(ctx context.Context, blobName []byte) (*shardInfo, error) {
	return nil, nil
}

func (s *shardControllerImpl) auth(ctx context.Context) error {
	// todo: next MR. wait TangDeYi API
	// token, err := clustermgr.EncodeAuthInfo(&clustermgr.AuthInfo{
	//	AccessKey: s.conf.space.AK,
	//	SecretKey: s.conf.space.SK,
	// })
	// if err != nil {
	//	return err
	// }
	//
	// err = s.cmCli.AuthSpace(ctx, &clustermgr.AuthSpaceArgs{
	//	Name:  s.conf.space.Name,
	//	Token: token,
	// })
	// if err != nil {
	//	return err
	// }

	return nil
}

func (s *shardControllerImpl) initShard(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("shard loader for cluster:%d", s.conf.clusterID)

	// todo: next MR, construct all shard ranges. wait TangDeYi API
	// ret, err := s.cmCli.GetCatalogChanges(ctx, &clustermgr.GetCatalogChangesArgs{
	//	RouteVersion: 0,
	// })
	// if err != nil {
	//	span.Errorf("fail to get catalog from clusterMgr. err:%+v", err)
	//	return err
	// }
	//
	// s.version = ret.RouteVersion
	// for _, item := range ret.Items {
	//	if item.Type == proto.CatalogChangeItemAddShard {
	//		shard := clustermgr.CatalogChangeShardAdd{}
	//		err := json.Unmarshal(item.Item, &shard)
	//		if err != nil {
	//			span.Warnf("json unmarshal failed. type=%d, version=%d err=%+v", item.Type, item.RouteVersion, err)
	//			continue
	//		}
	//
	//		s.addShard(shard)
	//	}
	// }

	return nil
}

func (s *shardControllerImpl) incrementalShard() {
	tk := time.NewTicker(time.Second * time.Duration(s.conf.reloadSecs))
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			s.updateShard()
		case <-s.stopCh:
			return
		}
	}
}

// called by period task, or read/write fail
func (s *shardControllerImpl) updateShard() error {
	// todo: next MR, construct incremental shard
	// get cm catalog, increment. compare s.version
	// add shard or del shard
	return nil
}

func (s *shardControllerImpl) addShard(shard clustermgr.Shard) {
	item := &shardInfo{
		Shard: shard,
	}
	s.shards[shard.ShardID] = item
	s.ranges.ReplaceOrInsert(item)
}

func (s *shardControllerImpl) delShard(shard clustermgr.Shard) {
	item, ok := s.shards[shard.ShardID]
	if ok {
		s.ranges.Delete(item)
		delete(s.shards, shard.ShardID)
	}
}

type shardInfo struct {
	clustermgr.Shard
}

// todo: next MR

func (i shardInfo) Less(than btree.Item) bool {
	return false
}

func (i shardInfo) Copy() btree.Item {
	return i
}

func (i shardInfo) String() string {
	return ""
}
