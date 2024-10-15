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

package shardnode

import (
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	_service *service
	conf     Config
)

func init() {
	mod := &cmd.Module{
		Name:       proto.ServiceNameShardNode,
		InitConfig: initConfig,
		SetUp2:     setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterModule(mod)
}

type RpcService struct {
	*service
}

func (s *RpcService) CreateBlob(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.CreateBlobArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive CreateBlob request, args:%+v", args)

	ret, err := s.createBlob(ctx, args)
	if err != nil {
		span.Errorf("create blob failed, err: %v", errors.Detail(err))
		return err
	}
	return w.WriteOK(&ret)
}

func (s *RpcService) DeleteBlob(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.DeleteBlobArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive DeleteBlob request, args:%+v", args)

	return s.deleteBlob(ctx, args)
}

func (s *RpcService) SealBlob(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.SealBlobArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive SealBlob request, args:%+v", args)

	if err := s.sealBlob(ctx, args); err != nil {
		span.Errorf("seal blob failed, err: %s", errors.Detail(err))
		return err
	}
	return nil
}

func (s *RpcService) GetBlob(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()
	args := &shardnode.GetBlobArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive GetBlob request, args:%+v", args)

	ret, err := s.getBlob(ctx, args)
	if err != nil {
		span.Errorf("get blob failed, err: %s", errors.Detail(err))
		return err
	}
	return w.WriteOK(&ret)
}

func (s *RpcService) ListBlob(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.ListBlobArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive ListBlob request, args:%+v", args)

	ret, err := s.listBlob(ctx, args)
	if err != nil {
		span.Errorf("list blob failed, err: %s", errors.Detail(err))
		return err
	}
	span.Debugf("list blob result: %+v", ret)
	return w.WriteOK(&ret)
}

func (s *RpcService) AllocSlice(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.AllocSliceArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive AllocSlice request, args:%+v", args)

	ret, err := s.allocSlice(ctx, args)
	if err != nil {
		span.Errorf("alloc slice failed, err: %s", errors.Detail(err))
		return err
	}
	return w.WriteOK(&ret)
}

func (s *RpcService) InsertItem(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.InsertItemArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive InsertItem request, args:%+v", args)

	return s.insertItem(ctx, args)
}

func (s *RpcService) UpdateItem(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.UpdateItemArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive UpdateItem request, args:%+v", args)

	return s.updateItem(ctx, args)
}

func (s *RpcService) DeleteItem(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.DeleteItemArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive DeleteItem request, args:%+v", args)

	return s.deleteItem(ctx, args)
}

func (s *RpcService) GetItem(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.GetItemArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive GetItem request, args:%+v", args)

	ret, err := s.getItem(ctx, args)
	if err != nil {
		return err
	}
	return w.WriteOK(&ret)
}

func (s *RpcService) ListItem(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.ListItemArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Debugf("receive ListItem request, args:%+v", args)

	ret, err := s.listItem(ctx, args)
	if err != nil {
		return err
	}
	return w.WriteOK(&ret)
}

func (s *RpcService) AddShard(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.AddShardArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Infof("receive AddShard request, args:%+v, shardID:%d", args, args.GetSuid().ShardID())

	if err := s.addShard(ctx, args); err != nil {
		span.Errorf("add shard failed, err: %s", errors.Detail(err))
		return err
	}
	return nil
}

func (s *RpcService) UpdateShard(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.UpdateShardArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Infof("receive UpdateShard request, args:%+v", args)

	if err := s.updateShard(ctx, args); err != nil {
		span.Errorf("update shard failed, err: %s", errors.Detail(err))
		return err
	}
	return nil
}

func (s *RpcService) TransferShardLeader(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.TransferShardLeaderArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Infof("receive TransferShardLeader request, args:%+v", args)

	if err := s.transferShardLeader(ctx, args); err != nil {
		span.Errorf("transfer shard leader failed, err: %s", errors.Detail(err))
		return err
	}
	return nil
}

func (s *RpcService) GetShardInfo(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.GetShardArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Infof("receive GetShardInfo request, args:%+v", args)

	ret, err := s.getShardUintInfo(ctx, args.GetDiskID(), args.GetSuid())
	if err != nil {
		span.Errorf("get shard unit info failed, err: %s", errors.Detail(err))
		return err
	}
	return w.WriteOK(&ret)
}

func (s *RpcService) GetShardStats(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.GetShardArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Infof("receive GetShardStats request, args:%+v", args)

	ret, err := s.getShardStats(ctx, args.GetDiskID(), args.GetSuid())
	if err != nil {
		span.Errorf("get shard stats failed, err: %s", errors.Detail(err))
		return err
	}
	return w.WriteOK(&ret)
}

func (s *RpcService) ListShard(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.ListShardArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Infof("receive ListShard request, args:%+v", args)

	shards, err := s.listShards(ctx, args.GetDiskID(), args.GetCount())
	if err != nil {
		return err
	}
	ret := &shardnode.ListShardRet{
		Shards: shards,
	}
	return w.WriteOK(ret)
}

func (s *RpcService) ListVolume(w rpc2.ResponseWriter, req *rpc2.Request) error {
	ctx := req.Context()
	span := req.Span()

	args := &shardnode.ListVolumeArgs{}
	if err := req.ParseParameter(args); err != nil {
		return err
	}
	span.Infof("receive ListVolume request, args:%+v", args)

	volInfos, err := s.listVolume(ctx, args.GetCodeMode())
	if err != nil {
		return err
	}
	vids := make([]proto.Vid, len(volInfos))
	for i, info := range volInfos {
		vids[i] = info.Vid
	}
	ret := &shardnode.ListVolumeRet{
		Vids: vids,
	}
	return w.WriteOK(ret)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "shardnode.conf")
	if err := config.Load(&conf); err != nil {
		return nil, err
	}
	conf.Rpc2Server.Addresses = []rpc2.NetworkAddress{
		{Network: "tcp", Address: conf.BindAddr},
	}
	return &conf.Config, nil
}

func newHandler(s *RpcService) *rpc2.Router {
	handler := &rpc2.Router{}
	handler.Register("/blob/create", s.CreateBlob)
	handler.Register("/blob/delete", s.DeleteBlob)
	handler.Register("/blob/seal", s.SealBlob)
	handler.Register("/blob/get", s.GetBlob)
	handler.Register("/blob/list", s.ListBlob)
	handler.Register("/slice/alloc", s.AllocSlice)

	handler.Register("/item/insert", s.InsertItem)
	handler.Register("/item/delete", s.DeleteItem)
	handler.Register("/item/update", s.UpdateItem)
	handler.Register("/item/get", s.GetItem)
	handler.Register("/item/list", s.ListItem)

	handler.Register("/shard/add", s.AddShard)
	handler.Register("/shard/update", s.UpdateShard)
	handler.Register("/shard/leadertransfer", s.TransferShardLeader)

	handler.Register("/shard/info", s.GetShardInfo)
	handler.Register("/shard/stats", s.GetShardStats)
	handler.Register("/shard/list", s.ListShard)
	handler.Register("/volume/list", s.ListVolume)

	return handler
}

func setUp() (*rpc2.Router, []rpc2.Interceptor) {
	_service = newService(&conf)
	return newHandler(&RpcService{_service}), nil
}

func tearDown() {
	_service.closer.Close()
}
