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

package clustermgr

import (
	"bytes"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *Service) SpaceCreate(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.CreateSpaceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept SpaceCreate request, args: %v", args)

	err := s.CatalogMgr.CreateSpace(ctx, args)
	if err != nil {
		span.Error(errors.Detail(err))
		c.RespondError(err)
		return
	}
}

func (s *Service) SpaceGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.GetSpaceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	if args.Name == "" && args.SpaceID == proto.InvalidSpaceID {
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	span.Infof("accept SpaceGet request, args: %v", args)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("get space read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}
	var (
		err       error
		spaceInfo *clustermgr.Space
	)
	if args.Name != "" {
		spaceInfo, err = s.CatalogMgr.GetSpaceInfoByName(ctx, args.Name)
	} else {
		spaceInfo, err = s.CatalogMgr.GetSpaceInfoByID(ctx, args.SpaceID)
	}
	if err != nil {
		span.Errorf("get space err =>", errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(spaceInfo)
}

func (s *Service) SpaceAuth(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	args := new(clustermgr.AuthSpaceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept SpaceAuth request, args: %v", args)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("space auth read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}
	spaceInfo, err := s.CatalogMgr.GetSpaceInfoByName(ctx, args.Name)
	if err != nil {
		span.Errorf("get space err =>", errors.Detail(err))
		c.RespondError(err)
		return
	}

	timeStamp, hash, err := clustermgr.DecodeAuthInfo(args.Token)
	if err != nil {
		span.Errorf("auth space err =>", errors.Detail(err))
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	authInfo := &clustermgr.AuthInfo{
		AccessKey: spaceInfo.AccessKey,
		SecretKey: spaceInfo.SecretKey,
	}
	if h := clustermgr.CalculateHash(authInfo, timeStamp); !bytes.Equal(h, hash) {
		span.Errorf("auth space hash:%s not equal server's hash:%s", string(hash), string(h))
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
}

func (s *Service) SpaceList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListSpaceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept SpaceList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("list read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	infos, err := s.CatalogMgr.ListSpaceInfo(ctx, args)
	if err != nil {
		span.Errorf("list shard error,args is: %v, error:%v", args, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}

	if len(infos) > 0 {
		c.RespondJSON(&clustermgr.ListSpaceRet{Spaces: infos, Marker: infos[len(infos)-1].SpaceID})
		return
	}
}
