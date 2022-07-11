// Copyright 2022 The CubeFS Authors.
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

package blobnode

import (
	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

/*
 *  method:         POST
 *  url:            /chunk/create/diskid/{diskid}/vuid/{vuid}?chunksize={chunksize}
 *  request body:   json.Marshal(bnapi.ChunkCreateArgs)
 */
func (s *Service) ChunkCreate(c *rpc.Context) {
	args := new(bnapi.CreateChunkArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("chunk create args:%v", args)

	if args.ChunkSize < 0 || args.ChunkSize > disk.MaxChunkSize {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	if args.ChunkSize == 0 {
		args.ChunkSize = core.DefaultChunkSize
	}

	limitKey := args.Vuid
	err := s.ChunkLimitPerVuid.Acquire(limitKey)
	if err != nil {
		span.Errorf("can not create chunk with same vuid(%v) at the same time", args.Vuid)
		c.RespondError(bloberr.ErrOutOfLimit)
		return
	}
	defer s.ChunkLimitPerVuid.Release(limitKey)

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("diskId:%d not exist", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	cs, err := ds.CreateChunk(ctx, args.Vuid, args.ChunkSize)
	if err != nil {
		span.Errorf("Failed register vuid:%v, err:%v", args.DiskID, err)
		c.RespondError(err)
		return
	}

	span.Infof("create vuid:%d success, bind chunkId:%s", args.Vuid, cs.ID())
}

/*
 *  method:         POST
 *  url:            /chunk/inspect/diskid/{diskid}/vuid/{vuid}
 */
func (s *Service) ChunkInspect(c *rpc.Context) {
	args := new(bnapi.ChunkInspectArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	span, ctx := trace.StartSpanFromContext(s.ctx, "")

	span.Debugf("chunk inspect args: %v", args)
	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("diskID %d not exist", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	err := s.InspectLimiterPerKey.Acquire(args.Vuid)
	if err != nil {
		c.RespondError(bloberr.ErrOutOfLimit)
		return
	}
	defer s.InspectLimiterPerKey.Release(args.Vuid)

	cs, found := ds.GetChunkStorage(args.Vuid)
	if !found {
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}
	badShards, err := startInspect(ctx, cs)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(badShards)
}

/*
 *  method:         POST
 *  url:            /chunk/release/diskid/{diskid}/vuid/{vuid}
 *  request body:   json.Marshal(ChunkArgs)
 */
func (s *Service) ChunkRelease(c *rpc.Context) {
	args := new(bnapi.ChangeChunkStatusArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("args: %v", args)

	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	limitKey := args.Vuid
	err := s.ChunkLimitPerVuid.Acquire(limitKey)
	if err != nil {
		span.Errorf("vuid(%v) status concurry conflict", args.Vuid)
		c.RespondError(bloberr.ErrOverload)
		return
	}
	defer s.ChunkLimitPerVuid.Release(limitKey)

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("disk:%v not found", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		span.Errorf("vuid:%v not found", args.Vuid)
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	// only readonly chunk can be release
	if !args.Force && cs.Status() != bnapi.ChunkStatusReadOnly {
		span.Errorf("vuid:%v/chunk:%s (status:%v) not readonly", args.Vuid, cs.ID(), cs.Status())
		c.RespondError(bloberr.ErrChunkNotReadonly)
		return
	}

	err = ds.ReleaseChunk(ctx, args.Vuid, args.Force)
	if err != nil {
		span.Errorf("release args:(%v) failed: %v", args, err)
		c.RespondError(err)
		return
	}

	span.Infof("disk release vuid:%v success", args.Vuid)
}

/*
 *  method:         POST
 *  url:            /chunk/readonly/diskid/{diskid}/vuid/{vuid}/
 *  request body:   json.Marshal(ChunkArgs)
 */
func (s *Service) ChunkReadonly(c *rpc.Context) {
	args := new(bnapi.ChangeChunkStatusArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("args: %v", args)

	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	limitKey := args.Vuid
	err := s.ChunkLimitPerVuid.Acquire(limitKey)
	if err != nil {
		span.Errorf("vuid(%v) status concurry conflict", args.Vuid)
		c.RespondError(bloberr.ErrOverload)
		return
	}
	defer s.ChunkLimitPerVuid.Release(limitKey)

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("disk:%v not found", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		span.Errorf("vuid:%v not found", args.Vuid)
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	if cs.Status() == bnapi.ChunkStatusReadOnly {
		span.Warnf("chunk(%s) already in readonly", cs.ID())
		return
	}

	if cs.Status() != bnapi.ChunkStatusNormal {
		span.Warnf("chunk(%s) status no normal", cs.ID())
		c.RespondError(bloberr.ErrChunkNotNormal)
		return
	}

	// change persistence status
	err = ds.UpdateChunkStatus(ctx, args.Vuid, bnapi.ChunkStatusReadOnly)
	if err != nil {
		span.Errorf("set args:(%s) readOnly failed: %v", args, err)
		c.RespondError(err)
		return
	}

	span.Debugf("update diskid: %v vuid:%v readonly success", args.DiskID, args.Vuid)
}

/*
 *  method:         POST
 *  url:            /chunk/readwrite/diskid/{diskid}/vuid/{vuid}
 *  request body:   json.Marshal(ChunkArgs)
 */
func (s *Service) ChunkReadwrite(c *rpc.Context) {
	args := new(bnapi.ChangeChunkStatusArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("args: %v", args)

	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	limitKey := args.Vuid
	err := s.ChunkLimitPerVuid.Acquire(limitKey)
	if err != nil {
		span.Errorf("vuid(%v) status concurry conflict", args.Vuid)
		c.RespondError(bloberr.ErrOverload)
		return
	}
	defer s.ChunkLimitPerVuid.Release(limitKey)

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("disk:%v not found", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		span.Errorf("vuid:%v not found", args.Vuid)
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	if cs.Status() == bnapi.ChunkStatusNormal {
		span.Warnf("chunk(%s) already normal", cs.ID())
		return
	}

	// only readonly -> normal
	if cs.Status() != bnapi.ChunkStatusReadOnly {
		span.Warnf("chunk(%s) status no readonly", cs.ID())
		c.RespondError(bloberr.ErrChunkNotReadonly)
		return
	}

	// change persistence status
	err = ds.UpdateChunkStatus(ctx, args.Vuid, bnapi.ChunkStatusNormal)
	if err != nil {
		span.Errorf("set args:(%s) readWrite failed: %v", args, err)
		c.RespondError(err)
		return
	}

	span.Infof("update disk:%v vuid:%v normal success", args.DiskID, args.Vuid)
}

/*
 *  method:         GET
 *  url:            /chunk/list/diskid/{diskid}
 */
func (s *Service) ChunkList(c *rpc.Context) {
	args := new(bnapi.ListChunkArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("chunk list args: %v", args)

	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("diskid(%v) no such disk", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	chunks := make([]core.ChunkAPI, 0)
	_ = ds.WalkChunksWithLock(ctx, func(cs core.ChunkAPI) (err error) {
		chunks = append(chunks, cs)
		return nil
	})

	infos := make([]*bnapi.ChunkInfo, 0)
	for _, cs := range chunks {
		info := cs.ChunkInfo(ctx)
		infos = append(infos, &info)
	}

	ret := bnapi.ListChunkRet{
		ChunkInfos: infos,
	}
	c.RespondJSON(ret)
}

/*
 *  method:         GET
 *  url:            /chunk/stat/diskid/{diskid}/vuid/{vuid}
 *  response body:  json.Marshal(ChunkInfo)
 */
func (s *Service) ChunkStat(c *rpc.Context) {
	args := new(bnapi.StatChunkArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("chunk stat args:%v", args)

	if !bnapi.IsValidDiskID(args.DiskID) {
		span.Debugf("args:%v", args)
		c.RespondError(bloberr.ErrInvalidDiskId)
		return
	}

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("disk:%v not found", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		span.Errorf("no such vuid, args:%v", args)
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	chunk := cs.ChunkInfo(ctx)
	c.RespondJSON(&chunk)
}
