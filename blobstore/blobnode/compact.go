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
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

/*
 *  method:         POST
 *  url:            /chunk/compact/diskid/{diskid}/vuid/{vuid}
 *  request body:   json.Marshal(CompactChunkArgs)
 */
func (s *Service) ChunkCompact(c *rpc.Context) {
	args := new(bnapi.CompactChunkArgs)
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

	s.lock.RLock()
	ds, exist := s.Disks[args.DiskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("diskid(%v) no such disk", args.DiskID)
		c.RespondError(bloberr.ErrNoSuchDisk)
		return
	}

	cs, exist := ds.GetChunkStorage(args.Vuid)
	if !exist {
		span.Errorf("vuid:%v not exist", args.Vuid)
		c.RespondError(bloberr.ErrNoSuchVuid)
		return
	}

	if !cs.NeedCompact(ctx) && !s.Conf.DiskConfig.AllowForceCompact {
		span.Infof("no need compact vuid:%v. skip", args.Vuid)
		return
	}

	ds.EnqueueCompact(ctx, args.Vuid)
	span.Infof("compact enqueue vuid:%v success", args.Vuid)
}
