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
	"path/filepath"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

/*
 *  method:         POST
 *  url:            /disk/probe
 *  request body:   json.Marshal(DiskProbeArgs)
 */
func (s *Service) DiskProbe(c *rpc.Context) {
	args := new(bnapi.DiskProbeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("disk probe args: %v", args)

	probePath, err := filepath.Abs(args.Path)
	if err != nil {
		span.Errorf("Failed abs(path):%s invalid: err:%v", args.Path, err)
		c.RespondError(err)
		return
	}

	span.Infof("probe path: %s", probePath)

	err = s.DiskLimitPerKey.Acquire(probePath)
	if err != nil {
		span.Errorf("probePath (%v) are loading at the same time", probePath)
		c.RespondError(bloberr.ErrOutOfLimit)
		return
	}
	defer s.DiskLimitPerKey.Release(probePath)

	// Verify that the directory path exists
	fileExists, err := base.IsFileExists(probePath)
	if err != nil || !fileExists {
		span.Errorf("probePath(%s) is not exist, err:%v", probePath, err)
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	// Must be empty
	empty, err := base.IsEmptyDisk(probePath)
	if err != nil || !empty {
		span.Errorf("probePath(%s) is not empty. err:%v", probePath, err)
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	var foundOnlineDisk bool
	s.lock.RLock()
	for _, d := range s.Disks {
		path, err := filepath.Abs(d.GetConfig().Path)
		if err != nil {
			s.lock.RUnlock()
			c.RespondError(err)
			return
		}
		if path == probePath {
			foundOnlineDisk = true
			break
		}
	}
	s.lock.RUnlock()

	// must be no corresponding active handle
	if foundOnlineDisk {
		span.Errorf("path<%s> found online disk.", probePath)
		c.RespondError(bloberr.ErrInvalidParam)
		return
	}

	// The corresponding configuration file must exist
	foundIdx := -1
	for idx, conf := range s.Conf.Disks {
		path, err := filepath.Abs(conf.Path)
		if err != nil {
			c.RespondError(err)
			return
		}
		if probePath == path {
			foundIdx = idx
		}
	}

	if foundIdx < 0 {
		span.Errorf("can not found<%s> disk config", probePath)
		c.RespondError(bloberr.ErrNotFound)
		return
	}

	// fix init config
	diskConf := s.Conf.Disks[foundIdx]
	s.fixDiskConf(&diskConf)

	// new disk storage
	ds, err := disk.NewDiskStorage(ctx, diskConf)
	if err != nil {
		span.Errorf("Failed Open DiskStorage. conf:%v, err:%v", diskConf, err)
		c.RespondError(err)
		return
	}

	// add disk to cluster mgr
	diskInfo := ds.DiskInfo()
	err = s.ClusterMgrClient.AddDisk(ctx, &diskInfo)
	if err != nil {
		span.Errorf("Failed register disk: %v, err:%v", diskInfo, err)
		c.RespondError(err)
		return
	}

	// add to service map
	s.lock.Lock()
	s.Disks[ds.DiskID] = ds
	s.lock.Unlock()

	span.Infof("probe path<%s> diskId:%d success.", probePath, ds.DiskID)
}
