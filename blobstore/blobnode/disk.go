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
	"context"
	"path/filepath"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
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

	// check path valid
	probePath, err := filepath.Abs(args.Path)
	if err != nil {
		span.Errorf("Failed abs(path):%s invalid: err:%v", args.Path, err)
		c.RespondError(err)
		return
	}
	span.Infof("probe path: %s", probePath)

	// limit req
	err = s.DiskLimitRegister.Acquire(probePath)
	if err != nil {
		span.Errorf("probePath (%v) are loading at the same time", probePath)
		c.RespondError(bloberr.ErrOutOfLimit)
		return
	}
	defer s.DiskLimitRegister.Release(probePath)

	// Verify that the directory path exists
	fileExists, err := base.IsFileExists(probePath)
	if err != nil || !fileExists {
		span.Errorf("probePath(%s) is not exist, err:%v", probePath, err)
		c.RespondError(bloberr.ErrPathNotExist)
		return
	}

	// Must be empty
	empty, err := base.IsEmptyDisk(probePath)
	if err != nil || !empty {
		span.Errorf("probePath(%s) is not empty. err:%v", probePath, err)
		c.RespondError(bloberr.ErrPathNotEmpty)
		return
	}

	// check disk status(repaired/dropped)
	// get all registered disks
	registeredDisks, err := s.ClusterMgrClient.ListHostDisk(ctx, conf.Host)
	if err != nil {
		span.Errorf("Failed ListDisk from clusterMgr. err:%+v", err)
		c.RespondError(err)
		return
	}
	span.Infof("registered disks: %v", registeredDisks)

	// find the old disk info in cluster, use path
	oldDisk := cmapi.BlobNodeDiskInfo{}
	for _, disk := range registeredDisks {
		// this id is uniq increasing, so we take the latest(maximum) diskID in the same path
		if disk.Path == args.Path && disk.DiskID > oldDisk.DiskID {
			oldDisk = *disk
		}
	}
	if oldDisk.DiskID == 0 {
		span.Errorf("disk path %s is not found in cluster, refuse replace it", args.Path)
		c.RespondError(bloberr.ErrPathNotExist)
		return
	}
	// check disk status
	if oldDisk.Status != proto.DiskStatusRepaired && oldDisk.Status != proto.DiskStatusDropped {
		span.Errorf("disk[%d:%s] is not repaired/dropped, refuse replace it", oldDisk.DiskID, oldDisk.Status)
		c.RespondError(bloberr.ErrInternal)
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
		c.RespondError(bloberr.ErrPathFindOnline)
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

	s.reportOnlineDisk(&diskConf.HostInfo, diskInfo.Path)
	span.Infof("probe path<%s> diskId:%d success.", probePath, ds.DiskID)

	// find old bad disk, by host+path disk, clean it
	if err = s.cleanOldDiskInspectMetric(ctx, ds); err != nil {
		span.Warnf("fail to cleanOldDiskInspectMetric, newID=%d, path=%s, err=%+v", ds.ID(), ds.DataPath, err)
	}
}

func (s *Service) cleanOldDiskInspectMetric(ctx context.Context, ds core.DiskAPI) (err error) {
	marker := proto.DiskID(0)
	ret := clustermgr.ListDiskRet{}
	cleanDisks := make([]*clustermgr.BlobNodeDiskInfo, 0, 8)
	newID := ds.ID()
	path := ds.DiskInfo().Path

	// 1. get all disk in this host, find old bad disk, by host+path.
	for {
		ret, err = s.ClusterMgrClient.ListDisk(ctx, &clustermgr.ListOptionArgs{Host: s.Conf.Host, Count: 200, Marker: marker})
		if err != nil {
			return err
		}

		for _, disk := range ret.Disks {
			if disk.Path == path && disk.DiskID != newID {
				cleanDisks = append(cleanDisks, disk)
			}
		}

		if ret.Marker == proto.InvalidDiskID {
			break
		}
		marker = ret.Marker
	}

	// 2. clean metric, old disks
	for _, disk := range cleanDisks {
		s.inspectMgr.cleanDiskInspectMetric(ds, disk.DiskID)
	}
	return nil
}

// allowBrokenDisk reports whether marking diskID broken still keeps broken and
// repairing disks within Config.MaxClusterBadDiskNodes distinct hosts. This is a
// cluster-wide NodeID count and assumes HostAware placement. It does not look
// at one volume's code mode, so it does not prove that volume still has enough
// EC redundancy. Repaired disks are already recovered. A query failure does not
// allow the mark: the caller waits and retries.
func (s *Service) allowBrokenDisk(ctx context.Context, diskID proto.DiskID) (bool, error) {
	s.lock.RLock()
	ds, exist := s.Disks[diskID]
	s.lock.RUnlock()
	if !exist || !ds.IsWritable() {
		return true, nil
	}

	nodes := make(map[proto.NodeID]struct{})
	for _, status := range []proto.DiskStatus{proto.DiskStatusBroken, proto.DiskStatusRepairing} {
		disks, err := listDisksByStatus(ctx, s.ClusterMgrClient.ListDisk, status)
		if err != nil {
			return false, err
		}
		for _, disk := range disks {
			nodes[disk.NodeID] = struct{}{}
		}
	}

	// Reporting broken disks to CM is not transactional, so concurrent blobnodes
	// can push the host count over the limit. Ops may also mark disks broken manually.
	// Still refuse the automatic report in these cases to reduce data loss risks.
	if _, ok := nodes[ds.NodeID()]; !ok {
		nodes[ds.NodeID()] = struct{}{}
	}
	return len(nodes) <= s.Conf.MaxClusterBadDiskNodes, nil
}

func listDisksByStatus(ctx context.Context,
	list func(context.Context, *cmapi.ListOptionArgs) (cmapi.ListDiskRet, error), status proto.DiskStatus,
) ([]*cmapi.BlobNodeDiskInfo, error) {
	var (
		marker proto.DiskID
		disks  []*cmapi.BlobNodeDiskInfo
	)
	for {
		ret, err := list(ctx, &cmapi.ListOptionArgs{
			Status: status,
			Marker: marker,
			Count:  200,
		})
		if err != nil {
			return nil, err
		}
		disks = append(disks, ret.Disks...)
		if ret.Marker <= proto.InvalidDiskID {
			break
		}
		marker = ret.Marker
	}
	return disks, nil
}

func (s *Service) handleDataIOError(ctx context.Context, diskID proto.DiskID, diskErr error) {
	if s.isPendingBrokenDisk(diskID) {
		return
	}

	allow, err := s.allowBrokenDisk(ctx, diskID)
	if err != nil {
		trace.SpanFromContextSafe(ctx).Errorf("check data broken diskID:%d err:%v", diskID, err)
		s.addPendingBrokenDisk(diskID, diskErr)
		return
	}
	if !allow {
		s.addPendingBrokenDisk(diskID, diskErr)
		return
	}

	s.removePendingBrokenDisk(diskID)
	s.markDiskBroken(ctx, diskID, diskErr)
}

func (s *Service) isPendingBrokenDisk(diskID proto.DiskID) bool {
	s.pendingBrokenDisksMu.RLock()
	_, ok := s.pendingBrokenDisks[diskID]
	s.pendingBrokenDisksMu.RUnlock()
	return ok
}

func (s *Service) addPendingBrokenDisk(diskID proto.DiskID, diskErr error) {
	s.pendingBrokenDisksMu.Lock()
	if s.pendingBrokenDisks == nil {
		s.pendingBrokenDisks = make(map[proto.DiskID]error)
	}
	_, exist := s.pendingBrokenDisks[diskID]
	if !exist {
		s.pendingBrokenDisks[diskID] = diskErr
	}
	s.pendingBrokenDisksMu.Unlock()

	if exist {
		return
	}
	s.reportPendingBrokenDisks()
	log.Warnf("pending broken disk, diskID:%d err:%v", diskID, diskErr)
}

func (s *Service) removePendingBrokenDisk(diskID proto.DiskID) {
	s.pendingBrokenDisksMu.Lock()
	delete(s.pendingBrokenDisks, diskID)
	s.pendingBrokenDisksMu.Unlock()
	s.reportPendingBrokenDisks()
}

func (s *Service) loopPendingBrokenDisks() {
	ticker := time.NewTicker(time.Duration(s.Conf.DiskStatusCheckIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.tryPendingBrokenDisks()
		}
	}
}

func (s *Service) tryPendingBrokenDisks() {
	s.pendingBrokenDisksMu.RLock()
	pending := make(map[proto.DiskID]error, len(s.pendingBrokenDisks))
	for id, diskErr := range s.pendingBrokenDisks {
		pending[id] = diskErr
	}
	s.pendingBrokenDisksMu.RUnlock()
	if len(pending) == 0 {
		return
	}

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "PendingBrokenDisks")
	for diskID, diskErr := range pending {
		if !s.isPendingBrokenDisk(diskID) {
			continue
		}

		allow, err := s.allowBrokenDisk(ctx, diskID)
		if err != nil {
			span.Errorf("check data broken diskID:%d err:%v", diskID, err)
			continue
		}
		if !allow {
			span.Warnf("data broken diskID:%d still spans more than %d nodes", diskID, s.Conf.MaxClusterBadDiskNodes)
			continue
		}

		s.removePendingBrokenDisk(diskID)
		s.markDiskBroken(ctx, diskID, diskErr)
	}
}
