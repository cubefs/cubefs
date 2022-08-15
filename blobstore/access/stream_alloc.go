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

package access

import (
	"context"
	"sync/atomic"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

var errAllocatePunishedVolume = errors.New("allocate punished volume")

// Alloc access interface /alloc
//     required: size, file size
//     optional: blobSize > 0, alloc with blobSize
//               assignClusterID > 0, assign to alloc in this cluster certainly
//               codeMode > 0, alloc in this codemode
//     return: a location of file
func (h *Handler) Alloc(ctx context.Context, size uint64, blobSize uint32,
	assignClusterID proto.ClusterID, codeMode codemode.CodeMode) (*access.Location, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("alloc request with size:%d blobsize:%d cluster:%d codemode:%d",
		size, blobSize, assignClusterID, codeMode)

	if int64(size) > h.maxObjectSize {
		span.Info("exceed max object size", h.maxObjectSize)
		return nil, errcode.ErrAccessExceedSize
	}

	if blobSize == 0 {
		blobSize = atomic.LoadUint32(&h.MaxBlobSize)
		span.Debugf("fill blobsize:%d", blobSize)
	}

	if codeMode == 0 {
		codeMode = h.allCodeModes.SelectCodeMode(int64(size))
		span.Debugf("select codemode:%d", codeMode)
	}
	if !codeMode.IsValid() {
		span.Infof("invalid codemode:%d", codeMode)
		return nil, errcode.ErrIllegalArguments
	}

	clusterID, blobs, err := h.allocFromAllocatorWithHystrix(ctx, codeMode, size, blobSize, assignClusterID)
	if err != nil {
		span.Error("alloc from proxy", errors.Detail(err))
		return nil, err
	}
	span.Debugf("allocated from %d %+v", clusterID, blobs)

	location := &access.Location{
		ClusterID: clusterID,
		CodeMode:  codeMode,
		Size:      size,
		BlobSize:  blobSize,
		Blobs:     blobs,
	}
	span.Debugf("alloc ok %+v", location)
	return location, nil
}

func (h *Handler) allocFromAllocatorWithHystrix(ctx context.Context, codeMode codemode.CodeMode, size uint64, blobSize uint32,
	clusterID proto.ClusterID) (cid proto.ClusterID, bidRets []access.SliceInfo, err error) {
	err = hystrix.Do(allocCommand, func() error {
		cid, bidRets, err = h.allocFromAllocator(ctx, codeMode, size, blobSize, clusterID)
		return err
	}, nil)
	return
}

func (h *Handler) allocFromAllocator(ctx context.Context, codeMode codemode.CodeMode, size uint64, blobSize uint32,
	clusterID proto.ClusterID) (proto.ClusterID, []access.SliceInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	if blobSize == 0 {
		blobSize = atomic.LoadUint32(&h.MaxBlobSize)
	}
	if clusterID == 0 {
		clusterChosen, err := h.clusterController.ChooseOne()
		if err != nil {
			return 0, nil, err
		}
		clusterID = clusterChosen.ClusterID
	}

	args := proxy.AllocVolsArgs{
		Fsize:    size,
		CodeMode: codeMode,
		BidCount: blobCount(size, blobSize),
	}

	var allocRets []proxy.AllocRet
	var allocHost string
	hostsSet := make(map[string]struct{}, 1)
	if err := retry.ExponentialBackoff(h.AllocRetryTimes, uint32(h.AllocRetryIntervalMS)).On(func() error {
		serviceController, err := h.clusterController.GetServiceController(clusterID)
		if err != nil {
			span.Warn(err)
			return errors.Info(err, "get service controller", clusterID)
		}

		var host string
		for range [10]struct{}{} {
			host, err = serviceController.GetServiceHost(ctx, serviceProxy)
			if err != nil {
				span.Warn(err)
				return errors.Info(err, "get proxy host", clusterID)
			}
			if _, ok := hostsSet[host]; ok {
				continue
			}
			hostsSet[host] = struct{}{}
			break
		}
		allocHost = host

		allocRets, err = h.proxyClient.VolumeAlloc(ctx, host, &args)
		if err != nil {
			if errorTimeout(err) || errorConnectionRefused(err) {
				span.Info("punish unreachable proxy host:", host)
				reportUnhealth(clusterID, "punish", serviceProxy, host, "Timeout")
				serviceController.PunishServiceWithThreshold(ctx, serviceProxy, host, h.ServicePunishIntervalS)
			}
			span.Warn(host, err)
			return errors.Base(err, "alloc from proxy", host)
		}

		// filter punished volume in allocating progress
		for _, ret := range allocRets {
			vInfo, err := h.getVolume(ctx, clusterID, ret.Vid, true)
			if err != nil {
				span.Warn(err)
				return err
			}
			if vInfo.IsPunish {
				// return err and retry allocate
				err = errAllocatePunishedVolume
				args.Excludes = append(args.Excludes, vInfo.Vid)
				span.Warn("next retry exclude vid:", vInfo.Vid, err)
				return err
			}
		}

		return nil
	}); err != nil {
		if err != errAllocatePunishedVolume {
			reportUnhealth(clusterID, "allocate", "-", "-", "failed")
			return 0, nil, err
		}
		// still write to storage if allocating punished volume
		reportUnhealth(clusterID, "allocate", "-", "-", "punished")
	}

	// cache vid in which allocator
	for _, ret := range allocRets {
		setCacheVidHost(clusterID, ret.Vid, allocHost)
	}

	blobN := blobCount(size, blobSize)
	blobs := make([]access.SliceInfo, 0, blobN)
	for _, bidRet := range allocRets {
		if blobN <= 0 {
			break
		}

		count := minU64(blobN, uint64(bidRet.BidEnd)-uint64(bidRet.BidStart)+1)
		blobN -= count

		blobs = append(blobs, access.SliceInfo{
			MinBid: bidRet.BidStart,
			Vid:    bidRet.Vid,
			Count:  uint32(count),
		})
	}
	if blobN > 0 {
		return 0, nil, errors.New("no enough blob ids from allocator")
	}

	if len(blobs) > access.MaxLocationBlobs {
		span.Errorf("alloc exceed max blobs %d>%d", len(blobs), access.MaxLocationBlobs)
		return 0, nil, errors.New("alloc exceed max blobs of location")
	}

	return clusterID, blobs, nil
}
