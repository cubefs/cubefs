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

package allocator

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type Allocator interface {
	AllocSlices(ctx context.Context, codeMode codemode.CodeMode, fileSize uint64, sliceSize uint32) ([]proto.Slice, error)
	Close()
	ListVolume(ctx context.Context, codeMode codemode.CodeMode) ([]clustermgr.AllocVolumeInfo, error)
}

type allocator struct {
	volumeMgr
}

func NewAllocator(ctx context.Context, blobCfg BlobConfig, volCfg VolConfig, tp base.Transport) (Allocator, error) {
	vm, err := newVolumeMgr(ctx, blobCfg, volCfg, tp)
	if err != nil {
		return nil, err
	}
	return &allocator{
		volumeMgr: vm,
	}, nil
}

func (alc *allocator) AllocSlices(ctx context.Context, codeMode codemode.CodeMode, fileSize uint64, sliceSize uint32) ([]proto.Slice, error) {
	args := &AllocVolsArgs{
		Fsize:    fileSize,
		CodeMode: codeMode,
		BidCount: blobCount(fileSize, sliceSize),
	}

	allocRets, err := alc.alloc(ctx, args)
	if err != nil {
		return nil, err
	}

	blobN := blobCount(fileSize, sliceSize)
	blobs := make([]proto.Slice, 0, blobN)
	var size uint64
	for _, bidRet := range allocRets {
		if blobN <= 0 {
			break
		}

		count := minU64(blobN, uint64(bidRet.BidEnd)-uint64(bidRet.BidStart)+1)
		blobN -= count
		validSize := uint64(sliceSize) * count
		if blobN <= 0 {
			validSize = fileSize - size
		}

		blobs = append(blobs, proto.Slice{
			MinSliceID: bidRet.BidStart,
			Vid:        bidRet.Vid,
			Count:      uint32(count),
			ValidSize:  validSize,
		})
		size += validSize
	}
	if blobN > 0 {
		return nil, errors.New("no enough blob ids from allocator")
	}
	return blobs, nil
}

func (alc *allocator) ListVolume(ctx context.Context, codeMode codemode.CodeMode) ([]clustermgr.AllocVolumeInfo, error) {
	return alc.listVolume(ctx, codeMode)
}

func (alc *allocator) Close() {
	alc.volumeMgr.close()
}

// blobCount blobSize > 0 is certain
func blobCount(size uint64, blobSize uint32) uint64 {
	return (size + uint64(blobSize) - 1) / uint64(blobSize)
}

func minU64(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
