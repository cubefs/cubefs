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

package base

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/rs/xid"

	"github.com/cubefs/cubefs/blobstore/common/counter"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	comproto "github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

// IAllocVunit define the interface of clustermgr used for volume alloc
type IAllocVunit interface {
	AllocVolumeUnit(ctx context.Context, vuid comproto.Vuid, excludes []comproto.DiskID) (ret *client.AllocVunitInfo, err error)
}

// AllocVunitSafe alloc volume unit safe
func AllocVunitSafe(
	ctx context.Context,
	cli IAllocVunit,
	vuid comproto.Vuid,
	volReplicas []comproto.VunitLocation,
	excludes []comproto.DiskID,
) (ret *client.AllocVunitInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)

	allocVunit, err := cli.AllocVolumeUnit(ctx, vuid, excludes)
	if err != nil {
		return nil, err
	}

	// safety check
	for _, repl := range volReplicas {
		if repl.Vuid.Index() == allocVunit.Vuid.Index() {
			// allow alloc on same disk with old chunk
			continue
		}
		if repl.DiskID == allocVunit.DiskID {
			span.Panic("alloc chunk and others chunks are on same disk")
		}
	}

	return allocVunit, nil
}

type IAllocShardUnit interface {
	AllocShardUnit(ctx context.Context, suid comproto.Suid, excludes []comproto.DiskID) (ret *client.AllocShardUnitInfo, err error)
}

// AllocShardUnitSafe alloc volume unit safe
func AllocShardUnitSafe(
	ctx context.Context,
	cli IAllocShardUnit,
	src, dest comproto.ShardUnitInfoSimple,
	excludes []comproto.DiskID,
) (ret *client.AllocShardUnitInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)

	allocShardUnit, err := cli.AllocShardUnit(ctx, src.Suid, excludes)
	if err != nil {
		return nil, err
	}

	if allocShardUnit.DiskID == src.DiskID || allocShardUnit.DiskID == dest.DiskID {
		span.Panic("alloc chunk and others chunks are on same disk")
	}

	return allocShardUnit, nil
}

// Subtraction c = a - b
func Subtraction(a, b []comproto.Vuid) (c []comproto.Vuid) {
	m := make(map[comproto.Vuid]struct{})
	for _, vuid := range b {
		m[vuid] = struct{}{}
	}

	for _, vuid := range a {
		if _, ok := m[vuid]; !ok {
			c = append(c, vuid)
		}
	}
	return c
}

// SubSuids c = a - b
func SubSuids(a, b []comproto.Suid) (c []comproto.Suid) {
	m := make(map[comproto.Suid]struct{})
	for _, suid := range b {
		m[suid] = struct{}{}
	}

	for _, suid := range a {
		if _, ok := m[suid]; !ok {
			c = append(c, suid)
		}
	}
	return c
}

// GenTaskID return task id
func GenTaskID(prefix string, vid comproto.Vid) string {
	return fmt.Sprintf("%s-%d-%v", prefix, vid, xid.New().String())
}

// DataMountFormat format data
func DataMountFormat(dataMountBytes [counter.SLOT]int) string {
	var formatStr []string
	for _, dataMount := range dataMountBytes {
		formatStr = append(formatStr, bytesCntFormat(dataMount))
	}
	return fmt.Sprint(formatStr)
}

func bytesCntFormat(bytesCnt int) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	idx := 0
	bytesCnt2 := bytesCnt
	for {
		bytesCnt2 = bytesCnt2 / 1024
		if bytesCnt2 == 0 {
			break
		}
		idx++
		if idx == 5 {
			break
		}
	}

	num := float64(bytesCnt) / math.Pow(float64(1024), float64(idx))
	return fmt.Sprintf("%.3f%s", num, units[idx])
}

// ShouldAllocAndRedo return true if should alloc and redo task
func ShouldAllocAndRedo(errCode int) bool {
	return errCode == errors.CodeNewVuidNotMatch ||
		errCode == errors.CodeStatChunkFailed
}

// ShouldAllocShardUnitAndRedo return true if should alloc and redo task
func ShouldAllocShardUnitAndRedo(errCode int) bool {
	return errCode == errors.CodeNewSuidNotMatch
}

func InsistOn(ctx context.Context, errMsg string, on func() error) {
	span := trace.SpanFromContextSafe(ctx)
	attempt := 0
	retry.Insist(time.Second, on, func(err error) {
		attempt++
		span.Errorf("insist attempt-%d: %s %s", attempt, errMsg, err.Error())
	})
}
