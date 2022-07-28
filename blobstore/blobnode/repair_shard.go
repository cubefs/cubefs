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
	"bytes"
	"context"
	"io"
	"sync"

	api "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// IShardAccess define the interface of blobnode use by shard repair
type IShardAccess interface {
	PutShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, size int64, body io.Reader, ioType api.IOType) (err error)
	StatShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) (si *client.ShardInfo, err error)
	GetShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, ioType api.IOType) (body io.ReadCloser, crc32 uint32, err error)
}

// ShardInfoEx shard info execution
type ShardInfoEx struct {
	err  error
	info *client.ShardInfo
}

// IsBad returns true is shard return error
func (shard *ShardInfoEx) IsBad() bool {
	return shard.err != nil
}

// BadReason returns bad reason
func (shard *ShardInfoEx) BadReason() error {
	return shard.err
}

// MarkDeleted returns true if no err return and shard is mark deleted
func (shard *ShardInfoEx) MarkDeleted() bool {
	return shard.err == nil && shard.info.MarkDeleted()
}

// NotExist returns true if no err return and shard is not exist
func (shard *ShardInfoEx) NotExist() bool {
	return shard.err == nil && shard.info.NotExist()
}

// Normal returns true if no err return and shard status is normal
func (shard *ShardInfoEx) Normal() bool {
	return shard.err == nil && shard.info.Normal()
}

// ShardSize returns shard size
func (shard *ShardInfoEx) ShardSize() int64 {
	return shard.info.Size
}

// ShardRepairer used to repair shard data
type ShardRepairer struct {
	cli     IShardAccess
	bufPool *workutils.ByteBufferPool
}

// NewShardRepairer returns shard repairer
func NewShardRepairer(cli IShardAccess, bufPool *workutils.ByteBufferPool) *ShardRepairer {
	return &ShardRepairer{cli: cli, bufPool: bufPool}
}

// RepairShard repair shard data
func (repairer *ShardRepairer) RepairShard(ctx context.Context, task proto.ShardRepairTask) error {
	span := trace.SpanFromContextSafe(ctx)

	if !task.IsValid() {
		span.Errorf("shard repair task is illegal: task[%+v]", task)
		return errcode.ErrIllegalTask
	}

	shardInfos := repairer.listShardsInfo(ctx, task.Sources, task.Bid)
	// check missed shard has repaired
	badIdxs := sliceUint8ToInt(task.BadIdxs)
	repaired, err := hasRepaired(shardInfos, badIdxs)
	if err != nil {
		return err
	}
	if repaired {
		return nil
	}
	// get repair shards:
	shouldRepairIdxs, shardSize, err := getRepairShards(ctx, shardInfos, task.CodeMode, badIdxs)
	if err != nil {
		span.Errorf("get repair shards failed: task[%+v], err[%+v]", err, task)
		return err
	}
	if len(shouldRepairIdxs) == 0 {
		span.Infof("blob is ok or all not exist not need to repair and skip: bid[%d]", task.Bid)
		return nil
	}

	span.Infof("start recover blob: bid[%d], badIdx[%+v]", task.Bid, task.BadIdxs)
	bidInfos := []*ShardInfoSimple{{Bid: task.Bid, Size: shardSize}}
	shardRecover := NewShardRecover(task.Sources, task.CodeMode, bidInfos, repairer.bufPool, repairer.cli, 1, api.RepairIO)
	defer shardRecover.ReleaseBuf()
	err = shardRecover.RecoverShards(ctx, task.BadIdxs, false)
	if err != nil {
		span.Errorf("recover blob failed: err[%+v]", err)
		return err
	}

	// put shards to dest
	span.Infof("data has prepared and put data to dest")
	retErrs := make([]error, len(task.Sources))
	wg := sync.WaitGroup{}
	for _, badi := range shouldRepairIdxs {
		data, err := shardRecover.GetShard(uint8(badi), task.Bid)
		if err != nil {
			return err
		}
		dstLocation := task.Sources[badi]

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err = repairer.cli.PutShard(ctx, dstLocation, task.Bid, shardSize, bytes.NewReader(data), api.RepairIO)
			retErrs[i] = err
		}(badi)
	}
	wg.Wait()

	span.Debugf("put dest failed: ret errs[%+v]", retErrs)
	for idx, err := range retErrs {
		if err != nil {
			span.Errorf("repair shard failed: idx[%d], err[%+v]", idx, err)
			return err
		}
	}

	orphanedShard := repairer.checkOrphanShard(ctx, task.Sources, task.Bid, badIdxs)
	if orphanedShard {
		span.Warnf("blob is orphan shard: bid[%d], badIdxs[%+v]", task.Bid, badIdxs)
		err = errcode.ErrOrphanShard
	}
	return err
}

func hasRepaired(shardInfos []*ShardInfoEx, repairIdxs []int) (bool, error) {
	var repairCnt int
	for idx, shard := range shardInfos {
		if !contains(idx, repairIdxs) {
			continue
		}

		if shard.IsBad() {
			return false, errcode.ErrDestReplicaBad
		}
		if shard.Normal() {
			repairCnt++
		}
	}

	if repairCnt == len(repairIdxs) {
		return true, nil
	}
	return false, nil
}

func getRepairShards(
	ctx context.Context,
	shardInfos []*ShardInfoEx,
	mode codemode.CodeMode,
	repaireIdxs []int) (repairIdx []int, shardSize int64, err error) {
	span := trace.SpanFromContextSafe(ctx)
	// step1:
	// if any shards has mark delete will not need repair
	// other shard without repair are all not exist,will not need repair
	bidNotExistCnt := 0
	for idx, shard := range shardInfos {
		if shard.MarkDeleted() {
			return []int{}, 0, nil
		}

		if contains(idx, repaireIdxs) {
			continue
		}

		if shard.NotExist() {
			bidNotExistCnt++
			continue
		}
	}
	if bidNotExistCnt == workutils.AllReplCnt(mode)-len(repaireIdxs) {
		span.Info("all shards without repair are not exist")
		return []int{}, 0, nil
	}

	// step2:check miss too many shards to repair
	existStatus := workutils.NewBidExistStatus(mode)
	for _, shard := range shardInfos {
		if shard.Normal() {
			shardSize = shard.ShardSize()
			existStatus.Exist(shard.info.Vuid.Index())
			continue
		}
	}
	if !existStatus.CanRecover() {
		span.Errorf("shard maybe lost: mode[%d], existStatus[%+v]", mode, existStatus)
		return nil, 0, errcode.ErrShardMayBeLost
	}

	// step 3:collect need repair shards
	var shouldRepairIdx []int
	for idx, shard := range shardInfos {
		if !contains(idx, repaireIdxs) {
			continue
		}
		if shard.NotExist() {
			shouldRepairIdx = append(shouldRepairIdx, idx)
		}
	}
	return shouldRepairIdx, shardSize, nil
}

// due to the existence of blob delete and shard repair concurrent scenarios
// step 1:repair shard task get shards and generate repair shard data
// step 2:delete task delete all shards of blob
// step 3:repair shard task put repair shard data to destination
// so will appear the case that the repair shard exist only in a blob which we called orphan shard
// to resolve this problem,we check whether the shard is an orphan shard when repair shard finish
// and simply record the shard in a table for troubleshooting
func (repairer *ShardRepairer) checkOrphanShard(
	ctx context.Context,
	repls []proto.VunitLocation,
	bid proto.BlobID,
	badIdxs []int) bool {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start check orphan shard: bid[%d], badIdxs[%+v]", bid, badIdxs)

	shardInfos := repairer.listShardsInfo(ctx, repls, bid)
	for idx, shardInfo := range shardInfos {
		if contains(idx, badIdxs) {
			continue
		}
		if shardInfo.Normal() {
			// as long as one shard is normal.it will not appear orphan
			return false
		}
	}
	return true
}

func (repairer *ShardRepairer) listShardsInfo(ctx context.Context, repls []proto.VunitLocation, bid proto.BlobID) []*ShardInfoEx {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start list shards info: bid[%d], replicas len[%d]", bid, len(repls))

	shardInfos := make([]*ShardInfoEx, len(repls))
	wg := sync.WaitGroup{}
	for i, replica := range repls {
		location := replica
		idx := i
		span.Infof("stat shard: location[%+v], idx[%d]", location, idx)

		wg.Add(1)
		go func() {
			defer wg.Done()
			si, err := repairer.cli.StatShard(ctx, location, bid)
			if err == nil {
				shardInfos[idx] = &ShardInfoEx{err: nil, info: si}
			} else {
				shardInfos[idx] = &ShardInfoEx{err: err, info: nil}
			}
		}()
	}
	wg.Wait()

	span.Infof("finish list shards")
	return shardInfos
}

// e := 1 s := []int{1,2,3}
// contains(e,s) return true
// e := 1 s := []int{2,3}
// contains(e,s) return false
func contains(e int, s []int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func sliceUint8ToInt(s []uint8) []int {
	var ret []int
	for _, e := range s {
		ret = append(ret, int(e))
	}
	return ret
}
