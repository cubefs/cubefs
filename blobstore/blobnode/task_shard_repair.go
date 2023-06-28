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
	cli client.IBlobNode
}

// NewShardRepairer returns shard repairer
func NewShardRepairer(cli client.IBlobNode) *ShardRepairer {
	return &ShardRepairer{cli: cli}
}

// RepairShard repair shard data
func (r *ShardRepairer) RepairShard(ctx context.Context, task *proto.ShardRepairTask) error {
	span := trace.SpanFromContextSafe(ctx)

	shardInfos := r.listShardsInfo(ctx, task.Sources, task.Bid)
	// check missed shard has repaired
	badIdxs := sliceUint8ToInt(task.BadIdxes)
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

	span.Infof("start recover blob: bid[%d], badIdx[%+v]", task.Bid, task.BadIdxes)
	bidInfos := []*ShardInfoSimple{{Bid: task.Bid, Size: shardSize}}
	shardRecover := NewShardRecover(task.Sources, task.CodeMode, bidInfos,
		r.cli, 1, proto.TaskTypeShardRepair, task.EnableAssist)
	defer shardRecover.ReleaseBuf()
	err = shardRecover.RecoverShards(ctx, task.BadIdxes, false)
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
			err = r.cli.PutShard(ctx, dstLocation, task.Bid, shardSize, bytes.NewReader(data), api.ShardRepairIO)
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

	orphanedShard := r.checkOrphanShard(ctx, task.Sources, task.Bid, badIdxs)
	if orphanedShard {
		span.Warnf("blob is orphan shard: bid[%d], badIdxs[%+v]", task.Bid, badIdxs)
		err = errcode.ErrOrphanShard
	}
	return err
}

// ShardPartialRepair repair within az
func (r *ShardRepairer) ShardPartialRepair(ctx context.Context, arg api.ShardPartialRepairArgs) (ret *api.ShardPartialRepairRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start shard partial repair, args[%v]", arg)

	shards := r.cli.GetPartialShards(ctx, arg)
	numShard := arg.CodeMode.GetShardNum()
	shardData := make([][]byte, numShard)

	defer func() {
		// release source
		for _, shard := range shards {
			if shard.Body != nil {
				io.Copy(io.Discard, shard.Body)
				shard.Body.Close()
			}
		}
	}()
	// check err
	ret = &api.ShardPartialRepairRet{}
	for idx, shard := range shards {
		location := arg.Sources[idx]
		if shard.Err != nil {
			span.Warnf("read chunk[%v] data failed from disk[%v], host[%s], err: %v",
				location.Vuid, location.DiskID, location.Host, shard.Err)
			ret.BadIdxes = append(ret.BadIdxes, arg.SurvivalIndex[idx])
		}
	}
	if len(ret.BadIdxes) != 0 {
		return ret, nil
	}
	bufferSize := len(arg.Sources) * int(arg.Size)
	buffer, err := workutils.TaskBufPool.GetBufBySize(bufferSize)
	if err != nil {
		span.Errorf("alloc buf failed: err[%+v]", err)
		return nil, err
	}
	defer workutils.TaskBufPool.Put(buffer)

	start := 0
	// read data from reader
	for idx, shard := range shards {
		location := arg.Sources[idx]
		vIdx := location.Vuid.Index()
		shardData[vIdx] = buffer[start : start+int(arg.Size)]
		n, errRead := io.ReadFull(shard.Body, shardData[vIdx])
		if errRead != nil || n != int(arg.Size) {
			span.Errorf("read data from body failed, err: %v", errRead)
			err = errShardDataNotPrepared
			return nil, err
		}
		start = start + int(arg.Size)
	}

	encoder, err := workutils.GetEncoder(arg.CodeMode)
	if err != nil {
		span.Errorf("get encoder failed: code_mode[%s], err[%+v]", arg.CodeMode.String(), err)
		return nil, err
	}

	// partial reconstruct in az
	if err = encoder.PartialReconstruct(shardData, arg.SurvivalIndex, []int{arg.BadIdx}); err != nil {
		span.Errorf("partial reconstruct failed, err: %v", err)
		return nil, err
	}
	ret.Data = shardData[arg.BadIdx]
	span.Debugf("shard partial repair success, args[%v]", arg)
	return ret, nil
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

func getRepairShards(ctx context.Context, shardInfos []*ShardInfoEx, mode codemode.CodeMode, repaireIdxs []int,
) (repairIdx []int, shardSize int64, err error) {
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
	if bidNotExistCnt == mode.GetShardNum()-len(repaireIdxs) {
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
func (r *ShardRepairer) checkOrphanShard(ctx context.Context, repls []proto.VunitLocation, bid proto.BlobID, badIdxs []int) bool {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start check orphan shard: bid[%d], badIdxs[%+v]", bid, badIdxs)

	shardInfos := r.listShardsInfo(ctx, repls, bid)
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

func (r *ShardRepairer) listShardsInfo(ctx context.Context, repls []proto.VunitLocation, bid proto.BlobID) []*ShardInfoEx {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start list shards info: bid[%d], locations len[%d]", bid, len(repls))

	shardInfos := make([]*ShardInfoEx, len(repls))
	wg := sync.WaitGroup{}
	for i, location := range repls {
		l := location
		idx := i
		span.Infof("stat shard: location[%+v], idx[%d]", location, idx)

		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := r.cli.StatShard(ctx, l, bid)
			shardInfos[idx] = &ShardInfoEx{err: err, info: info}
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
