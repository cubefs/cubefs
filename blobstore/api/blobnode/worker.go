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
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"io"

	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type WorkerStats struct {
	CancelCount  string `json:"cancel_count"`
	ReclaimCount string `json:"reclaim_count"`
}

func (c *client) RepairShard(ctx context.Context, host string, args *proto.ShardRepairTask) (err error) {
	return c.PostWith(ctx, host+"/shard/repair", nil, args)
}

func (c *client) WorkerStats(ctx context.Context, host string) (ret WorkerStats, err error) {
	err = c.GetWith(ctx, host+"/worker/stats", &ret)
	return
}

type ShardPartialRepairRet struct {
	BadIdxes []int  `json:"bad_idxes"`
	Data     []byte `json:"data"`
}

func (s *ShardPartialRepairRet) Marshal() ([]byte, string, error) {
	buffer := bytes.NewBuffer(nil)
	var err error
	numBadIdx := int32(len(s.BadIdxes))
	numData := int32(len(s.Data))
	if err = binary.Write(buffer, binary.BigEndian, numBadIdx); err != nil {
		return nil, "", err
	}
	for i := int32(0); i < numBadIdx; i++ {
		if err = binary.Write(buffer, binary.BigEndian, int32(s.BadIdxes[i])); err != nil {
			return nil, "", err
		}
	}

	if err = binary.Write(buffer, binary.BigEndian, numData); err != nil {
		return nil, "", err
	}
	if _, err = buffer.Write(s.Data); err != nil {
		return nil, "", err
	}
	return buffer.Bytes(), rpc.MIMEStream, nil
}

func (s *ShardPartialRepairRet) UnmarshalFrom(body io.Reader) (err error) {
	numBadIdx := int32(0)
	numData := int32(0)
	if err = binary.Read(body, binary.BigEndian, &numBadIdx); err != nil {
		return
	}
	badIdxes := make([]int, numBadIdx)
	for i := int32(0); i < numBadIdx; i++ {
		idx := int32(0)
		if err = binary.Read(body, binary.BigEndian, &idx); err != nil {
			return
		}
		badIdxes[i] = int(idx)
	}

	if err = binary.Read(body, binary.BigEndian, &numData); err != nil {
		return
	}
	data := make([]byte, numData)
	if _, err = body.Read(data); err != nil {
		return err
	}
	s.BadIdxes = badIdxes
	s.Data = data
	return nil
}

func (c *client) ShardPartialRepair(ctx context.Context, host string, args *ShardPartialRepairArgs) (ret *ShardPartialRepairRet, err error) {
	if !args.IsValid() {
		return nil, errors.ErrIllegalArguments
	}
	urlStr := fmt.Sprintf("%v/shard/partial/repair", host)

	err = c.PostWith(ctx, urlStr, ret, args)
	return
}
