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

package proxy

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type Allocator interface {
	VolumeAlloc(ctx context.Context, host string, args *AllocVolsArgs) (ret []AllocRet, err error)
}

type ListVolsArgs struct {
	CodeMode codemode.CodeMode `json:"code_mode"`
}

type VolumeList struct {
	Vids    []proto.Vid                  `json:"vids"`
	Volumes []clustermgr.AllocVolumeInfo `json:"volumes"`
}

type AllocRet struct {
	BidStart proto.BlobID `json:"bid_start"`
	BidEnd   proto.BlobID `json:"bid_end"`
	Vid      proto.Vid    `json:"vid"`
}

type AllocVolsArgs struct {
	Fsize    uint64            `json:"fsize"`
	CodeMode codemode.CodeMode `json:"code_mode"`
	BidCount uint64            `json:"bid_count"`
	Excludes []proto.Vid       `json:"excludes"`
	Discards []proto.Vid       `json:"discards"`
}
