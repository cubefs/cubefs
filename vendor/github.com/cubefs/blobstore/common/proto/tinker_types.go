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

package proto

import (
	"github.com/cubefs/blobstore/common/codemode"
)

const (
	TinkerModule = "TINKER"
)

type ShardRepairTask struct {
	Bid      BlobID            `json:"bid"` // blobId
	CodeMode codemode.CodeMode `json:"code_mode"`
	Sources  []VunitLocation   `json:"sources"`
	BadIdxs  []uint8           `json:"bad_idxs"`
	Reason   string            `json:"reason"`
}

func (task *ShardRepairTask) IsValid() bool {
	if !task.CodeMode.IsValid() {
		return false
	}
	if !CheckVunitLocations(task.Sources) {
		return false
	}
	return true
}
