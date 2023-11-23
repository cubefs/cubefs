// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"encoding/binary"
	"github.com/cubefs/cubefs/proto"
)

type fsmEvictUniqCheckerRequest struct {
	Idx    int
	UniqID uint64
}

type UniqIdResp struct {
	Start  uint64
	End    uint64
	Status uint8
}

func (mp *metaPartition) fsmUniqID(val []byte) (resp *UniqIdResp) {
	resp = &UniqIdResp{
		Status: proto.OpOk,
	}

	num := binary.BigEndian.Uint32(val)
	resp.Start, resp.End = mp.allocateUniqID(num)
	return resp
}

func (mp *metaPartition) fsmUniqCheckerEvict(req *fsmEvictUniqCheckerRequest) error {
	mp.uniqChecker.doEvict(req.UniqID)
	return nil
}
