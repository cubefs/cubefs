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
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
)

type fsmEvictUniqCheckerRequest struct {
	Idx    int
	UniqID uint64
}

func (mp *metaPartition) fsmUniqID(val []byte) (status uint8) {
	var id uint64
	id = binary.BigEndian.Uint64(val)
	for {
		cur := mp.GetUniqId()
		if id <= cur {
			break
		}
		if atomic.CompareAndSwapUint64(&mp.config.UniqId, cur, id) {
			break
		}
	}
	return proto.OpOk
}

func (mp *metaPartition) fsmUniqCheckerEvict(req *fsmEvictUniqCheckerRequest) error {
	mp.uniqChecker.doEvict(req.UniqID)
	return nil
}
