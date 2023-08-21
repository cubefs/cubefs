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
	"encoding/json"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
)

func (mp *metaPartition) GetUniqID(p *Packet, num uint32) (err error) {

	idBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(idBuf, num)
	resp, err := mp.submit(opFSMUniqID, idBuf)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}

	var (
		status = proto.OpErr
		reply  []byte
	)

	idResp := resp.(*UniqIdResp)
	if idResp.Status == proto.OpOk {
		resp := &GetUniqIDResp{
			Start: idResp.Start,
		}
		status = proto.OpOk
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
			reply = []byte(err.Error())
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) allocateUniqID(num uint32) (start, end uint64) {
	for {
		//cur is the last allocated id
		cur := mp.GetUniqId()
		start = cur + 1
		end := cur + uint64(num)
		if atomic.CompareAndSwapUint64(&mp.config.UniqId, cur, end) {
			return start, end
		}
	}
}

func (mp *metaPartition) uniqCheckerEvict() (left int, evict int, err error) {
	checker := mp.uniqChecker
	left, idx, op := checker.evictIndex()
	if op == nil {
		return left, 0, nil
	}

	fsmReq := &fsmEvictUniqCheckerRequest{
		Idx:    idx,
		UniqID: op.uniqid,
	}
	reqBytes, err := json.Marshal(fsmReq)
	if err != nil {
		panic(err)
	}
	_, err = mp.submit(opFSMUniqCheckerEvict, reqBytes)
	return left, idx + 1, err
}

var inodeOnceSize = 16

type InodeOnce struct {
	UniqID uint64
	Inode  uint64 // Inode ID
}

func (i *InodeOnce) Marshal() (val []byte) {
	val = make([]byte, inodeOnceSize)
	binary.BigEndian.PutUint64(val[0:8], i.UniqID)
	binary.BigEndian.PutUint64(val[8:16], i.Inode)
	return val
}

func InodeOnceUnlinkMarshal(req *UnlinkInoReq) []byte {
	inoOnce := &InodeOnce{
		UniqID: req.UniqID,
		Inode:  req.Inode,
	}
	return inoOnce.Marshal()
}

func InodeOnceLinkMarshal(req *LinkInodeReq) []byte {
	inoOnce := &InodeOnce{
		UniqID: req.UniqID,
		Inode:  req.Inode,
	}
	return inoOnce.Marshal()
}

func InodeOnceUnmarshal(val []byte) *InodeOnce {
	i := &InodeOnce{}
	if len(val) != inodeOnceSize {
		return i
	}
	i.UniqID = binary.BigEndian.Uint64(val[0:8])
	i.Inode = binary.BigEndian.Uint64(val[8:16])
	return i
}
