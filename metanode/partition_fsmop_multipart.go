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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) fsmCreateMultipart(dbHandle interface{}, multipart *Multipart) (status uint8, err error) {
	var ok bool
	_, ok, err = mp.multipartTree.Create(dbHandle, multipart, false)
	if err != nil {
		status = proto.OpErr
		return
	}
	if !ok {
		return proto.OpExistErr, nil
	}
	return proto.OpOk, nil
}

func (mp *metaPartition) fsmRemoveMultipart(dbHandle interface{}, multipart *Multipart) (status uint8, err error) {
	var ok bool
	ok, err = mp.multipartTree.Delete(dbHandle, multipart.key, multipart.id)
	if err != nil {
		status = proto.OpErr
		return
	}
	if !ok {
		status =  proto.OpNotExistErr
	}
	return proto.OpOk, nil
}

func (mp *metaPartition) fsmAppendMultipart(dbHandle interface{}, multipart *Multipart) (resp proto.AppendMultipartResponse, err error) {
	var storedMultipart *Multipart
	resp.Status = proto.OpOk
	storedMultipart, err = mp.multipartTree.Get(multipart.key, multipart.id)
	if err != nil {
		resp.Status = proto.OpErr
		return
	}
	if storedMultipart == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	for _, part := range multipart.Parts() {
		oldInode, updated, conflict := storedMultipart.UpdateOrStorePart(part)
		if conflict {
			resp.Status = proto.OpUploadPartConflictErr
			return
		}
		if updated {
			resp.OldInode = oldInode
			resp.Update = true
		}
	}
	resp.Status = proto.OpOk
	if err = mp.multipartTree.Put(dbHandle, storedMultipart); err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("[fsmAppendMultipart] update multipart info failed, multipart id:%s, multipart key:%s, error:%v",
			multipart.id, multipart.key, err)
	}
	return
}
