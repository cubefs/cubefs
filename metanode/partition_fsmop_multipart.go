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

func (mp *metaPartition) fsmCreateMultipart(multipart *Multipart) (status uint8) {
	_, ok, err := mp.multipartTree.ReplaceOrInsert(multipart, false)
	if err != nil {
		return proto.OpErr
	}
	if !ok {
		return proto.OpExistErr
	}
	return proto.OpOk
}

func (mp *metaPartition) fsmRemoveMultipart(multipart *Multipart) (status uint8) {
	ok, err := mp.multipartTree.Delete(multipart)
	if err != nil {
		return proto.OpErr
	}
	if !ok {
		return proto.OpNotExistErr
	}
	return proto.OpOk
}

func (mp *metaPartition) fsmAppendMultipart(multipart *Multipart) (resp proto.AppendMultipartResponse) {
	storedMultipart, err := mp.multipartTree.CopyGet(multipart)
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
	if err := mp.multipartTree.Put(storedMultipart); err != nil {
		resp.Status = proto.OpErr
		log.LogErrorf("[fsmAppendMultipart] update multipart info failed, multipart id:%s, multipart key:%s, error:%v",
			multipart.id, multipart.key, err)
	}
	return
}
