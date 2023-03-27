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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func (mp *metaPartition) fsmCreateMultipart(dbHandle interface{}, multipart *Multipart) (status uint8, err error) {
	status = proto.OpOk

	var ok bool
	_, ok, err = mp.multipartTree.Create(dbHandle, multipart, false)
	if err != nil {
		status = proto.OpErr
		return
	}
	if !ok {
		status = proto.OpExistErr
	}
	return
}

func (mp *metaPartition) fsmRemoveMultipart(dbHandle interface{}, multipart *Multipart) (status uint8, err error) {
	status = proto.OpOk

	var ok bool
	ok, err = mp.multipartTree.Delete(dbHandle, multipart.key, multipart.id)
	if err != nil {
		status = proto.OpErr
		return
	}
	if !ok {
		status =  proto.OpNotExistErr
	}
	return
}

func (mp *metaPartition) fsmAppendMultipart(dbHandle interface{}, multipart *Multipart) (status uint8, err error) {
	var storedMultipart *Multipart
	status = proto.OpOk
	storedMultipart, err = mp.multipartTree.Get(multipart.key, multipart.id)
	if err != nil {
		status = proto.OpErr
		return
	}
	if storedMultipart == nil {
		status = proto.OpNotExistErr
		return
	}

	for _, part := range multipart.Parts() {
		actual, stored := storedMultipart.LoadOrStorePart(part)
		if !stored && !actual.Equal(part) {
			status = proto.OpExistErr
			return
		}
	}

	if err = mp.multipartTree.Put(dbHandle, storedMultipart); err != nil {
		status = proto.OpErr
		log.LogErrorf("[fsmAppendMultipart] update multipart info failed, multipart id:%s, multipart key:%s, error:%v",
			multipart.id, multipart.key, err)
	}
	return
}
