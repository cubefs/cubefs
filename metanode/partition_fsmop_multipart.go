// Copyright 2018 The Chubao Authors.
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

func (mp *metaPartition) fsmCreateMultipart(multipart *Multipart) (status uint8, err error) {
	status = proto.OpOk

	err = mp.multipartTree.Create(multipart, false)
	if err != nil {
		if err == existsError {
			status = proto.OpExistErr
		} else {
			status = proto.OpErr
		}
	}
	return
}

func (mp *metaPartition) fsmRemoveMultipart(multipart *Multipart) (status uint8, err error) {
	var mul *Multipart
	status = proto.OpOk
	mul, err = mp.multipartTree.Get(multipart.key, multipart.id)
	if err != nil {
		status = proto.OpErr
		return
	}
	if mul == nil {
		status = proto.OpNotExistErr
		return
	}

	_, err = mp.multipartTree.Delete(multipart.key, multipart.id)
	if err != nil {
		if err == notExistsError {
			status =  proto.OpNotExistErr
		} else {
			status = proto.OpErr
		}
	}
	return
}

func (mp *metaPartition) fsmAppendMultipart(multipart *Multipart) (status uint8, err error) {
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

	if err = mp.multipartTree.Put(storedMultipart); err != nil {
		status = proto.OpErr
		log.LogErrorf("[fsmAppendMultipart] update multipart info failed, multipart id:%s, multipart key:%s, error:%v",
			multipart.id, multipart.key, err)
	}
	return
}
