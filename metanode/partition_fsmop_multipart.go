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

func (mp *MetaPartition) fsmCreateMultipart(multipart *Multipart) (status uint8) {
	if err := mp.multipartTree.Create(multipart); err != nil {
		if err == existsError {
			return proto.OpExistErr
		} else {
			log.LogErrorf("create multipart has err:[%s]", err.Error())
			return proto.OpErr
		}
	}
	return proto.OpOk
}

func (mp *MetaPartition) fsmRemoveMultipart(multipart *Multipart) (status uint8) {
	deletedItem := mp.multipartTree.Delete(multipart.key, multipart.id)
	if deletedItem == nil {
		return proto.OpNotExistErr
	}
	return proto.OpOk
}

func (mp *MetaPartition) fsmAppendMultipart(multipart *Multipart) (status uint8) {
	storedMultipart, err := mp.multipartTree.Get(multipart.key, multipart.id)
	if err != nil {
		log.LogError(err)
		return proto.OpErr
	}

	if storedMultipart == nil {
		log.LogErrorf("not found multipart by key:[%s] id:[%s]", multipart.key, multipart.id)
		return proto.OpNotExistErr
	}

	for _, part := range multipart.Parts() {
		actual, stored := storedMultipart.LoadOrStorePart(part)
		if !stored && !actual.Equal(part) {
			return proto.OpExistErr
		}
	}
	log.LogIfNotNil(mp.multipartTree.Update(storedMultipart))
	return proto.OpOk
}
