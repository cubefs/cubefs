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

import "fmt"

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) (err error) {
	treeItem := mp.extendTree.CopyGet(extend)
	var e *Extend
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		// attr multi-ver copy all attr for simplify management
		e = treeItem.(*Extend)
		if e.verSeq != extend.verSeq {
			if extend.verSeq < e.verSeq {
				return fmt.Errorf("seq error assign %v but less than %v", extend.verSeq, e.verSeq)
			}
			e.multiVers = append(e.multiVers, e.Copy().(*Extend))
			e.verSeq = extend.verSeq
		}
		e.Merge(extend, true)
	}

	return
}

// todo(leon chang):check snapshot delete relation with attr
func (mp *metaPartition) fsmRemoveXAttr(extend *Extend) (err error) {
	treeItem := mp.extendTree.CopyGet(extend)
	if treeItem == nil {
		return
	}
	e := treeItem.(*Extend)
	if extend.verSeq < e.verSeq {
		return fmt.Errorf("seq error assign %v but less than %v", extend.verSeq, e.verSeq)
	}
	// attr multi-ver copy all attr for simplify management
	if e.verSeq > extend.verSeq {
		e.multiVers = append(e.multiVers, e)
		e.verSeq = extend.verSeq
	}

	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})
	return
}
