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

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *MetaPartition) fsmSetXAttr(extend *Extend) error {
	e, err := mp.extendTree.Get(extend.inode)
	if err != nil {
		return err
	}
	if e == nil {
		e = NewExtend(extend.inode)
	}
	e.Merge(extend, true)

	return mp.extendTree.Update(e)
}

func (mp *MetaPartition) fsmRemoveXAttr(extend *Extend) error {
	e, err := mp.extendTree.Get(extend.inode)
	if err == nil {
		return err
	}
	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})
	return mp.extendTree.Update(e)
}
