// Copyright 2018 The ChuBao Authors.
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

// FIXME: improve btree performance

package metanode

import (
	"github.com/chubaoio/cbfs/util/btree"
	"sync"
)

const defaultBTreeDegree = 32

type (
	BtreeItem = btree.Item
)
type BTree struct {
	sync.RWMutex
	tree *btree.BTree
}

func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

func (b *BTree) Get(key BtreeItem) BtreeItem {
	b.RLock()
	defer b.RUnlock()
	return b.tree.Get(key)
}

func (b *BTree) Find(key BtreeItem, fn func(i BtreeItem)) {
	b.Lock()
	defer b.Unlock()
	item := b.tree.Get(key)
	if item == nil {
		return
	}
	fn(item)
}

func (b *BTree) Has(key BtreeItem) bool {
	b.RLock()
	defer b.RUnlock()
	return b.tree.Has(key)
}

func (b *BTree) Delete(key BtreeItem) BtreeItem {
	b.Lock()
	defer b.Unlock()
	return b.tree.Delete(key)
}

func (b *BTree) ReplaceOrInsert(key BtreeItem, replace bool) (BtreeItem, bool) {
	b.Lock()
	defer b.Unlock()
	item := b.tree.Get(key)
	if item == nil {
		return b.tree.ReplaceOrInsert(key), true
	}
	if !replace {
		return item, false
	}
	return b.tree.ReplaceOrInsert(key), true
}

func (b *BTree) Ascend(fn func(i BtreeItem) bool) {
	b.Lock()
	t := b.tree.Clone()
	b.Unlock()
	t.Ascend(fn)
}

func (b *BTree) AscendRange(greaterOrEqual, lessThan BtreeItem, iterator func(i BtreeItem) bool) {
	b.Lock()
	t := b.tree.Clone()
	b.Unlock()
	t.AscendRange(greaterOrEqual, lessThan, iterator)
}

func (b *BTree) AscendGreaterOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool) {
	b.Lock()
	t := b.tree.Clone()
	b.Unlock()
	t.AscendGreaterOrEqual(pivot, iterator)
}

func (b *BTree) GetTree() *BTree {
	b.Lock()
	t := b.tree.Clone()
	b.Unlock()
	nb := NewBtree()
	nb.tree = t
	return nb
}

func (b *BTree) Reset() {
	b.Lock()
	b.tree.Clear(false)
	b.Unlock()
}

func (b *BTree) Len() int {
	b.RLock()
	b.RUnlock()
	return b.tree.Len()
}
