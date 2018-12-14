// Copyright 2018 The Containerfs Authors.
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
	"github.com/google/btree"
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

func (b *BTree) Get(key BtreeItem) (item BtreeItem) {
	b.RLock()
	item = b.tree.Get(key)
	b.RUnlock()
	return
}

func (b *BTree) Find(key BtreeItem, fn func(i BtreeItem)) {
	b.RLock()
	item := b.tree.Get(key)
	b.RUnlock()
	if item == nil {
		return
	}
	fn(item)
}

func (b *BTree) Has(key BtreeItem) (ok bool) {
	b.RLock()
	ok = b.tree.Has(key)
	b.RUnlock()
	return
}

func (b *BTree) Delete(key BtreeItem) (item BtreeItem) {
	b.Lock()
	item = b.tree.Delete(key)
	b.Unlock()
	return
}

func (b *BTree) ReplaceOrInsert(key BtreeItem, replace bool) (item BtreeItem,
	ok bool) {
	b.Lock()
	item = b.tree.Get(key)
	if item == nil {
		item, ok = b.tree.ReplaceOrInsert(key), true
		b.Unlock()
		return
	}
	if !replace {
		b.Unlock()
		ok = false
		return
	}
	item, ok = b.tree.ReplaceOrInsert(key), true
	b.Unlock()
	return
}

func (b *BTree) Ascend(fn func(i BtreeItem) bool) {
	b.RLock()
	b.tree.Ascend(fn)
	b.RUnlock()
}

func (b *BTree) AscendRange(greaterOrEqual, lessThan BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendRange(greaterOrEqual, lessThan, iterator)
	b.RUnlock()
}

func (b *BTree) AscendGreaterOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendGreaterOrEqual(pivot, iterator)
	b.RUnlock()
}

func (b *BTree) GetTree() *BTree {
	b.RLock()
	t := b.tree.Clone()
	b.RUnlock()
	nb := NewBtree()
	nb.tree = t
	return nb
}

func (b *BTree) Reset() {
	b.Lock()
	b.tree.Clear(false)
	b.Unlock()
}

func (b *BTree) Len() (size int) {
	b.RLock()
	size = b.tree.Len()
	b.RUnlock()
	return
}

func (b *BTree) MaxItem() BtreeItem {
	b.RLock()
	item := b.tree.Max()
	b.RUnlock()
	return item
}
