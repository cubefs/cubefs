// Copyright 2018 The Container File System Authors.
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
	"github.com/tiglabs/containerfs/util/btree"
	"sync"
)

const defaultBTreeDegree = 32

type (
	// BtreeItem Type alias google btree Item
	BtreeItem = btree.Item
)

// wrapper of Google's btree
type BTree struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewBtree create a new btree
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// Get returns the object of the given key in the btree
func (b *BTree) Get(key BtreeItem) (item BtreeItem) {
	b.RLock()
	item = b.tree.Get(key)
	b.RUnlock()
	return
}

// Find searches for the given key in the btree
func (b *BTree) Find(key BtreeItem, fn func(i BtreeItem)) {
	b.RLock()
	item := b.tree.Get(key)
	b.RUnlock()
	if item == nil {
		return
	}
	fn(item)
}

// Has checks if the key exists in the btree
func (b *BTree) Has(key BtreeItem) (ok bool) {
	b.RLock()
	ok = b.tree.Has(key)
	b.RUnlock()
	return
}

// Delete deletes the object by the given key
func (b *BTree) Delete(key BtreeItem) (item BtreeItem) {
	b.Lock()
	item = b.tree.Delete(key)
	b.Unlock()
	return
}

// ReplaceOrInsert wrapper google btree ReplaceOrInsert
// 通过参数来控制是否在插入新对象时，如果发现Btree中包含此对象后的操作；如果replace设置
// 为True；则直接替换；否则不替换
func (b *BTree) ReplaceOrInsert(key BtreeItem, replace bool) (item BtreeItem,
	ok bool) {
	b.Lock()
	if replace {
		item = b.tree.ReplaceOrInsert(key)
		b.Unlock()
		ok = true
		return
	}

	item = b.tree.Get(key)
	if item == nil {
		item = b.tree.ReplaceOrInsert(key)
		b.Unlock()
		ok = true
		return
	}
	ok = false
	b.Unlock()
	return
}

// Ascend wrapper google btree Ascend
// 从头到尾遍历整个Btree，把每个对象逐一的传递给函数
// 当数据量很大是，尽力不要在线使用整个函数，应该首先
// 调用GetTree函数，获取一份当前Btree的快照，再在快照
// 中遍历
func (b *BTree) Ascend(fn func(i BtreeItem) bool) {
	b.RLock()
	b.tree.Ascend(fn)
	b.RUnlock()
}

// AscendRange wrapper google btree AscendRange
// 逐一遍历在[greaterOrEqual, lessThan)之间的对象，并把对象
// 做为迭代函数的参数
func (b *BTree) AscendRange(greaterOrEqual, lessThan BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendRange(greaterOrEqual, lessThan, iterator)
	b.RUnlock()
}

// AscendGreaterOrEqual wrapper google btree AscendGreaterOrEqual
// 逐一遍历在[pivot, end)之间的对象，并把对象做为迭代函数的参数
func (b *BTree) AscendGreaterOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendGreaterOrEqual(pivot, iterator)
	b.RUnlock()
}

// GetTree clone btree
func (b *BTree) GetTree() *BTree {
	b.RLock()
	t := b.tree.Clone()
	b.RUnlock()
	nb := NewBtree()
	nb.tree = t
	return nb
}

// Reset resets the current btree
func (b *BTree) Reset() {
	b.Lock()
	b.tree.Clear(false)
	b.Unlock()
}

// Len returns the total number of items in the btree
func (b *BTree) Len() (size int) {
	b.RLock()
	size = b.tree.Len()
	b.RUnlock()
	return
}

// MaxItem returns the max item in the btree
func (b *BTree) MaxItem() BtreeItem {
	b.RLock()
	item := b.tree.Max()
	b.RUnlock()
	return item
}
