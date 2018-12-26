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

package metanode

import (
	"github.com/google/btree"
	"sync"
)

const defaultBTreeDegree = 32

type (
	BtreeItem = btree.Item
)

// 封装Google Btree
type BTree struct {
	sync.RWMutex
	tree *btree.BTree
}

func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// 返回指定key的对象，如果在树中找到；未找到返回nil
func (b *BTree) Get(key BtreeItem) (item BtreeItem) {
	b.RLock()
	item = b.tree.Get(key)
	b.RUnlock()
	return
}

// 查找指定Key对象，如果找到则传递给函数；否则不执行此函数
func (b *BTree) Find(key BtreeItem, fn func(i BtreeItem)) {
	b.RLock()
	item := b.tree.Get(key)
	b.RUnlock()
	if item == nil {
		return
	}
	fn(item)
}

// 检查Key对象是否在Btree中
func (b *BTree) Has(key BtreeItem) (ok bool) {
	b.RLock()
	ok = b.tree.Has(key)
	b.RUnlock()
	return
}

// 从Btree中删除指定Key对象
func (b *BTree) Delete(key BtreeItem) (item BtreeItem) {
	b.Lock()
	item = b.tree.Delete(key)
	b.Unlock()
	return
}

// 通过参数来控制是否在插入新对象时，如果发现Btree中包含此对象后的操作；如果replace设置
// 为True；则直接替换；否则不替换
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

// 从头到尾遍历整个Btree，把每个对象逐一的传递给函数
// 当数据量很大是，尽力不要在线使用整个函数，应该首先
// 调用GetTree函数，获取一份当前Btree的快照，再在快照
// 中遍历
func (b *BTree) Ascend(fn func(i BtreeItem) bool) {
	b.RLock()
	b.tree.Ascend(fn)
	b.RUnlock()
}

// 逐一遍历在[greaterOrEqual, lessThan)之间的对象，并把对象
// 做为迭代函数的参数
func (b *BTree) AscendRange(greaterOrEqual, lessThan BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendRange(greaterOrEqual, lessThan, iterator)
	b.RUnlock()
}

// 逐一遍历在[pivot, end)之间的对象，并把对象做为迭代函数的参数
func (b *BTree) AscendGreaterOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendGreaterOrEqual(pivot, iterator)
	b.RUnlock()
}

// 克隆一份当前Btree的快照，并返回快照指针
func (b *BTree) GetTree() *BTree {
	b.RLock()
	t := b.tree.Clone()
	b.RUnlock()
	nb := NewBtree()
	nb.tree = t
	return nb
}

// 重置当前Btree
func (b *BTree) Reset() {
	b.Lock()
	b.tree.Clear(false)
	b.Unlock()
}

// 获取当前Btree 所有对象个数
func (b *BTree) Len() (size int) {
	b.RLock()
	size = b.tree.Len()
	b.RUnlock()
	return
}

// 返回当前Btree的最大对象
func (b *BTree) MaxItem() BtreeItem {
	b.RLock()
	item := b.tree.Max()
	b.RUnlock()
	return item
}
