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

package metanode

import "sync"

type BTreeTx interface {
	TxGet(item BtreeItem) BtreeItem
	TxReplaceOrInsert(item BtreeItem) BtreeItem
	TxDelete(item BtreeItem) BtreeItem
	TxMin() BtreeItem
	TxMax() BtreeItem
	TxHas(item BtreeItem) bool
	TxDeleteMin() BtreeItem
	TxDeleteMax() BtreeItem
	TxAscendRange(greaterOrEqual, lessThan BtreeItem,
		iterator func(i BtreeItem) bool)
	TxAscendLessThan(pivot BtreeItem, iterator func(i BtreeItem) bool)
	TxAscendGreaterOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool)
	TxAscend(iterator func(i BtreeItem) bool)
	TxDescendRange(lessOrEqual, greaterThan BtreeItem,
		iterator func(i BtreeItem) bool)
	TxDescendLessOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool)
	TxDescend(iterator func(i BtreeItem) bool)
	TxLen() int
	TxClear()
	TxClose()
}

var (
	btreeTxPool = sync.Pool{
		New: func() interface{} {
			return &btreeTx{}
		},
	}
)

func GetBTreeTxBuffer() *btreeTx {
	item := btreeTxPool.Get()
	return item.(*btreeTx)
}

func PutBtreeTxBuffer(bt *btreeTx) {
	bt.BTree = nil
	btreeTxPool.Put(bt)
}

func (b *BTree) BeginTx() BTreeTx {
	bt := GetBTreeTxBuffer()
	bt.BTree = b
	b.Lock()
	return bt

}
func (b *BTree) closeTx() {
	b.Unlock()
}

type btreeTx struct {
	*BTree
}

func NewBtreeTx(tree *BTree) *btreeTx {
	return &btreeTx{
		tree,
	}
}
func (bt *btreeTx) TxGet(item BtreeItem) BtreeItem {
	return bt.tree.Get(item)
}

func (bt *btreeTx) TxReplaceOrInsert(item BtreeItem) BtreeItem {
	return bt.tree.ReplaceOrInsert(item)
}

func (bt *btreeTx) TxDelete(item BtreeItem) BtreeItem {
	return bt.tree.Delete(item)
}

func (bt *btreeTx) TxMax() BtreeItem {
	return bt.tree.Max()
}

func (bt *btreeTx) TxMin() BtreeItem {
	return bt.tree.Min()
}

func (bt *btreeTx) TxHas(item BtreeItem) bool {
	return bt.tree.Has(item)
}

func (bt *btreeTx) TxLen() int {
	return bt.tree.Len()
}

func (bt *btreeTx) TxDeleteMin() BtreeItem {
	return bt.tree.DeleteMin()
}

func (bt *btreeTx) TxDeleteMax() BtreeItem {
	return bt.tree.DeleteMax()
}

func (bt *btreeTx) TxAscendRange(greaterOrEqual, lessThan BtreeItem,
	iterator func(i BtreeItem) bool) {
	bt.tree.AscendRange(greaterOrEqual, lessThan, iterator)
}

func (bt *btreeTx) TxAscendLessThan(pivot BtreeItem,
	iterator func(i BtreeItem) bool) {
	bt.tree.AscendLessThan(pivot, iterator)
}

func (bt *btreeTx) TxAscendGreaterOrEqual(pivot BtreeItem,
	iterator func(i BtreeItem) bool) {
	bt.tree.AscendGreaterOrEqual(pivot, iterator)
}

func (bt *btreeTx) TxAscend(iterator func(i BtreeItem) bool) {
	bt.tree.Ascend(iterator)
}

func (bt *btreeTx) TxDescendRange(lessOrEqual, greaterThan BtreeItem,
	iterator func(i BtreeItem) bool) {
	bt.tree.DescendRange(lessOrEqual, greaterThan, iterator)
}

func (bt *btreeTx) TxDescendLessOrEqual(pivot BtreeItem,
	iterator func(i BtreeItem) bool) {
	bt.tree.DescendGreaterThan(pivot, iterator)
}

func (bt *btreeTx) TxDescend(iterator func(i BtreeItem) bool) {
	bt.tree.Descend(iterator)
}

func (bt *btreeTx) TxClear() {
	bt.tree.Clear(false)
}

func (bt *btreeTx) TxClose() {
	bt.closeTx()
	PutBtreeTxBuffer(bt)
}
