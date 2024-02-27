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
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
)

const defaultBTreeDegree = 32

type (
	// BtreeItem type alias google btree Item
	BtreeItem = btree.Item
)

var _ Snapshot = &MemSnapShot{}

type MemSnapShot struct {
	applyID             uint64
	inode               *InodeBTree
	dentry              *DentryBTree
	extend              *ExtendBTree
	multipart           *MultipartBTree
	transaction         *TransactionBTree
	transactionRbInode  *TransactionRollbackInodeBTree
	transactionRbDentry *TransactionRollbackDentryBTree
	deletedExtents      *DeletedExtentsBTree
	deletedObjExtents   *DeletedObjExtentsBTree
	txID                uint64
	deletedExtentId     uint64
}

func (b *MemSnapShot) Range(tp TreeType, cb func(item interface{}) (bool, error)) error {
	return b.RangeWithScope(tp, nil, nil, cb)
}

func (b *MemSnapShot) RangeWithScope(tp TreeType, start, end interface{}, cb func(item interface{}) (bool, error)) error {
	switch tp {
	case InodeType:
		callBackFunc := func(inode *Inode) (bool, error) {
			return cb(inode)
		}
		var startInode, endInode *Inode
		startInode = &Inode{}
		if start != nil {
			startInode = start.(*Inode)
		}
		if end != nil {
			endInode = start.(*Inode)
		}
		return b.inode.Range(startInode, endInode, callBackFunc)
	case DentryType:
		callBackFunc := func(dentry *Dentry) (bool, error) {
			return cb(dentry)
		}
		var startDentry, endDentry *Dentry
		startDentry = &Dentry{}
		if start != nil {
			startDentry = start.(*Dentry)
		}
		if end != nil {
			endDentry = end.(*Dentry)
		}
		return b.dentry.Range(startDentry, endDentry, callBackFunc)
	case ExtendType:
		callBackFunc := func(extend *Extend) (bool, error) {
			return cb(extend)
		}
		var startExtend, endExtend *Extend
		startExtend = &Extend{}
		if start != nil {
			startExtend = start.(*Extend)
		}
		if end != nil {
			endExtend = start.(*Extend)
		}
		return b.extend.Range(startExtend, endExtend, callBackFunc)
	case MultipartType:
		callBackFunc := func(multipart *Multipart) (bool, error) {
			return cb(multipart)
		}
		var startMultipart, endMultipart *Multipart
		if start != nil {
			startMultipart = start.(*Multipart)
		}
		if end != nil {
			endMultipart = end.(*Multipart)
		}
		return b.multipart.Range(startMultipart, endMultipart, callBackFunc)
	case TransactionType:
		callBackFunc := func(tx *proto.TransactionInfo) (bool, error) {
			return cb(tx)
		}
		var startTx, endTx *proto.TransactionInfo
		if start != nil {
			startTx = start.(*proto.TransactionInfo)
		}
		if end != nil {
			endTx = end.(*proto.TransactionInfo)
		}
		return b.transaction.Range(startTx, endTx, callBackFunc)
	case TransactionRollbackInodeType:
		callBackFunc := func(inode *TxRollbackInode) (bool, error) {
			return cb(inode)
		}
		var startRbInode, endRbInode *TxRollbackInode
		if start != nil {
			startRbInode = start.(*TxRollbackInode)
		}
		if end != nil {
			endRbInode = start.(*TxRollbackInode)
		}
		return b.transactionRbInode.Range(startRbInode, endRbInode, callBackFunc)
	case TransactionRollbackDentryType:
		callBackFunc := func(dentry *TxRollbackDentry) (bool, error) {
			return cb(dentry)
		}
		var startRbDentry, endRbDentry *TxRollbackDentry
		if start != nil {
			startRbDentry = start.(*TxRollbackDentry)
		}
		if end != nil {
			endRbDentry = end.(*TxRollbackDentry)
		}
		return b.transactionRbDentry.Range(startRbDentry, endRbDentry, callBackFunc)
	case DeletedExtentsType:
		callbackFunc := func(dek *DeletedExtentKey) (bool, error) {
			return cb(dek)
		}
		var startDek, endDek *DeletedExtentKey
		if start != nil {
			startDek = start.(*DeletedExtentKey)
		}
		if end != nil {
			endDek = end.(*DeletedExtentKey)
		}
		return b.deletedExtents.Range(startDek, endDek, callbackFunc)
	case DeletedObjExtentsType:
		callbackFunc := func(doek *DeletedObjExtentKey) (bool, error) {
			return cb(doek)
		}
		var startDoek, endDoek *DeletedObjExtentKey
		if start != nil {
			startDoek = start.(*DeletedObjExtentKey)
		}
		if end != nil {
			endDoek = start.(*DeletedObjExtentKey)
		}
		return b.deletedObjExtents.Range(startDoek, endDoek, callbackFunc)
	default:
	}
	panic("out of type")
}

func (b *MemSnapShot) Close() {}

func (b *MemSnapShot) Count(tp TreeType) uint64 {
	switch tp {
	case InodeType:
		return b.inode.Count()
	case DentryType:
		return b.dentry.Count()
	case ExtendType:
		return b.extend.Count()
	case MultipartType:
		return b.multipart.Count()
	case TransactionType:
		return b.transaction.Count()
	case TransactionRollbackInodeType:
		return b.transactionRbInode.Count()
	case TransactionRollbackDentryType:
		return b.transactionRbDentry.Count()
	case DeletedExtentsType:
		return b.deletedExtents.Count()
	case DeletedObjExtentsType:
		return b.deletedObjExtents.Count()
	default:
	}
	panic("out of type")
}

func (b *MemSnapShot) CrcSum(tp TreeType) (crcSum uint32, err error) {
	var (
		crc  = crc32.NewIEEE()
		data []byte
	)
	switch tp {
	case InodeType:
		cb := func(i *Inode) (bool, error) {
			if data, err = i.Marshal(); err != nil {
				return false, err
			}
			binary.BigEndian.PutUint64(data[AccessTimeOffset:AccessTimeOffset+8], 0)
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.inode.Range(&Inode{}, nil, cb)
	case DentryType:
		cb := func(d *Dentry) (bool, error) {
			if data, err = d.Marshal(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.dentry.Range(&Dentry{}, nil, cb)
	case ExtendType:
		cb := func(extend *Extend) (bool, error) {
			if data, err = extend.Bytes(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.extend.Range(&Extend{}, nil, cb)
	case MultipartType:
		cb := func(multipart *Multipart) (bool, error) {
			if data, err = multipart.Bytes(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.multipart.Range(&Multipart{}, nil, cb)
	case TransactionType:
		cb := func(tx *proto.TransactionInfo) (bool, error) {
			if data, err = tx.Marshal(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.transaction.Range(nil, nil, cb)
	case TransactionRollbackInodeType:
		cb := func(inode *TxRollbackInode) (bool, error) {
			if data, err = inode.Marshal(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.transactionRbInode.Range(nil, nil, cb)
	case TransactionRollbackDentryType:
		cb := func(dentry *TxRollbackDentry) (bool, error) {
			if data, err = dentry.Marshal(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.transactionRbDentry.Range(nil, nil, cb)
	case DeletedExtentsType:
		cb := func(dek *DeletedExtentKey) (bool, error) {
			if data, err = dek.Marshal(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.deletedExtents.Range(nil, nil, cb)
	case DeletedObjExtentsType:
		cb := func(doek *DeletedObjExtentKey) (bool, error) {
			if data, err = doek.Marshal(); err != nil {
				return false, err
			}
			if _, err = crc.Write(data); err != nil {
				return false, err
			}
			return true, nil
		}
		err = b.deletedObjExtents.Range(nil, nil, cb)
	default:
		panic("out of type")
	}
	if err != nil {
		return
	}
	crcSum = crc.Sum32()
	return
}

func (b *MemSnapShot) ApplyID() uint64 {
	return b.applyID
}

func (b *MemSnapShot) TxID() uint64 {
	return b.txID
}

func (b *MemSnapShot) DeletedExtentId() uint64 {
	return b.deletedExtentId
}

var _ InodeTree = &InodeBTree{}
var _ DentryTree = &DentryBTree{}
var _ ExtendTree = &ExtendBTree{}
var _ MultipartTree = &MultipartBTree{}
var _ TransactionTree = &TransactionBTree{}
var _ TransactionRollbackInodeTree = &TransactionRollbackInodeBTree{}
var _ TransactionRollbackDentryTree = &TransactionRollbackDentryBTree{}
var _ DeletedExtentsTree = &DeletedExtentsBTree{}

type InodeBTree struct {
	*BTree
}

type DentryBTree struct {
	*BTree
}

type ExtendBTree struct {
	*BTree
}

type MultipartBTree struct {
	*BTree
}

type TransactionBTree struct {
	*BTree
}

type TransactionRollbackInodeBTree struct {
	*BTree
}

type TransactionRollbackDentryBTree struct {
	*BTree
}

type DeletedExtentsBTree struct {
	*BTree
}

type DeletedObjExtentsBTree struct {
	*BTree
}

func (i *InodeBTree) GetMaxInode() (uint64, error) {
	i.Lock()
	item := i.tree.Max()
	i.Unlock()
	if item == nil {
		return 0, nil
	}
	return item.(*Inode).Inode, nil
}

// get
func (i *InodeBTree) RefGet(ino uint64) (*Inode, error) {
	item := i.BTree.Get(&Inode{Inode: ino})
	if item != nil {
		return item.(*Inode), nil
	}
	return nil, nil
}

func (i *InodeBTree) Get(ino uint64) (*Inode, error) {
	item := i.BTree.CopyGet(&Inode{Inode: ino})
	if item != nil {
		return item.(*Inode), nil
	}
	return nil, nil
}

func (i *DentryBTree) RefGet(pid uint64, name string) (*Dentry, error) {
	item := i.BTree.Get(&Dentry{ParentId: pid, Name: name})
	if item != nil {
		return item.(*Dentry), nil
	}
	return nil, nil
}

func (i *DentryBTree) Get(pid uint64, name string) (*Dentry, error) {
	item := i.BTree.CopyGet(&Dentry{ParentId: pid, Name: name})
	if item != nil {
		return item.(*Dentry), nil
	}
	return nil, nil
}

func (i *ExtendBTree) RefGet(ino uint64) (*Extend, error) {
	item := i.BTree.Get(&Extend{inode: ino})
	if item != nil {
		return item.(*Extend), nil
	}
	return nil, nil
}

func (i *ExtendBTree) Get(ino uint64) (*Extend, error) {
	item := i.BTree.CopyGet(&Extend{inode: ino})
	if item != nil {
		return item.(*Extend), nil
	}
	return nil, nil
}

func (i *MultipartBTree) RefGet(key, id string) (*Multipart, error) {
	item := i.BTree.Get(&Multipart{key: key, id: id})
	if item != nil {
		return item.(*Multipart), nil
	}
	return nil, nil
}

func (i *MultipartBTree) Get(key, id string) (*Multipart, error) {
	item := i.BTree.CopyGet(&Multipart{key: key, id: id})
	if item != nil {
		return item.(*Multipart), nil
	}
	return nil, nil
}

func (i *TransactionBTree) RefGet(txId string) (tx *proto.TransactionInfo, err error) {
	item := i.BTree.Get(&proto.TransactionInfo{TxID: txId})
	if item != nil {
		tx = item.(*proto.TransactionInfo)
	}
	return
}

func (i *TransactionBTree) Get(txId string) (tx *proto.TransactionInfo, err error) {
	item := i.BTree.CopyGet(&proto.TransactionInfo{TxID: txId})
	if item != nil {
		tx = item.(*proto.TransactionInfo)
	}
	return
}

func (i *TransactionRollbackInodeBTree) RefGet(ino uint64) (inode *TxRollbackInode, err error) {
	item := i.BTree.Get(&TxRollbackInode{
		inode: NewInode(ino, 0),
	})
	if item != nil {
		inode = item.(*TxRollbackInode)
	}
	return
}

func (i *TransactionRollbackInodeBTree) Get(ino uint64) (inode *TxRollbackInode, err error) {
	item := i.BTree.CopyGet(&TxRollbackInode{
		inode: NewInode(ino, 0),
	})
	if item != nil {
		inode = item.(*TxRollbackInode)
	}
	return
}

func (i *TransactionRollbackDentryBTree) RefGet(parentId uint64, name string) (dentry *TxRollbackDentry, err error) {
	item := i.BTree.Get(&TxRollbackDentry{
		txDentryInfo: proto.NewTxDentryInfo("", parentId, name, 0),
	})
	if item != nil {
		dentry = item.(*TxRollbackDentry)
	}
	return
}

func (i *TransactionRollbackDentryBTree) Get(parentId uint64, name string) (dentry *TxRollbackDentry, err error) {
	item := i.BTree.CopyGet(&TxRollbackDentry{
		txDentryInfo: proto.NewTxDentryInfo("", parentId, name, 0),
	})
	if item != nil {
		dentry = item.(*TxRollbackDentry)
	}
	return
}

// put
func (i *InodeBTree) Update(dbHandle interface{}, inode *Inode) error {
	i.BTree.ReplaceOrInsert(inode, false)
	return nil
}

func (i *InodeBTree) Put(dbHandle interface{}, inode *Inode) error {
	i.BTree.ReplaceOrInsert(inode, true)
	return nil
}
func (i *DentryBTree) Update(dbHandle interface{}, dentry *Dentry) error {
	i.BTree.ReplaceOrInsert(dentry, false)
	return nil
}
func (i *DentryBTree) Put(dbHandle interface{}, dentry *Dentry) error {
	i.BTree.ReplaceOrInsert(dentry, true)
	return nil
}
func (i *ExtendBTree) Update(dbHandle interface{}, extend *Extend) error {
	i.BTree.ReplaceOrInsert(extend, false)
	return nil
}
func (i *ExtendBTree) Put(dbHandle interface{}, extend *Extend) error {
	i.BTree.ReplaceOrInsert(extend, true)
	return nil
}
func (i *MultipartBTree) Update(dbHandle interface{}, multipart *Multipart) error {
	i.BTree.ReplaceOrInsert(multipart, false)
	return nil
}
func (i *MultipartBTree) Put(dbHandle interface{}, multipart *Multipart) error {
	i.BTree.ReplaceOrInsert(multipart, true)
	return nil
}

func (i *TransactionBTree) Update(dbHandle interface{}, tx *proto.TransactionInfo) error {
	i.BTree.ReplaceOrInsert(tx, false)
	return nil
}

func (i *TransactionBTree) Put(dbHandle interface{}, tx *proto.TransactionInfo) error {
	i.BTree.ReplaceOrInsert(tx, true)
	return nil
}

func (i *DeletedExtentsBTree) Put(dbHandle interface{}, dek *DeletedExtentKey) error {
	i.BTree.ReplaceOrInsert(dek, true)
	return nil
}

func (i *DeletedObjExtentsBTree) Put(dbHandle interface{}, doek *DeletedObjExtentKey) error {
	i.BTree.ReplaceOrInsert(doek, true)
	return nil
}

func (i *TransactionRollbackInodeBTree) Update(dbHandle interface{}, inode *TxRollbackInode) error {
	i.BTree.ReplaceOrInsert(inode, false)
	return nil
}

func (i *TransactionRollbackInodeBTree) Put(dbHandle interface{}, inode *TxRollbackInode) error {
	i.BTree.ReplaceOrInsert(inode, true)
	return nil
}

func (i *TransactionRollbackDentryBTree) Update(dbHandle interface{}, dentry *TxRollbackDentry) error {
	i.BTree.ReplaceOrInsert(dentry, false)
	return nil
}

func (i *TransactionRollbackDentryBTree) Put(dbHandle interface{}, dentry *TxRollbackDentry) error {
	i.BTree.ReplaceOrInsert(dentry, true)
	return nil
}

// create
func (i *InodeBTree) Create(dbHandle interface{}, inode *Inode, replace bool) (*Inode, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(inode, replace)
	if !ok {
		return item.(*Inode), ok, nil
	}
	return inode, ok, nil
}
func (i *DentryBTree) Create(dbHandle interface{}, dentry *Dentry, replace bool) (*Dentry, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(dentry, replace)
	if !ok {
		return item.(*Dentry), ok, nil
	}
	return dentry, ok, nil
}
func (i *ExtendBTree) Create(dbHandle interface{}, extend *Extend, replace bool) (*Extend, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(extend, replace)
	if !ok {
		return item.(*Extend), ok, nil
	}
	return extend, ok, nil
}
func (i *MultipartBTree) Create(dbHandle interface{}, mul *Multipart, replace bool) (*Multipart, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(mul, replace)
	if !ok {
		return item.(*Multipart), ok, nil
	}
	return mul, ok, nil
}
func (i *TransactionBTree) Create(dbHandle interface{}, tx *proto.TransactionInfo, replace bool) (*proto.TransactionInfo, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(tx, replace)
	if !ok {
		return item.(*proto.TransactionInfo), ok, nil
	}
	return tx, ok, nil
}

func (i *TransactionRollbackInodeBTree) Create(dbHandle interface{}, inode *TxRollbackInode, replace bool) (*TxRollbackInode, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(inode, replace)
	if !ok {
		return item.(*TxRollbackInode), ok, nil
	}
	return inode, ok, nil
}

func (i *TransactionRollbackDentryBTree) Create(dbHandle interface{}, dentry *TxRollbackDentry, replace bool) (*TxRollbackDentry, bool, error) {
	item, ok := i.BTree.ReplaceOrInsert(dentry, replace)
	if !ok {
		return item.(*TxRollbackDentry), ok, nil
	}
	return dentry, ok, nil
}

func (i *InodeBTree) Delete(dbHandle interface{}, ino uint64) (bool, error) {
	if v := i.BTree.Delete(&Inode{Inode: ino}); v == nil {
		return false, nil
	}
	return true, nil
}
func (i *DentryBTree) Delete(dbHandle interface{}, pid uint64, name string) (bool, error) {
	if v := i.BTree.Delete(&Dentry{ParentId: pid, Name: name}); v == nil {
		return false, nil
	}
	return true, nil
}
func (i *ExtendBTree) Delete(dbHandle interface{}, ino uint64) (bool, error) {
	if v := i.BTree.Delete(&Extend{inode: ino}); v == nil {
		return false, nil
	}
	return true, nil
}
func (i *MultipartBTree) Delete(dbHandle interface{}, key, id string) (bool, error) {
	if mul := i.BTree.Delete(&Multipart{key: key, id: id}); mul == nil {
		return false, nil
	}
	return true, nil
}
func (i *TransactionBTree) Delete(dbHandle interface{}, txId string) (bool, error) {
	if tx := i.BTree.Delete(&proto.TransactionInfo{TxID: txId}); tx == nil {
		return false, nil
	}
	return true, nil
}

func (i *TransactionRollbackInodeBTree) Delete(dbHandle interface{}, inode uint64) (bool, error) {
	if inode := i.BTree.Delete(&TxRollbackInode{inode: NewInode(inode, 0)}); inode == nil {
		return false, nil
	}
	return true, nil
}

func (i *TransactionRollbackDentryBTree) Delete(dbHandle interface{}, parentInode uint64, name string) (bool, error) {
	if dentry := i.BTree.Delete(&TxRollbackDentry{txDentryInfo: proto.NewTxDentryInfo("", parentInode, name, 0)}); dentry == nil {
		return false, nil
	}
	return true, nil
}

func (i *DeletedExtentsBTree) Delete(dbHandle interface{}, dek *DeletedExtentKey) (bool, error) {
	if old := i.BTree.Delete(dek); old == nil {
		return false, nil
	}
	return true, nil
}

func (i *DeletedObjExtentsBTree) Delete(dbHandle interface{}, doek *DeletedObjExtentKey) (bool, error) {
	if old := i.BTree.Delete(doek); old == nil {
		return false, nil
	}
	return true, nil
}

// range
func (i *InodeBTree) Range(start, end *Inode, cb func(i *Inode) (bool, error)) error {
	var (
		err  error
		next bool
	)
	if start == nil {
		start = NewInode(0, 0)
	}

	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*Inode))
		if err != nil {
			return false
		}
		return next
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *DentryBTree) Range(start, end *Dentry, cb func(d *Dentry) (bool, error)) error {
	var (
		err  error
		next bool
	)
	if start == nil {
		start = &Dentry{0, "", 0, 0, nil}
	}

	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*Dentry))
		if err != nil {
			return false
		}
		return next
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *DentryBTree) RangeWithPrefix(prefix, start, end *Dentry, cb func(d *Dentry) (bool, error)) error {
	return i.Range(start, end, cb)
}

func (i *ExtendBTree) Range(start, end *Extend, cb func(e *Extend) (bool, error)) error {
	var (
		err  error
		next bool
	)
	if start == nil {
		start = &Extend{inode: 0}
	}

	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*Extend))
		if err != nil {
			return false
		}
		return next
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}

	return err
}

func (i *MultipartBTree) Range(start, end *Multipart, cb func(m *Multipart) (bool, error)) error {
	var (
		err  error
		next bool
	)
	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*Multipart))
		if err != nil {
			return false
		}
		return next
	}

	if start == nil {
		start = &Multipart{key: "", id: ""}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *MultipartBTree) RangeWithPrefix(prefix, start, end *Multipart, cb func(m *Multipart) (bool, error)) error {
	return i.Range(start, end, cb)
}

func (i *TransactionBTree) Range(start, end *proto.TransactionInfo, cb func(tx *proto.TransactionInfo) (bool, error)) error {
	var (
		err  error
		next bool
	)
	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*proto.TransactionInfo))
		if err != nil {
			return false
		}
		return next
	}
	if start == nil {
		start = &proto.TransactionInfo{TxID: ""}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *TransactionRollbackInodeBTree) Range(start, end *TxRollbackInode, cb func(inode *TxRollbackInode) (bool, error)) error {
	var (
		err  error
		next bool
	)
	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*TxRollbackInode))
		if err != nil {
			return false
		}
		return next
	}
	if start == nil {
		start = &TxRollbackInode{
			inode: NewInode(0, 0),
		}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *TransactionRollbackDentryBTree) Range(start, end *TxRollbackDentry, cb func(dentry *TxRollbackDentry) (bool, error)) error {
	var (
		err  error
		next bool
	)
	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*TxRollbackDentry))
		if err != nil {
			return false
		}
		return next
	}

	if start == nil {
		start = &TxRollbackDentry{
			txDentryInfo: proto.NewTxDentryInfo("", 0, "", 0),
		}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *TransactionRollbackDentryBTree) RangeWithPrefix(prefix, start, end *TxRollbackDentry, cb func(dentry *TxRollbackDentry) (bool, error)) error {
	return i.Range(start, end, cb)
}

func (i *DeletedExtentsBTree) Range(start, end *DeletedExtentKey, cb func(dek *DeletedExtentKey) (bool, error)) error {
	var (
		err  error
		next bool
	)
	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*DeletedExtentKey))
		if err != nil {
			return false
		}
		return next
	}
	if start == nil {
		start = &DeletedExtentKey{}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *DeletedExtentsBTree) RangeWithPrefix(prefix, start, end *DeletedExtentKey, cb func(dek *DeletedExtentKey) (bool, error)) error {
	return i.Range(start, end, cb)
}

func (i *DeletedObjExtentsBTree) Range(start, end *DeletedObjExtentKey, cb func(doek *DeletedObjExtentKey) (bool, error)) error {
	var (
		err  error
		next bool
	)
	callback := func(i BtreeItem) bool {
		next, err = cb(i.(*DeletedObjExtentKey))
		if err != nil {
			return false
		}
		return next
	}
	if start == nil {
		start = &DeletedObjExtentKey{}
	}

	if end == nil {
		i.BTree.AscendGreaterOrEqual(start, callback)
	} else {
		i.BTree.AscendRange(start, end, callback)
	}
	return err
}

func (i *DeletedObjExtentsBTree) RangeWithPrefix(prefix, start, end *DeletedObjExtentKey, cb func(dek *DeletedObjExtentKey) (bool, error)) error {
	return i.Range(start, end, cb)
}

// MaxItem returns the largest item in the btree.
func (i *InodeBTree) MaxItem() *Inode {
	i.RLock()
	item := i.tree.Max()
	i.RUnlock()
	if item == nil {
		return nil
	}
	return item.(*Inode)
}

// BTree is the wrapper of Google's btree.
type BTree struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewBtree creates a new btree.
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// Get returns the object of the given key in the btree.
func (b *BTree) Get(key BtreeItem) (item BtreeItem) {
	b.RLock()
	item = b.tree.Get(key)
	b.RUnlock()
	return
}

func (b *BTree) CopyGet(key BtreeItem) (item BtreeItem) {
	b.Lock()
	item = b.tree.CopyGet(key)
	b.Unlock()
	return
}

// Find searches for the given key in the btree.
func (b *BTree) Find(key BtreeItem, fn func(i BtreeItem)) {
	b.RLock()
	item := b.tree.Get(key)
	b.RUnlock()
	if item == nil {
		return
	}
	fn(item)
}

func (b *BTree) CopyFind(key BtreeItem, fn func(i BtreeItem)) {
	b.Lock()
	item := b.tree.CopyGet(key)
	fn(item)
	b.Unlock()
}

// Has checks if the key exists in the btree.
func (b *BTree) Has(key BtreeItem) (ok bool) {
	b.RLock()
	ok = b.tree.Has(key)
	b.RUnlock()
	return
}

// Delete deletes the object by the given key.
func (b *BTree) Delete(key BtreeItem) (item BtreeItem) {
	b.Lock()
	item = b.tree.Delete(key)
	b.Unlock()
	return
}

func (b *BTree) Execute(fn func(tree interface{}) interface{}) interface{} {
	b.Lock()
	defer b.Unlock()
	return fn(b)
}

// ReplaceOrInsert is the wrapper of google's btree ReplaceOrInsert.
func (b *BTree) ReplaceOrInsert(key BtreeItem, replace bool) (item BtreeItem, ok bool) {
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

// Ascend is the wrapper of the google's btree Ascend.
// This function scans the entire btree. When the data is huge, it is not recommended to use this function online.
// Instead, it is recommended to call GetTree to obtain the snapshot of the current btree, and then do the scan on the snapshot.
func (b *BTree) Ascend(fn func(i BtreeItem) bool) {
	b.RLock()
	b.tree.Ascend(fn)
	b.RUnlock()
}

// AscendRange is the wrapper of the google's btree AscendRange.
func (b *BTree) AscendRange(greaterOrEqual, lessThan BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendRange(greaterOrEqual, lessThan, iterator)
	b.RUnlock()
}

// AscendGreaterOrEqual is the wrapper of the google's btree AscendGreaterOrEqual
func (b *BTree) AscendGreaterOrEqual(pivot BtreeItem, iterator func(i BtreeItem) bool) {
	b.RLock()
	b.tree.AscendGreaterOrEqual(pivot, iterator)
	b.RUnlock()
}

// GetTree returns the snapshot of a btree.
func (b *BTree) GetTree() *BTree {
	b.Lock()
	t := b.tree.Clone()
	b.Unlock()
	nb := NewBtree()
	nb.tree = t
	return nb
}

// Reset resets the current btree.
func (b *BTree) Reset() {
	b.Lock()
	b.tree.Clear(true)
	b.Unlock()
}

func (i *BTree) Release() {
	i.Reset()
}

func (i *BTree) SetApplyID(index uint64) {
}

func (i *BTree) GetApplyID() uint64 {
	return 0
}

func (i *BTree) GetPersistentApplyID() uint64 {
	return 0
}

func (i *BTree) SetTxId(txId uint64) {
}

func (i *BTree) GetTxId() uint64 {
	return 0
}

func (i *BTree) GetDeletedExtentId() uint64 {
	return 0
}

func (i *BTree) SetDeletedExtentId(id uint64) {
}

func (i *BTree) PersistBaseInfo() error {
	return nil
}

func (i *BTree) CreateBatchWriteHandle() (interface{}, error) {
	return i, nil
}

func (i *BTree) CommitBatchWrite(handle interface{}, needCommitApplyID bool) error {
	return nil
}

func (i *BTree) ReleaseBatchWriteHandle(handle interface{}) error {
	return nil
}

func (i *BTree) BatchWriteCount(handle interface{}) (int, error) {
	return 0, nil
}

func (i *BTree) CommitAndReleaseBatchWriteHandle(handle interface{}, needCommitApplyID bool) error {
	return nil
}

func (i *BTree) ClearBatchWriteHandle(handle interface{}) error {
	return nil
}

func (i *BTree) SetCursor(cursor uint64) {
	return
}

func (i *BTree) GetCursor() uint64 {
	return 0
}

func (i *BTree) Flush() error {
	return nil
}

func (i *BTree) Count() uint64 {
	return uint64(i.Len())
}

// real count by type
func (i *BTree) RealCount() uint64 {
	return uint64(i.Len())
}

// Len returns the total number of items in the btree.
func (b *BTree) Len() (size int) {
	b.RLock()
	size = b.tree.Len()
	b.RUnlock()
	return
}
