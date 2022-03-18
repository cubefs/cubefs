package metanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/tecbot/gorocksdb"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/chubaofs/chubaofs/util/log"
)

type RocksBaseInfo struct {
	version      uint32
	length       uint32
	applyId      uint64
	inodeCnt     uint64
	dentryCnt    uint64
	extendCnt    uint64
	multiCnt     uint64
	delDentryCnt uint64
	delInodeCnt  uint64
}

func (info *RocksBaseInfo) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint32(&info.version)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint32(&info.length)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.applyId)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.inodeCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.dentryCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.extendCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.multiCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.delDentryCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.delInodeCnt)); err != nil {
		panic(err)
	}
	return buff.Bytes(), nil
}

// Unmarshal unmarshals the inode.
func (info *RocksBaseInfo) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &info.version); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.length); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.applyId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.inodeCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.dentryCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.extendCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.multiCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.delDentryCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.delInodeCnt); err != nil {
		return
	}
	return
}

type RocksTree struct {
	db             *RocksDbInfo
	currentApplyID uint64
	latch          [8]sync.Mutex
	baseInfo       RocksBaseInfo
}

func DefaultRocksTree(dbInfo *RocksDbInfo) (*RocksTree, error) {
	return NewRocksTree(dbInfo)
}

func NewRocksTree(dbInfo *RocksDbInfo) (*RocksTree, error) {
	if dbInfo == nil {
		return nil, errors.NewErrorf("dbInfo is null")
	}
	tree := &RocksTree{db: dbInfo}
	_ = tree.LoadBaseInfo()
	if tree.baseInfo.length == 0 {
		tree.baseInfo.length = uint32(unsafe.Sizeof(tree.baseInfo))
	}
	return tree, nil
}

func (r *RocksTree) LoadBaseInfo() error {
	buff, err := r.GetBytes(baseInfoKey)
	if err != nil {
		return err
	}
	return r.baseInfo.Unmarshal(buff)
}

func (r *RocksTree) SetApplyID(id uint64) {
	atomic.StoreUint64(&r.baseInfo.applyId, id)
}

func (r *RocksTree) GetApplyID() uint64 {
	return atomic.LoadUint64(&r.baseInfo.applyId)
}

func (r *RocksTree) PersistBaseInfo() error {
	var handle interface{}
	buffBaseInfo, err := r.baseInfo.Marshal()
	if err != nil {
		return err
	}

	if handle, err = r.db.CreateBatchHandler(); err != nil {
		return err
	}
	defer func() {
		_ = r.db.ReleaseBatchHandle(handle)
	}()

	if err = r.db.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil{
		return err
	}

	if err = r.db.CommitBatch(handle); err != nil {
		log.LogErrorf("action[UpdateBaseInfo] commit batch err:%v", err)
		return err
	}
	return nil
}

func (r *RocksTree) Flush() error {
	return r.db.Flush()
}

func (r *RocksTree) Count(tp TreeType) (uint64, error) {
	baseInfoBytes, err := r.GetBytes(baseInfoKey)
	if err != nil {
		err = fmt.Errorf("load base info from rocksdb err:[%s]", err.Error())
		log.LogErrorf(err.Error())
		return 0, err
	}

	if len(baseInfoBytes) == 0 {
		return 0, nil
	}

	var baseInfo RocksBaseInfo
	if err = baseInfo.Unmarshal(baseInfoBytes); err != nil {
		err = fmt.Errorf("unmarsh base info bytes err:[%s]", err.Error())
		log.LogErrorf(err.Error())
		return 0, err
	}
	switch tp {
	case InodeType:
		return baseInfo.inodeCnt, nil
	case DentryType:
		return baseInfo.dentryCnt, nil
	case ExtendType:
		return baseInfo.extendCnt, nil
	case MultipartType:
		return baseInfo.multiCnt, nil
	case DelDentryType:
		return baseInfo.delDentryCnt, nil
	case DelInodeType:
		return baseInfo.delInodeCnt, nil
	default:
		return 0, fmt.Errorf("error tree type:%v", tp)
	}
}

// This requires global traversal to call carefully
func (r *RocksTree) IteratorCount(tableType TableType) uint64 {
	start, end := []byte{byte(tableType)}, byte(tableType)+1
	var count uint64
	snapshot := r.db.OpenSnap()
	if snapshot == nil {
		log.LogErrorf("IteratorCount openSnap failed.")
		return 0
	}
	it := r.db.iterator(snapshot)
	defer func() {
		it.Close()
		r.db.ReleaseSnap(snapshot)
	}()
	it.Seek(start)
	for ; it.ValidForPrefix(start); it.Next() {
		key := it.Key().Data()
		if key[0] >= end {
			break
		}
		count += 1
	}
	return count
}

func (r *RocksTree) Range(start, end []byte, cb func(v []byte) (bool, error)) error {
	snapshot := r.db.OpenSnap()
	if snapshot == nil {
		return errors.NewErrorf("open snap failed")
	}
	defer r.db.ReleaseSnap(snapshot)
	callbackFunc := func(k, v []byte) (bool, error) {
		return cb(v)
	}
	return r.db.RangeWithSnap(start, end, snapshot, callbackFunc)
}

func (r *RocksTree) HasKey(key []byte) (bool, error) {
	bs, err := r.GetBytes(key)
	if err != nil {
		return false, err
	}
	return len(bs) > 0, nil
}

func (r *RocksTree) GetBytes(key []byte) ([]byte, error) {
	return r.db.GetBytes(key)
}

func (r *RocksTree) Put(count *uint64, key []byte, value []byte) error {
	var buffBaseInfo []byte
	var handle interface{}
	lock := r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	has, err := r.HasKey(key)
	if err != nil {
		return err
	}
	if !has {
		atomic.AddUint64(count, 1)
	}

	buffBaseInfo, err = r.baseInfo.Marshal()
	if err != nil {
		if !has {
			atomic.AddUint64(count, ^uint64(0))
		}
		return err
	}

	if handle, err = r.db.CreateBatchHandler(); err != nil {
		return err
	}
	defer func() {
		_ = r.db.ReleaseBatchHandle(handle)
	}()

	if err = r.db.AddItemToBatch(handle, key, value); err != nil{
		return err
	}

	if err = r.db.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil{
		return err
	}

	if err = r.db.CommitBatch(handle); err != nil {
		log.LogErrorf("action[rocksdb Put] err:%v", err)
		if !has {
			atomic.AddUint64(count, ^uint64(0))
		}
		return err
	}
	return nil
}

func (r *RocksTree) Update(key []byte, value []byte) (err error) {
	var handle interface{}
	lock := r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	if handle, err = r.db.CreateBatchHandler(); err != nil {
		return
	}
	defer func() {
		_ = r.db.ReleaseBatchHandle(handle)
	}()

	if err = r.db.AddItemToBatch(handle, key, value); err != nil{
		return
	}

	if err = r.db.CommitBatch(handle); err != nil {
		log.LogErrorf("action[rocksdb Update] error:%v", err)
		return
	}
	return
}

func (r *RocksTree) Create(count *uint64, key []byte, value []byte, force bool) (ok bool, err error) {
	var (
		buffBaseInfo []byte
		handle       interface{}
		has          bool
	)

	lock := r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	has, err = r.HasKey(key)
	if err != nil {
		return
	}

	if has && !force {
		return
	}

	if !has {
		atomic.AddUint64(count, 1)
	}

	buffBaseInfo, err = r.baseInfo.Marshal()
	if err != nil {
		if !has {
			atomic.AddUint64(count, ^uint64(0))
		}
		return
	}

	if handle, err = r.db.CreateBatchHandler(); err != nil {
		return
	}
	defer func() {
		_ = r.db.ReleaseBatchHandle(handle)
	}()

	if err = r.db.AddItemToBatch(handle, key, value); err != nil{
		return
	}

	if err = r.db.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil{
		return
	}

	if err = r.db.CommitBatch(handle); err != nil {
		log.LogErrorf("action[rocksdb Create] error:%v", err)
		atomic.AddUint64(count, ^uint64(0))
		return
	}
	ok = true
	return
}

// Has checks if the key exists in the btree. return is exist and err
func (r *RocksTree) Delete(count *uint64, key []byte) (ok bool, err error) {
	var (
		buffBaseInfo []byte
		handle interface{}
		has bool
	)
	lock := r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	has, err = r.HasKey(key)
	if err != nil {
		return
	}
	if !has {
		return
	}

	atomic.AddUint64(count, ^uint64(0))
	buffBaseInfo, err = r.baseInfo.Marshal()
	if err != nil {
		atomic.AddUint64(count, 1)
		return
	}

	if handle, err = r.db.CreateBatchHandler(); err != nil {
		return
	}
	defer func() {
		_ = r.db.ReleaseBatchHandle(handle)
	}()

	if err = r.db.DelItemToBatch(handle, key); err != nil{
		return
	}

	if err = r.db.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil{
		return
	}

	if err = r.db.CommitBatch(handle); err != nil {
		log.LogErrorf("action[rocksdb Delete] error:%v", err)
		if has{
			atomic.AddUint64(count, 1)
		}
		return
	}
	ok = true
	return
}

// drop the current btree.
func (r *RocksTree) Release() {
	//do nothing, db will be closed in the outside
}

// todo:execute unuse, so remove?
func (r *RocksTree) Execute(fn func(tree interface{}) interface{}) interface{} {
	if err := r.db.accessDb(); err != nil {
		return nil
	}
	defer r.db.releaseDb()
	return fn(r)
}

func (r *RocksTree) rocksClear(count *uint64, tableType TableType) error {
	snapshot := r.db.OpenSnap()
	if snapshot == nil {
		return errors.NewErrorf("open rocks tree snapshot failed")
	}
	it := r.db.iterator(snapshot)
	defer func() {
		it.Close()
		r.db.ReleaseSnap(snapshot)
	}()

	start, end := []byte{byte(tableType)}, byte(tableType)+1
	it.Seek(start)
	batchDelHandle, err := r.db.CreateBatchHandler()
	if err != nil {
		return err
	}
	defer func() {
		_ = r.db.ReleaseBatchHandle(batchDelHandle)
	}()

	cnt := 0
	for ; it.ValidForPrefix(start); it.Next() {
		key := it.Key().Data()
		if key[0] >= end {
			break
		}

		_ = r.db.DelItemToBatch(batchDelHandle, key)
		cnt++
		if cnt == 10000 {
			atomic.AddUint64(count, ^uint64(cnt - 1))
			if err = r.db.CommitBatch(batchDelHandle); err != nil {
				return err
			}
			_ = r.db.ReleaseBatchHandle(batchDelHandle)
			cnt = 0
			batchDelHandle, err = r.db.CreateBatchHandler()
			if err != nil {
				return err
			}
		}
	}
	atomic.AddUint64(count, ^uint64(cnt - 1))
	var buffBaseInfo []byte
	if buffBaseInfo, err = r.baseInfo.Marshal(); err != nil {
		return err
	}
	_ = r.db.AddItemToBatch(batchDelHandle, baseInfoKey, buffBaseInfo)
	if err = r.db.CommitBatch(batchDelHandle); err != nil {
		return err
	}

	if atomic.LoadUint64(count) != 0 {
		log.LogErrorf("clean type:[%v] count:[%d] not zero", tableType, count)
	}
	return r.db.Flush()
}

var _ InodeTree = &InodeRocks{}
var _ DentryTree = &DentryRocks{}
var _ ExtendTree = &ExtendRocks{}
var _ MultipartTree = &MultipartRocks{}
var _ DeletedDentryTree = &DeletedDentryRocks{}
var _ DeletedInodeTree = &DeletedInodeRocks{}

func NewInodeRocks(tree *RocksTree) (*InodeRocks, error) {
	return &InodeRocks{
		RocksTree: tree,
		count:     tree.baseInfo.inodeCnt,
	}, nil
}

type InodeRocks struct {
	*RocksTree
	count uint64
}

func NewDentryRocks(tree *RocksTree) (*DentryRocks, error) {
	return &DentryRocks{
		RocksTree: tree,
		count:     tree.baseInfo.dentryCnt,
	}, nil
}

type DentryRocks struct {
	*RocksTree
	count uint64
}

func NewExtendRocks(tree *RocksTree) (*ExtendRocks, error) {
	return &ExtendRocks{
		RocksTree: tree,
		count:     tree.baseInfo.extendCnt,
	}, nil
}

type ExtendRocks struct {
	*RocksTree
	count uint64
}

func NewMultipartRocks(tree *RocksTree) (*MultipartRocks, error) {
	return &MultipartRocks{
		RocksTree: tree,
		count:     tree.baseInfo.multiCnt,
	}, nil
}

type MultipartRocks struct {
	*RocksTree
	count uint64
}

func NewDeletedDentryRocks(tree *RocksTree) (*DeletedDentryRocks, error) {
	return &DeletedDentryRocks{
		RocksTree: tree,
		count:     tree.baseInfo.delDentryCnt,
	}, nil
}

type DeletedDentryRocks struct {
	*RocksTree
	count uint64
}

func NewDeletedInodeRocks(tree *RocksTree) (*DeletedInodeRocks, error) {
	return &DeletedInodeRocks{
		RocksTree: tree,
		count:     tree.baseInfo.delInodeCnt,
	}, nil
}

type DeletedInodeRocks struct {
	*RocksTree
	count uint64
}

func inodeEncodingKey(ino uint64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(InodeTable))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func dentryEncodingKey(parentId uint64, name string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	buff.WriteString(name)
	return buff.Bytes()
}

func extendEncodingKey(ino uint64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(ExtendTable))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func multipartEncodingKey(key string, id string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(MultipartTable))
	buff.WriteString(key)
	buff.WriteByte(0)
	buff.WriteString(id)
	return buff.Bytes()
}

func deletedDentryEncodingKey(parentId uint64, name string, timeStamp int64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DelDentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	buff.WriteString(name)
	buff.WriteByte(0)
	_ = binary.Write(buff, binary.BigEndian, timeStamp)
	return buff.Bytes()
}

func deletedInodeEncodingKey(ino uint64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DelInodeTable))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func (b *InodeRocks) GetMaxInode() (uint64, error) {
	snapshot := b.RocksTree.db.OpenSnap()
	if snapshot == nil {
		return 0, errors.NewErrorf("open snapshot failed")
	}
	defer b.RocksTree.db.ReleaseSnap(snapshot)
	iterator := b.RocksTree.db.iterator(snapshot)
	defer iterator.Close()
	var maxInode uint64 = 0
	err := b.db.descRangeWithIter(iterator, []byte{byte(InodeTable)}, []byte{byte(InodeTable) + 1}, func(k, v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if e := inode.Unmarshal(context.Background(), v); e != nil {
			return false, e
		}
		maxInode = inode.Inode
		return false, nil
	})
	if err != nil {
		return 0, err
	}
	return maxInode, nil
}

//clear
func (b *InodeRocks) Clear() error {
	return b.RocksTree.rocksClear(&b.baseInfo.inodeCnt, InodeTable)
}
func (b *DentryRocks) Clear() error {
	return b.RocksTree.rocksClear(&b.baseInfo.dentryCnt, DentryTable)
}
func (b *ExtendRocks) Clear() error {
	return b.RocksTree.rocksClear(&b.baseInfo.extendCnt, ExtendTable)
}
func (b *MultipartRocks) Clear() error {
	return b.RocksTree.rocksClear(&b.baseInfo.multiCnt, MultipartTable)
}
func (b *DeletedDentryRocks) Clear() error {
	return b.RocksTree.rocksClear(&b.baseInfo.delDentryCnt, DelDentryTable)
}
func (b *DeletedInodeRocks) Clear() error {
	return b.RocksTree.rocksClear(&b.baseInfo.delInodeCnt, DelInodeTable)
}

// count by type
func (b *InodeRocks) Count() uint64 {
	return b.baseInfo.inodeCnt
}

func (b *DentryRocks) Count() uint64 {
	return b.baseInfo.dentryCnt
}

func (b *ExtendRocks) Count() uint64 {
	return b.baseInfo.extendCnt
}

func (b *MultipartRocks) Count() uint64 {
	return b.baseInfo.multiCnt
}

func (b *DeletedDentryRocks) Count() uint64 {
	return b.baseInfo.delDentryCnt
}

func (b *DeletedInodeRocks) Count() uint64 {
	return b.baseInfo.delInodeCnt
}

// real count by type
func (b *InodeRocks) RealCount() uint64 {
	return b.IteratorCount(InodeTable)
}

func (b *DentryRocks) RealCount() uint64 {
	return b.IteratorCount(DentryTable)
}

func (b *ExtendRocks) RealCount() uint64 {
	return b.IteratorCount(ExtendTable)
}

func (b *MultipartRocks) RealCount() uint64 {
	return b.IteratorCount(MultipartTable)
}

func (b *DeletedDentryRocks) RealCount() uint64 {
	return b.IteratorCount(DelDentryTable)
}

func (b *DeletedInodeRocks) RealCount() uint64 {
	return b.IteratorCount(DelInodeTable)
}

//Get
func (b *InodeRocks) RefGet(ino uint64) (*Inode, error) {
	return b.Get(ino)
}
func (b *InodeRocks) Get(ino uint64) (inode *Inode, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[InodeRocks] Get failed, inode:%v err:%v", ino, err)
			err = rocksdbError
		}
	}()

	var bs []byte
	bs, err = b.RocksTree.GetBytes(inodeEncodingKey(ino))
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	inode = NewInode(ino, 0)
	if err = inode.Unmarshal(context.Background(), bs); err != nil {
		log.LogErrorf("[InodeRocks] unmarshal value error : %v", err)
	}
	return
}

func (b *DentryRocks) RefGet(ino uint64, name string) (*Dentry, error) {
	return b.Get(ino, name)
}
func (b *DentryRocks) Get(ino uint64, name string) (dentry *Dentry, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[DentryRocks] Get failed, parentId: %v, name: %v, error: %v", ino, name, err)
			err = rocksdbError
		}
	}()

	var bs, key []byte
	key = dentryEncodingKey(ino, name)
	bs, err = b.RocksTree.GetBytes(key)
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	dentry = &Dentry{}
	if err = dentry.Unmarshal(bs); err != nil {
		log.LogErrorf("[DentryRocks] unmarshal value error : %v", err)
	}
	return
}

func (b *ExtendRocks) RefGet(ino uint64) (*Extend, error) {
	return b.Get(ino)
}
func (b *ExtendRocks) Get(ino uint64) (extend *Extend, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[ExtendRocks] Get failed, inode %v, error: %v", ino, err)
			err = rocksdbError
		}
	}()

	var bs, key []byte
	key = extendEncodingKey(ino)
	bs, err = b.RocksTree.GetBytes(key)
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	if extend, err = NewExtendFromBytes(bs); err != nil {
		log.LogErrorf("[ExtendRocks] unmarshal failed, error: %v", err)
	}

	return
}

func (b *MultipartRocks) RefGet(key, id string) (*Multipart, error) {
	return b.Get(key, id)
}
func (b *MultipartRocks) Get(key, id string) (multipart *Multipart, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[MultipartRocks] Get failed, key: %v, id: %v, error: %v", key, id, err)
			err = rocksdbError
		}
	}()

	var bs, encodingKey []byte
	encodingKey = multipartEncodingKey(key, id)
	bs, err = b.RocksTree.GetBytes(encodingKey)
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	multipart = MultipartFromBytes(bs)
	return
}

func(b *DeletedDentryRocks) RefGet(pino uint64, name string, timeStamp int64) (*DeletedDentry, error) {
	return b.Get(pino, name, timeStamp)
}
func(b *DeletedDentryRocks) Get(pino uint64, name string, timeStamp int64) (dd *DeletedDentry, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[DeletedDentryRocks] Get failed, pino: %v, name: %v, timeStamp: %v, error: %v", pino, name, timeStamp, err)
			err = rocksdbError
		}
	}()

	var bs, key []byte
	key = deletedDentryEncodingKey(pino, name, timeStamp)
	bs, err = b.RocksTree.GetBytes(key)
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	dd = new(DeletedDentry)
	if err = dd.Unmarshal(bs); err != nil {
		log.LogErrorf("[DeletedDentryRocks] unmarshal error: %v", err)
	}
	return
}

//delete inode
func (b *DeletedInodeRocks) RefGet(ino uint64) (*DeletedINode, error) {
	return b.Get(ino)
}
func (b *DeletedInodeRocks) Get(ino uint64) (di *DeletedINode, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("[DeletedInodeRocks] Get failed, ino: %v, error: %v", ino, err)
			err = rocksdbError
		}
	}()

	var bs, key []byte
	key = deletedInodeEncodingKey(ino)
	bs, err = b.RocksTree.GetBytes(key)
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return
	}
	di = new(DeletedINode)
	if err = di.Unmarshal(context.Background(), bs); err != nil {
		log.LogErrorf("[DeletedInodeRocks] unmarshal error: %v", err)
	}
	return
}

//put inode into rocksdb
func (b *InodeRocks) Put(inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.Put(&b.baseInfo.inodeCnt, inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks put failed, inode:%v, error:%v", inode, err)
		err = rocksdbError
	}
	return
}

func (b *DentryRocks) Put(dentry *Dentry) (err error) {
	var bs []byte
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("DentryRocks dentry marshal failed, dentry:%v, error:%v", dentry, err)
		return
	}
	if err = b.RocksTree.Put(&b.baseInfo.dentryCnt, dentryEncodingKey(dentry.ParentId, dentry.Name), bs); err != nil {
		log.LogErrorf("DentryRocks put failed, dentry:%v, error:%v", dentry, err)
		err = rocksdbError
	}
	return
}

func (b *ExtendRocks) Put(extend *Extend) (err error) {
	var bs []byte
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("ExtendRocks extend marshal failed, extend:%v, error:%v", extend, err)
		return
	}
	if err = b.RocksTree.Put(&b.baseInfo.extendCnt, extendEncodingKey(extend.inode), bs); err != nil {
		log.LogErrorf("ExtendRocks extend put failed, extend:%v, error:%v", extend, err)
		err = rocksdbError
	}
	return
}

func (b *MultipartRocks) Put(multipart *Multipart) (err error) {
	var bs []byte
	bs, err = multipart.Bytes()
	if err != nil {
		log.LogErrorf("MultipartRocks multipart marshal failed, multipart:%v, error:%v", multipart, err)
		return
	}
	if err = b.RocksTree.Put(&b.baseInfo.multiCnt, multipartEncodingKey(multipart.key, multipart.id), bs); err != nil {
		log.LogErrorf("MultipartRocks multipart put failed, multipart:%v, error:%v", multipart, err)
		err = rocksdbError
	}
	return
}

//update
func (b *InodeRocks) Update(inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.Update(inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks inode update failed, inode:%v, error:%v", inode, err)
		err = rocksdbError
	}
	return
}

func (b *ExtendRocks) Update(extend *Extend) (err error) {
	var bs []byte
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("ExtendRocks extend marshal failed, extend:%v, error:%v", extend, err)
		return
	}
	if err = b.RocksTree.Update(extendEncodingKey(extend.inode), bs); err != nil {
		log.LogErrorf("ExtendRocks extend update failed, extend:%v, error:%v", extend, err)
		err = rocksdbError
	}
	return
}

func (b *MultipartRocks) Update(multipart *Multipart) (err error) {
	var bs []byte
	bs, err = multipart.Bytes()
	if err != nil {
		log.LogErrorf("MultipartRocks multipart marshal failed, multipart:%v, error:%v", multipart, err)
		return
	}
	if err = b.RocksTree.Update(multipartEncodingKey(multipart.key, multipart.id), bs); err != nil {
		log.LogErrorf("MultipartRocks multipart put failed, multipart:%v, error:%v", multipart, err)
		err = rocksdbError
	}
	return
}

//Create if exists , return old, false,   if not  return nil , true
func (b *InodeRocks) Create(inode *Inode, replace bool) (err error) {
	var (
		key, bs []byte
		ok      bool
	)
	key = inodeEncodingKey(inode.Inode)
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] haskey error %v, %v", key, err)
		return
	}

	ok, err = b.RocksTree.Create(&b.baseInfo.inodeCnt,  key, bs, replace)
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] inodeRocks error %v, %v", key, err)
		err = rocksdbError
		return
	}

	if !ok {
		err = existsError
	}
	return
}

func (b *DentryRocks) Create(dentry *Dentry, replace bool) (err error) {
	var (
		key, bs []byte
		ok      bool
	)
	key = dentryEncodingKey(dentry.ParentId, dentry.Name)

	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocks] marshal: %v, err: %v", dentry, err)
		return
	}

	ok, err = b.RocksTree.Create(&b.baseInfo.dentryCnt,  key, bs, replace)
	if err != nil {
		log.LogErrorf("[DentryRocks] Create dentry: %v key: %v, err: %v", dentry, key, err)
		err = rocksdbError
		return
	}

	if !ok {
		err = existsError
	}
	return
}

func (b *ExtendRocks) Create(ext *Extend, replace bool) (err error) {
	var (
		key, bs []byte
		ok      bool
	)
	key = extendEncodingKey(ext.inode)
	bs, err = ext.Bytes()
	if err != nil {
		log.LogErrorf("[ExtendRocks] marshal: %v, err: %v", ext, err)
		return
	}

	ok, err = b.RocksTree.Create(&b.baseInfo.extendCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[ExtendRocks] Create extend: %v key: %v, err: %v", ext, key, err)
		err = rocksdbError
		return
	}
	if !ok {
		err = existsError
	}
	return
}

func (b *MultipartRocks) Create(mul *Multipart, replace bool) (err error) {
	var (
		key, bs []byte
		ok      bool
	)
	key = multipartEncodingKey(mul.key, mul.id)
	bs, err = mul.Bytes()
	if err != nil {
		log.LogErrorf("[MultipartRocks] marshal: %v, err: %v", mul, err)
		return
	}

	ok, err = b.RocksTree.Create(&b.baseInfo.multiCnt, multipartEncodingKey(mul.key, mul.id), bs, replace)
	if err != nil {
		log.LogErrorf("[MultipartRocks] Create multipart: %v key: %v, err: %v", mul, key, err)
		err = rocksdbError
		return
	}
	if !ok {
		err = existsError
	}
	return
}

func (b *DeletedDentryRocks) Create(delDentry *DeletedDentry, replace bool) (err error) {
	var (
		key, bs []byte
		ok      bool
	)
	key = deletedDentryEncodingKey(delDentry.ParentId, delDentry.Name, delDentry.Timestamp)
	bs, err = delDentry.Marshal()
	if err != nil {
		log.LogErrorf("[DeletedDentryRocks] marshal: %v, err: %v", delDentry, err)
		return
	}

	ok, err = b.RocksTree.Create(&b.baseInfo.delDentryCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[DeletedDentryRocks] Create deleted dentry: %v key: %v, err: %v", delDentry, key, err)
		err = rocksdbError
		return
	}
	if !ok {
		err = existsError
	}
	return
}

func (b *DeletedInodeRocks) Create(delIno *DeletedINode, replace bool) (err error) {
	var (
		key, bs []byte
		ok      bool
	)
	key = deletedInodeEncodingKey(delIno.Inode.Inode)
	bs, err = delIno.Marshal()
	if err != nil {
		log.LogErrorf("[DeletedInodeRocks] marshal: %v, err: %v", delIno, err)
		return
	}

	ok, err = b.RocksTree.Create(&b.baseInfo.delInodeCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[DeletedInodeRocks] Create deleted inode: %v key: %v, err: %v", delIno, key, err)
		err = rocksdbError
		return
	}
	if !ok {
		err = existsError
	}
	return
}

//Delete
func (b *InodeRocks) Delete(ino uint64) (bool, error) {
	ok, err := b.RocksTree.Delete(&b.baseInfo.inodeCnt, inodeEncodingKey(ino))
	if err != nil {
		log.LogErrorf("InodeRocks Delete failed, inode:%v, error:%v", ino, err)
		return false, rocksdbError
	}
	if !ok {
		return false, notExistsError
	}
	return true, nil
}
func (b *DentryRocks) Delete(pid uint64, name string) (bool, error) {
	ok, err := b.RocksTree.Delete(&b.baseInfo.dentryCnt, dentryEncodingKey(pid, name))
	if err != nil {
		log.LogErrorf("DentryRocks Delete failed, dentry pid:%v, name:%s, error:%v", pid, name, err)
		return false, rocksdbError
	}
	if !ok {
		return false, notExistsError
	}
	return true, nil
}
func (b *ExtendRocks) Delete(ino uint64) (bool, error) {
	ok, err := b.RocksTree.Delete(&b.baseInfo.extendCnt, extendEncodingKey(ino))
	if err != nil {
		log.LogErrorf("ExtendRocks Delete failed, extend ino:%v, error:%v", ino, err)
		return false, rocksdbError
	}
	if !ok {
		return false, notExistsError
	}
	return true, nil
}
func (b *MultipartRocks) Delete(key, id string) (bool, error) {
	ok, err := b.RocksTree.Delete(&b.baseInfo.multiCnt, multipartEncodingKey(key, id))
	if err != nil {
		log.LogErrorf("MultipartRocks Delete failed, multipart key:%s, id:%s, error:%v", key, id, err)
		return false, rocksdbError
	}
	if !ok {
		return false, notExistsError
	}
	return true, nil
}
func (b *DeletedDentryRocks) Delete(pino uint64, name string, timeStamp int64) (bool, error) {
	ok, err := b.RocksTree.Delete(&b.baseInfo.delDentryCnt, deletedDentryEncodingKey(pino, name, timeStamp))
	if err != nil {
		log.LogErrorf("DeletedDentryRocks Delete failed, deleted dentry pino:%v, name:%s, timeStamp:%v error:%v", pino, name, timeStamp, err)
		return false, rocksdbError
	}
	if !ok {
		return false, notExistsError
	}
	return true, nil
}
func (b *DeletedInodeRocks) Delete(ino uint64) (bool, error) {
	ok, err := b.RocksTree.Delete(&b.baseInfo.delInodeCnt, deletedInodeEncodingKey(ino))
	if err != nil {
		log.LogErrorf("DeletedInodeRocks Delete failed, deleted inode:%v, error:%v", ino, err)
		return false, rocksdbError
	}
	if !ok {
		return false, notExistsError
	}
	return true, nil
}

// Range begin
//Range , if end is nil , it will range all of this type , it range not include end
func (b *InodeRocks) Range(start, end *Inode, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
		callBackFunc func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(InodeTable)}, []byte{byte(InodeTable) + 1}
	if end != nil {
		endByte = inodeEncodingKey(end.Inode)
	}

	if start == nil {
		callBackFunc = cb
	} else {
		callBackFunc = func(data []byte) (bool, error) {
			inode := NewInode(0, 0)
			inode.Unmarshal(context.Background(), data)
			if inode.Less(start) {
				return true, nil
			}
			return cb(data)
		}
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

//Range , just for current scenario(readDir)
//todo:lizhenzhen comment
func (b *DentryRocks) Range(start, end *Dentry, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
		cbFunc    func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(DentryTable)}, []byte{byte(DentryTable) + 1}
	if end != nil {
		endByte = dentryEncodingKey(end.ParentId, end.Name)
	}

	if start != nil && start.ParentId != 0 {
		startByte = dentryEncodingKey(start.ParentId, "")
		cbFunc = func(data []byte) (bool, error) {
			dentry := new(Dentry)
			if err := dentry.Unmarshal(data); err != nil {
				return false, err
			}
			if dentry.Less(start) {
				return true, nil
			}
			return cb(data)

		}
	} else {
		cbFunc = cb
	}
	return b.RocksTree.Range(startByte, endByte, cbFunc)
}

//Range , if end is nil , it will range all of this type , it range not include end
func (b *ExtendRocks) Range(start, end *Extend, cb func(v []byte) (bool, error)) error {
	var (
		startByte    []byte
		endByte      []byte
		callBackFunc func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(ExtendTable)}, []byte{byte(ExtendTable) + 1}
	if end != nil {
		endByte = extendEncodingKey(end.inode)
	}
	if start == nil {
		callBackFunc = cb
	} else {
		callBackFunc = func(data []byte) (bool, error) {
			extent, err := NewExtendFromBytes(data)
			if err != nil {
				return false, err
			}
			if extent.Less(start) {
				return true, nil
			}
			return cb(data)
		}
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

//Range, if start with name key, start key encoding with name key, else if start is nil, it will range MultipartTable start to endBytes,
//note: Just for current scenario,
//todo:lizhenzhen comment
func (b *MultipartRocks) Range(start, end *Multipart, cb func(v []byte) (bool, error)) error {
	var startByte, endByte = []byte{byte(MultipartTable)}, []byte{byte(MultipartTable) + 1}
	if end != nil {
		endByte = multipartEncodingKey(end.key, end.id)
	}

	if start != nil {
		buff := new(bytes.Buffer)
		buff.WriteByte(byte(MultipartTable))
		buff.WriteString(start.key)
		startByte = buff.Bytes()
	}

	return b.RocksTree.Range(startByte, endByte, cb)
}

//todo:lizhenzhen just for Current scenario
//Range, just for current scenario
func (b *DeletedDentryRocks) Range(start, end *DeletedDentry, cb func(v []byte) (bool, error)) error {
	var (
		startByte    []byte
		endByte      []byte
		callBackFunc func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(DelDentryTable)}, []byte{byte(DelDentryTable) + 1}
	if end != nil {
		endByte = deletedDentryEncodingKey(end.ParentId, end.Name, end.Timestamp)
	}

	if start == nil || start.ParentId == 0 {
		return b.RocksTree.Range(startByte, endByte, cb)
	}

	//start != nil
	callBackFunc = func(v []byte) (bool, error) {
		dd := new(DeletedDentry)
		if err := dd.Unmarshal(v); err != nil {
			return false, err
		}
		if dd.Less(start) {
			return true, nil
		}
		return cb(v)
	}

	if end != nil {
		if start.ParentId == end.ParentId && start.Name == end.Name {
			buff := new(bytes.Buffer)
			buff.WriteByte(byte(DelDentryTable))
			_ = binary.Write(buff, binary.BigEndian, start.ParentId)
			buff.WriteString(start.Name)
			startByte = buff.Bytes()
		} else { //todo:lizhenzhen startByte with pid just for current usage scenario
			buff := new(bytes.Buffer)
			buff.WriteByte(byte(DelDentryTable))
			_ = binary.Write(buff, binary.BigEndian, start.ParentId)
			startByte = buff.Bytes()
		}
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

func (b *DeletedDentryRocks) RangeBackup(start, end *DeletedDentry, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
		callBackFunc func(v []byte) (bool, error)
	)

	startByte, endByte = []byte{byte(DelDentryTable)}, []byte{byte(DelDentryTable) + 1}
	if end != nil {
		endByte = deletedDentryEncodingKey(end.ParentId, end.Name, end.Timestamp)
	}

	if start == nil || start.ParentId == 0 {
		callBackFunc = cb
	} else {
		callBackFunc = func(v []byte) (bool, error) {
			dd := new(DeletedDentry)
			if err := dd.Unmarshal(v); err != nil {
				return false, err
			}
			if dd.Less(start) {
				return true, nil
			}
			return cb(v)
		}
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

//todo:just for current scenario
func (b *DeletedInodeRocks) Range(start, end *DeletedINode, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
	)
	startByte, endByte = []byte{byte(DelInodeTable)}, []byte{byte(DelInodeTable) + 1}
	if start != nil {
		startByte = inodeEncodingKey(start.Inode.Inode)
	}

	if end != nil {
		endByte = inodeEncodingKey(end.Inode.Inode)
	}
	return b.RocksTree.Range(startByte, endByte, cb)
}

func (b *InodeRocks) MaxItem() *Inode {
	var maxItem *Inode
	snapshot := b.RocksTree.db.OpenSnap()
	if snapshot == nil {
		log.LogErrorf("InodeRocks MaxItem snap is nil")
		return nil
	}
	defer b.RocksTree.db.ReleaseSnap(snapshot)
	iterator := b.RocksTree.db.iterator(snapshot)
	defer iterator.Close()
	err := b.db.descRangeWithIter(iterator, []byte{byte(InodeTable)}, []byte{byte(InodeTable) + 1}, func(k, v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if e := inode.Unmarshal(context.Background(), v); e != nil {
			return false, e
		}
		maxItem = inode
		return false, nil
	})
	if err != nil {
		return nil
	}
	return maxItem
}

var _ Snapshot = &RocksSnapShot{}

type RocksSnapShot struct {
	snap     *gorocksdb.Snapshot
	tree     *RocksTree
	baseInfo RocksBaseInfo
}

func NewRocksSnapShot(mp *metaPartition) Snapshot {
	s := mp.db.OpenSnap()
	if s == nil {
		return nil
	}
	return &RocksSnapShot{
		snap:     s,
		tree:     mp.inodeTree.(*InodeRocks).RocksTree,
		baseInfo: mp.inodeTree.(*InodeRocks).baseInfo,
	}
}

func (r *RocksSnapShot) Count(tp TreeType) (uint64) {
	var count uint64
	switch tp {
	case InodeType:
		count = r.baseInfo.inodeCnt
	case DentryType:
		count = r.baseInfo.dentryCnt
	case ExtendType:
		count = r.baseInfo.extendCnt
	case MultipartType:
		count = r.baseInfo.multiCnt
	case DelDentryType:
		count = r.baseInfo.delDentryCnt
	case DelInodeType:
		count = r.baseInfo.delInodeCnt
	}
	return count
}

func (r *RocksSnapShot) Range(tp TreeType, cb func(v []byte) (bool, error)) error {
	tableType := getTableTypeKey(tp)
	callbackFunc := func(k, v []byte) (bool, error) {
		return cb(v)
	}
	return r.tree.db.RangeWithSnap([]byte{byte(tableType)}, []byte{byte(tableType) + 1}, r.snap, callbackFunc)
}

func (r *RocksSnapShot) Close() {
	if r.snap == nil {
		return
	}
	r.tree.db.ReleaseSnap(r.snap)
}
