package metanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/tecbot/gorocksdb"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	DefBatchDelCount = 10000
)

type RocksBaseInfo struct {
	version           uint32
	length            uint32
	applyId           uint64
	inodeCnt          uint64
	dentryCnt         uint64
	extendCnt         uint64
	multiCnt          uint64
	delDentryCnt      uint64
	delInodeCnt       uint64
	persistentApplyId uint64
	cursor            uint64
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
	info.persistentApplyId = info.applyId
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.cursor)); err != nil {
		panic(err)
	}
	return buff.Bytes(), nil
}

func (info *RocksBaseInfo) MarshalWithoutApplyID() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint32(&info.version)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint32(&info.length)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.persistentApplyId)); err != nil {
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
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.cursor)); err != nil {
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
	info.persistentApplyId = info.applyId
	if err = binary.Read(buff, binary.BigEndian, &info.cursor); err != nil {
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

func (r *RocksTree) GetPersistentApplyID() uint64 {
	return atomic.LoadUint64(&r.baseInfo.persistentApplyId)
}

func (r *RocksTree) CreateBatchWriteHandle() (interface{}, error) {
	return r.db.CreateBatchHandler()
}

func (r *RocksTree) CommitBatchWrite(handle interface{}, needCommitApplyID bool) error {
	var (
		count        int
		err          error
		buffBaseInfo []byte
	)
	if count, err = r.db.HandleBatchCount(handle); err != nil {
		return err
	}

	//no need to commit
	if count == 0 {
		return nil
	}

	if needCommitApplyID {
		if buffBaseInfo, err = r.baseInfo.Marshal(); err != nil {
			return err
		}
	} else {
		if buffBaseInfo, err = r.baseInfo.MarshalWithoutApplyID(); err != nil {
			return err
		}
	}

	if err = r.db.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil {
		return err
	}
	return r.db.CommitBatch(handle)
}

func (r *RocksTree) ReleaseBatchWriteHandle(handle interface{}) error {
	return r.db.ReleaseBatchHandle(handle)
}

func (r *RocksTree) BatchWriteCount(handle interface{}) (int, error) {
	return r.db.HandleBatchCount(handle)
}

func (r *RocksTree) CommitAndReleaseBatchWriteHandle(handle interface{}, needCommitApplyID bool) error {
	defer r.db.ReleaseBatchHandle(handle)
	var (
		count        int
		err          error
		buffBaseInfo []byte
	)
	if count, err = r.db.HandleBatchCount(handle); err != nil {
		return err
	}

	//no need to commit
	if count == 0 {
		return nil
	}

	if needCommitApplyID {
		if buffBaseInfo, err = r.baseInfo.Marshal(); err != nil {
			return err
		}
	} else {
		if buffBaseInfo, err = r.baseInfo.MarshalWithoutApplyID(); err != nil {
			return err
		}
	}

	if err = r.db.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil {
		return err
	}
	if err =  r.db.CommitBatch(handle); err != nil {
		return err
	}
	return nil
}

func (r *RocksTree) ClearBatchWriteHandle(handle interface{}) error {
	return r.db.ClearBatchWriteHandle(handle)
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

func (r *RocksTree) SetCursor(cursor uint64) {
	if cursor < r.baseInfo.cursor {
		return
	}
	atomic.StoreUint64(&r.baseInfo.cursor, cursor)
}

func (r *RocksTree) GetCursor() uint64 {
	return atomic.LoadUint64(&r.baseInfo.cursor)
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

func (r *RocksTree) RangeWithPrefix(prefix, start, end []byte, cb func(v []byte) (bool, error)) error {
	snapshot := r.db.OpenSnap()
	if snapshot == nil {
		return errors.NewErrorf("open snap failed")
	}
	defer r.db.ReleaseSnap(snapshot)
	callbackFunc := func(k, v []byte) (bool, error) {
		return cb(v)
	}
	return r.db.RangeWithSnapByPrefix(prefix, start, end, snapshot, callbackFunc)
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

func (r *RocksTree) Put(handle interface{}, count *uint64, key []byte, value []byte) error {
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

	if err = r.db.AddItemToBatch(handle, key, value); err != nil{
		return err
	}
	return nil
}

func (r *RocksTree) Update(handle interface{}, key []byte, value []byte) (err error) {
	lock := r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	if err = r.db.AddItemToBatch(handle, key, value); err != nil{
		return
	}
	return
}

func (r *RocksTree) Create(handle interface{}, count *uint64, key []byte, value []byte, force bool) (ok bool, v []byte, err error) {
	lock := r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	v, err = r.GetBytes(key)
	if err != nil {
		return
	}

	if len(v) > 0 && !force {
		return
	}

	if len(v) <= 0 {
		//not exist
		atomic.AddUint64(count, 1)
	}

	if err = r.db.AddItemToBatch(handle, key, value); err != nil{
		return
	}
	ok = true
	v = value
	return
}

// Has checks if the key exists in the btree. return is exist and err
func (r *RocksTree) Delete(handle interface{}, count *uint64, key []byte) (ok bool, err error) {
	var has = false
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
	if err = r.db.DelItemToBatch(handle, key); err != nil{
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

func dentryEncodingPrefix(parentId uint64, name string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	if name != "" {
		buff.WriteString(name)
	}
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

func multipartEncodingPrefix(key string, id string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(MultipartTable))
	buff.WriteString(key)
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

func deletedDentryEncodingPrefix(parentId uint64, name string, timeStamp int64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DelDentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	if name == "" {
		return buff.Bytes()
	}
	buff.WriteString(name)
	buff.WriteByte(0)
	if timeStamp == 0 {
		return buff.Bytes()
	}
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
func (b *InodeRocks) Put(dbHandle interface{}, inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.Put(dbHandle, &b.baseInfo.inodeCnt, inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks put failed, inode:%v, error:%v", inode, err)
	}
	return
}

func (b *DentryRocks) Put(dbHandle interface{}, dentry *Dentry) (err error) {
	var bs []byte
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("DentryRocks dentry marshal failed, dentry:%v, error:%v", dentry, err)
		return
	}
	if err = b.RocksTree.Put(dbHandle, &b.baseInfo.dentryCnt, dentryEncodingKey(dentry.ParentId, dentry.Name), bs); err != nil {
		log.LogErrorf("DentryRocks put failed, dentry:%v, error:%v", dentry, err)
	}
	return
}

func (b *ExtendRocks) Put(dbHandle interface{}, extend *Extend) (err error) {
	var bs []byte
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("ExtendRocks extend marshal failed, extend:%v, error:%v", extend, err)
		return
	}
	if err = b.RocksTree.Put(dbHandle, &b.baseInfo.extendCnt, extendEncodingKey(extend.inode), bs); err != nil {
		log.LogErrorf("ExtendRocks extend put failed, extend:%v, error:%v", extend, err)
	}
	return
}

func (b *MultipartRocks) Put(dbHandle interface{}, multipart *Multipart) (err error) {
	var bs []byte
	bs, err = multipart.Bytes()
	if err != nil {
		log.LogErrorf("MultipartRocks multipart marshal failed, multipart:%v, error:%v", multipart, err)
		return
	}
	if err = b.RocksTree.Put(dbHandle, &b.baseInfo.multiCnt, multipartEncodingKey(multipart.key, multipart.id), bs); err != nil {
		log.LogErrorf("MultipartRocks multipart put failed, multipart:%v, error:%v", multipart, err)
	}
	return
}

//update
func (b *InodeRocks) Update(dbHandle interface{}, inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.Update(dbHandle, inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks inode update failed, inode:%v, error:%v", inode, err)
	}
	return
}

func (b *DentryRocks) Update(dbHandle interface{}, dentry *Dentry) (err error) {
	var bs []byte
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("DentryRocks dentry marshal failed, dentry:%v, error:%v", dentry, err)
		return
	}

	if err = b.RocksTree.Update(dbHandle, dentryEncodingKey(dentry.ParentId, dentry.Name), bs); err != nil {
		log.LogErrorf("DentryRocks dentry update failed, dentry:%v, error:%v", dentry, err)
	}
	return
}

func (b *ExtendRocks) Update(dbHandle interface{}, extend *Extend) (err error) {
	var bs []byte
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("ExtendRocks extend marshal failed, extend:%v, error:%v", extend, err)
		return
	}
	if err = b.RocksTree.Update(dbHandle, extendEncodingKey(extend.inode), bs); err != nil {
		log.LogErrorf("ExtendRocks extend update failed, extend:%v, error:%v", extend, err)
	}
	return
}

func (b *MultipartRocks) Update(dbHandle interface{}, multipart *Multipart) (err error) {
	var bs []byte
	bs, err = multipart.Bytes()
	if err != nil {
		log.LogErrorf("MultipartRocks multipart marshal failed, multipart:%v, error:%v", multipart, err)
		return
	}
	if err = b.RocksTree.Update(dbHandle, multipartEncodingKey(multipart.key, multipart.id), bs); err != nil {
		log.LogErrorf("MultipartRocks multipart update failed, multipart:%v, error:%v", multipart, err)
	}
	return
}

func (b *DeletedInodeRocks) Update(dbHandle interface{}, delIno *DeletedINode) (err error) {
	var bs []byte
	bs, err = delIno.Marshal()
	if err != nil {
		log.LogErrorf("DeletedInodeRocks delInode marshal failed, delIno:%v, error:%v", delIno, err)
		return
	}
	if err = b.RocksTree.Update(dbHandle, deletedInodeEncodingKey(delIno.Inode.Inode), bs); err != nil {
		log.LogErrorf("DeletedInodeRocks delInode update failed, delIno:%v, error:%v", delIno, err)
	}
	return
}

//Create if exists , return old, false,   if not  return nil , true
func (b *InodeRocks) Create(dbHandle interface{}, inode *Inode, replace bool) (ino *Inode, ok bool, err error) {
	var key, bs, v []byte
	key = inodeEncodingKey(inode.Inode)
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] haskey error %v, %v", key, err)
		return
	}

	ok, v, err = b.RocksTree.Create(dbHandle, &b.baseInfo.inodeCnt,  key, bs, replace)
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] inodeRocks error %v, %v", key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			log.LogErrorf("[InodeRocksCreate] invalid value len, inode:%v", inode)
			err = fmt.Errorf("invalid value len")
			return
		}
		//exist
		ino = NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), v); err != nil {
			log.LogErrorf("[InodeRocksCreate] unmarshal exist inode value failed, inode:%v, err:%v", inode, err)
			return
		}
		return
	}
	ino = inode
	return
}

func (b *DentryRocks) Create(dbHandle interface{}, dentry *Dentry, replace bool) (den *Dentry, ok bool, err error) {
	var key, bs, v []byte
	key = dentryEncodingKey(dentry.ParentId, dentry.Name)
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocks] marshal: %v, err: %v", dentry, err)
		return
	}

	ok, v, err = b.RocksTree.Create(dbHandle, &b.baseInfo.dentryCnt,  key, bs, replace)
	if err != nil {
		log.LogErrorf("[DentryRocks] Create dentry: %v key: %v, err: %v", dentry, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = fmt.Errorf("invalid value len")
			log.LogErrorf("[DentryRocks] invalid value len, dentry:%v", dentry)
			return
		}
		den = new(Dentry)
		if err = den.Unmarshal(v); err != nil {
			log.LogErrorf("[DentryRocks] unmarshal exist dentry value failed, dentry:%v, err:%v", dentry, err)
			return
		}
		return
	}
	den = dentry
	return
}

func (b *ExtendRocks) Create(dbHandle interface{}, extend *Extend, replace bool) (ext *Extend, ok bool, err error) {
	var key, bs, v []byte
	key = extendEncodingKey(extend.inode)
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("[ExtendRocks] marshal: %v, err: %v", extend, err)
		return
	}

	ok, v, err = b.RocksTree.Create(dbHandle, &b.baseInfo.extendCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[ExtendRocks] Create extend: %v key: %v, err: %v", extend, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = fmt.Errorf("invalid value len")
			log.LogErrorf("[ExtendRocks] invalid value len, extend:%v", extend)
			return
		}
		if ext, err = NewExtendFromBytes(v); err != nil {
			log.LogErrorf("[ExtendRocks] unmarshal exist extend value failed, extend:%v, err:%v", extend, err)
			return
		}
		return
	}
	ext = extend
	return
}

func (b *MultipartRocks) Create(dbHandle interface{}, mul *Multipart, replace bool) (multipart *Multipart, ok bool, err error) {
	var key, bs, v []byte
	key = multipartEncodingKey(mul.key, mul.id)
	bs, err = mul.Bytes()
	if err != nil {
		log.LogErrorf("[MultipartRocks] marshal: %v, err: %v", mul, err)
		return
	}

	ok, v, err = b.RocksTree.Create(dbHandle, &b.baseInfo.multiCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[MultipartRocks] Create multipart: %v key: %v, err: %v", mul, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = fmt.Errorf("invalid value len")
			log.LogErrorf("[MultipartRocks] invalid value len, mul:%v", mul)
			return
		}
		multipart = MultipartFromBytes(v)
		return
	}
	multipart = mul
	return
}

func (b *DeletedDentryRocks) Create(dbHandle interface{}, delDentry *DeletedDentry, replace bool) (delDen *DeletedDentry, ok bool, err error) {
	var key, bs, v []byte
	key = deletedDentryEncodingKey(delDentry.ParentId, delDentry.Name, delDentry.Timestamp)
	bs, err = delDentry.Marshal()
	if err != nil {
		log.LogErrorf("[DeletedDentryRocks] marshal: %v, err: %v", delDentry, err)
		return
	}

	ok, v, err = b.RocksTree.Create(dbHandle, &b.baseInfo.delDentryCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[DeletedDentryRocks] Create deleted dentry: %v key: %v, err: %v", delDentry, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = fmt.Errorf("invalid value len")
			log.LogErrorf("[DeletedDentryRocks] invalid value len, delDentry:%v", delDentry)
			return
		}
		delDen = new(DeletedDentry)
		if err = delDen.Unmarshal(v); err != nil {
			log.LogErrorf("[DeletedDentryRocks] unmarshal exist delDen value failed, extend:%v, err:%v", delDentry, err)
			return
		}
		return
	}
	delDen = delDentry
	return
}

func (b *DeletedInodeRocks) Create(dbHandle interface{}, delIno *DeletedINode, replace bool) (delInode *DeletedINode, ok bool, err error) {
	var key, bs, v []byte
	key = deletedInodeEncodingKey(delIno.Inode.Inode)
	bs, err = delIno.Marshal()
	if err != nil {
		log.LogErrorf("[DeletedInodeRocks] marshal: %v, err: %v", delIno, err)
		return
	}

	ok, v, err = b.RocksTree.Create(dbHandle, &b.baseInfo.delInodeCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[DeletedInodeRocks] Create deleted inode: %v key: %v, err: %v", delIno, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = fmt.Errorf("invalid value len")
			log.LogErrorf("[DeletedInodeRocks] invalid value len, delIno:%v", delIno)
			return
		}
		delInode = new(DeletedINode)
		if err = delInode.Unmarshal(context.Background(), v); err != nil {
			log.LogErrorf("[DeletedInodeRocks] unmarshal exist delIno value failed, extend:%v, err:%v", delIno, err)
			return
		}
		return
	}
	delInode = delIno
	return
}

//Delete
func (b *InodeRocks) Delete(dbHandle interface{}, ino uint64) (bool, error) {
	return b.RocksTree.Delete(dbHandle, &b.baseInfo.inodeCnt, inodeEncodingKey(ino))
}
func (b *DentryRocks) Delete(dbHandle interface{}, pid uint64, name string) (bool, error) {
	return b.RocksTree.Delete(dbHandle, &b.baseInfo.dentryCnt, dentryEncodingKey(pid, name))
}
func (b *ExtendRocks) Delete(dbHandle interface{}, ino uint64) (bool, error) {
	return b.RocksTree.Delete(dbHandle, &b.baseInfo.extendCnt, extendEncodingKey(ino))
}
func (b *MultipartRocks) Delete(dbHandle interface{}, key, id string) (bool, error) {
	return b.RocksTree.Delete(dbHandle, &b.baseInfo.multiCnt, multipartEncodingKey(key, id))
}
func (b *DeletedDentryRocks) Delete(dbHandle interface{}, pino uint64, name string, timeStamp int64) (bool, error) {
	return b.RocksTree.Delete(dbHandle, &b.baseInfo.delDentryCnt, deletedDentryEncodingKey(pino, name, timeStamp))
}
func (b *DeletedInodeRocks) Delete(dbHandle interface{}, ino uint64) (bool, error) {
	return b.RocksTree.Delete(dbHandle, &b.baseInfo.delInodeCnt, deletedInodeEncodingKey(ino))
}

// Range begin
//Range , if end is nil , it will range all of this type , it range not include end
func (b *InodeRocks) Range(start, end *Inode, cb func(i *Inode) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
		callBackFunc func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(InodeTable)}, []byte{byte(InodeTable) + 1}
	if end != nil {
		endByte = inodeEncodingKey(end.Inode)
	}

	callBackFunc = func(v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if err := inode.Unmarshal(context.Background(), v); err != nil {
			return false, err
		}
		if start != nil && inode.Less(start) {
			return true, nil
		}
		return cb(inode)
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

//Range , just for range dentry table from the beginning of dentry table
func (b *DentryRocks) Range(start, end *Dentry, cb func(d *Dentry) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
		cbFunc    func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(DentryTable)}, []byte{byte(DentryTable) + 1}
	if end != nil {
		endByte = dentryEncodingKey(end.ParentId, end.Name)
	}

	cbFunc = func(v []byte) (bool, error) {
		d := new(Dentry)
		if err := d.Unmarshal(v); err != nil {
			return false, err
		}
		if start != nil && start.ParentId != 0 && d.Less(start) {
			return true, nil
		}
		return cb(d)
	}
	return b.RocksTree.Range(startByte, endByte, cbFunc)
}

func (b *DentryRocks) RangeWithPrefix(prefix, start, end *Dentry, cb func(d *Dentry) (bool, error)) error {
	var (
		startByte, endByte, prefixByte []byte
		cbFunc    func(v []byte) (bool, error)
	)
	prefixByte, startByte, endByte = []byte{byte(DentryTable)}, []byte{byte(DentryTable)}, []byte{byte(DentryTable) + 1}
	if end != nil {
		endByte = dentryEncodingKey(end.ParentId, end.Name)
	}

	if start != nil && start.ParentId != 0 {
		startByte = dentryEncodingKey(start.ParentId, start.Name)
	}

	if prefix != nil {
		prefixByte = dentryEncodingPrefix(prefix.ParentId, prefix.Name)
	}

	cbFunc = func(v []byte) (bool, error) {
		d := new(Dentry)
		if err := d.Unmarshal(v); err != nil {
			return false, err
		}
		return cb(d)
	}
	return b.RocksTree.RangeWithPrefix(prefixByte, startByte, endByte, cbFunc)
}

//Range , if end is nil , it will range all of this type , it range not include end
func (b *ExtendRocks) Range(start, end *Extend, cb func(e *Extend) (bool, error)) error {
	var (
		startByte    []byte
		endByte      []byte
		callBackFunc func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(ExtendTable)}, []byte{byte(ExtendTable) + 1}
	if end != nil {
		endByte = extendEncodingKey(end.inode)
	}

	callBackFunc = func(data []byte) (bool, error) {
		extent, err := NewExtendFromBytes(data)
		if err != nil {
			return false, err
		}
		if start != nil && extent.Less(start) {
			return true, nil
		}
		return cb(extent)
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

//Range, just for range multipart table from the beginning of multipart table
func (b *MultipartRocks) Range(start, end *Multipart, cb func(m *Multipart) (bool, error)) error {
	var startByte, endByte = []byte{byte(MultipartTable)}, []byte{byte(MultipartTable) + 1}
	if end != nil {
		endByte = multipartEncodingKey(end.key, end.id)
	}

	callBackFunc := func(v []byte) (bool, error) {
		mul := MultipartFromBytes(v)
		if start != nil && mul.Less(start) {
			return true, nil
		}
		return cb(mul)
	}

	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

func (b *MultipartRocks) RangeWithPrefix(prefix, start, end *Multipart, cb func(m *Multipart) (bool, error)) error {
	var prefixByte, startByte, endByte = []byte{byte(MultipartTable)}, []byte{byte(MultipartTable)}, []byte{byte(MultipartTable) + 1}
	if end != nil {
		endByte = multipartEncodingKey(end.key, end.id)
	}

	if start != nil {
		startByte = multipartEncodingKey(start.key, start.id)
	}

	if prefix != nil {
		prefixByte = multipartEncodingPrefix(prefix.key, prefix.id)
	}

	callBackFunc := func(v []byte) (bool, error) {
		mul := MultipartFromBytes(v)
		return cb(mul)
	}
	return b.RocksTree.RangeWithPrefix(prefixByte, startByte, endByte, callBackFunc)
}

//just for range deleted dentry table from table start
func (b *DeletedDentryRocks) Range(start, end *DeletedDentry, cb func(deleteDentry *DeletedDentry) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
		callBackFunc func(v []byte) (bool, error)
	)

	startByte, endByte = []byte{byte(DelDentryTable)}, []byte{byte(DelDentryTable) + 1}
	if end != nil {
		endByte = deletedDentryEncodingKey(end.ParentId, end.Name, end.Timestamp)
	}

	callBackFunc = func(v []byte) (bool, error) {
		dd := new(DeletedDentry)
		if err := dd.Unmarshal(v); err != nil {
			return false, err
		}
		if start != nil && dd.Less(start) {
			return true, nil
		}
		return cb(dd)
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

func (b *DeletedDentryRocks) RangeWithPrefix(prefix, start, end *DeletedDentry, cb func(deleteDentry *DeletedDentry) (bool, error)) error {
	var (
		startByte, endByte, prefixByte []byte
		callBackFunc                   func(v []byte) (bool, error)
	)

	prefixByte, startByte, endByte = []byte{byte(DelDentryTable)}, []byte{byte(DelDentryTable)}, []byte{byte(DelDentryTable) + 1}
	if end != nil {
		endByte = deletedDentryEncodingKey(end.ParentId, end.Name, end.Timestamp)
	}

	if start != nil {
		startByte = deletedDentryEncodingKey(start.ParentId, start.Name, start.Timestamp)
	}

	if prefix != nil {
		prefixByte = deletedDentryEncodingPrefix(prefix.ParentId, prefix.Name, prefix.Timestamp)
	}

	callBackFunc = func(v []byte) (bool, error) {
		dd := new(DeletedDentry)
		if err := dd.Unmarshal(v); err != nil {
			return false, err
		}
		return cb(dd)
	}

	return b.RocksTree.RangeWithPrefix(prefixByte, startByte, endByte, callBackFunc)
}

func (b *DeletedInodeRocks) Range(start, end *DeletedINode, cb func(deletedInode *DeletedINode) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
	)
	startByte, endByte = []byte{byte(DelInodeTable)}, []byte{byte(DelInodeTable) + 1}
	if end != nil {
		endByte = inodeEncodingKey(end.Inode.Inode)
	}

	callBackFunc := func(v []byte) (bool, error) {
		di := new(DeletedINode)
		if err := di.Unmarshal(context.Background(), v); err != nil {
			return false, err
		}
		if start != nil && di.Less(start) {
			return true, nil
		}
		return cb(di)
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
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

func (r *RocksSnapShot) Range(tp TreeType, cb func(item interface{}) (bool, error)) error {
	tableType := getTableTypeKey(tp)
	callbackFunc := func(k, v []byte) (bool, error) {
		switch tp {
		case InodeType:
			inode := NewInode(0, 0)
			if err := inode.Unmarshal(context.Background(), v); err != nil {
				return false, err
			}
			return cb(inode)
		case DentryType:
			dentry := new(Dentry)
			if err := dentry.Unmarshal(v); err != nil {
				return false, err
			}
			return cb(dentry)
		case ExtendType:
			extent, err := NewExtendFromBytes(v)
			if err != nil {
				return false, err
			}
			return cb(extent)
		case MultipartType:
			return cb(MultipartFromBytes(v))
		case DelDentryType:
			delDentry := new(DeletedDentry)
			if err := delDentry.Unmarshal(v); err != nil {
				return false, err
			}
			return cb(delDentry)
		case DelInodeType:
			delInode := new(DeletedINode)
			if err := delInode.Unmarshal(context.Background(), v); err != nil {
				return false, err
			}
			return cb(delInode)
		default:
			return false, fmt.Errorf("error type")
		}
	}
	return r.tree.db.RangeWithSnap([]byte{byte(tableType)}, []byte{byte(tableType) + 1}, r.snap, callbackFunc)
}

func (r *RocksSnapShot) Close() {
	if r.snap == nil {
		return
	}
	r.tree.db.ReleaseSnap(r.snap)
}

func (r *RocksSnapShot) CrcSum(tp TreeType) (crcSum uint32, err error) {
	tableType := getTableTypeKey(tp)
	crc := crc32.NewIEEE()
	cb := func(k, v []byte) (bool, error) {
		if tp == InodeType || tp == DelInodeType {
			if len(v) < AccessTimeOffset + 8 {
				return false, fmt.Errorf("")
			}
			binary.BigEndian.PutUint64(v[AccessTimeOffset: AccessTimeOffset+8], 0)
		}
		if _, err := crc.Write(v); err != nil {
			return false, err
		}
		return true, nil
	}

	if err = r.tree.db.RangeWithSnap([]byte{byte(tableType)}, []byte{byte(tableType) + 1}, r.snap, cb); err != nil {
		return
	}
	crcSum = crc.Sum32()
	return
}

func (r *RocksSnapShot) ApplyID() uint64 {
	return r.baseInfo.applyId
}
