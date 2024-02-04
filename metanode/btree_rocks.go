package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/tecbot/gorocksdb"

	"github.com/cubefs/cubefs/util/log"
)

const (
	DefBatchDelCount = 10000
)

var ErrInvalidRocksdbValueLen = fmt.Errorf("invalid value len")

// NOTE: for compatibility, new field should append
// to the end of structure
type RocksBaseInfo struct {
	version              uint32
	length               uint32
	applyId              uint64
	inodeCnt             uint64
	dentryCnt            uint64
	extendCnt            uint64
	multiCnt             uint64
	persistentApplyId    uint64
	cursor               uint64
	txCnt                uint64
	txRbInodeCnt         uint64
	txRbDentryCnt        uint64
	txId                 uint64
	deletedExtentId      uint64
	deletedExtentsCnt    uint64
	deletedObjExtentsCnt uint64
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
	info.persistentApplyId = info.applyId
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.cursor)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txRbInodeCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txRbDentryCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txId)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.deletedExtentId)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.deletedExtentsCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.deletedObjExtentsCnt)); err != nil {
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
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.cursor)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txRbInodeCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txRbDentryCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.txId)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.deletedExtentId)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.deletedExtentsCnt)); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.deletedObjExtentsCnt)); err != nil {
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
	info.persistentApplyId = info.applyId
	if err = binary.Read(buff, binary.BigEndian, &info.cursor); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.txCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.txRbInodeCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.txRbDentryCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.txId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.deletedExtentId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.deletedExtentsCnt); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.deletedObjExtentsCnt); err != nil {
		return
	}
	return
}

type RocksTree struct {
	partitionId uint64
	db          *RocksdbOperator
	latch       [MaxTable]sync.Mutex
	baseInfo    RocksBaseInfo
}

func DefaultRocksTree(dbInfo *RocksdbOperator, partitionId uint64) (*RocksTree, error) {
	return NewRocksTree(dbInfo, partitionId)
}

func NewRocksTree(dbInfo *RocksdbOperator, partitionId uint64) (*RocksTree, error) {
	if dbInfo == nil {
		return nil, errors.NewErrorf("dbInfo is null")
	}
	tree := &RocksTree{
		partitionId: partitionId,
		db:          dbInfo,
	}
	_ = tree.LoadBaseInfo()
	if tree.baseInfo.length == 0 {
		tree.baseInfo.length = uint32(unsafe.Sizeof(tree.baseInfo))
	}
	return tree, nil
}

func (r *RocksTree) warpPartitionKey(id uint64, k []byte) (key []byte) {
	buff := bytes.NewBuffer([]byte{})
	binary.Write(buff, binary.BigEndian, id)
	buff.Write(k)
	key = buff.Bytes()
	return
}

func (r *RocksTree) warpKey(k []byte) (key []byte) {
	key = r.warpPartitionKey(r.partitionId, k)
	return
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

func (r *RocksTree) GetTxId() uint64 {
	return atomic.LoadUint64(&r.baseInfo.txId)
}

func (r *RocksTree) SetTxId(txId uint64) {
	// NOTE: txid is increase only
	now := r.GetTxId()
	for now < txId {
		if atomic.CompareAndSwapUint64(&r.baseInfo.txId, now, txId) {
			return
		}
		now = r.GetTxId()
	}
}

func (r *RocksTree) GetDeletedExtentId() uint64 {
	return atomic.LoadUint64(&r.baseInfo.deletedExtentId)
}

func (r *RocksTree) SetDeletedExtentId(id uint64) {
	// NOTE: dek id is increase only
	now := r.GetDeletedExtentId()
	for now < id {
		if atomic.CompareAndSwapUint64(&r.baseInfo.deletedExtentId, now, id) {
			return
		}
		now = r.GetDeletedExtentId()
	}
}

func (r *RocksTree) CreateBatchWriteHandle() (interface{}, error) {
	return r.db.CreateBatchHandler()
}

func (r *RocksTree) ReleaseBatchHandle(handle interface{}) (err error) {
	return r.db.ReleaseBatchHandle(handle)
}

func (r *RocksTree) AddItemToBatch(handle interface{}, key []byte, value []byte) error {
	return r.db.AddItemToBatch(handle, r.warpKey(key), value)
}

func (r *RocksTree) SaveToDb(key []byte, value []byte) error {
	return r.db.Put(r.warpKey(key), value)
}

func (r *RocksTree) HandleBatchCount(handle interface{}) (count int, err error) {
	return r.db.HandleBatchCount(handle)
}

func (r *RocksTree) CommitBatch(handle interface{}) (err error) {
	return r.db.CommitBatch(handle)
}

func (r *RocksTree) CommitBatchWrite(handle interface{}, needCommitApplyID bool) error {
	var (
		count        int
		err          error
		buffBaseInfo []byte
	)
	if count, err = r.HandleBatchCount(handle); err != nil {
		return err
	}

	// no need to commit
	if count == 0 && !needCommitApplyID {
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

	if err = r.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil {
		return err
	}
	return r.CommitBatch(handle)
}

func (r *RocksTree) ReleaseBatchWriteHandle(handle interface{}) error {
	return r.db.ReleaseBatchHandle(handle)
}

func (r *RocksTree) CommitAndReleaseBatchWriteHandle(handle interface{}, needCommitApplyID bool) error {
	defer r.ReleaseBatchHandle(handle)
	var (
		count        int
		err          error
		buffBaseInfo []byte
	)
	if count, err = r.HandleBatchCount(handle); err != nil {
		return err
	}

	// no need to commit
	if count == 0 && !needCommitApplyID {
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

	if err = r.AddItemToBatch(handle, baseInfoKey, buffBaseInfo); err != nil {
		return err
	}
	if err = r.CommitBatch(handle); err != nil {
		return err
	}
	return nil
}

func (r *RocksTree) CommitAndReleaseBatchWriteForClear(handle interface{}) error {
	defer r.ReleaseBatchHandle(handle)
	var err error
	if err = r.CommitBatch(handle); err != nil {
		return err
	}
	return nil
}

func (r *RocksTree) ClearBatchWriteHandle(handle interface{}) error {
	return r.db.ClearBatchWriteHandle(handle)
}

func (r *RocksTree) CreateBatchHandler() (handle interface{}, err error) {
	return r.db.CreateBatchHandler()
}

func (r *RocksTree) PersistBaseInfo() error {
	buffBaseInfo, err := r.baseInfo.Marshal()
	if err != nil {
		return err
	}

	if err = r.SaveToDb(baseInfoKey, buffBaseInfo); err != nil {
		return err
	}

	return nil
}

func (r *RocksTree) SetCursor(cursor uint64) {
	now := atomic.LoadUint64(&r.baseInfo.cursor)
	for now < cursor {
		if atomic.CompareAndSwapUint64(&r.baseInfo.cursor, now, cursor) {
			return
		}
		now = atomic.LoadUint64(&r.baseInfo.cursor)
	}
}

func (r *RocksTree) GetCursor() uint64 {
	return atomic.LoadUint64(&r.baseInfo.cursor)
}

// NOTE: we disable WAL, flush operation write all data to sst files
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
	default:
		return 0, fmt.Errorf("error tree type:%v", tp)
	}
}

// This requires global traversal to call carefully
func (r *RocksTree) RangeWithSnap(start []byte, end []byte, snap *gorocksdb.Snapshot, iter func(k, v []byte) (bool, error)) (err error) {
	err = r.db.RangeWithSnap(r.warpKey(start), r.warpKey(end), snap, iter)
	return
}

func (r *RocksTree) DescRangeWithSnap(start []byte, end []byte, snap *gorocksdb.Snapshot, iter func(k, v []byte) (bool, error)) (err error) {
	err = r.db.DescRangeWithSnap(r.warpKey(start), r.warpKey(end), snap, iter)
	return
}

func (r *RocksTree) OpenSnap() (snap *gorocksdb.Snapshot) {
	return r.db.OpenSnap()
}

func (r *RocksTree) ReleaseSnap(snap *gorocksdb.Snapshot) {
	r.db.ReleaseSnap(snap)
}

func (r *RocksTree) IteratorCount(tableType TableType) uint64 {
	start, end := []byte{byte(tableType)}, []byte{byte(tableType) + 1}
	var count uint64
	dbSnap := r.OpenSnap()
	if dbSnap == nil {
		log.LogErrorf("IteratorCount openSnap failed.")
		return 0
	}
	defer r.ReleaseSnap(dbSnap)
	if err := r.RangeWithSnap(start, end, dbSnap, func(k, v []byte) (bool, error) {
		count++
		return true, nil
	}); err != nil {
		log.LogErrorf("IteratorCount range with snap failed:%v", err)
		return 0
	}
	return count
}

func (r *RocksTree) Range(start, end []byte, cb func(v []byte) (bool, error)) error {
	snapshot := r.OpenSnap()
	if snapshot == nil {
		return errors.NewErrorf("open snap failed")
	}
	defer r.ReleaseSnap(snapshot)
	callbackFunc := func(k, v []byte) (bool, error) {
		return cb(v)
	}
	return r.RangeWithSnap(start, end, snapshot, callbackFunc)
}

func (r *RocksTree) RangeWithSnapByPrefix(prefix, start, end []byte, snap *gorocksdb.Snapshot, cb func(k, v []byte) (bool, error)) (err error) {
	err = r.db.RangeWithSnapByPrefix(r.warpKey(prefix), r.warpKey(start), r.warpKey(end), snap, cb)
	return
}

func (r *RocksTree) RangeWithPrefix(prefix, start, end []byte, cb func(v []byte) (bool, error)) error {
	snapshot := r.OpenSnap()
	if snapshot == nil {
		return errors.NewErrorf("open snap failed")
	}
	defer r.ReleaseSnap(snapshot)
	callbackFunc := func(k, v []byte) (bool, error) {
		return cb(v)
	}
	return r.RangeWithSnapByPrefix(prefix, start, end, snapshot, callbackFunc)
}

func (r *RocksTree) HasKey(key []byte) (bool, error) {
	bs, err := r.GetBytes(key)
	if err != nil {
		return false, err
	}
	return len(bs) > 0, nil
}

func (r *RocksTree) GetBytes(key []byte) ([]byte, error) {
	return r.db.GetBytes(r.warpKey(key))
}

func (r *RocksTree) GetBytesWithSnap(snap *gorocksdb.Snapshot, key []byte) ([]byte, error) {
	return r.db.GetBytesWithSnap(snap, r.warpKey(key))
}

func (r *RocksTree) Put(count *uint64, key []byte, value []byte) error {
	var err error
	lock := &r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	if count != nil {
		has, err := r.HasKey(key)
		if err != nil {
			return err
		}
		if !has {
			atomic.AddUint64(count, 1)
		}
	}

	if err = r.SaveToDb(key, value); err != nil {
		return err
	}
	return nil
}

func (r *RocksTree) BatchPut(handle interface{}, count *uint64, key []byte, value []byte) error {
	var err error
	lock := &r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	if count != nil {
		has, err := r.HasKey(key)
		if err != nil {
			return err
		}
		if !has {
			atomic.AddUint64(count, 1)
		}
	}

	if err = r.AddItemToBatch(handle, key, value); err != nil {
		return err
	}
	return nil
}

func (r *RocksTree) Update(key []byte, value []byte) (err error) {
	lock := &r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	if err = r.SaveToDb(key, value); err != nil {
		return
	}
	return
}

func (r *RocksTree) BatchUpdate(handle interface{}, key []byte, value []byte) (err error) {
	lock := &r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	if err = r.AddItemToBatch(handle, key, value); err != nil {
		return
	}
	return
}

func (r *RocksTree) Create(count *uint64, key []byte, value []byte, force bool) (ok bool, v []byte, err error) {
	lock := &r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	v, err = r.GetBytes(key)
	if err != nil {
		return
	}

	if len(v) > 0 && !force {
		return
	}

	if count != nil {
		if len(v) <= 0 {
			// not exist
			atomic.AddUint64(count, 1)
		}
	}

	if err = r.SaveToDb(key, value); err != nil {
		return
	}
	ok = true
	v = value
	return
}

func (r *RocksTree) BatchCreate(handle interface{}, count *uint64, key []byte, value []byte, force bool) (ok bool, v []byte, err error) {
	lock := &r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	v, err = r.GetBytes(key)
	if err != nil {
		return
	}

	if len(v) > 0 && !force {
		return
	}

	if count != nil {
		if len(v) <= 0 {
			// not exist
			atomic.AddUint64(count, 1)
		}
	}

	if err = r.AddItemToBatch(handle, key, value); err != nil {
		return
	}
	ok = true
	v = value
	return
}

// Has checks if the key exists in the btree. return is exist and err
func (r *RocksTree) Delete(count *uint64, key []byte) (ok bool, err error) {
	has := false
	lock := &r.latch[key[0]]
	lock.Lock()
	defer lock.Unlock()

	if count != nil {
		has, err = r.HasKey(key)
		if err != nil {
			return
		}
		if !has {
			return
		}
	}

	atomic.AddUint64(count, ^uint64(0))
	if err = r.db.Del(r.warpKey(key)); err != nil {
		return
	}
	ok = true
	return
}

// todo:execute unuse, so remove?
func (r *RocksTree) Execute(fn func(tree interface{}) interface{}) interface{} {
	if err := r.db.accessDb(); err != nil {
		return nil
	}
	defer r.db.releaseDb()
	return fn(r)
}

func (r *RocksTree) DelRangeToBatch(handle interface{}, start []byte, end []byte) (err error) {
	err = r.db.DelRangeToBatch(handle, r.warpKey(start), r.warpKey(end))
	return
}

func (r *RocksTree) DeleteMetadata(handle interface{}) (err error) {
	err = r.db.DelItemToBatch(handle, r.warpKey(baseInfoKey))
	return
}

var (
	_ InodeTree                     = &InodeRocks{}
	_ DentryTree                    = &DentryRocks{}
	_ ExtendTree                    = &ExtendRocks{}
	_ MultipartTree                 = &MultipartRocks{}
	_ TransactionTree               = &TransactionRocks{}
	_ TransactionRollbackInodeTree  = &TransactionRollbackInodeRocks{}
	_ TransactionRollbackDentryTree = &TransactionRollbackDentryRocks{}
)

func NewInodeRocks(tree *RocksTree) (*InodeRocks, error) {
	return &InodeRocks{
		RocksTree: tree,
	}, nil
}

type InodeRocks struct {
	*RocksTree
}

func NewDentryRocks(tree *RocksTree) (*DentryRocks, error) {
	return &DentryRocks{
		RocksTree: tree,
	}, nil
}

type DentryRocks struct {
	*RocksTree
}

func NewExtendRocks(tree *RocksTree) (*ExtendRocks, error) {
	return &ExtendRocks{
		RocksTree: tree,
	}, nil
}

type ExtendRocks struct {
	*RocksTree
}

func NewMultipartRocks(tree *RocksTree) (*MultipartRocks, error) {
	return &MultipartRocks{
		RocksTree: tree,
	}, nil
}

type MultipartRocks struct {
	*RocksTree
}

type TransactionRocks struct {
	*RocksTree
}

func NewTransactionRocks(tree *RocksTree) (*TransactionRocks, error) {
	return &TransactionRocks{
		RocksTree: tree,
	}, nil
}

type TransactionRollbackInodeRocks struct {
	*RocksTree
}

func NewTransactionRollbackInodeRocks(tree *RocksTree) (*TransactionRollbackInodeRocks, error) {
	return &TransactionRollbackInodeRocks{
		RocksTree: tree,
	}, nil
}

type TransactionRollbackDentryRocks struct {
	*RocksTree
}

func NewTransactionRollbackDentryRocks(tree *RocksTree) (*TransactionRollbackDentryRocks, error) {
	return &TransactionRollbackDentryRocks{
		RocksTree: tree,
	}, nil
}

type DeletedExtentsRocks struct {
	*RocksTree
}

func NewDeletedExtentsRocks(tree *RocksTree) (*DeletedExtentsRocks, error) {
	return &DeletedExtentsRocks{
		RocksTree: tree,
	}, nil
}

type DeletedObjExtentsRocks struct {
	*RocksTree
}

func NewDeletedObjExtentsRocks(tree *RocksTree) (*DeletedObjExtentsRocks, error) {
	return &DeletedObjExtentsRocks{
		RocksTree: tree,
	}, nil
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
	buff.WriteByte(0)
	buff.WriteString(name)
	return buff.Bytes()
}

func dentryEncodingPrefix(parentId uint64, name string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	buff.WriteByte(0)
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
	buff.WriteByte(0)
	if id != "" {
		buff.WriteString(id)
	}
	return buff.Bytes()
}

func transactionEncodingKey(txId string) []byte {
	buff := &bytes.Buffer{}
	buff.WriteByte(byte(TransactionTable))
	buff.WriteString(txId)
	return buff.Bytes()
}

func transactionRollbackInodeEncodingKey(ino uint64) []byte {
	buff := &bytes.Buffer{}
	buff.WriteByte(byte(TransactionRollbackInodeTable))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func transactionRollbackDentryEncodingKey(parentId uint64, name string) []byte {
	buff := &bytes.Buffer{}
	buff.WriteByte(byte(TransactionRollbackDentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	buff.WriteString(name)
	return buff.Bytes()
}

func transactionRollbackDentryEncodingPrefix(parentId uint64, name string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(TransactionRollbackDentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	if name != "" {
		buff.WriteString(name)
	}
	return buff.Bytes()
}

func (b *InodeRocks) GetMaxInode() (uint64, error) {
	snapshot := b.RocksTree.OpenSnap()
	if snapshot == nil {
		return 0, errors.NewErrorf("open snapshot failed")
	}
	defer b.RocksTree.ReleaseSnap(snapshot)
	var maxInode uint64 = 0
	err := b.DescRangeWithSnap([]byte{byte(InodeTable)}, []byte{byte(InodeTable) + 1}, snapshot, func(k, v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if e := inode.UnmarshalRocksdb(v); e != nil {
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
	return atomic.LoadUint64(&b.baseInfo.inodeCnt)
}

func (b *DentryRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.dentryCnt)
}

func (b *ExtendRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.extendCnt)
}

func (b *MultipartRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.multiCnt)
}

func (b *TransactionRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.txCnt)
}

func (b *TransactionRollbackInodeRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.txRbInodeCnt)
}

func (b *TransactionRollbackDentryRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.txRbDentryCnt)
}

func (b *DeletedExtentsRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.deletedExtentsCnt)
}

func (b *DeletedObjExtentsRocks) Count() uint64 {
	return atomic.LoadUint64(&b.baseInfo.deletedObjExtentsCnt)
}

func (b *InodeRocks) Len() int {
	return int(b.Count())
}

func (b *DentryRocks) Len() int {
	return int(b.Count())
}

func (b *ExtendRocks) Len() int {
	return int(b.Count())
}

func (b *MultipartRocks) Len() int {
	return int(b.Count())
}

func (b *TransactionRocks) Len() int {
	return int(b.Count())
}

func (b *TransactionRollbackInodeRocks) Len() int {
	return int(b.Count())
}

func (b *TransactionRollbackDentryRocks) Len() int {
	return int(b.Count())
}

func (b *DeletedExtentsRocks) Len() int {
	return int(b.Count())
}

func (b *DeletedObjExtentsRocks) Len() int {
	return int(b.Count())
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

func (b *TransactionRocks) RealCount() uint64 {
	return b.IteratorCount(TransactionTable)
}

func (b *TransactionRollbackInodeRocks) RealCount() uint64 {
	return b.IteratorCount(TransactionRollbackInodeTable)
}

func (b *TransactionRollbackDentryRocks) RealCount() uint64 {
	return b.IteratorCount(TransactionRollbackDentryTable)
}

func (b *DeletedExtentsRocks) RealCount() uint64 {
	return b.IteratorCount(DeletedExtentsTable)
}

func (b *DeletedObjExtentsRocks) RealCount() uint64 {
	return b.IteratorCount(DeletedObjExtentsTable)
}

// Get
func (b *InodeRocks) CopyGet(ino *Inode) (*Inode, error) {
	return b.Get(ino)
}

func (b *InodeRocks) Get(ino *Inode) (*Inode, error) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("[InodeRocks] Get failed, inode:%v err:%v", ino, err)
		}
	}()

	var bs []byte
	bs, err = b.RocksTree.GetBytes(inodeEncodingKey(ino.Inode))
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	inode := NewInode(0, 0)
	if err = inode.UnmarshalRocksdb(bs); err != nil {
		log.LogErrorf("[InodeRocks] unmarshal value error : %v", err)
		return nil, err
	}
	return inode, nil
}

func (b *DentryRocks) CopyGet(dent *Dentry) (*Dentry, error) {
	return b.Get(dent)
}

func (b *DentryRocks) Get(dent *Dentry) (*Dentry, error) {
	var err error
	var dentry *Dentry
	defer func() {
		if err != nil {
			log.LogErrorf("[DentryRocks] Get failed, parentId: %v, name: %v, error: %v", dent.ParentId, dent.Name, err)
		}
	}()

	var bs, key []byte
	key = dentryEncodingKey(dent.ParentId, dent.Name)
	bs, err = b.RocksTree.GetBytes(key)
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	dentry = &Dentry{}
	if err = dentry.Unmarshal(bs); err != nil {
		log.LogErrorf("[DentryRocks] unmarshal value error : %v", err)
		return nil, err
	}
	return dentry, nil
}

func (b *ExtendRocks) CopyGet(extent *Extend) (*Extend, error) {
	return b.Get(extent)
}

func (b *ExtendRocks) Get(extent *Extend) (*Extend, error) {
	var err error
	var ret *Extend
	defer func() {
		if err != nil {
			log.LogErrorf("[ExtendRocks] Get failed, inode %v, error: %v", extent.inode, err)
		}
	}()

	var bs, key []byte
	key = extendEncodingKey(extent.inode)
	bs, err = b.RocksTree.GetBytes(key)
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	if ret, err = NewExtendFromBytes(bs); err != nil {
		log.LogErrorf("[ExtendRocks] unmarshal failed, error: %v", err)
		return nil, err
	}

	return ret, nil
}

func (b *MultipartRocks) CopyGet(multi *Multipart) (*Multipart, error) {
	return b.Get(multi)
}

func (b *MultipartRocks) Get(multi *Multipart) (*Multipart, error) {
	var err error
	var ret *Multipart
	defer func() {
		if err != nil {
			log.LogErrorf("[MultipartRocks] Get failed, key: %v, id: %v, error: %v", multi.key, multi.id, err)
		}
	}()

	var bs, encodingKey []byte
	encodingKey = multipartEncodingKey(multi.key, multi.id)
	bs, err = b.RocksTree.GetBytes(encodingKey)
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	ret = MultipartFromBytes(bs)
	return ret, nil
}

func (b *TransactionRocks) Get(tx *proto.TransactionInfo) (*proto.TransactionInfo, error) {
	var err error
	var ret *proto.TransactionInfo
	defer func() {
		if err != nil {
			log.LogErrorf("[TransactionRocks] Get failed txId: %v", tx.TxID)
		}
	}()

	var bs, encodingKey []byte
	encodingKey = transactionEncodingKey(tx.TxID)
	if bs, err = b.RocksTree.GetBytes(encodingKey); err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	ret = &proto.TransactionInfo{}
	if err = ret.Unmarshal(bs); err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *TransactionRocks) CopyGet(tx *proto.TransactionInfo) (*proto.TransactionInfo, error) {
	return b.Get(tx)
}

func (b *TransactionRollbackInodeRocks) Get(inode *TxRollbackInode) (*TxRollbackInode, error) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("[TransactionRollbackInodeRocks] get ino failed, ino: %v", inode.inode.Inode)
		}
	}()

	var bs, encodingKey []byte
	encodingKey = transactionRollbackInodeEncodingKey(inode.inode.Inode)
	if bs, err = b.RocksTree.GetBytes(encodingKey); err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	inode = &TxRollbackInode{}
	if err = inode.Unmarshal(bs); err != nil {
		return nil, err
	}
	return inode, nil
}

func (b *TransactionRollbackInodeRocks) CopyGet(inode *TxRollbackInode) (*TxRollbackInode, error) {
	return b.Get(inode)
}

func (b *TransactionRollbackDentryRocks) Get(dentry *TxRollbackDentry) (*TxRollbackDentry, error) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("[TransactionRollbackDentryRocks] get dentry failed, parent: %v, name: %v", dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name)
		}
	}()

	var bs, encodingKey []byte
	encodingKey = transactionRollbackDentryEncodingKey(dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name)
	if bs, err = b.RocksTree.GetBytes(encodingKey); err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	den := &TxRollbackDentry{}
	if err = den.Unmarshal(bs); err != nil {
		return nil, err
	}
	return den, nil
}

func (b *TransactionRollbackDentryRocks) CopyGet(dentry *TxRollbackDentry) (*TxRollbackDentry, error) {
	return b.Get(dentry)
}

// put inode into rocksdb
func (b *InodeRocks) Put(inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.MarshalRocksdb()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.Put(&b.baseInfo.inodeCnt, inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks put failed, inode:%v, error:%v", inode, err)
	}
	return
}

func (b *InodeRocks) BatchPut(handle interface{}, inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.MarshalRocksdb()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.BatchPut(handle, &b.baseInfo.inodeCnt, inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks put failed, inode:%v, error:%v", inode, err)
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
	}
	return
}

func (b *DentryRocks) BatchPut(handle interface{}, dentry *Dentry) (err error) {
	var bs []byte
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("DentryRocks dentry marshal failed, dentry:%v, error:%v", dentry, err)
		return
	}
	if err = b.RocksTree.BatchPut(handle, &b.baseInfo.dentryCnt, dentryEncodingKey(dentry.ParentId, dentry.Name), bs); err != nil {
		log.LogErrorf("DentryRocks put failed, dentry:%v, error:%v", dentry, err)
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
	}
	return
}

func (b *ExtendRocks) BatchPut(handle interface{}, extend *Extend) (err error) {
	var bs []byte
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("ExtendRocks extend marshal failed, extend:%v, error:%v", extend, err)
		return
	}
	if err = b.RocksTree.BatchPut(handle, &b.baseInfo.extendCnt, extendEncodingKey(extend.inode), bs); err != nil {
		log.LogErrorf("ExtendRocks extend put failed, extend:%v, error:%v", extend, err)
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
	}
	return
}

func (b *MultipartRocks) BatchPut(handle interface{}, multipart *Multipart) (err error) {
	var bs []byte
	bs, err = multipart.Bytes()
	if err != nil {
		log.LogErrorf("MultipartRocks multipart marshal failed, multipart:%v, error:%v", multipart, err)
		return
	}
	if err = b.RocksTree.BatchPut(handle, &b.baseInfo.multiCnt, multipartEncodingKey(multipart.key, multipart.id), bs); err != nil {
		log.LogErrorf("MultipartRocks multipart put failed, multipart:%v, error:%v", multipart, err)
	}
	return
}

func (b *TransactionRocks) Put(tx *proto.TransactionInfo) (err error) {
	var bs []byte
	if bs, err = tx.Marshal(); err != nil {
		log.LogErrorf("TransactionRocks tx marshal failed, tx: %v, error: %v", tx, err)
		return
	}
	if err = b.RocksTree.Put(&b.baseInfo.txCnt, transactionEncodingKey(tx.TxID), bs); err != nil {
		log.LogErrorf("TransactionRocks tx put failed, tx: %v, error: %v", tx, err)
		return
	}
	return
}

func (b *TransactionRocks) BatchPut(handle interface{}, tx *proto.TransactionInfo) (err error) {
	var bs []byte
	if bs, err = tx.Marshal(); err != nil {
		log.LogErrorf("TransactionRocks tx marshal failed, tx: %v, error: %v", tx, err)
		return
	}
	if err = b.RocksTree.BatchPut(handle, &b.baseInfo.txCnt, transactionEncodingKey(tx.TxID), bs); err != nil {
		log.LogErrorf("TransactionRocks tx put failed, tx: %v, error: %v", tx, err)
		return
	}
	return
}

func (b *TransactionRollbackInodeRocks) Put(ino *TxRollbackInode) (err error) {
	var bs []byte
	if bs, err = ino.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino marshal failed, ino: %v, error: %v", ino, err)
		return
	}
	if err = b.RocksTree.Put(&b.baseInfo.txRbInodeCnt, transactionRollbackInodeEncodingKey(ino.inode.Inode), bs); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino put failed, ino: %v, error: %v", ino, err)
		return
	}
	return
}

func (b *TransactionRollbackInodeRocks) BatchPut(handle interface{}, ino *TxRollbackInode) (err error) {
	var bs []byte
	if bs, err = ino.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino marshal failed, ino: %v, error: %v", ino, err)
		return
	}
	if err = b.RocksTree.BatchPut(handle, &b.baseInfo.txRbInodeCnt, transactionRollbackInodeEncodingKey(ino.inode.Inode), bs); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino put failed, ino: %v, error: %v", ino, err)
		return
	}
	return
}

func (b *TransactionRollbackDentryRocks) Put(dentry *TxRollbackDentry) (err error) {
	var bs []byte
	if bs, err = dentry.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry marshal failed, dentry: %v, error: %v", dentry, err)
		return
	}
	err = b.RocksTree.Put(&b.baseInfo.txRbDentryCnt, transactionRollbackDentryEncodingKey(dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name), bs)
	if err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry put failed, dentry: %v, error: %v", dentry, err)
		return
	}
	return
}

func (b *TransactionRollbackDentryRocks) BatchPut(handle interface{}, dentry *TxRollbackDentry) (err error) {
	var bs []byte
	if bs, err = dentry.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry marshal failed, dentry: %v, error: %v", dentry, err)
		return
	}
	err = b.RocksTree.BatchPut(handle, &b.baseInfo.txRbDentryCnt, transactionRollbackDentryEncodingKey(dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name), bs)
	if err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry put failed, dentry: %v, error: %v", dentry, err)
		return
	}
	return
}

// update
func (b *InodeRocks) Update(inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.MarshalRocksdb()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.Update(inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks inode update failed, inode:%v, error:%v", inode, err)
	}
	return
}

func (b *InodeRocks) BatchUpdate(handle interface{}, inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.MarshalRocksdb()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}
	if err = b.RocksTree.BatchUpdate(handle, inodeEncodingKey(inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks inode update failed, inode:%v, error:%v", inode, err)
	}
	return
}

func (b *DentryRocks) Update(dentry *Dentry) (err error) {
	var bs []byte
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("DentryRocks dentry marshal failed, dentry:%v, error:%v", dentry, err)
		return
	}

	if err = b.RocksTree.Update(dentryEncodingKey(dentry.ParentId, dentry.Name), bs); err != nil {
		log.LogErrorf("DentryRocks dentry update failed, dentry:%v, error:%v", dentry, err)
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
		log.LogErrorf("MultipartRocks multipart update failed, multipart:%v, error:%v", multipart, err)
	}
	return
}

func (b *TransactionRocks) Update(tx *proto.TransactionInfo) (err error) {
	var bs []byte
	if bs, err = tx.Marshal(); err != nil {
		log.LogErrorf("TransactionRocks tx marshal failed, tx: %v, error: %v", tx, err)
		return
	}
	if err = b.RocksTree.Update(transactionEncodingKey(tx.TxID), bs); err != nil {
		log.LogErrorf("MultipartRocks tx update failed, tx: %v, error: %v", tx, err)
	}
	return
}

func (b *TransactionRollbackInodeRocks) Update(ino *TxRollbackInode) (err error) {
	var bs []byte
	if bs, err = ino.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino marshal failed, ino: %v, error: %v", ino, err)
		return
	}
	if err = b.RocksTree.Update(transactionRollbackInodeEncodingKey(ino.inode.Inode), bs); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino update failed, ino: %v, error: %v", ino, err)
		return
	}
	return
}

func (b *TransactionRollbackDentryRocks) Update(dentry *TxRollbackDentry) (err error) {
	var bs []byte
	if bs, err = dentry.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry marshal failed, dentry: %v, error: %v", dentry, err)
		return
	}
	err = b.RocksTree.Update(transactionRollbackDentryEncodingKey(dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name), bs)
	if err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry update failed, dentry: %v, error: %v", dentry, err)
		return
	}
	return
}

// Create if exists , return old, false,   if not  return nil , true
func (b *InodeRocks) ReplaceOrInsert(inode *Inode, replace bool) (ino *Inode, ok bool, err error) {
	var key, bs, v []byte
	key = inodeEncodingKey(inode.Inode)
	bs, err = inode.MarshalRocksdb()
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] haskey error %v, %v", key, err)
		return
	}

	ok, v, err = b.RocksTree.Create(&b.baseInfo.inodeCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] inodeRocks error %v, %v", key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			log.LogErrorf("[InodeRocksCreate] invalid value len, inode:%v", inode)
			err = ErrInvalidRocksdbValueLen
			return
		}
		// exist
		ino = NewInode(0, 0)
		if err = ino.UnmarshalRocksdb(v); err != nil {
			log.LogErrorf("[InodeRocksCreate] unmarshal exist inode value failed, inode:%v, err:%v", inode, err)
			return
		}
		return
	}
	ino = inode
	return
}

func (b *InodeRocks) BatchReplaceOrInsert(handle interface{}, inode *Inode, replace bool) (ino *Inode, ok bool, err error) {
	var key, bs, v []byte
	key = inodeEncodingKey(inode.Inode)
	bs, err = inode.MarshalRocksdb()
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] haskey error %v, %v", key, err)
		return
	}

	ok, v, err = b.RocksTree.BatchCreate(handle, &b.baseInfo.inodeCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] inodeRocks error %v, %v", key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			log.LogErrorf("[InodeRocksCreate] invalid value len, inode:%v", inode)
			err = ErrInvalidRocksdbValueLen
			return
		}
		// exist
		ino = NewInode(0, 0)
		if err = ino.UnmarshalRocksdb(v); err != nil {
			log.LogErrorf("[InodeRocksCreate] unmarshal exist inode value failed, inode:%v, err:%v", inode, err)
			return
		}
		return
	}
	ino = inode
	return
}

func (b *DentryRocks) ReplaceOrInsert(dentry *Dentry, replace bool) (den *Dentry, ok bool, err error) {
	var key, bs, v []byte
	key = dentryEncodingKey(dentry.ParentId, dentry.Name)
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocks] marshal: %v, err: %v", dentry, err)
		return
	}

	ok, v, err = b.RocksTree.Create(&b.baseInfo.dentryCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[DentryRocks] Create dentry: %v key: %v, err: %v", dentry, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
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

func (b *DentryRocks) BatchReplaceOrInsert(handle interface{}, dentry *Dentry, replace bool) (den *Dentry, ok bool, err error) {
	var key, bs, v []byte
	key = dentryEncodingKey(dentry.ParentId, dentry.Name)
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocks] marshal: %v, err: %v", dentry, err)
		return
	}

	ok, v, err = b.RocksTree.BatchCreate(handle, &b.baseInfo.dentryCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[DentryRocks] Create dentry: %v key: %v, err: %v", dentry, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
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

func (b *ExtendRocks) ReplaceOrInsert(extend *Extend, replace bool) (ext *Extend, ok bool, err error) {
	var key, bs, v []byte
	key = extendEncodingKey(extend.inode)
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("[ExtendRocks] marshal: %v, err: %v", extend, err)
		return
	}

	ok, v, err = b.RocksTree.Create(&b.baseInfo.extendCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[ExtendRocks] Create extend: %v key: %v, err: %v", extend, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
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

func (b *ExtendRocks) BatchReplaceOrInsert(handle interface{}, extend *Extend, replace bool) (ext *Extend, ok bool, err error) {
	var key, bs, v []byte
	key = extendEncodingKey(extend.inode)
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("[ExtendRocks] marshal: %v, err: %v", extend, err)
		return
	}

	ok, v, err = b.RocksTree.BatchCreate(handle, &b.baseInfo.extendCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[ExtendRocks] Create extend: %v key: %v, err: %v", extend, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
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

func (b *MultipartRocks) ReplaceOrInsert(mul *Multipart, replace bool) (multipart *Multipart, ok bool, err error) {
	var key, bs, v []byte
	key = multipartEncodingKey(mul.key, mul.id)
	bs, err = mul.Bytes()
	if err != nil {
		log.LogErrorf("[MultipartRocks] marshal: %v, err: %v", mul, err)
		return
	}

	ok, v, err = b.RocksTree.Create(&b.baseInfo.multiCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[MultipartRocks] Create multipart: %v key: %v, err: %v", mul, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[MultipartRocks] invalid value len, mul:%v", mul)
			return
		}
		multipart = MultipartFromBytes(v)
		return
	}
	multipart = mul
	return
}

func (b *MultipartRocks) BatchReplaceOrInsert(handle interface{}, mul *Multipart, replace bool) (multipart *Multipart, ok bool, err error) {
	var key, bs, v []byte
	key = multipartEncodingKey(mul.key, mul.id)
	bs, err = mul.Bytes()
	if err != nil {
		log.LogErrorf("[MultipartRocks] marshal: %v, err: %v", mul, err)
		return
	}

	ok, v, err = b.RocksTree.BatchCreate(handle, &b.baseInfo.multiCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[MultipartRocks] Create multipart: %v key: %v, err: %v", mul, key, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[MultipartRocks] invalid value len, mul:%v", mul)
			return
		}
		multipart = MultipartFromBytes(v)
		return
	}
	multipart = mul
	return
}

func (b *TransactionRocks) ReplaceOrInsert(tx *proto.TransactionInfo, replace bool) (transaction *proto.TransactionInfo, ok bool, err error) {
	var key, bs, v []byte
	key = transactionEncodingKey(tx.TxID)
	bs, err = tx.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRocks] marshal: %v, err: %v", tx, err)
		return
	}

	ok, v, err = b.RocksTree.Create(&b.baseInfo.txCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[TransactionRocks] Create transaction: %v id: %v, err: %v", tx, tx.TxID, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[TransactionRocks] invalid value len, tx:%v", tx)
			return
		}
		if err = transaction.Unmarshal(v); err != nil {
			log.LogErrorf("[TransactionRocks] failed to unmarshal transaction: %v, err: %v", tx.TxID, err)
			return
		}
		return
	}
	transaction = tx
	return
}

func (b *TransactionRocks) BatchReplaceOrInsert(handle interface{}, tx *proto.TransactionInfo, replace bool) (transaction *proto.TransactionInfo, ok bool, err error) {
	var key, bs, v []byte
	key = transactionEncodingKey(tx.TxID)
	bs, err = tx.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRocks] marshal: %v, err: %v", tx, err)
		return
	}

	ok, v, err = b.RocksTree.BatchCreate(handle, &b.baseInfo.txCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[TransactionRocks] Create transaction: %v id: %v, err: %v", tx, tx.TxID, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[TransactionRocks] invalid value len, tx:%v", tx)
			return
		}
		if err = transaction.Unmarshal(v); err != nil {
			log.LogErrorf("[TransactionRocks] failed to unmarshal transaction: %v, err: %v", tx.TxID, err)
			return
		}
		return
	}
	transaction = tx
	return
}

func (b *TransactionRollbackInodeRocks) ReplaceOrInsert(ino *TxRollbackInode, replace bool) (inode *TxRollbackInode, ok bool, err error) {
	var key, bs, v []byte
	key = transactionRollbackInodeEncodingKey(ino.inode.Inode)
	bs, err = ino.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRollbackInodeRocks] marshal: %v, err: %v", ino, err)
		return
	}
	ok, v, err = b.RocksTree.Create(&b.baseInfo.txRbInodeCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[TransactionRollbackInodeRocks] Create ino: %v, err: %v", ino, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[TransactionRollbackInodeRocks] invalid value len, ino:%v", ino)
			return
		}
		if err = inode.Unmarshal(v); err != nil {
			log.LogErrorf("[TransactionRollbackInodeRocks] failed to unmarshal inode: %v, err: %v", ino.inode.Inode, err)
			return
		}
		return
	}
	inode = ino
	return
}

func (b *TransactionRollbackInodeRocks) BatchReplaceOrInsert(handle interface{}, ino *TxRollbackInode, replace bool) (inode *TxRollbackInode, ok bool, err error) {
	var key, bs, v []byte
	key = transactionRollbackInodeEncodingKey(ino.inode.Inode)
	bs, err = ino.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRollbackInodeRocks] marshal: %v, err: %v", ino, err)
		return
	}
	ok, v, err = b.RocksTree.BatchCreate(handle, &b.baseInfo.txRbInodeCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[TransactionRollbackInodeRocks] Create ino: %v, err: %v", ino, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[TransactionRollbackInodeRocks] invalid value len, ino:%v", ino)
			return
		}
		if err = inode.Unmarshal(v); err != nil {
			log.LogErrorf("[TransactionRollbackInodeRocks] failed to unmarshal inode: %v, err: %v", ino.inode.Inode, err)
			return
		}
		return
	}
	inode = ino
	return
}

func (b *TransactionRollbackDentryRocks) ReplaceOrInsert(den *TxRollbackDentry, replace bool) (dentry *TxRollbackDentry, ok bool, err error) {
	var key, bs, v []byte
	key = transactionRollbackDentryEncodingKey(den.txDentryInfo.ParentId, den.txDentryInfo.Name)
	bs, err = den.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRollbackDentryRocks] marshal: %v, err: %v", den, err)
		return
	}
	ok, v, err = b.RocksTree.Create(&b.baseInfo.txRbDentryCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[TransactionRollbackDentryRocks] Create dentry failed, parent: %v, name: %v, err: %v", den.txDentryInfo.ParentId, den.txDentryInfo.Name, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[TransactionRollbackDentryRocks] invalid value len, den:%v", den)
			return
		}
		if err = dentry.Unmarshal(v); err != nil {
			log.LogErrorf("[TransactionRollbackDentryRocks] failed to unmarshal parent: %v, name: %v, err: %v", den.txDentryInfo.ParentId, den.txDentryInfo.Name, err)
			return
		}
		return
	}
	return
}

func (b *TransactionRollbackDentryRocks) BatchReplaceOrInsert(handle interface{}, den *TxRollbackDentry, replace bool) (dentry *TxRollbackDentry, ok bool, err error) {
	var key, bs, v []byte
	key = transactionRollbackDentryEncodingKey(den.txDentryInfo.ParentId, den.txDentryInfo.Name)
	bs, err = den.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRollbackDentryRocks] marshal: %v, err: %v", den, err)
		return
	}
	ok, v, err = b.RocksTree.BatchCreate(handle, &b.baseInfo.txRbDentryCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[TransactionRollbackDentryRocks] Create dentry failed, parent: %v, name: %v, err: %v", den.txDentryInfo.ParentId, den.txDentryInfo.Name, err)
		return
	}

	if !ok {
		if len(v) == 0 {
			err = ErrInvalidRocksdbValueLen
			log.LogErrorf("[TransactionRollbackDentryRocks] invalid value len, den:%v", den)
			return
		}
		if err = dentry.Unmarshal(v); err != nil {
			log.LogErrorf("[TransactionRollbackDentryRocks] failed to unmarshal parent: %v, name: %v, err: %v", den.txDentryInfo.ParentId, den.txDentryInfo.Name, err)
			return
		}
		return
	}
	return
}

// Delete
func (b *InodeRocks) Delete(inode *Inode) (bool, error) {
	return b.RocksTree.Delete(&b.baseInfo.inodeCnt, inodeEncodingKey(inode.Inode))
}

func (b *DentryRocks) Delete(dentry *Dentry) (bool, error) {
	return b.RocksTree.Delete(&b.baseInfo.dentryCnt, dentryEncodingKey(dentry.ParentId, dentry.Name))
}

func (b *ExtendRocks) Delete(extend *Extend) (bool, error) {
	return b.RocksTree.Delete(&b.baseInfo.extendCnt, extendEncodingKey(extend.inode))
}

func (b *MultipartRocks) Delete(mutipart *Multipart) (bool, error) {
	return b.RocksTree.Delete(&b.baseInfo.multiCnt, multipartEncodingKey(mutipart.key, mutipart.id))
}

func (b *TransactionRocks) Delete(txId string) (bool, error) {
	return b.RocksTree.Delete(&b.baseInfo.txCnt, transactionEncodingKey(txId))
}

func (b *TransactionRollbackInodeRocks) Delete(inode *TxRollbackInode) (bool, error) {
	return b.RocksTree.Delete(&b.baseInfo.txRbInodeCnt, transactionRollbackInodeEncodingKey(inode.txInodeInfo.Ino))
}

func (b *TransactionRollbackDentryRocks) Delete(dentry *TxRollbackDentry) (bool, error) {
	return b.RocksTree.Delete(&b.baseInfo.txRbDentryCnt, transactionRollbackDentryEncodingKey(dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name))
}

// Range begin
// Range , if end is nil , it will range all of this type , it range not include end
func (b *InodeRocks) Range(start, end *Inode, cb func(i *Inode) bool) error {
	var (
		startByte    []byte
		endByte      []byte
		callBackFunc func(v []byte) (bool, error)
	)
	startByte, endByte = []byte{byte(InodeTable)}, []byte{byte(InodeTable) + 1}
	if end != nil {
		endByte = inodeEncodingKey(end.Inode)
	}

	callBackFunc = func(v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if err := inode.UnmarshalRocksdb(v); err != nil {
			return false, err
		}
		if start != nil && inode.Less(start) {
			return true, nil
		}
		return cb(inode), nil
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

// Range , just for range dentry table from the beginning of dentry table
func (b *DentryRocks) Range(start, end *Dentry, cb func(d *Dentry) bool) error {
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
		return cb(d), nil
	}
	return b.RocksTree.Range(startByte, endByte, cbFunc)
}

func (b *DentryRocks) RangeWithPrefix(prefix, start, end *Dentry, cb func(d *Dentry) bool) error {
	var (
		startByte, endByte, prefixByte []byte
		cbFunc                         func(v []byte) (bool, error)
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
		return cb(d), nil
	}
	return b.RocksTree.RangeWithPrefix(prefixByte, startByte, endByte, cbFunc)
}

// Range , if end is nil , it will range all of this type , it range not include end
func (b *ExtendRocks) Range(start, end *Extend, cb func(e *Extend) bool) error {
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
		return cb(extent), nil
	}
	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

// Range, just for range multipart table from the beginning of multipart table
func (b *MultipartRocks) Range(start, end *Multipart, cb func(m *Multipart) bool) error {
	startByte, endByte := []byte{byte(MultipartTable)}, []byte{byte(MultipartTable) + 1}
	if end != nil {
		endByte = multipartEncodingKey(end.key, end.id)
	}

	callBackFunc := func(v []byte) (bool, error) {
		mul := MultipartFromBytes(v)
		if start != nil && mul.Less(start) {
			return true, nil
		}
		return cb(mul), nil
	}

	return b.RocksTree.Range(startByte, endByte, callBackFunc)
}

func (b *MultipartRocks) RangeWithPrefix(prefix, start, end *Multipart, cb func(m *Multipart) bool) error {
	prefixByte, startByte, endByte := []byte{byte(MultipartTable)}, []byte{byte(MultipartTable)}, []byte{byte(MultipartTable) + 1}
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
		return cb(mul), nil
	}
	return b.RocksTree.RangeWithPrefix(prefixByte, startByte, endByte, callBackFunc)
}

func (b *TransactionRocks) Range(start, end *proto.TransactionInfo, cb func(tx *proto.TransactionInfo) bool) error {
	startByte := []byte{byte(TransactionTable)}
	endByte := []byte{byte(TransactionTable) + 1}
	if end != nil {
		endByte = transactionEncodingKey(end.TxID)
	}
	callback := func(v []byte) (bool, error) {
		tx := &proto.TransactionInfo{}
		err := tx.Unmarshal(v)
		if err != nil {
			return false, err
		}
		if start != nil && tx.Less(start) {
			return true, nil
		}
		return cb(tx), nil
	}
	return b.RocksTree.Range(startByte, endByte, callback)
}

func (b *TransactionRollbackInodeRocks) Range(start, end *TxRollbackInode, cb func(ino *TxRollbackInode) bool) error {
	startByte := []byte{byte(TransactionRollbackInodeTable)}
	endByte := []byte{byte(TransactionRollbackInodeTable) + 1}
	if end != nil {
		endByte = transactionRollbackInodeEncodingKey(end.inode.Inode)
	}
	callback := func(v []byte) (bool, error) {
		ino := &TxRollbackInode{}
		err := ino.Unmarshal(v)
		if err != nil {
			return false, err
		}
		if start != nil && ino.Less(start) {
			return true, nil
		}
		return cb(ino), nil
	}
	return b.RocksTree.Range(startByte, endByte, callback)
}

func (b *TransactionRollbackDentryRocks) Range(start, end *TxRollbackDentry, cb func(den *TxRollbackDentry) bool) error {
	startByte := []byte{byte(TransactionRollbackDentryTable)}
	endByte := []byte{byte(TransactionRollbackDentryTable) + 1}
	if end != nil {
		endByte = transactionRollbackDentryEncodingKey(end.txDentryInfo.ParentId, end.txDentryInfo.Name)
	}
	callback := func(v []byte) (bool, error) {
		den := &TxRollbackDentry{}
		err := den.Unmarshal(v)
		if err != nil {
			return false, err
		}
		if start != nil && den.Less(start) {
			return true, nil
		}
		return cb(den), nil
	}
	return b.RocksTree.Range(startByte, endByte, callback)
}

func (b *TransactionRollbackDentryRocks) RangeWithPrefix(prefix, start, end *TxRollbackDentry, cb func(den *TxRollbackDentry) bool) error {
	prefixByte := []byte{byte(TransactionRollbackDentryTable)}
	startByte := []byte{byte(TransactionRollbackDentryTable)}
	endByte := []byte{byte(TransactionRollbackDentryTable) + 1}
	if end != nil {
		endByte = transactionRollbackDentryEncodingKey(end.txDentryInfo.ParentId, end.txDentryInfo.Name)
	}
	if start != nil {
		startByte = transactionRollbackDentryEncodingKey(start.txDentryInfo.ParentId, start.txDentryInfo.Name)
	}
	if prefix != nil {
		prefixByte = transactionRollbackDentryEncodingPrefix(prefix.txDentryInfo.ParentId, prefix.txDentryInfo.Name)
	}

	callback := func(v []byte) (bool, error) {
		den := &TxRollbackDentry{}
		err := den.Unmarshal(v)
		if err != nil {
			return false, err
		}
		return cb(den), nil
	}
	return b.RocksTree.RangeWithPrefix(prefixByte, startByte, endByte, callback)
}

func (b *InodeRocks) MaxItem() *Inode {
	var maxItem *Inode
	snapshot := b.RocksTree.OpenSnap()
	if snapshot == nil {
		log.LogErrorf("InodeRocks MaxItem snap is nil")
		return nil
	}
	defer b.RocksTree.ReleaseSnap(snapshot)
	err := b.DescRangeWithSnap([]byte{byte(InodeTable)}, []byte{byte(InodeTable) + 1}, snapshot, func(k, v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if e := inode.UnmarshalRocksdb(v); e != nil {
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

// NOTE: clear
func (b *InodeRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(InodeTable)}, []byte{byte(InodeTable + 1)})
	return
}

func (b *DentryRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(DentryTable)}, []byte{byte(DentryTable + 1)})
	return
}

func (b *ExtendRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(ExtendTable)}, []byte{byte(ExtendTable + 1)})
	return
}

func (b *MultipartRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(MultipartTable)}, []byte{byte(MultipartTable + 1)})
	return
}

func (b *TransactionRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(TransactionTable)}, []byte{byte(TransactionTable + 1)})
	return
}

func (b *TransactionRollbackInodeRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(TransactionRollbackInodeTable)}, []byte{byte(TransactionRollbackInodeTable + 1)})
	return
}

func (b *TransactionRollbackDentryRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(TransactionRollbackDentryTable)}, []byte{byte(TransactionRollbackDentryTable + 1)})
	return
}

func (b *DeletedExtentsRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(DeletedExtentsTable)}, []byte{byte(DeletedExtentsTable + 1)})
	return
}

func (b *DeletedObjExtentsRocks) Clear(handle interface{}) (err error) {
	err = b.DelRangeToBatch(handle, []byte{byte(DeletedObjExtentsTable)}, []byte{byte(DeletedObjExtentsTable + 1)})
	return
}

var _ Snapshot = &RocksSnapShot{}

type RocksSnapShot struct {
	snap     *gorocksdb.Snapshot
	tree     *RocksTree
	baseInfo RocksBaseInfo
}

func NewRocksSnapShot(mp *metaPartition) Snapshot {
	var err error
	s := mp.db.OpenSnap()
	if s == nil {
		return nil
	}
	defer func() {
		if err != nil {
			mp.db.ReleaseSnap(s)
		}
	}()
	rocksTree := mp.inodeTree.(*InodeRocks).RocksTree
	v, err := rocksTree.GetBytesWithSnap(s, baseInfoKey)
	if err != nil {
		log.LogErrorf("[NewRocksSnapShot] failed to get base info")
		return nil
	}
	snap := &RocksSnapShot{
		snap: s,
		tree: rocksTree,
	}
	if len(v) != 0 {
		err = snap.baseInfo.Unmarshal(v)
		if err != nil {
			log.LogErrorf("[NewRocksSnapShot] failed to unmarshal base info, err(%v)", err)
			return nil
		}
	}
	return snap
}

func (r *RocksSnapShot) Count(tp TreeType) uint64 {
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
	case TransactionType:
		count = r.baseInfo.txCnt
	case TransactionRollbackInodeType:
		count = r.baseInfo.txRbInodeCnt
	case TransactionRollbackDentryType:
		count = r.baseInfo.txRbDentryCnt
	case DeletedExtentsType:
		count = r.baseInfo.deletedExtentsCnt
	case DeletedObjExtentsType:
		count = r.baseInfo.deletedObjExtentsCnt
	}
	return count
}

func (r *RocksSnapShot) Range(tp TreeType, cb func(item interface{}) bool) error {
	return r.RangeWithScope(tp, nil, nil, cb)
}

func (r *RocksSnapShot) RangeWithScope(tp TreeType, start, end interface{}, cb func(item interface{}) bool) error {
	tableType := getTableTypeKey(tp)
	callbackFunc := func(k, v []byte) (bool, error) {
		switch tp {
		case InodeType:
			inode := NewInode(0, 0)
			if err := inode.UnmarshalRocksdb(v); err != nil {
				return false, err
			}
			return cb(inode), nil
		case DentryType:
			dentry := new(Dentry)
			if err := dentry.Unmarshal(v); err != nil {
				return false, err
			}
			return cb(dentry), nil
		case ExtendType:
			extent, err := NewExtendFromBytes(v)
			if err != nil {
				return false, err
			}
			return cb(extent), nil
		case MultipartType:
			return cb(MultipartFromBytes(v)), nil
		case TransactionType:
			tx := &proto.TransactionInfo{}
			if err := tx.Unmarshal(v); err != nil {
				return false, err
			}
			return cb(tx), nil
		case TransactionRollbackInodeType:
			inode := &TxRollbackInode{}
			if err := inode.Unmarshal(v); err != nil {
				return false, err
			}
			return cb(inode), nil
		case TransactionRollbackDentryType:
			dentry := &TxRollbackDentry{}
			if err := dentry.Unmarshal(v); err != nil {
				return false, err
			}
			return cb(dentry), nil
		default:
			return false, fmt.Errorf("error type")
		}
	}
	startBytes := []byte{byte(tableType)}
	endBytes := []byte{byte(tableType + 1)}
	marshal := func(item interface{}) ([]byte, error) {
		switch tp {
		case InodeType:
			inode := item.(*Inode)
			return inode.MarshalRocksdb()
		case DentryType:
			dentry := item.(*Dentry)
			return dentry.Marshal()
		case ExtendType:
			extend := item.(*Extend)
			return extend.Bytes()
		case MultipartType:
			multipart := item.(*Multipart)
			return multipart.Bytes()
		case TransactionType:
			tx := item.(*proto.TransactionInfo)
			return tx.Marshal()
		case TransactionRollbackInodeType:
			inode := item.(*TxRollbackInode)
			return inode.Marshal()
		case TransactionRollbackDentryType:
			dentry := item.(*TxRollbackDentry)
			return dentry.Marshal()
		default:
			return nil, fmt.Errorf("error type")
		}
	}
	var err error
	if start != nil {
		startBytes, err = marshal(start)
		if err != nil {
			return err
		}
	}
	if end != nil {
		endBytes, err = marshal(end)
		if err != nil {
			return err
		}
	}
	return r.tree.RangeWithSnap(startBytes, endBytes, r.snap, callbackFunc)
}

func (r *RocksSnapShot) Close() {
	if r.snap == nil {
		return
	}
	r.tree.ReleaseSnap(r.snap)
}

func (r *RocksSnapShot) CrcSum(tp TreeType) (crcSum uint32, err error) {
	tableType := getTableTypeKey(tp)
	crc := crc32.NewIEEE()
	cb := func(k, v []byte) (bool, error) {
		if tp == InodeType {
			if len(v) < AccessTimeOffset+8 {
				return false, fmt.Errorf("")
			}
			binary.BigEndian.PutUint64(v[AccessTimeOffset:AccessTimeOffset+8], 0)
		}
		if _, err := crc.Write(v); err != nil {
			return false, err
		}
		return true, nil
	}

	if err = r.tree.RangeWithSnap([]byte{byte(tableType)}, []byte{byte(tableType) + 1}, r.snap, cb); err != nil {
		return
	}
	crcSum = crc.Sum32()
	return
}

func (r *RocksSnapShot) ApplyID() uint64 {
	return r.baseInfo.applyId
}

func (r *RocksSnapShot) TxID() uint64 {
	return r.baseInfo.txId
}

func (r *RocksSnapShot) DeletedExtentId() uint64 {
	return r.baseInfo.deletedExtentId
}
