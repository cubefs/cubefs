package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/tecbot/gorocksdb"

	"github.com/cubefs/cubefs/util/log"
)

const (
	DefBatchDelCount = 10000

	RocksdbNormalKeySize = 32
	RocksdbLongKeySize   = 1024
	RocksdbTypeIndex     = 8
)

var ErrInvalidRocksdbValueLen = fmt.Errorf("invalid value len")

// NOTE: for compatibility, new field should append
// to the end of structure
type RocksBaseInfo struct {
	version           uint32
	length            uint32
	applyId           uint64
	inodeCnt          uint64
	dentryCnt         uint64
	extendCnt         uint64
	multiCnt          uint64
	persistentApplyId uint64
	cursor            uint64
	txCnt             uint64
	txRbInodeCnt      uint64
	txRbDentryCnt     uint64
	txId              uint64
	uniqID            uint64
}

var RocksdbNormalKeyPool = sync.Pool{
	New: func() interface{} {
		return buf.NewByteBufEx(RocksdbNormalKeySize)
	},
}

var RocksdbLongKeyPool = sync.Pool{
	New: func() interface{} {
		return buf.NewByteBufEx(RocksdbLongKeySize)
	},
}

func GetRocksdbNormalKey() *buf.ByteBufExt {
	return RocksdbNormalKeyPool.Get().(*buf.ByteBufExt)
}

func PutRocksdbNormalKey(buf *buf.ByteBufExt) {
	buf.Reset()
	RocksdbNormalKeyPool.Put(buf)
}

func GetRocksdbLongKey() *buf.ByteBufExt {
	return RocksdbLongKeyPool.Get().(*buf.ByteBufExt)
}

func PutRocksdbLongKey(buf *buf.ByteBufExt) {
	buf.Reset()
	RocksdbLongKeyPool.Put(buf)
}

func (info *RocksBaseInfo) MarshalV0() (result []byte, err error) {
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
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.uniqID)); err != nil {
		panic(err)
	}
	return buff.Bytes(), nil
}

func (info *RocksBaseInfo) Marshal() (result []byte, err error) {
	buff := buf.NewByteBufEx(128)
	if err = buff.PutUint32(atomic.LoadUint32(&info.version)); err != nil {
		panic(err)
	}
	if err = buff.PutUint32(atomic.LoadUint32(&info.length)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.applyId)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.inodeCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.dentryCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.extendCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.multiCnt)); err != nil {
		panic(err)
	}
	info.persistentApplyId = info.applyId
	if err = buff.PutUint64(atomic.LoadUint64(&info.cursor)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txRbInodeCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txRbDentryCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txId)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.uniqID)); err != nil {
		panic(err)
	}
	return buff.Bytes(), nil
}

func (info *RocksBaseInfo) MarshalWithoutApplyIDV0() (result []byte, err error) {
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
	if err = binary.Write(buff, binary.BigEndian, atomic.LoadUint64(&info.uniqID)); err != nil {
		panic(err)
	}
	return buff.Bytes(), nil
}

func (info *RocksBaseInfo) MarshalWithoutApplyID() (result []byte, err error) {
	buff := buf.NewByteBufEx(128)
	if err = buff.PutUint32(atomic.LoadUint32(&info.version)); err != nil {
		panic(err)
	}
	if err = buff.PutUint32(atomic.LoadUint32(&info.length)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.persistentApplyId)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.inodeCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.dentryCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.extendCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.multiCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.cursor)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txRbInodeCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txRbDentryCnt)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.txId)); err != nil {
		panic(err)
	}
	if err = buff.PutUint64(atomic.LoadUint64(&info.uniqID)); err != nil {
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
	if err = binary.Read(buff, binary.BigEndian, &info.uniqID); err != nil {
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

func (r *RocksTree) warpPartitionKeyV0(id uint64, k []byte) (key []byte) {
	buff := bytes.NewBuffer([]byte{})
	binary.Write(buff, binary.BigEndian, id)
	buff.Write(k)
	key = buff.Bytes()
	return
}

func (r *RocksTree) warpKeyV0(k []byte) (key []byte) {
	key = r.warpPartitionKeyV0(r.partitionId, k)
	return
}

func (r *RocksTree) GetRocksdbNormalKey(tableType byte) *buf.ByteBufExt {
	buf := GetRocksdbNormalKey()
	err := buf.PutUint64(r.partitionId)
	if err != nil {
		panic(err)
	}
	err = buf.WriteByte(tableType)
	if err != nil {
		panic(err)
	}
	return buf
}

func (r *RocksTree) GetRocksdbLongKey(tableType byte) *buf.ByteBufExt {
	buf := GetRocksdbLongKey()
	err := buf.PutUint64(r.partitionId)
	if err != nil {
		panic(err)
	}
	err = buf.WriteByte(tableType)
	if err != nil {
		panic(err)
	}
	return buf
}

func (r *RocksTree) LoadBaseInfo() error {
	keyBuf := r.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()
	buff, err := r.GetBytes(baseKey)
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

func (r *RocksTree) GetUniqID() uint64 {
	return atomic.LoadUint64(&r.baseInfo.uniqID)
}

func (r *RocksTree) SetUniqID(id uint64) {
	atomic.StoreUint64(&r.baseInfo.uniqID, id)
}

func (r *RocksTree) CreateBatchWriteHandle() (interface{}, error) {
	return r.db.CreateBatchHandler()
}

func (r *RocksTree) ReleaseBatchHandle(handle interface{}) (err error) {
	return r.db.ReleaseBatchHandle(handle)
}

func (r *RocksTree) AddItemToBatch(handle interface{}, key []byte, value []byte) error {
	return r.db.AddItemToBatch(handle, key, value)
}

func (r *RocksTree) SaveToDb(key []byte, value []byte) error {
	return r.db.Put(key, value)
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

	keyBuf := r.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()

	if err = r.AddItemToBatch(handle, baseKey, buffBaseInfo); err != nil {
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

	keyBuf := r.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()

	if err = r.AddItemToBatch(handle, baseKey, buffBaseInfo); err != nil {
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

	keyBuf := r.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()

	if err = r.SaveToDb(baseKey, buffBaseInfo); err != nil {
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
func (r *RocksTree) Flush(block bool) error {
	return r.db.Flush(block)
}

func (r *RocksTree) Count(tp TreeType) (uint64, error) {
	keyBuf := r.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()

	baseInfoBytes, err := r.GetBytes(baseKey)
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
	err = r.db.RangeWithSnap(start, end, snap, iter)
	return
}

func (r *RocksTree) DescRangeWithSnap(start []byte, end []byte, snap *gorocksdb.Snapshot, iter func(k, v []byte) (bool, error)) (err error) {
	err = r.db.DescRangeWithSnap(start, end, snap, iter)
	return
}

func (r *RocksTree) OpenSnap() (snap *gorocksdb.Snapshot) {
	return r.db.OpenSnap()
}

func (r *RocksTree) ReleaseSnap(snap *gorocksdb.Snapshot) {
	r.db.ReleaseSnap(snap)
}

func (r *RocksTree) IteratorCount(tableType TableType) uint64 {
	startBuf := r.GetRocksdbNormalKey(byte(tableType))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := r.GetRocksdbNormalKey(byte(tableType) + 1)
	defer PutRocksdbNormalKey(endBuf)
	start := startBuf.Bytes()
	end := endBuf.Bytes()

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
	err = r.db.RangeWithSnapByPrefix(prefix, start, end, snap, cb)
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
	return r.db.GetBytes(key)
}

func (r *RocksTree) GetBytesWithSnap(snap *gorocksdb.Snapshot, key []byte) ([]byte, error) {
	return r.db.GetBytesWithSnap(snap, key)
}

func (r *RocksTree) Put(handle interface{}, count *uint64, key []byte, value []byte) error {
	var err error
	lock := &r.latch[key[RocksdbTypeIndex]]
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

func (r *RocksTree) Update(handle interface{}, key []byte, value []byte) (err error) {
	lock := &r.latch[key[RocksdbTypeIndex]]
	lock.Lock()
	defer lock.Unlock()

	if err = r.AddItemToBatch(handle, key, value); err != nil {
		return
	}
	return
}

func (r *RocksTree) Create(handle interface{}, count *uint64, key []byte, value []byte, force bool) (ok bool, v []byte, err error) {
	lock := &r.latch[key[RocksdbTypeIndex]]
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

// CreateWithoutGet is a fast path for snapshot replay: it skips existence check
// and does not read the current value, only enqueuing the put and bumping count.
func (r *RocksTree) CreateWithoutGet(handle interface{}, count *uint64, key []byte, value []byte) (err error) {
	lock := &r.latch[key[RocksdbTypeIndex]]
	lock.Lock()
	defer lock.Unlock()

	if count != nil {
		atomic.AddUint64(count, 1)
	}

	if err = r.AddItemToBatch(handle, key, value); err != nil {
		return
	}
	return
}

func (r *RocksTree) DelItemToBatch(handle interface{}, key []byte) (err error) {
	err = r.db.DelItemToBatch(handle, key)
	return
}

// Has checks if the key exists in the btree. return is exist and err
func (r *RocksTree) Delete(handle interface{}, count *uint64, key []byte) (ok bool, err error) {
	has := false
	lock := &r.latch[key[RocksdbTypeIndex]]
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
	if err = r.DelItemToBatch(handle, key); err != nil {
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
	err = r.db.DelRangeToBatch(handle, start, end)
	return
}

func (r *RocksTree) DeleteMetadata(handle interface{}) (err error) {
	r.baseInfo.applyId = 0
	r.baseInfo.inodeCnt = 0
	r.baseInfo.dentryCnt = 0
	r.baseInfo.extendCnt = 0
	r.baseInfo.multiCnt = 0
	r.baseInfo.persistentApplyId = 0
	r.baseInfo.cursor = 0
	r.baseInfo.txCnt = 0
	r.baseInfo.txRbInodeCnt = 0
	r.baseInfo.txRbDentryCnt = 0
	r.baseInfo.txId = 0
	r.baseInfo.uniqID = 0

	keyBuf := r.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()

	err = r.db.DelItemToBatch(handle, baseKey)
	return
}

func (r *RocksTree) GetStoreMode() proto.StoreMode {
	return proto.StoreModeRocksDb
}

func (r *RocksTree) GetApplyIdFromDisk() (uint64, error) {
	keyBuf := r.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()

	buff, err := r.db.GetBytesFromDisk(baseKey)
	if err != nil {
		return 0, err
	}

	if len(buff) == 0 {
		return 0, nil
	}

	var baseInfo RocksBaseInfo
	if err = baseInfo.Unmarshal(buff); err != nil {
		return 0, err
	}

	return baseInfo.applyId, nil
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

func inodeEncodingKeyV0(ino uint64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(InodeTable))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func inodeEncodingKey(keyBuf *buf.ByteBufExt, ino uint64) []byte {
	keyBuf.PutUint64(ino)
	return keyBuf.Bytes()
}

func dentryEncodingKeyV0(parentId uint64, name string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	buff.WriteByte(0)
	buff.WriteString(name)
	return buff.Bytes()
}

func dentryEncodingKey(keyBuf *buf.ByteBufExt, parentId uint64, name string) []byte {
	keyBuf.PutUint64(parentId)
	keyBuf.WriteByte(0)
	keyBuf.WriteString(name)
	return keyBuf.Bytes()
}

func dentryEncodingPrefix(keyBuf *buf.ByteBufExt, parentId uint64, name string) []byte {
	keyBuf.PutUint64(parentId)
	keyBuf.WriteByte(0)
	if name != "" {
		keyBuf.WriteString(name)
	}
	return keyBuf.Bytes()
}

func extendEncodingKeyV0(ino uint64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(ExtendTable))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func extendEncodingKey(keyBuf *buf.ByteBufExt, ino uint64) []byte {
	keyBuf.PutUint64(ino)
	return keyBuf.Bytes()
}

func multipartEncodingKeyV0(key string, id string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(MultipartTable))
	buff.WriteString(key)
	buff.WriteByte(0)
	buff.WriteString(id)
	return buff.Bytes()
}

func multipartEncodingKey(keyBuf *buf.ByteBufExt, key string, id string) []byte {
	keyBuf.WriteString(key)
	keyBuf.WriteByte(0)
	keyBuf.WriteString(id)
	return keyBuf.Bytes()
}

func multipartEncodingPrefix(keyBuf *buf.ByteBufExt, key string, id string) []byte {
	keyBuf.WriteString(key)
	keyBuf.WriteByte(0)
	if id != "" {
		keyBuf.WriteString(id)
	}
	return keyBuf.Bytes()
}

func transactionEncodingKeyV0(txId string) []byte {
	buff := &bytes.Buffer{}
	buff.WriteByte(byte(TransactionTable))
	buff.WriteString(txId)
	return buff.Bytes()
}

func transactionEncodingKey(keyBuf *buf.ByteBufExt, txId string) []byte {
	keyBuf.WriteString(txId)
	return keyBuf.Bytes()
}

func transactionRollbackInodeEncodingKeyV0(ino uint64) []byte {
	buff := &bytes.Buffer{}
	buff.WriteByte(byte(TransactionRollbackInodeTable))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func transactionRollbackInodeEncodingKey(keyBuf *buf.ByteBufExt, ino uint64) []byte {
	keyBuf.PutUint64(ino)
	return keyBuf.Bytes()
}

func transactionRollbackDentryEncodingKeyV0(parentId uint64, name string) []byte {
	buff := &bytes.Buffer{}
	buff.WriteByte(byte(TransactionRollbackDentryTable))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	buff.WriteString(name)
	return buff.Bytes()
}

func transactionRollbackDentryEncodingKey(keyBuf *buf.ByteBufExt, parentId uint64, name string) []byte {
	keyBuf.PutUint64(parentId)
	keyBuf.WriteString(name)
	return keyBuf.Bytes()
}

func transactionRollbackDentryEncodingPrefix(keyBuf *buf.ByteBufExt, parentId uint64, name string) []byte {
	keyBuf.PutUint64(parentId)
	if name != "" {
		keyBuf.WriteString(name)
	}
	return keyBuf.Bytes()
}

func (b *InodeRocks) GetMaxInode() (uint64, error) {
	snapshot := b.RocksTree.OpenSnap()
	if snapshot == nil {
		return 0, errors.NewErrorf("open snapshot failed")
	}
	defer b.RocksTree.ReleaseSnap(snapshot)
	startBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(InodeTable) + 1)
	defer PutRocksdbNormalKey(endBuf)
	start := startBuf.Bytes()
	end := endBuf.Bytes()

	var maxInode uint64 = 0
	err := b.DescRangeWithSnap(start, end, snapshot, func(k, v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if e := inode.Unmarshal(v); e != nil {
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
	return 0
}

func (b *DeletedObjExtentsRocks) Count() uint64 {
	return 0
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

	keyBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	var bs []byte
	bs, err = b.RocksTree.GetBytes(inodeEncodingKey(keyBuf, ino.Inode))
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	inode := NewInode(0, 0)
	if err = inode.Unmarshal(bs); err != nil {
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

	keyBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)

	var bs, key []byte
	key = dentryEncodingKey(keyBuf, dent.ParentId, dent.Name)
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

	keyBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)

	var bs, key []byte
	key = extendEncodingKey(keyBuf, extent.inode)
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

	keyBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)

	var bs, encodingKey []byte
	encodingKey = multipartEncodingKey(keyBuf, multi.key, multi.id)
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

	keyBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)

	var bs, encodingKey []byte
	encodingKey = transactionEncodingKey(keyBuf, tx.TxID)
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

	keyBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	var bs, encodingKey []byte
	encodingKey = transactionRollbackInodeEncodingKey(keyBuf, inode.inode.Inode)
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

	keyBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)

	var bs, encodingKey []byte
	encodingKey = transactionRollbackDentryEncodingKey(keyBuf, dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name)
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
func (b *InodeRocks) Put(handle interface{}, inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}

	keyBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	if err = b.RocksTree.Put(handle, &b.baseInfo.inodeCnt, inodeEncodingKey(keyBuf, inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks put failed, inode:%v, error:%v", inode, err)
	}
	if b.baseInfo.cursor < inode.Inode {
		b.SetCursor(inode.Inode)
	}
	return
}

func (b *DentryRocks) Put(handle interface{}, dentry *Dentry) (err error) {
	var bs []byte
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("DentryRocks dentry marshal failed, dentry:%v, error:%v", dentry, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)

	if err = b.RocksTree.Put(handle, &b.baseInfo.dentryCnt, dentryEncodingKey(keyBuf, dentry.ParentId, dentry.Name), bs); err != nil {
		log.LogErrorf("DentryRocks put failed, dentry:%v, error:%v", dentry, err)
	}
	return
}

func (b *ExtendRocks) Put(handle interface{}, extend *Extend) (err error) {
	var bs []byte
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("ExtendRocks extend marshal failed, extend:%v, error:%v", extend, err)
		return
	}

	keyBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)

	if err = b.RocksTree.Put(handle, &b.baseInfo.extendCnt, extendEncodingKey(keyBuf, extend.inode), bs); err != nil {
		log.LogErrorf("ExtendRocks extend put failed, extend:%v, error:%v", extend, err)
	}
	return
}

func (b *MultipartRocks) Put(handle interface{}, multipart *Multipart) (err error) {
	var bs []byte
	bs, err = multipart.Bytes()
	if err != nil {
		log.LogErrorf("MultipartRocks multipart marshal failed, multipart:%v, error:%v", multipart, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)

	if err = b.RocksTree.Put(handle, &b.baseInfo.multiCnt, multipartEncodingKey(keyBuf, multipart.key, multipart.id), bs); err != nil {
		log.LogErrorf("MultipartRocks multipart put failed, multipart:%v, error:%v", multipart, err)
	}
	return
}

func (b *TransactionRocks) Put(handle interface{}, tx *proto.TransactionInfo) (err error) {
	var bs []byte
	if bs, err = tx.Marshal(); err != nil {
		log.LogErrorf("TransactionRocks tx marshal failed, tx: %v, error: %v", tx, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)

	if err = b.RocksTree.Put(handle, &b.baseInfo.txCnt, transactionEncodingKey(keyBuf, tx.TxID), bs); err != nil {
		log.LogErrorf("TransactionRocks tx put failed, tx: %v, error: %v", tx, err)
		return
	}
	return
}

func (b *TransactionRollbackInodeRocks) Put(handle interface{}, ino *TxRollbackInode) (err error) {
	var bs []byte
	if bs, err = ino.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino marshal failed, ino: %v, error: %v", ino, err)
		return
	}

	keyBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	if err = b.RocksTree.Put(handle, &b.baseInfo.txRbInodeCnt, transactionRollbackInodeEncodingKey(keyBuf, ino.inode.Inode), bs); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino put failed, ino: %v, error: %v", ino, err)
		return
	}
	return
}

func (b *TransactionRollbackDentryRocks) Put(handle interface{}, dentry *TxRollbackDentry) (err error) {
	var bs []byte
	if bs, err = dentry.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry marshal failed, dentry: %v, error: %v", dentry, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)

	err = b.RocksTree.Put(handle, &b.baseInfo.txRbDentryCnt, transactionRollbackDentryEncodingKey(keyBuf, dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name), bs)
	if err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry put failed, dentry: %v, error: %v", dentry, err)
		return
	}
	return
}

// update
func (b *InodeRocks) Update(handle interface{}, inode *Inode) (err error) {
	var bs []byte
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("InodeRocks inode marshal failed, inode:%v, error:%v", inode, err)
		return
	}

	keyBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	if err = b.RocksTree.Update(handle, inodeEncodingKey(keyBuf, inode.Inode), bs); err != nil {
		log.LogErrorf("InodeRocks inode update failed, inode:%v, error:%v", inode, err)
	}
	return
}

func (b *DentryRocks) Update(handle interface{}, dentry *Dentry) (err error) {
	var bs []byte
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("DentryRocks dentry marshal failed, dentry:%v, error:%v", dentry, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)

	if err = b.RocksTree.Update(handle, dentryEncodingKey(keyBuf, dentry.ParentId, dentry.Name), bs); err != nil {
		log.LogErrorf("DentryRocks dentry update failed, dentry:%v, error:%v", dentry, err)
	}
	return
}

func (b *ExtendRocks) Update(handle interface{}, extend *Extend) (err error) {
	var bs []byte
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("ExtendRocks extend marshal failed, extend:%v, error:%v", extend, err)
		return
	}

	keyBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)

	if err = b.RocksTree.Update(handle, extendEncodingKey(keyBuf, extend.inode), bs); err != nil {
		log.LogErrorf("ExtendRocks extend update failed, extend:%v, error:%v", extend, err)
	}
	return
}

func (b *MultipartRocks) Update(handle interface{}, multipart *Multipart) (err error) {
	var bs []byte
	bs, err = multipart.Bytes()
	if err != nil {
		log.LogErrorf("MultipartRocks multipart marshal failed, multipart:%v, error:%v", multipart, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)

	if err = b.RocksTree.Update(handle, multipartEncodingKey(keyBuf, multipart.key, multipart.id), bs); err != nil {
		log.LogErrorf("MultipartRocks multipart update failed, multipart:%v, error:%v", multipart, err)
	}
	return
}

func (b *TransactionRocks) Update(handle interface{}, tx *proto.TransactionInfo) (err error) {
	var bs []byte
	if bs, err = tx.Marshal(); err != nil {
		log.LogErrorf("TransactionRocks tx marshal failed, tx: %v, error: %v", tx, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)

	if err = b.RocksTree.Update(handle, transactionEncodingKey(keyBuf, tx.TxID), bs); err != nil {
		log.LogErrorf("MultipartRocks tx update failed, tx: %v, error: %v", tx, err)
	}
	return
}

func (b *TransactionRollbackInodeRocks) Update(handle interface{}, ino *TxRollbackInode) (err error) {
	var bs []byte
	if bs, err = ino.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino marshal failed, ino: %v, error: %v", ino, err)
		return
	}

	keyBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	if err = b.RocksTree.Update(handle, transactionRollbackInodeEncodingKey(keyBuf, ino.inode.Inode), bs); err != nil {
		log.LogErrorf("TransactionRollbackInodeRocks ino update failed, ino: %v, error: %v", ino, err)
		return
	}
	return
}

func (b *TransactionRollbackDentryRocks) Update(handle interface{}, dentry *TxRollbackDentry) (err error) {
	var bs []byte
	if bs, err = dentry.Marshal(); err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry marshal failed, dentry: %v, error: %v", dentry, err)
		return
	}

	keyBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)

	err = b.RocksTree.Update(handle, transactionRollbackDentryEncodingKey(keyBuf, dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name), bs)
	if err != nil {
		log.LogErrorf("TransactionRollbackDentryRocks dentry update failed, dentry: %v, error: %v", dentry, err)
		return
	}
	return
}

// Create if exists , return old, false,   if not  return nil , true
func (b *InodeRocks) ReplaceOrInsert(handle interface{}, inode *Inode, replace bool) (ino *Inode, ok bool, err error) {
	var key, bs, v []byte

	keyBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	key = inodeEncodingKey(keyBuf, inode.Inode)
	bs, err = inode.Marshal()
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] haskey error %v, %v", key, err)
		return
	}

	ok, v, err = b.RocksTree.Create(handle, &b.baseInfo.inodeCnt, key, bs, replace)
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] inodeRocks error %v, %v", key, err)
		return
	}

	if b.baseInfo.cursor < inode.Inode {
		b.SetCursor(inode.Inode)
	}

	if !ok {
		if len(v) == 0 {
			log.LogErrorf("[InodeRocksCreate] invalid value len, inode:%v", inode)
			err = ErrInvalidRocksdbValueLen
			return
		}
		// exist
		ino = NewInode(0, 0)
		if err = ino.Unmarshal(v); err != nil {
			log.LogErrorf("[InodeRocksCreate] unmarshal exist inode value failed, inode:%v, err:%v", inode, err)
			return
		}
		return
	}
	ino = inode
	return
}

// Insert inserts without prior GetBytes, for snapshot replay fast path.
func (b *InodeRocks) Insert(handle interface{}, inode *Inode) (err error) {
	keyBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	key := inodeEncodingKey(keyBuf, inode.Inode)
	bs, err := inode.Marshal()
	if err != nil {
		log.LogErrorf("[InodeRocksCreateSnapshot] marshal error %v, %v", key, err)
		return
	}

	if err = b.RocksTree.CreateWithoutGet(handle, &b.baseInfo.inodeCnt, key, bs); err != nil {
		log.LogErrorf("[InodeRocksCreateSnapshot] write error %v, %v", key, err)
		return
	}
	if b.baseInfo.cursor < inode.Inode {
		b.SetCursor(inode.Inode)
	}
	return
}

func (b *DentryRocks) ReplaceOrInsert(handle interface{}, dentry *Dentry, replace bool) (den *Dentry, ok bool, err error) {
	var key, bs, v []byte

	keyBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)

	key = dentryEncodingKey(keyBuf, dentry.ParentId, dentry.Name)
	bs, err = dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocks] marshal: %v, err: %v", dentry, err)
		return
	}

	ok, v, err = b.RocksTree.Create(handle, &b.baseInfo.dentryCnt, key, bs, replace)
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

// Insert inserts without prior GetBytes, for snapshot replay fast path.
func (b *DentryRocks) Insert(handle interface{}, dentry *Dentry) (err error) {
	keyBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)

	key := dentryEncodingKey(keyBuf, dentry.ParentId, dentry.Name)
	bs, err := dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocksSnapshot] marshal: %v, err: %v", dentry, err)
		return
	}

	if err = b.RocksTree.CreateWithoutGet(handle, &b.baseInfo.dentryCnt, key, bs); err != nil {
		log.LogErrorf("[DentryRocksSnapshot] write dentry: %v key: %v, err: %v", dentry, key, err)
		return
	}
	return
}

func (b *ExtendRocks) ReplaceOrInsert(handle interface{}, extend *Extend, replace bool) (ext *Extend, ok bool, err error) {
	var key, bs, v []byte

	keyBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)

	key = extendEncodingKey(keyBuf, extend.inode)
	bs, err = extend.Bytes()
	if err != nil {
		log.LogErrorf("[ExtendRocks] marshal: %v, err: %v", extend, err)
		return
	}

	ok, v, err = b.RocksTree.Create(handle, &b.baseInfo.extendCnt, key, bs, replace)
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

// Insert inserts without prior GetBytes, for snapshot replay fast path.
func (b *ExtendRocks) Insert(handle interface{}, extend *Extend) (err error) {
	keyBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)

	key := extendEncodingKey(keyBuf, extend.inode)
	bs, err := extend.Bytes()
	if err != nil {
		log.LogErrorf("[ExtendRocksSnapshot] marshal: %v, err: %v", extend, err)
		return
	}

	if err = b.RocksTree.CreateWithoutGet(handle, &b.baseInfo.extendCnt, key, bs); err != nil {
		log.LogErrorf("[ExtendRocksSnapshot] write extend: %v key: %v, err: %v", extend, key, err)
		return
	}
	return
}

func (b *MultipartRocks) ReplaceOrInsert(handle interface{}, mul *Multipart, replace bool) (multipart *Multipart, ok bool, err error) {
	var key, bs, v []byte

	keyBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)

	key = multipartEncodingKey(keyBuf, mul.key, mul.id)
	bs, err = mul.Bytes()
	if err != nil {
		log.LogErrorf("[MultipartRocks] marshal: %v, err: %v", mul, err)
		return
	}

	ok, v, err = b.RocksTree.Create(handle, &b.baseInfo.multiCnt, key, bs, replace)
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

// Insert inserts without prior GetBytes, for snapshot replay fast path.
func (b *MultipartRocks) Insert(handle interface{}, mul *Multipart) (err error) {
	keyBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)

	key := multipartEncodingKey(keyBuf, mul.key, mul.id)
	bs, err := mul.Bytes()
	if err != nil {
		log.LogErrorf("[MultipartRocksSnapshot] marshal: %v, err: %v", mul, err)
		return
	}

	if err = b.RocksTree.CreateWithoutGet(handle, &b.baseInfo.multiCnt, key, bs); err != nil {
		log.LogErrorf("[MultipartRocksSnapshot] write multipart: %v key: %v, err: %v", mul, key, err)
		return
	}
	return
}

func (b *TransactionRocks) ReplaceOrInsert(handle interface{}, tx *proto.TransactionInfo, replace bool) (transaction *proto.TransactionInfo, ok bool, err error) {
	var key, bs, v []byte

	keyBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)

	key = transactionEncodingKey(keyBuf, tx.TxID)
	bs, err = tx.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRocks] marshal: %v, err: %v", tx, err)
		return
	}

	ok, v, err = b.RocksTree.Create(handle, &b.baseInfo.txCnt, key, bs, replace)
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

// Insert inserts without prior GetBytes, for snapshot replay fast path.
func (b *TransactionRocks) Insert(handle interface{}, tx *proto.TransactionInfo) (err error) {
	keyBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)

	key := transactionEncodingKey(keyBuf, tx.TxID)
	bs, err := tx.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRocksSnapshot] marshal: %v, err: %v", tx, err)
		return
	}

	if err = b.RocksTree.CreateWithoutGet(handle, &b.baseInfo.txCnt, key, bs); err != nil {
		log.LogErrorf("[TransactionRocksSnapshot] write tx: %v key: %v, err: %v", tx, key, err)
		return
	}
	return
}

func (b *TransactionRollbackInodeRocks) ReplaceOrInsert(handle interface{}, ino *TxRollbackInode, replace bool) (inode *TxRollbackInode, ok bool, err error) {
	var key, bs, v []byte

	keyBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	key = transactionRollbackInodeEncodingKey(keyBuf, ino.inode.Inode)
	bs, err = ino.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRollbackInodeRocks] marshal: %v, err: %v", ino, err)
		return
	}
	ok, v, err = b.RocksTree.Create(handle, &b.baseInfo.txRbInodeCnt, key, bs, replace)
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

// Insert inserts without prior GetBytes, for snapshot replay fast path.
func (b *TransactionRollbackInodeRocks) Insert(handle interface{}, ino *TxRollbackInode) (err error) {
	keyBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	key := transactionRollbackInodeEncodingKey(keyBuf, ino.inode.Inode)
	bs, err := ino.Marshal()
	if err != nil {
		log.LogErrorf("[TxRbInodeRocksSnapshot] marshal error %v, %v", key, err)
		return
	}

	if err = b.RocksTree.CreateWithoutGet(handle, &b.baseInfo.txRbInodeCnt, key, bs); err != nil {
		log.LogErrorf("[TxRbInodeRocksSnapshot] write error %v, %v", key, err)
		return
	}
	return
}

func (b *TransactionRollbackDentryRocks) ReplaceOrInsert(handle interface{}, den *TxRollbackDentry, replace bool) (dentry *TxRollbackDentry, ok bool, err error) {
	var key, bs, v []byte

	keyBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)

	key = transactionRollbackDentryEncodingKey(keyBuf, den.txDentryInfo.ParentId, den.txDentryInfo.Name)
	bs, err = den.Marshal()
	if err != nil {
		log.LogErrorf("[TransactionRollbackDentryRocks] marshal: %v, err: %v", den, err)
		return
	}
	ok, v, err = b.RocksTree.Create(handle, &b.baseInfo.txRbDentryCnt, key, bs, replace)
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

// Insert inserts without prior GetBytes, for snapshot replay fast path.
func (b *TransactionRollbackDentryRocks) Insert(handle interface{}, den *TxRollbackDentry) (err error) {
	keyBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)

	key := transactionRollbackDentryEncodingKey(keyBuf, den.txDentryInfo.ParentId, den.txDentryInfo.Name)
	bs, err := den.Marshal()
	if err != nil {
		log.LogErrorf("[TxRbDentryRocksSnapshot] marshal error %v, %v", key, err)
		return
	}

	if err = b.RocksTree.CreateWithoutGet(handle, &b.baseInfo.txRbDentryCnt, key, bs); err != nil {
		log.LogErrorf("[TxRbDentryRocksSnapshot] write error %v, %v", key, err)
		return
	}
	return
}

// Delete
func (b *InodeRocks) Delete(handle interface{}, inode *Inode) (bool, error) {
	keyBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	return b.RocksTree.Delete(handle, &b.baseInfo.inodeCnt, inodeEncodingKey(keyBuf, inode.Inode))
}

func (b *DentryRocks) Delete(handle interface{}, dentry *Dentry) (bool, error) {
	keyBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(keyBuf)

	return b.RocksTree.Delete(handle, &b.baseInfo.dentryCnt, dentryEncodingKey(keyBuf, dentry.ParentId, dentry.Name))
}

func (b *ExtendRocks) Delete(handle interface{}, extend *Extend) (bool, error) {
	keyBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(keyBuf)

	return b.RocksTree.Delete(handle, &b.baseInfo.extendCnt, extendEncodingKey(keyBuf, extend.inode))
}

func (b *MultipartRocks) Delete(handle interface{}, mutipart *Multipart) (bool, error) {
	keyBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(keyBuf)

	return b.RocksTree.Delete(handle, &b.baseInfo.multiCnt, multipartEncodingKey(keyBuf, mutipart.key, mutipart.id))
}

func (b *TransactionRocks) Delete(handle interface{}, txId string) (bool, error) {
	keyBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(keyBuf)

	return b.RocksTree.Delete(handle, &b.baseInfo.txCnt, transactionEncodingKey(keyBuf, txId))
}

func (b *TransactionRollbackInodeRocks) Delete(handle interface{}, inode *TxRollbackInode) (bool, error) {
	keyBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(keyBuf)

	return b.RocksTree.Delete(handle, &b.baseInfo.txRbInodeCnt, transactionRollbackInodeEncodingKey(keyBuf, inode.inode.Inode))
}

func (b *TransactionRollbackDentryRocks) Delete(handle interface{}, dentry *TxRollbackDentry) (bool, error) {
	keyBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(keyBuf)

	return b.RocksTree.Delete(handle, &b.baseInfo.txRbDentryCnt, transactionRollbackDentryEncodingKey(keyBuf, dentry.txDentryInfo.ParentId, dentry.txDentryInfo.Name))
}

// Range begin
// Range , if end is nil , it will range all of this type , it range not include end
func (b *InodeRocks) Range(start, end *Inode, cb func(i *Inode) bool) error {
	var (
		startByte    []byte
		endByte      []byte
		callBackFunc func(v []byte) (bool, error)
	)

	startBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(InodeTable) + 1)
	defer PutRocksdbNormalKey(endBuf)

	if end != nil {
		inodeEncodingKey(endBuf, end.Inode)
	}
	startByte = startBuf.Bytes()
	endByte = endBuf.Bytes()

	callBackFunc = func(v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if err := inode.Unmarshal(v); err != nil {
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

	startBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(DentryTable) + 1)
	defer PutRocksdbLongKey(endBuf)

	if start != nil {
		dentryEncodingKey(startBuf, start.ParentId, start.Name)
	}
	if end != nil {
		dentryEncodingKey(endBuf, end.ParentId, end.Name)
	}
	startByte = startBuf.Bytes()
	endByte = endBuf.Bytes()

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

	prefixBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(prefixBuf)
	startBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(DentryTable) + 1)
	defer PutRocksdbLongKey(endBuf)

	if end != nil {
		dentryEncodingKey(endBuf, end.ParentId, end.Name)
	}

	if start != nil && start.ParentId != 0 {
		dentryEncodingKey(startBuf, start.ParentId, start.Name)
	}

	if prefix != nil {
		dentryEncodingPrefix(prefixBuf, prefix.ParentId, prefix.Name)
	}
	prefixByte = prefixBuf.Bytes()
	startByte = startBuf.Bytes()
	endByte = endBuf.Bytes()

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

	startBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(ExtendTable) + 1)
	defer PutRocksdbNormalKey(endBuf)

	if end != nil {
		extendEncodingKey(endBuf, end.inode)
	}
	startByte = startBuf.Bytes()
	endByte = endBuf.Bytes()

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
	startBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(MultipartTable) + 1)
	defer PutRocksdbLongKey(endBuf)

	if end != nil {
		multipartEncodingKey(endBuf, end.key, end.id)
	}
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

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
	prefixBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(prefixBuf)
	startBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(MultipartTable) + 1)
	defer PutRocksdbLongKey(endBuf)

	if end != nil {
		multipartEncodingKey(endBuf, end.key, end.id)
	}

	if start != nil {
		multipartEncodingKey(startBuf, start.key, start.id)
	}

	if prefix != nil {
		multipartEncodingPrefix(prefixBuf, prefix.key, prefix.id)
	}
	prefixByte := prefixBuf.Bytes()
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	callBackFunc := func(v []byte) (bool, error) {
		mul := MultipartFromBytes(v)
		return cb(mul), nil
	}
	return b.RocksTree.RangeWithPrefix(prefixByte, startByte, endByte, callBackFunc)
}

func (b *TransactionRocks) Range(start, end *proto.TransactionInfo, cb func(tx *proto.TransactionInfo) bool) error {
	startBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(TransactionTable) + 1)
	defer PutRocksdbLongKey(endBuf)

	if end != nil {
		transactionEncodingKey(endBuf, end.TxID)
	}

	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

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
	startBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable) + 1)
	defer PutRocksdbNormalKey(endBuf)

	if end != nil {
		transactionRollbackInodeEncodingKey(endBuf, end.inode.Inode)
	}

	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

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
	startBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable) + 1)
	defer PutRocksdbLongKey(endBuf)

	if end != nil {
		transactionRollbackDentryEncodingKey(endBuf, end.txDentryInfo.ParentId, end.txDentryInfo.Name)
	}

	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

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
	prefixBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(prefixBuf)
	startBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable) + 1)
	defer PutRocksdbLongKey(endBuf)

	if end != nil {
		transactionRollbackDentryEncodingKey(endBuf, end.txDentryInfo.ParentId, end.txDentryInfo.Name)
	}
	if start != nil {
		transactionRollbackDentryEncodingKey(startBuf, start.txDentryInfo.ParentId, start.txDentryInfo.Name)
	}
	if prefix != nil {
		transactionRollbackDentryEncodingPrefix(prefixBuf, prefix.txDentryInfo.ParentId, prefix.txDentryInfo.Name)
	}

	prefixByte := prefixBuf.Bytes()
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

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

	startBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(InodeTable) + 1)
	defer PutRocksdbNormalKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err := b.DescRangeWithSnap(startByte, endByte, snapshot, func(k, v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if e := inode.Unmarshal(v); e != nil {
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
	startBuf := b.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(InodeTable) + 1)
	defer PutRocksdbNormalKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err = b.DelRangeToBatch(handle, startByte, endByte)
	return
}

func (b *DentryRocks) Clear(handle interface{}) (err error) {
	startBuf := b.GetRocksdbLongKey(byte(DentryTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(DentryTable) + 1)
	defer PutRocksdbLongKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err = b.DelRangeToBatch(handle, startByte, endByte)
	return
}

func (b *ExtendRocks) Clear(handle interface{}) (err error) {
	startBuf := b.GetRocksdbNormalKey(byte(ExtendTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(ExtendTable) + 1)
	defer PutRocksdbNormalKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err = b.DelRangeToBatch(handle, startByte, endByte)
	return
}

func (b *MultipartRocks) Clear(handle interface{}) (err error) {
	startBuf := b.GetRocksdbLongKey(byte(MultipartTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(MultipartTable) + 1)
	defer PutRocksdbLongKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err = b.DelRangeToBatch(handle, startByte, endByte)
	return
}

func (b *TransactionRocks) Clear(handle interface{}) (err error) {
	startBuf := b.GetRocksdbLongKey(byte(TransactionTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(TransactionTable) + 1)
	defer PutRocksdbLongKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err = b.DelRangeToBatch(handle, startByte, endByte)
	return
}

func (b *TransactionRollbackInodeRocks) Clear(handle interface{}) (err error) {
	startBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := b.GetRocksdbNormalKey(byte(TransactionRollbackInodeTable) + 1)
	defer PutRocksdbNormalKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err = b.DelRangeToBatch(handle, startByte, endByte)
	return
}

func (b *TransactionRollbackDentryRocks) Clear(handle interface{}) (err error) {
	startBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable))
	defer PutRocksdbLongKey(startBuf)
	endBuf := b.GetRocksdbLongKey(byte(TransactionRollbackDentryTable) + 1)
	defer PutRocksdbLongKey(endBuf)
	startByte := startBuf.Bytes()
	endByte := endBuf.Bytes()

	err = b.DelRangeToBatch(handle, startByte, endByte)
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
	if mp.db == nil {
		log.LogErrorf("NewRocksSnapShot the mp.db is nil")
		return nil
	}
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

	keyBuf := rocksTree.GetRocksdbNormalKey(byte(BaseInfoType))
	defer PutRocksdbNormalKey(keyBuf)
	baseKey := keyBuf.Bytes()

	v, err := rocksTree.GetBytesWithSnap(s, baseKey)
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
	}
	return count
}

func (r *RocksSnapShot) Range(tp TreeType, cb func(item interface{}) bool) error {
	tableType := getTableTypeKey(tp)
	callbackFunc := func(k, v []byte) (bool, error) {
		switch tp {
		case InodeType:
			inode := NewInode(0, 0)
			if err := inode.Unmarshal(v); err != nil {
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
	startBuf := r.tree.GetRocksdbNormalKey(byte(tableType))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := r.tree.GetRocksdbNormalKey(byte(tableType) + 1)
	defer PutRocksdbNormalKey(endBuf)
	startBytes := startBuf.Bytes()
	endBytes := endBuf.Bytes()

	return r.tree.RangeWithSnap(startBytes, endBytes, r.snap, callbackFunc)
}

func (r *RocksSnapShot) RangeReuseInode(cb func(item *Inode) bool) error {
	inode := NewInode(0, 0)
	callbackFunc := func(k, v []byte) (bool, error) {
		inode.ResetValue()
		if err := inode.Unmarshal(v); err != nil {
			return false, err
		}
		return cb(inode), nil
	}
	startBuf := r.tree.GetRocksdbNormalKey(byte(InodeTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := r.tree.GetRocksdbNormalKey(byte(InodeTable) + 1)
	defer PutRocksdbNormalKey(endBuf)
	startBytes := startBuf.Bytes()
	endBytes := endBuf.Bytes()

	return r.tree.RangeWithSnap(startBytes, endBytes, r.snap, callbackFunc)
}

func (r *RocksSnapShot) RangeReuseDentry(cb func(item *Dentry) bool) error {
	dentry := new(Dentry)
	callbackFunc := func(k, v []byte) (bool, error) {
		dentry.ResetValue()
		if err := dentry.Unmarshal(v); err != nil {
			return false, err
		}
		return cb(dentry), nil
	}
	startBuf := r.tree.GetRocksdbNormalKey(byte(DentryTable))
	defer PutRocksdbNormalKey(startBuf)
	endBuf := r.tree.GetRocksdbNormalKey(byte(DentryTable) + 1)
	defer PutRocksdbNormalKey(endBuf)
	startBytes := startBuf.Bytes()
	endBytes := endBuf.Bytes()

	return r.tree.RangeWithSnap(startBytes, endBytes, r.snap, callbackFunc)
}

func (r *RocksSnapShot) Close() {
	if r.snap == nil {
		return
	}
	r.tree.ReleaseSnap(r.snap)
}

func (r *RocksSnapShot) ApplyID() uint64 {
	return r.baseInfo.applyId
}

func (r *RocksSnapShot) TxID() uint64 {
	return r.baseInfo.txId
}

func (b *InodeRocks) SetInodeCount(count uint64) {
	atomic.StoreUint64(&b.baseInfo.inodeCnt, count)
}

func (b *InodeRocks) GetApproximateSizes() (size uint64, err error) {
	startBuf := b.GetRocksdbNormalKey(byte(InodeType))
	defer PutRocksdbNormalKey(startBuf)
	startKey := startBuf.Bytes()
	endBuf := b.GetRocksdbNormalKey(byte(InodeType) + 1)
	defer PutRocksdbNormalKey(endBuf)
	endKey := endBuf.Bytes()
	size, err = b.db.GetApproximateSizes(startKey, endKey)
	return
}

func (b *InodeRocks) GetLevelNum() (int, error) {
	return b.db.GetLevelNum()
}

func (b *InodeRocks) GetLevelNumMap() (map[string]int, error) {
	return b.db.GetLevelNumMap()
}

func (b *InodeRocks) GetStatistics() string {
	return b.db.GetStatistics()
}

func (b *InodeRocks) GetProperty(property string) (string, error) {
	return b.db.GetProperty(property)
}
