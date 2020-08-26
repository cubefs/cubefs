package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tecbot/gorocksdb"
)

var readOption = gorocksdb.NewDefaultReadOptions()
var writeOption = gorocksdb.NewDefaultWriteOptions()
var flushOption = gorocksdb.NewDefaultFlushOptions()

func init() {
	readOption.SetFillCache(false)
	writeOption.SetSync(false)
}

type RocksTree struct {
	dir            string
	db             *gorocksdb.DB
	currentApplyID uint64
	sync.Mutex
}

func DefaultRocksTree(dir string) (*RocksTree, error) {
	return NewRocksTree(dir, 256*util.MB, 4*util.MB)
}

func NewRocksTree(dir string, lruCacheSize int, writeBufferSize int) (*RocksTree, error) {
	if stat, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				log.LogInfof("NewRocksTree mkidr error: dir: %v, err: %v", dir, err)
				return nil, err
			}
			log.LogInfof("create dir:[%s] for rocks tree", dir)
		} else {
			return nil, err
		}
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("path:[%s] is not dir", dir)
	} else {
		log.LogInfof("load dir:[%s] for rocks tree", dir)
	}

	tree := &RocksTree{dir: dir}
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(lruCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(writeBufferSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)
	db, err := gorocksdb.OpenDb(opts, tree.dir)
	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return nil, err
	}
	tree.db = db
	return tree, nil
}

func (r *RocksTree) SetApplyID(id uint64) {
	atomic.StoreUint64(&r.currentApplyID, id)
}

func (r *RocksTree) GetApplyID() (uint64, error) {
	apply, err := r.GetBytes(applyIDKey)
	if err != nil {
		return 0, err
	}
	if len(apply) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(apply), nil

}

func (r *RocksTree) Flush() error {
	return r.db.Flush(flushOption)
}

var _ Snapshot = &RocksSnapShot{}

type RocksSnapShot struct {
	snap *gorocksdb.Snapshot
	tree *RocksTree
}

func (r *RocksSnapShot) Count(tp TreeType) (uint64, error) {
	var count uint64
	err := r.Range(tp, func(v []byte) (b bool, err error) {
		count += 1
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (r *RocksSnapShot) Range(tp TreeType, cb func(v []byte) (bool, error)) error {
	return r.tree.RangeWithSnap(r.snap, []byte{byte(tp)}, []byte{byte(tp) + 1}, cb)
}

func (r *RocksSnapShot) Close() {
	r.tree.db.ReleaseSnapshot(r.snap)
}

// This requires global traversal to call carefully
func (r *RocksTree) Count(tp TreeType) uint64 {
	start, end := []byte{byte(tp)}, byte(tp)+1
	var count uint64
	snapshot := r.db.NewSnapshot()
	it := r.Iterator(snapshot)
	defer func() {
		it.Close()
		r.db.ReleaseSnapshot(snapshot)
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

func (r *RocksTree) RangeWithSnap(snapshot *gorocksdb.Snapshot, start, end []byte, cb func(v []byte) (bool, error)) error {
	it := r.Iterator(snapshot)
	defer func() {
		it.Close()
	}()
	return r.RangeWithIter(it, start, end, cb)
}

func (r *RocksTree) RangeWithIter(it *gorocksdb.Iterator, start []byte, end []byte, cb func(v []byte) (bool, error)) error {
	it.Seek(start)
	for ; it.ValidForPrefix(start); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		if bytes.Compare(end, key) < 0 {
			break
		}
		if hasNext, err := cb(value); err != nil {
			log.LogErrorf("[RocksTree] RangeWithIter key: %v value: %v err: %v", key, value, err)
			return err
		} else if !hasNext {
			return nil
		}
	}
	return nil
}

func (r *RocksTree) Range(start, end []byte, cb func(v []byte) (bool, error)) error {
	snapshot := r.db.NewSnapshot()
	defer func() {
		r.db.ReleaseSnapshot(snapshot)
	}()
	return r.RangeWithSnap(snapshot, start, end, cb)
}

func (r *RocksTree) Iterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetSnapshot(snapshot)
	return r.db.NewIterator(ro)
}

// Has checks if the key exists in the btree.
func (r *RocksTree) HasKey(key []byte) (bool, error) {
	bs, err := r.GetBytes(key)
	if err != nil {
		return false, err
	}
	return len(bs) > 0, nil
}

// Has checks if the key exists in the btree.
func (r *RocksTree) GetBytes(key []byte) ([]byte, error) {
	return r.db.GetBytes(readOption, key)
}

// Has checks if the key exists in the btree.
func (r *RocksTree) Put(key []byte, value []byte) error {
	batch := gorocksdb.NewWriteBatch()
	batch.Put(key, value)
	apply := make([]byte, 8)
	binary.BigEndian.PutUint64(apply, r.currentApplyID)
	batch.Put(applyIDKey, apply)
	return r.db.Write(writeOption, batch)
}

// drop the current btree.
func (r *RocksTree) Release() {
	if r.db != nil {
		r.Lock()
		defer r.Unlock()
		r.db.Close()
		r.db = nil
	}
}

func (r *RocksTree) Clear() error {
	if r.db != nil {
		r.Lock()
		defer r.Unlock()
		snapshot := r.db.NewSnapshot()
		it := r.Iterator(snapshot)
		defer func() {
			it.Close()
			r.db.ReleaseSnapshot(snapshot)
		}()
		start := []byte{0}
		it.Seek(start)
		batch := gorocksdb.NewWriteBatch()
		for ; it.ValidForPrefix(start); it.Next() {
			key := it.Key().Data()
			batch.Delete(key)
			if batch.Count() > 10000 {
				if err := r.db.Write(writeOption, batch); err != nil {
					return err
				}
			}
		}
		if batch.Count() > 0 {
			if err := r.db.Write(writeOption, batch); err != nil {
				return err
			}
		}
		return r.db.Flush(flushOption)
	}
	return nil
}

var _ InodeTree = &InodeRocks{}
var _ DentryTree = &DentryRocks{}
var _ ExtendTree = &ExtendRocks{}
var _ MultipartTree = &MultipartRocks{}

type InodeRocks struct {
	*RocksTree
}
type DentryRocks struct {
	*RocksTree
}
type ExtendRocks struct {
	*RocksTree
}

type MultipartRocks struct {
	*RocksTree
}

func inodeEncodingKey(ino uint64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(InodeType))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func dentryEncodingKey(parentId uint64, name string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(DentryType))
	_ = binary.Write(buff, binary.BigEndian, parentId)
	buff.WriteString(name)
	return buff.Bytes()
}

func extendEncodingKey(ino uint64) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(ExtendType))
	_ = binary.Write(buff, binary.BigEndian, ino)
	return buff.Bytes()
}

func multipartEncodingKey(key string, id string) []byte {
	buff := new(bytes.Buffer)
	buff.WriteByte(byte(MultipartType))
	_ = binary.Write(buff, binary.BigEndian, int(len(key)))
	buff.WriteString(key)
	buff.WriteString(id)
	return buff.Bytes()
}

// count by type
func (b *InodeRocks) Count() uint64 {
	return b.RocksTree.Count(InodeType)
}

func (b *DentryRocks) Count() uint64 {
	return b.RocksTree.Count(DentryType)
}

func (b *ExtendRocks) Count() uint64 {
	return b.RocksTree.Count(ExtendType)
}

func (b *MultipartRocks) Count() uint64 {
	return b.RocksTree.Count(MultipartType)
}

//Get
func (b *InodeRocks) RefGet(ino uint64) (*Inode, error) {
	return b.Get(ino)
}
func (b *InodeRocks) Get(ino uint64) (*Inode, error) {
	bs, err := b.RocksTree.GetBytes(inodeEncodingKey(ino))
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	inode := &Inode{}
	if err := inode.Unmarshal(bs); err != nil {
		return nil, err
	}
	return inode, nil
}

func (b *DentryRocks) RefGet(ino uint64, name string) (*Dentry, error) {
	return b.Get(ino, name)
}
func (b *DentryRocks) Get(ino uint64, name string) (*Dentry, error) {
	key := dentryEncodingKey(ino, name)
	bs, err := b.RocksTree.GetBytes(key)
	if err != nil {
		log.LogErrorf("[DentryRocks] Get parentId: %v, name: %v, error: %v", ino, name, err)
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	dentry := &Dentry{}
	if err := dentry.Unmarshal(bs); err != nil {
		log.LogErrorf("[DentryRocks] Get unmarshal error parentId: %v, name: %v, error: %v", ino, name, err)
		return nil, err
	}
	return dentry, nil
}

func (b *ExtendRocks) RefGet(ino uint64) (*Extend, error) {
	return b.Get(ino)
}
func (b *ExtendRocks) Get(ino uint64) (*Extend, error) {
	bs, err := b.RocksTree.GetBytes(extendEncodingKey(ino))
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	return NewExtendFromBytes(bs)
}

func (b *MultipartRocks) RefGet(key, id string) (*Multipart, error) {
	return b.Get(key, id)
}
func (b *MultipartRocks) Get(key, id string) (*Multipart, error) {
	bs, err := b.RocksTree.GetBytes(multipartEncodingKey(key, id))
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, nil
	}
	return MultipartFromBytes(bs), nil
}

func (b *InodeRocks) Update(inode *Inode) error {
	return b.Put(inode)
}

//put inode into rocksdb
func (b *InodeRocks) Put(inode *Inode) error {
	bs, err := inode.Marshal()
	if err != nil {
		return err
	}
	return b.RocksTree.Put(inodeEncodingKey(inode.Inode), bs)
}

func (b *DentryRocks) Put(dentry *Dentry) error {
	bs, err := dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocks] Put dentry %v error %v", dentry, err)
		return err
	}
	return b.RocksTree.Put(dentryEncodingKey(dentry.ParentId, dentry.Name), bs)
}

func (b *ExtendRocks) Update(extend *Extend) error {
	return b.Put(extend)
}

func (b *ExtendRocks) Put(extend *Extend) error {
	bs, err := extend.Bytes()
	if err != nil {
		return err
	}
	return b.RocksTree.Put(extendEncodingKey(extend.inode), bs)
}

func (b *MultipartRocks) Update(mutipart *Multipart) error {
	return b.Put(mutipart)
}

func (b *MultipartRocks) Put(mutipart *Multipart) error {
	bs, err := mutipart.Bytes()
	if err != nil {
		return err
	}
	return b.RocksTree.Put(multipartEncodingKey(mutipart.key, mutipart.id), bs)
}

//Create if exists , return old, false,   if not  return nil , true
func (b *InodeRocks) Create(inode *Inode) error {
	key := inodeEncodingKey(inode.Inode)
	if has, err := b.HasKey(key); err != nil {
		log.LogErrorf("[InodeRocksCreate] haskey error %v, %v", key, err)
		return err
	} else if has {
		return existsError
	}

	bs, err := inode.Marshal()
	if err != nil {
		log.LogErrorf("[InodeRocksCreate] haskey error %v, %v", key, err)
		return err
	}

	if err = b.RocksTree.Put(key, bs); err != nil {
		log.LogErrorf("[InodeRocksCreate] inodeRocks error %v, %v", key, err)
		return err
	}
	return nil
}

func (b *DentryRocks) Create(dentry *Dentry) error {
	key := dentryEncodingKey(dentry.ParentId, dentry.Name)

	if has, err := b.HasKey(key); err != nil {
		log.LogErrorf("[DentryRocks] Failed to has Key: %v, err: %v", key, err)
		return err
	} else if has {
		log.LogErrorf("[DentryRocks] has Key: %v", key)
		return existsError
	}

	bs, err := dentry.Marshal()
	if err != nil {
		log.LogErrorf("[DentryRocks] marshal: %v, err: %v", dentry, err)
		return err
	}

	if err = b.RocksTree.Put(key, bs); err != nil {
		log.LogErrorf("[DentryRocks] Put dentry: %v key: %v, err: %v", dentry, key, err)
		return err
	}

	return nil
}

func (b *ExtendRocks) Create(ext *Extend) error {
	key := extendEncodingKey(ext.inode)

	if has, err := b.HasKey(key); err != nil {
		return err
	} else if has {
		return existsError
	}

	bs, err := ext.Bytes()
	if err != nil {
		return err
	}

	if err = b.RocksTree.Put(key, bs); err != nil {
		return err
	}
	return nil
}

func (b *MultipartRocks) Create(mul *Multipart) error {
	key := multipartEncodingKey(mul.key, mul.id)

	if has, err := b.HasKey(key); err != nil {
		return err
	} else if has {
		return existsError
	}

	bs, err := mul.Bytes()
	if err != nil {
		return err
	}

	if err = b.RocksTree.Put(key, bs); err != nil {
		return err
	}
	return nil
}

//Delete
func (b *InodeRocks) Delete(ino uint64) error {
	return b.db.Delete(writeOption, inodeEncodingKey(ino))
}
func (b *DentryRocks) Delete(pid uint64, name string) error {
	return b.db.Delete(writeOption, dentryEncodingKey(pid, name))
}
func (b *ExtendRocks) Delete(ino uint64) error {
	return b.db.Delete(writeOption, extendEncodingKey(ino))
}
func (b *MultipartRocks) Delete(key, id string) error {
	return b.db.Delete(writeOption, multipartEncodingKey(key, id))
}

// Range begin
//Range , if end is nil , it will range all of this type , it range not include end
func (b *InodeRocks) Range(start, end *Inode, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
	)
	startByte = inodeEncodingKey(start.Inode)
	if end == nil {
		endByte = []byte{byte(InodeType) + 1}
	} else {
		endByte = inodeEncodingKey(end.Inode)
	}
	return b.RocksTree.Range(startByte, endByte, cb)
}

//Range , if end is nil , it will range all of this type , it range not include end
func (b *DentryRocks) Range(start, end *Dentry, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
	)
	startByte = dentryEncodingKey(start.ParentId, start.Name)
	if end == nil {
		endByte = []byte{byte(DentryType) + 1}
	} else {
		endByte = dentryEncodingKey(end.ParentId, end.Name)
	}
	return b.RocksTree.Range(startByte, endByte, cb)
}

//Range , if end is nil , it will range all of this type , it range not include end
func (b *ExtendRocks) Range(start, end *Extend, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
	)
	startByte = extendEncodingKey(start.inode)
	if end == nil {
		endByte = []byte{byte(ExtendType) + 1}
	} else {
		endByte = extendEncodingKey(end.inode)
	}
	return b.RocksTree.Range(startByte, endByte, cb)
}

//Range , if end is nil , it will range all of this type , it range not include end
func (b *MultipartRocks) Range(start, end *Multipart, cb func(v []byte) (bool, error)) error {
	var (
		startByte []byte
		endByte   []byte
	)
	startByte = multipartEncodingKey(start.key, start.id)
	if end == nil {
		endByte = []byte{byte(MultipartType) + 1}
	} else {
		endByte = multipartEncodingKey(end.key, end.id)
	}
	return b.RocksTree.Range(startByte, endByte, cb)
}
