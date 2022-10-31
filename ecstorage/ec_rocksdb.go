package ecstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/unit"
	"github.com/tecbot/gorocksdb"
	"os"
	"sync"
	"sync/atomic"
)

const (
	DefLRUCacheSize  = 256 * unit.MB
	DefWriteBuffSize = 4 * unit.MB
)

type EcRocksDbInfo struct {
	dir            string
	defReadOption  *gorocksdb.ReadOptions
	defWriteOption *gorocksdb.WriteOptions
	defFlushOption *gorocksdb.FlushOptions
	db             *gorocksdb.DB
	wait           sync.WaitGroup
}

func NewEcRocksDb() (dbInfo *EcRocksDbInfo) {
	dbInfo = &EcRocksDbInfo{}
	return
}

func (dbInfo *EcRocksDbInfo) OpenEcDb(dir string) (err error) {
	stat, statErr := os.Stat(dir)
	if statErr == nil && !stat.IsDir() {
		log.LogErrorf("interOpenDb path:[%s] is not dir", dir)
		return fmt.Errorf("path:[%s] is not dir", dir)
	}

	if err != nil && !os.IsNotExist(err) {
		log.LogErrorf("interOpenDb stat error: dir: %v, err: %v", dir, err)
		return err
	}

	//mkdir all  will return nil when path exist and path is dir
	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		log.LogErrorf("interOpenDb mkdir error: dir: %v, err: %v", dir, err)
		return err
	}

	log.LogInfof("rocks db dir:[%s]", dir)

	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(DefLRUCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(DefWriteBuffSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)
	//opts.SetParanoidChecks(true)
	dbInfo.db, err = gorocksdb.OpenDb(opts, dir)
	if err != nil {
		log.LogErrorf("interOpenDb open db err:%v", err)
		return err
	}
	dbInfo.dir = dir
	dbInfo.defReadOption = gorocksdb.NewDefaultReadOptions()
	dbInfo.defWriteOption = gorocksdb.NewDefaultWriteOptions()
	dbInfo.defFlushOption = gorocksdb.NewDefaultFlushOptions()
	return nil
}

func (dbInfo *EcRocksDbInfo) CloseEcDb() (err error) {
	//wait batch or snap release
	dbInfo.wait.Wait()

	dbInfo.db.Close()
	dbInfo.defReadOption.Destroy()
	dbInfo.defWriteOption.Destroy()
	dbInfo.defFlushOption.Destroy()

	dbInfo.db = nil
	dbInfo.defReadOption = nil
	dbInfo.defWriteOption = nil
	dbInfo.defFlushOption = nil
	return
}

func NewEcRocksdbHandle(newDir string) (db *EcRocksDbInfo, err error) {
	_, err = os.Stat(newDir)
	if err != nil && os.IsNotExist(err) {
		os.MkdirAll(newDir, 0x755)
		err = nil
	}
	db = NewEcRocksDb()
	if err = db.OpenEcDb(newDir); err != nil {
		log.LogErrorf("open db failed, error(%v)", err)
		return
	}
	return
}

func (dbInfo *EcRocksDbInfo) GetBytes(key []byte) (bytes []byte, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("rocksdb GetBytes failed, error:%v", err)
		}
	}()

	if bytes, err = dbInfo.db.GetBytes(dbInfo.defReadOption, key); err != nil {
		return
	}
	return
}

func (dbInfo *EcRocksDbInfo) HasKey(key []byte) (bool, error) {
	bs, err := dbInfo.GetBytes(key)
	if err != nil {
		return false, err
	}
	return len(bs) > 0, nil
}

func (dbInfo *EcRocksDbInfo) Put(key, value []byte) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("rocksdb Put failed, error:%v", err)
		}
	}()
	if err = dbInfo.db.Put(dbInfo.defWriteOption, key, value); err != nil {
		return
	}
	return
}

func (dbInfo *EcRocksDbInfo) Del(key []byte) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("rocksdb Del failed, error:%v", err)
		}
	}()
	if err = dbInfo.db.Delete(dbInfo.defWriteOption, key); err != nil {
		return
	}
	return
}

func (ecDb *EcRocksDbInfo) GetEcBlockCrcInfo(partitionId, extentId, blockNo uint64) (crc, size uint32, exist bool, err error) {
	var (
		key   []byte
		value []byte
	)
	key, err = MarshalCrcMapKey(CrcMapTable, partitionId, extentId, blockNo)
	if err != nil {
		return
	}
	exist, err = ecDb.HasKey(key)
	if err != nil || !exist {
		return
	}

	value, err = ecDb.GetBytes(key)
	if err != nil {
		return
	}
	crc, size, err = UnMarshaCrcMapValue(value)
	exist = true
	return
}

func (ecDb *EcRocksDbInfo) PersistenceEcBlockCrc(partitionId, extentId, blockNo uint64, size, crc uint32) (err error) {
	var (
		key       []byte
		value     []byte
		blockSize uint32
	)

	key, err = MarshalCrcMapKey(CrcMapTable, partitionId, extentId, blockNo)
	if err != nil {
		return
	}

	exist, err := ecDb.HasKey(key)
	if err != nil {
		return
	}
	if exist {
		value, err = ecDb.GetBytes(key)
		if err != nil {
			return
		}
		_, blockSize, err = UnMarshaCrcMapValue(value)
		if err != nil {
			return
		}
	} else {
		blockSize = size
	}

	value, err = MarshalCrcMapValue(crc, blockSize)
	if err != nil {
		return
	}
	err = ecDb.Put(key, value)
	return
}

func (ecDb *EcRocksDbInfo) getEcTinyDeleteCount(partitionID uint64) (count int64, err error) {
	count = 0
	it := ecDb.db.NewIterator(ecDb.defReadOption)
	defer it.Close()
	keyPrefix, err := MarshalTinyDelPartitionIdPrefixKey(TinyDeleteMapTable, partitionID)
	if err != nil {
		return
	}
	for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Next() {
		count++
	}
	return
}

func (ecDb *EcRocksDbInfo) DeleteExtentMeta(partitionID, extentId uint64) (err error) {
	it := ecDb.db.NewIterator(ecDb.defReadOption)
	defer it.Close()
	batch := gorocksdb.NewWriteBatch()
	keyCrcPrefix, err := MarshalCrcMapPrefixKey(CrcMapTable, partitionID, extentId)
	if err != nil {
		return
	}
	for it.Seek(keyCrcPrefix); it.ValidForPrefix(keyCrcPrefix); it.Next() {
		key := it.Key().Data()
		batch.Delete(key)
	}
	if err = ecDb.db.Write(ecDb.defWriteOption, batch); err != nil {
		return
	}
	batch.Destroy()
	return
}

func (ecDb *EcRocksDbInfo) checkHasDelete(partitionID, extentID, offset uint64, hostIndex, deleteStatus uint32) bool {
	key, err := MarshalTinyDeleteMapKey(TinyDeleteMapTable, partitionID, extentID, offset, deleteStatus, hostIndex)
	if err != nil {
		return false
	}

	exist, err := ecDb.hasDeleteKey(key)
	if exist {
		return true
	}
	return false
}

func (ecDb *EcRocksDbInfo) hasDeleteKey(key []byte) (bool, error) {
	bs, err := ecDb.GetBytes(key)
	if err != nil {
		return false, err
	}
	if len(bs) > 0 { //exist
		return true, nil
	}
	return false, nil
}

func (ecDb *EcRocksDbInfo) deleteTinyDelInfoKey(partitionID, extentID, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
	key, err := MarshalTinyDeleteMapKey(TinyDeleteMapTable, partitionID, extentID, offset, deleteStatus, hostIndex)
	if err != nil {
		return
	}

	exist, err := ecDb.hasDeleteKey(key)
	if err != nil || !exist {
		return
	}

	err = ecDb.Del(key)
	return
}

func (ecDb *EcRocksDbInfo) ecRecordTinyDelete(partitionID, extentID, offset, size uint64, deleteStatus, hostIndex uint32) (err error) {
	log.LogDebugf("ecRecordTinyDelete partitionId(%v) extentId(%v) offset(%v) size(%v) deleteStatus(%v) hostIndex(%v)",
		partitionID, extentID, offset, size, deleteStatus, hostIndex)
	key, err := MarshalTinyDeleteMapKey(TinyDeleteMapTable, partitionID, extentID, offset, deleteStatus, hostIndex)
	if err != nil {
		return
	}

	if deleteStatus == TinyDeleteMark {
		exist := ecDb.checkHasDelete(partitionID, extentID, offset, hostIndex, TinyDeleting)
		if exist {
			return
		}
		exist = ecDb.checkHasDelete(partitionID, extentID, offset, hostIndex, TinyDeleted)
		if exist {
			return
		}
	} else if deleteStatus == TinyDeleting {
		exist := ecDb.checkHasDelete(partitionID, extentID, offset, hostIndex, TinyDeleted)
		if exist {
			return
		}
		err = ecDb.tinyDelInfoExtentIdRange(partitionID, extentID, TinyDeleting, func(delOffset, delSize uint64, delHostIndex uint32) (cbErr error) {
			if delOffset != offset || hostIndex != delHostIndex {
				cbErr = fmt.Errorf("partitionId(%v) extentId(%v) delOffset(%v) size(%v) has deleting task",
					partitionID, extentID, delOffset, delSize)
			}
			return
		})
		if err != nil {
			return
		}

		err = ecDb.deleteTinyDelInfoKey(partitionID, extentID, offset, size, TinyDeleteMark, hostIndex)
		if err != nil {
			return
		}
	} else {
		err = ecDb.deleteTinyDelInfoKey(partitionID, extentID, offset, size, TinyDeleteMark, hostIndex)
		if err != nil {
			return
		}
		err = ecDb.deleteTinyDelInfoKey(partitionID, extentID, offset, size, TinyDeleting, hostIndex)
		if err != nil {
			return
		}
	}

	value, err := MarshalTinyDeleteMapValue(size)
	if err != nil {
		return
	}
	err = ecDb.Put(key, value)
	return
}

func (ecDb *EcRocksDbInfo) persistMapRange(mapTable, partitionID uint64, cb func(extentId uint64, v []byte) (err error)) (err error) {
	var (
		extentId       uint64
		keyStartPrefix []byte
	)
	it := ecDb.db.NewIterator(ecDb.defReadOption)
	defer it.Close()
	keyStartPrefix, err = MarshalMapKeyPrefix(mapTable, partitionID)
	log.LogDebugf("partitionID(%v) loadPartitionPersistMap (%v)", partitionID, mapTable)
	for it.Seek(keyStartPrefix); it.ValidForPrefix(keyStartPrefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		_, _, extentId, err = UnMarshalMapKey(key)
		if err != nil {
			log.LogWarn(err)
			return
		}
		if err = cb(extentId, value); err != nil {
			return
		}
	}
	return
}

func (ecDb *EcRocksDbInfo) persistMapInfo(mapTable, partitionID, extentId uint64, data []byte) (err error) {
	key, err := MarshalMapKey(mapTable, partitionID, extentId)
	if err != nil {
		return
	}
	err = ecDb.db.Put(ecDb.defWriteOption, key, data)
	return
}

func (ecDb *EcRocksDbInfo) deleteMapInfo(mapTable, partitionID, extentId uint64) (err error) {
	key, err := MarshalMapKey(mapTable, partitionID, extentId)
	if err != nil {
		return
	}
	err = ecDb.db.Delete(ecDb.defWriteOption, key)
	return
}

func (ecDb *EcRocksDbInfo) tinyDelInfoRange(partitionID uint64, delStatus uint32, cb func(extentId, offset, size uint64, deleteStatus, hostIndex uint32) (err error)) (err error) {
	var (
		extentId     uint64
		offset       uint64
		size         uint64
		deleteStatus uint32
		hostIndex    uint32
		keyPrefix    []byte
	)
	it := ecDb.db.NewIterator(ecDb.defReadOption)
	defer it.Close()
	if delStatus == BaseDeleteMark {
		keyPrefix, err = MarshalTinyDelPartitionIdPrefixKey(TinyDeleteMapTable, partitionID)
	} else {
		keyPrefix, err = MarshalTinyDelStatusPrefixKey(TinyDeleteMapTable, partitionID, delStatus)
	}
	if err != nil {
		return
	}
	for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		_, partitionID, extentId, offset, deleteStatus, hostIndex, err = UnMarshalTinyDeleteMapKey(key)
		if err != nil {
			log.LogWarn(err)
			continue
		}
		size, err = UnMarshalTinyDeleteMapValue(value)
		if err != nil {
			log.LogWarn(err)
			continue
		}
		if err = cb(extentId, offset, size, deleteStatus, hostIndex); err != nil {
			log.LogWarn(err)
			continue
		}
	}
	return
}

func (ecDb *EcRocksDbInfo) tinyDelInfoExtentIdRange(partitionID, extentId uint64, deleteStatus uint32, cb func(offset, size uint64, hostIndex uint32) (cbErr error)) (err error) {
	var (
		offset    uint64
		size      uint64
		hostIndex uint32
	)
	it := ecDb.db.NewIterator(ecDb.defReadOption)
	defer it.Close()
	keyPrefix, err := MarshalTinyDelExtentIdPrefixKey(TinyDeleteMapTable, partitionID, extentId, deleteStatus)
	if err != nil {
		return
	}
	for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		_, _, extentId, offset, _, hostIndex, err = UnMarshalTinyDeleteMapKey(key)
		if err != nil {
			log.LogWarn(err)
			continue
		}
		size, err = UnMarshalTinyDeleteMapValue(value)
		if err != nil {
			log.LogWarn(err)
			continue
		}
		log.LogDebugf("tinyDelInfoExtentIdRange partition(%v) extentId(%v) offset(%v) size(%v)",
			partitionID, extentId, offset, size)
		if err = cb(offset, size, hostIndex); err != nil {
			log.LogWarn(err)
			break
		}
	}
	return
}

func (ecDb *EcRocksDbInfo) persistBlockCrcRange(partitionID, extentId uint64, cb func(blockNum uint64, BlockCrc, blockSize uint32) (err error)) (err error) {
	var (
		keyPrefix   []byte
		mapTable    uint64
		partitionId uint64
		blockNum    uint64
		blockCrc    uint32
		blockSize   uint32
	)
	keyPrefix, err = MarshalCrcMapPrefixKey(CrcMapTable, partitionID, extentId)
	if err != nil {
		return
	}
	it := ecDb.db.NewIterator(ecDb.defReadOption)
	defer it.Close()
	for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		mapTable, partitionId, extentId, blockNum, err = UnMarshalCrcMapKey(key)
		if mapTable != CrcMapTable || partitionId != partitionID {
			log.LogWarnf("rocksdb read error")
			break
		}
		blockCrc, blockSize, err = UnMarshaCrcMapValue(value)
		if err != nil {
			log.LogWarn(err)
			break
		}
		if err = cb(blockNum, blockCrc, blockSize); err != nil {
			break
		}
	}
	return
}

func MarshalMapKeyPrefix(mapTable, partitionId uint64) (keyPrefix []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 16))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func MarshalMapKey(mapTable, partitionId, extentId uint64) (key []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 24))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&extentId)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func UnMarshalMapKey(key []byte) (mapTable, partitionId, extentId uint64, err error) {
	buff := bytes.NewBuffer(key)
	if err = binary.Read(buff, binary.BigEndian, &mapTable); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &partitionId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &extentId); err != nil {
		return
	}
	return
}

func MarshalTinyDeleteMapKey(mapTable, partitionId, extentId, offset uint64, deleteStatus, hostIndex uint32) (key []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 40))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint32(&deleteStatus)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&extentId)); err != nil {
		panic(err)
	}

	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint32(&hostIndex)); err != nil {
		panic(err)
	}

	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&offset)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func UnMarshalTinyDeleteMapKey(key []byte) (mapTable, partitionId, extentId, offset uint64, deleteStatus, hostIndex uint32, err error) {
	buff := bytes.NewBuffer(key)
	if err = binary.Read(buff, binary.BigEndian, &mapTable); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &partitionId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &deleteStatus); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &extentId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &hostIndex); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &offset); err != nil {
		return
	}
	return
}

func MarshalTinyDelPartitionIdPrefixKey(mapTable, partitionId uint64) (key []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 16))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func MarshalTinyDelStatusPrefixKey(mapTable, partitionId uint64, deleteStatus uint32) (key []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 20))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint32(&deleteStatus)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func MarshalTinyDelExtentIdPrefixKey(mapTable, partitionId, extentId uint64, deleteStatus uint32) (key []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 28))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint32(&deleteStatus)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&extentId)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func MarshalTinyDeleteMapValue(size uint64) (value []byte, err error) {
	valueBuff := bytes.NewBuffer(make([]byte, 0, 8))
	if err = binary.Write(valueBuff, binary.BigEndian, atomic.LoadUint64(&size)); err != nil {
		panic(err)
	}
	return valueBuff.Bytes(), nil
}

func UnMarshalTinyDeleteMapValue(value []byte) (size uint64, err error) {
	buff := bytes.NewBuffer(value)
	if err = binary.Read(buff, binary.BigEndian, &size); err != nil {
		return
	}
	return
}

func MarshalCrcMapKey(mapTable, partitionId, extentId, blockNo uint64) (key []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 32))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&extentId)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&blockNo)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func UnMarshalCrcMapKey(key []byte) (mapTable, partitionId, extentId, blockNum uint64, err error) {
	buff := bytes.NewBuffer(key)
	if err = binary.Read(buff, binary.BigEndian, &mapTable); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &partitionId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &extentId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &blockNum); err != nil {
		return
	}
	return
}

func UnMarshaCrcMapValue(value []byte) (crc, size uint32, err error) {
	buff := bytes.NewBuffer(value)
	if err = binary.Read(buff, binary.BigEndian, &crc); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &size); err != nil {
		return
	}
	return
}

func MarshalCrcMapPrefixKey(mapTable, partitionId, extentId uint64) (key []byte, err error) {
	keyBuff := bytes.NewBuffer(make([]byte, 0, 32))
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&mapTable)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&partitionId)); err != nil {
		panic(err)
	}
	if err = binary.Write(keyBuff, binary.BigEndian, atomic.LoadUint64(&extentId)); err != nil {
		panic(err)
	}
	return keyBuff.Bytes(), nil
}

func MarshalCrcMapValue(crc, size uint32) (value []byte, err error) {
	valueBuff := bytes.NewBuffer(make([]byte, 0, 8))
	if err = binary.Write(valueBuff, binary.BigEndian, atomic.LoadUint32(&crc)); err != nil {
		panic(err)
	}
	if err = binary.Write(valueBuff, binary.BigEndian, atomic.LoadUint32(&size)); err != nil {
		panic(err)
	}
	return valueBuff.Bytes(), nil
}
