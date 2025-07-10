package db

import (
	"fmt"

	"github.com/tecbot/gorocksdb"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

// RocksDB implements DB with RocksDB
type RocksDB struct {
	db *gorocksdb.DB // RocksDB instance for storing key-location mappings
}

func NewRocksDB(dbDir string) (DB, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, dbDir)
	if err != nil {
		return nil, err
	}
	return &RocksDB{db: db}, nil
}

func (r *RocksDB) Put(key string, loc proto.Location) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	n := 40 + len(loc.Slices)*20
	buf := bytespool.AllocPointer(n)
	n = loc.Encode2(*buf)

	err := r.db.Put(wo, []byte(key), (*buf)[:n])
	bytespool.FreePointer(buf)
	if err != nil {
		return fmt.Errorf("key(%s) put to rocksdb: %v", key, err)
	}
	return nil
}

func (r *RocksDB) Get(key string) (proto.Location, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	locationBytes, err := r.db.Get(ro, []byte(key))
	if err != nil {
		return proto.Location{}, fmt.Errorf("key(%s) retrieve from rocksdb: %v", key, err)
	}
	defer locationBytes.Free()

	if locationBytes.Size() == 0 {
		return proto.Location{}, fmt.Errorf("key(%s) not exist in rocksdb", key)
	}

	var location proto.Location
	if _, err := location.Decode(locationBytes.Data()); err != nil {
		return proto.Location{}, fmt.Errorf("key(%s) parse location: %v", key, err)
	}
	return location, nil
}

func (r *RocksDB) Del(key string) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := r.db.Delete(wo, []byte(key)); err != nil {
		return fmt.Errorf("key(%s) delete from rocksdb: %v", key, err)
	}
	return nil
}

func (r *RocksDB) Close() error {
	flushOpts := gorocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	err := r.db.Flush(flushOpts)
	if err != nil {
		return fmt.Errorf("flush database: %v", err)
	}
	r.db.Close()
	return nil
}
