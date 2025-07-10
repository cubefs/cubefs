package locstore

import (
	"fmt"
	"log"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/tecbot/gorocksdb"
)

type RocksDBStore struct {
	db *gorocksdb.DB // RocksDB instance for storing key-location mappings
}

func NewRocksDBMeta(dbDir string) (LocationStore, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	if db, err := gorocksdb.OpenDb(opts, dbDir); err != nil {
		return nil, err
	} else {
		return &RocksDBStore{
			db: db,
		}, nil
	}
}

func (r *RocksDBStore) Put(key string, loc proto.Location) error {
	keyBytes := []byte(key)
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	if err := r.db.Put(wo, keyBytes, loc.Encode()); err != nil {
		return fmt.Errorf("failed to store key-location mapping in RocksDB: %v", err)
	}
	return nil
}

func (r *RocksDBStore) Get(key string) (proto.Location, error) {
	keyBytes := []byte(key)
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	locationBytes, err := r.db.Get(ro, keyBytes)
	if err != nil {
		return proto.Location{}, fmt.Errorf("failed to retrieve location from RocksDB: %v", err)
	}
	defer locationBytes.Free()

	if locationBytes.Size() == 0 {
		return proto.Location{}, fmt.Errorf("key '%s''s not exist in RocksDB", key)
	}

	// Parse location
	var location proto.Location
	if _, err := location.Decode(locationBytes.Data()); err != nil {
		return proto.Location{}, fmt.Errorf("failed to parse location: %v", err)
	}
	return location, nil
}

func (r *RocksDBStore) Del(key string) error {
	keyBytes := []byte(key)
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	if err := r.db.Delete(wo, keyBytes); err != nil {
		return fmt.Errorf("failed to delete key '%s' from RocksDB: %v", key, err)
	}

	return nil
}

func (r *RocksDBStore) Close() {
	flushOpts := gorocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	err := r.db.Flush(flushOpts)
	if err != nil {
		log.Printf("Failed to flush database: %v", err)
	}

	r.db.Close()
}
