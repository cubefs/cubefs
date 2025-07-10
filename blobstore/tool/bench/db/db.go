package db

import (
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// DB defines metadata operations
type DB interface {
	Put(key string, location proto.Location) error
	Get(key string) (location proto.Location, err error)
	Del(key string) error
	Close() error
}

// memorydb implements DB with map
type memorydb struct {
	db sync.Map
}

func NewMemoryDB() (DB, error) {
	return &memorydb{}, nil
}

func (m *memorydb) Put(key string, loc proto.Location) error {
	m.db.Store(key, loc)
	return nil
}

func (m *memorydb) Get(key string) (proto.Location, error) {
	val, _ := m.db.Load(key)
	if val == nil {
		return proto.Location{}, fmt.Errorf("key(%s) not found", key)
	}
	return val.(proto.Location), nil
}

func (m *memorydb) Del(key string) error {
	m.db.Delete(key)
	return nil
}

func (r *memorydb) Close() error {
	return nil
}
