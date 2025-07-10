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

// memory implements DB with map
type memory struct {
	db sync.Map
}

func NewMemory() (DB, error) {
	return &memory{}, nil
}

func (m *memory) Put(key string, loc proto.Location) error {
	m.db.Store(key, loc)
	return nil
}

func (m *memory) Get(key string) (proto.Location, error) {
	val, _ := m.db.Load(key)
	if val == nil {
		return proto.Location{}, fmt.Errorf("key(%s) not found", key)
	}
	return val.(proto.Location), nil
}

func (m *memory) Del(key string) error {
	m.db.Delete(key)
	return nil
}

func (r *memory) Close() error {
	return nil
}
