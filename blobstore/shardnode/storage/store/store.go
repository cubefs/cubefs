package store

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/util/errors"

	"github.com/cubefs/cubefs/blobstore/common/kvstorev2"
)

type Config struct {
	KVOption   kvstore.Option  `json:"kv_option"`
	RaftOption kvstore.Option  `json:"raft_option"`
	Path       string          `json:"-"`
	HandleEIO  func(err error) `json:"-"`
}

type Store struct {
	kvStore      kvstore.Store
	raftStore    kvstore.Store
	defaultRawFS RawFS
	handleError  func(err error)

	cfg *Config
}

func NewStore(ctx context.Context, cfg *Config) (*Store, error) {
	handleError := func(err error) {
		if err == nil {
			return
		}
		if IsEIO(err) && cfg.HandleEIO != nil {
			cfg.HandleEIO(err)
		}
	}

	kvStorePath := cfg.Path + "/kv"
	// disable kv wal to optimized latency
	cfg.KVOption.DisableWal = true
	cfg.KVOption.HandleError = handleError
	kvStore, err := kvstore.NewKVStore(ctx, kvStorePath, kvstore.RocksdbLsmKVType, &cfg.KVOption)
	if err != nil {
		return nil, errors.Info(err, "open kv store failed")
	}

	raftStorePath := cfg.Path + "/raft"
	cfg.RaftOption.HandleError = handleError
	raftStore, err := kvstore.NewKVStore(ctx, raftStorePath, kvstore.RocksdbLsmKVType, &cfg.RaftOption)
	if err != nil {
		return nil, errors.Info(err, "open raft store failed")
	}

	return &Store{
		kvStore:      kvStore,
		raftStore:    raftStore,
		defaultRawFS: &posixRawFS{path: cfg.Path + "/raw", handleError: handleError},
		handleError:  handleError,
		cfg:          cfg,
	}, nil
}

func (s *Store) KVStore() kvstore.Store {
	return s.kvStore
}

func (s *Store) RaftStore() kvstore.Store {
	return s.raftStore
}

func (s *Store) NewRawFS(path string) RawFS {
	return &posixRawFS{path: s.cfg.Path + "/" + path, handleError: s.handleError}
}

func (s *Store) DefaultRawFS() RawFS {
	return s.defaultRawFS
}

func (s *Store) Stats() (Stats, error) {
	return StatFS(s.cfg.Path)
}
