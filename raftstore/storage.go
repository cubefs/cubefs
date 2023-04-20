package raftstore

import (
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage"
)

type StorageListener interface {
	StoredEntry(entry *proto.Entry)
	StoredHardState(st proto.HardState)
}

type funcStorageListener struct {
	storedEntryFunc     func(entry *proto.Entry)
	storedHardStateFunc func(st proto.HardState)
}

func (f *funcStorageListener) StoredHardState(st proto.HardState) {
	if f != nil && f.storedHardStateFunc != nil {
		f.storedHardStateFunc(st)
	}
}

func (f *funcStorageListener) StoredEntry(entry *proto.Entry) {
	if f != nil && f.storedEntryFunc != nil {
		f.storedEntryFunc(entry)
	}
}

type StorageListenerBuilder struct {
	ln *funcStorageListener
}

func (b *StorageListenerBuilder) ListenStoredEntry(f func(entry *proto.Entry)) *StorageListenerBuilder {
	b.ln.storedEntryFunc = f
	return b
}

func (b *StorageListenerBuilder) ListenStoredHardState(f func(st proto.HardState)) *StorageListenerBuilder {
	b.ln.storedHardStateFunc = f
	return b
}

func (b *StorageListenerBuilder) Build() StorageListener {
	return b.ln
}

func NewStorageListenerBuilder() *StorageListenerBuilder {
	return &StorageListenerBuilder{
		ln: &funcStorageListener{},
	}
}

type listenableStorage struct {
	storage.Storage
	ln StorageListener
}

func (s *listenableStorage) StoreEntries(entries []*proto.Entry) error {
	if err := s.Storage.StoreEntries(entries); err != nil {
		return err
	}
	if s.ln != nil {
		for _, entry := range entries {
			s.ln.StoredEntry(entry)
		}
	}
	return nil
}

func (s *listenableStorage) StoreHardState(st proto.HardState) error {
	if err := s.Storage.StoreHardState(st); err != nil {
		return err
	}
	if s.ln != nil {
		s.ln.StoredHardState(st)
	}
	return nil
}

func listenStorage(s storage.Storage, ln StorageListener) storage.Storage {
	return &listenableStorage{
		Storage: s,
		ln:      ln,
	}
}
