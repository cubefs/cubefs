package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path"
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/tecbot/gorocksdb"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type rocksdbWal struct {
	db        *gorocksdb.DB
	hs        pb.HardState
	st        Snapshot
	lastIndex uint64
	once      sync.Once
}

const (
	kMeta byte = iota
	kLogEntry
)

func dbKey(typ byte, key uint64) []byte {
	var skey [9]byte
	skey[0] = typ
	binary.BigEndian.PutUint64(skey[1:], key)
	return skey[:]
}

func hardStateKey() []byte {
	return dbKey(kMeta, 0)
}

func snapshotKey() []byte {
	return dbKey(kMeta, 1)
}

func logKey(index uint64) []byte {
	return dbKey(kLogEntry, index)
}

func OpenRocksdbWal(dir string) (Wal, error) {
	dir = path.Clean(dir)
	if err := InitPath(dir, false); err != nil {
		return nil, err
	}
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}
	wal := &rocksdbWal{
		db: db,
	}
	defer func() {
		if err != nil {
			db.Close()
		}
	}()
	if err = wal.loadHardState(); err != nil {
		log.Errorf("load hardstate form db error: %v", err)
		return nil, err
	}
	if err = wal.loadSnapshot(); err != nil {
		log.Errorf("load snapshot form db error: %v", err)
		return nil, err
	}
	if err = wal.loadLastIndex(); err != nil {
		log.Errorf("load last index form db error: %v", err)
		return nil, err
	}
	if wal.lastIndex < wal.st.Index {
		wal.lastIndex = wal.st.Index
	}
	log.Infof("load wal success: lastindex=%d hs=%s snapshot=%v", wal.lastIndex, wal.hs.String(), wal.st)
	return wal, nil
}

func (w *rocksdbWal) loadHardState() error {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	val, err := w.db.GetBytes(ro, hardStateKey())
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return nil
	}
	return w.hs.Unmarshal(val)
}

func (w *rocksdbWal) loadSnapshot() error {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	val, err := w.db.GetBytes(ro, snapshotKey())
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return nil
	} else if len(val) != 16 {
		return errors.New("invalid snapshot")
	}
	w.st.Index = binary.BigEndian.Uint64(val)
	w.st.Term = binary.BigEndian.Uint64(val[8:])
	return nil
}

func (w *rocksdbWal) loadLastIndex() error {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := w.db.NewIterator(ro)
	defer func() {
		it.Close()
		ro.Destroy()
	}()
	it.SeekToLast()
	if err := it.Err(); err != nil {
		return err
	}

	prefix := make([]byte, 1)
	prefix[0] = kLogEntry
	if it.ValidForPrefix(prefix) {
		key := it.Key()
		skey := key.Data()
		key.Free()
		if len(skey) != 9 && skey[0] != kLogEntry {
			return errors.New("invalid log key")
		}
		w.lastIndex = binary.BigEndian.Uint64(skey[1:])
	}
	return nil
}

func (w *rocksdbWal) InitialState() pb.HardState {
	return w.hs
}

func (w *rocksdbWal) Entries(lo, hi uint64, maxSize uint64) (entries []pb.Entry, err error) {
	if lo >= hi {
		return nil, raft.ErrUnavailable
	} else if lo <= w.st.Index {
		return nil, raft.ErrCompacted
	}

	if hi > w.lastIndex+1 {
		return nil, fmt.Errorf("entries's hi(%d) is out of bound lastindex(%d)", hi, w.lastIndex)
	}

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetIterateUpperBound(logKey(hi))
	it := w.db.NewIterator(ro)
	defer func() {
		it.Close()
		ro.Destroy()
	}()
	for it.Seek(logKey(lo)); it.Valid(); it.Next() {
		entry := pb.Entry{}
		value := it.Value()
		if err := entry.Unmarshal(value.Data()); err != nil {
			value.Free()
			return nil, err
		}
		value.Free()
		entries = append(entries, entry)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (w *rocksdbWal) Term(index uint64) (term uint64, err error) {
	if index < w.st.Index {
		return 0, raft.ErrCompacted
	} else if index == w.st.Index {
		return w.st.Term, nil
	}
	if index > w.lastIndex {
		return 0, raft.ErrUnavailable
	}

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	val, err := w.db.GetBytes(ro, logKey(index))
	if err != nil {
		return 0, err
	}
	ent := pb.Entry{}
	if err = ent.Unmarshal(val); err != nil {
		return 0, err
	}
	return ent.Term, nil
}

func (w *rocksdbWal) FirstIndex() uint64 {
	return w.st.Index + 1
}

func (w *rocksdbWal) LastIndex() uint64 {
	return w.lastIndex
}

func (w *rocksdbWal) Save(hs pb.HardState, entries []pb.Entry) error {
	wb := gorocksdb.NewWriteBatch()
	wo := gorocksdb.NewDefaultWriteOptions()
	defer func() {
		wb.Destroy()
		wo.Destroy()
	}()
	n := len(entries)
	if n > 0 {
		li := entries[0].Index
		if li < w.lastIndex {
			wb.DeleteRange(logKey(entries[0].Index), logKey(w.lastIndex))
		} else if li == w.lastIndex {
			wb.Delete(logKey(entries[0].Index))
		} else if li > w.lastIndex+1 {
			return fmt.Errorf("log index(%d) is larger than last index(%d)", entries[0].Index, w.lastIndex)
		}

		for i := 0; i < n; i++ {
			val, err := entries[i].Marshal()
			if err != nil {
				return err
			}
			wb.Put(logKey(entries[i].Index), val)
		}
	}

	if !raft.IsEmptyHardState(hs) {
		val, err := hs.Marshal()
		if err != nil {
			return err
		}
		wb.Put(hardStateKey(), val)
	}
	if err := w.db.Write(wo, wb); err != nil {
		return err
	}

	if n > 0 {
		w.lastIndex = entries[n-1].Index
	}
	if !raft.IsEmptyHardState(hs) {
		w.hs = hs
	}
	return nil
}

func (w *rocksdbWal) Truncate(index uint64) error {
	if index <= w.st.Index {
		return raft.ErrCompacted
	}
	term, err := w.Term(index)
	if err != nil {
		return err
	}

	st := Snapshot{
		Index: index,
		Term:  term,
	}
	wb := gorocksdb.NewWriteBatch()
	wo := gorocksdb.NewDefaultWriteOptions()
	defer func() {
		wb.Destroy()
		wo.Destroy()
	}()
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val, st.Index)
	binary.BigEndian.PutUint64(val[8:], st.Term)
	wb.Put(snapshotKey(), val)
	wb.DeleteRange(logKey(0), logKey(index))
	if err = w.db.Write(wo, wb); err != nil {
		return err
	}
	w.st = st
	return nil
}

func (w *rocksdbWal) ApplySnapshot(st Snapshot) error {
	hs := pb.HardState{
		Commit: st.Index,
		Term:   st.Term,
	}
	wb := gorocksdb.NewWriteBatch()
	wo := gorocksdb.NewDefaultWriteOptions()
	defer func() {
		wb.Destroy()
		wo.Destroy()
	}()
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val, st.Index)
	binary.BigEndian.PutUint64(val[8:], st.Term)
	wb.Put(snapshotKey(), val)

	value, err := hs.Marshal()
	if err != nil {
		return err
	}
	wb.Put(hardStateKey(), value)
	wb.DeleteRange(logKey(0), logKey(math.MaxUint64))
	if err := w.db.Write(wo, wb); err != nil {
		return err
	}
	w.st = st
	w.hs = hs
	w.lastIndex = st.Index
	return nil
}

func (w *rocksdbWal) Close() {
	w.once.Do(func() {
		w.db.Close()
	})
}
