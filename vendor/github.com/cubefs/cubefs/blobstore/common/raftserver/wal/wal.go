// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package wal

import (
	"path"
	"runtime"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type Snapshot struct {
	Index uint64
	Term  uint64
}

// Storage the storage
type Wal struct {
	// Log Entry
	sync        bool
	dir         string
	logfiles    []logName // all logs name
	last        *logFile
	nextFileSeq uint64
	cache       *logFileCache
	hs          pb.HardState
	st          Snapshot
	mt          *meta
	once        sync.Once
}

// OpenWal
func OpenWal(dir string, sync bool) (*Wal, error) {
	dir = path.Clean(dir)
	if err := InitPath(dir); err != nil {
		return nil, err
	}
	mt, st, hs, err := NewMeta(dir)
	if err != nil {
		return nil, err
	}
	w := &Wal{
		sync:        sync,
		dir:         dir,
		nextFileSeq: 1,
		cache: newLogFileCache(logfileCacheNum,
			func(name logName) (*logFile, error) {
				lf, err := openLogFile(dir, name, false)
				if err != nil {
					return nil, err
				}
				runtime.SetFinalizer(lf, func(lf *logFile) {
					lf.Close()
				})
				return lf, nil
			}),
		hs: hs,
		st: st,
		mt: mt,
	}

	err = w.reload(st.Index + 1)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Wal) InitialState() pb.HardState {
	return w.hs
}

func (w *Wal) Entries(lo, hi uint64, maxSize uint64) (entries []pb.Entry, err error) {
	if lo >= hi {
		return nil, raft.ErrUnavailable
	} else if lo <= w.st.Index {
		return nil, raft.ErrCompacted
	}
	return w.entries(lo, hi, maxSize)
}

func (w *Wal) Term(index uint64) (term uint64, err error) {
	if index < w.st.Index {
		return 0, raft.ErrCompacted
	} else if index == w.st.Index {
		return w.st.Term, nil
	}
	return w.term(index)
}

func (w *Wal) FirstIndex() uint64 {
	return w.st.Index + 1
}

func (w *Wal) LastIndex() uint64 {
	index := w.lastIndex()
	if index < w.st.Index {
		index = w.st.Index
	}
	return index
}

func (w *Wal) SaveEntries(entries []pb.Entry) error {
	if err := w.saveEntries(entries); err != nil {
		return err
	}
	if w.sync {
		return w.Sync()
	}
	return nil
}

func (w *Wal) SaveHardState(hs pb.HardState) error {
	w.mt.SaveHardState(hs)
	w.hs = hs
	return nil
}

func (w *Wal) Truncate(index uint64) error {
	if index <= w.st.Index {
		return raft.ErrCompacted
	}

	term, err := w.term(index)
	if err != nil {
		return err
	}

	// set st
	w.st = Snapshot{
		Index: index,
		Term:  term,
	}

	w.mt.SaveSnapshot(w.st)
	if err = w.mt.Sync(); err != nil {
		return err
	}

	if err = w.truncateFront(index); err != nil {
		return err
	}

	return nil
}

func (w *Wal) ApplySnapshot(st Snapshot) error {
	w.st = st
	w.hs = pb.HardState{
		Commit: st.Index,
		Term:   st.Term,
	}
	w.mt.SaveAll(st, w.hs)
	if err := w.mt.Sync(); err != nil {
		return err
	}

	if err := w.truncateAll(w.st.Index + 1); err != nil {
		return err
	}
	return nil
}

func (w *Wal) Close() {
	w.once.Do(func() {
		w.cache.Close()
		w.last.Close()
		w.mt.Close()
	})
}
