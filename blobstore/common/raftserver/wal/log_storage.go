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
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"syscall"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	logfileSize     = 64 * 1024 * 1024
	logfileCacheNum = 4
)

func (w *Wal) reload(firstIndex uint64) error {
	names, err := listLogFiles(w.dir)
	if err != nil {
		return err
	}

	if len(names) == 0 {
		f, err := w.createNew(firstIndex)
		if err != nil {
			return err
		}
		w.logfiles = append(w.logfiles, f.Name())
		w.last = f
		return nil
	}

	nlen := len(names)
	w.nextFileSeq = names[nlen-1].sequence + 1
	w.logfiles = append(w.logfiles, names...)
	f, err := openLogFile(w.dir, w.logfiles[nlen-1], true)
	if err != nil {
		return err
	}
	w.last = f
	return nil
}

func (w *Wal) term(i uint64) (term uint64, err error) {
	lf, err := w.locateFile(i)
	if err != nil {
		return
	}
	term, err = lf.Term(i)
	return
}

func (wal *Wal) lastIndex() uint64 {
	if wal.last.Len() == 0 {
		if len(wal.logfiles) > 1 {
			return wal.last.name.index - 1
		}
		return 0
	}
	return wal.last.LastIndex()
}

func (w *Wal) entries(lo, hi uint64, maxSize uint64) (entries []pb.Entry, err error) {
	if hi > w.lastIndex()+1 {
		err = fmt.Errorf("entries's hi(%d) is out of bound lastindex(%d)", hi, w.lastIndex())
		return
	}

	si := w.locate(lo)
	lfs := w.logfiles[si:]

	var ent pb.Entry
	var lf *logFile
	i := lo
	var size uint64
	for _, fn := range lfs {
		if fn.index >= hi {
			return
		}

		lf, err = w.get(fn)
		if err != nil {
			return
		}
		for i <= lf.LastIndex() {
			ent, err = lf.Get(i)
			if err != nil {
				return
			}
			size += uint64(ent.Size())
			if i >= hi || size > maxSize {
				return
			}
			entries = append(entries, ent)
			i++
		}
	}

	return
}

func (w *Wal) saveEntries(ents []pb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	if err := w.truncateBack(ents[0].Index); err != nil {
		return err
	}

	for _, ent := range ents {
		if err := w.saveEntry(&ent); err != nil {
			return err
		}
	}

	if err := w.last.Flush(); err != nil {
		return err
	}

	return nil
}

func (w *Wal) Sync() error {
	return w.last.Sync()
}

func (w *Wal) truncateFront(index uint64) error {
	truncFIndex := -1
	for i := 0; i < len(w.logfiles)-1; i++ {
		if w.logfiles[i+1].index-1 <= index {
			truncFIndex = i
		} else {
			break
		}
	}

	for i := 0; i <= truncFIndex; i++ {
		if err := w.remove(w.logfiles[i]); err != nil {
			return err
		}
	}

	if truncFIndex >= 0 {
		if err := syncDir(w.dir); err != nil {
			return err
		}
		w.logfiles = w.logfiles[truncFIndex+1:]
	}

	return nil
}

func (w *Wal) truncateAll(firstIndex uint64) error {
	for _, f := range w.logfiles {
		if err := w.remove(f); err != nil {
			return err
		}
	}
	w.nextFileSeq = 1
	w.logfiles = w.logfiles[:0]

	lf, err := w.createNew(firstIndex)
	if err != nil {
		return err
	}
	w.last.Close()
	w.last = lf
	w.logfiles = append(w.logfiles, lf.Name())

	return nil
}

func (w *Wal) truncateBack(index uint64) error {
	if w.lastIndex() < index {
		return nil
	}

	if w.logfiles[0].index >= index {
		return w.truncateAll(index)
	}

	idx := w.locate(index)
	if idx == len(w.logfiles)-1 {
		if err := w.last.Truncate(index); err != nil {
			return err
		}
	} else {
		for i := idx + 1; i < len(w.logfiles); i++ {
			if err := w.remove(w.logfiles[i]); err != nil {
				return err
			}
		}

		if err := syncDir(w.dir); err != nil {
			return err
		}

		n := w.logfiles[idx]
		lf, err := w.get(n)
		if err != nil {
			return err
		}
		w.cache.Delete(n)
		w.last = lf
		if err := w.last.OpenWrite(); err != nil {
			return err
		}
		if err := w.last.Truncate(index); err != nil {
			return err
		}

		w.logfiles = w.logfiles[:idx+1]
		w.nextFileSeq = n.sequence + 1
	}
	return nil
}

func (w *Wal) createNew(index uint64) (*logFile, error) {
	name := logName{sequence: w.nextFileSeq, index: index}
	f, err := createLogFile(w.dir, name)
	if err != nil {
		return nil, err
	}

	if err := syncDir(w.dir); err != nil {
		f.Close()
		return nil, err
	}

	w.nextFileSeq++

	return f, nil
}

func (w *Wal) get(name logName) (*logFile, error) {
	if name.sequence == w.last.Seq() {
		return w.last, nil
	}
	return w.cache.Get(name)
}

func (w *Wal) remove(name logName) error {
	filename := name.String()
	trashdir := path.Join(w.dir, TrashPath)
	return os.Rename(path.Join(w.dir, filename), path.Join(trashdir, filename))
}

func (w *Wal) rotate() error {
	prevLast := w.last.LastIndex()

	if err := w.last.FinishWrite(); err != nil {
		return err
	}
	if err := w.last.Close(); err != nil {
		return err
	}

	lf, err := w.createNew(prevLast + 1)
	if err != nil {
		return err
	}
	w.last = lf
	w.logfiles = append(w.logfiles, lf.Name())
	return nil
}

func (w *Wal) locate(logindex uint64) int {
	fi := sort.Search(len(w.logfiles), func(i int) bool {
		var nextIndex uint64
		if i == len(w.logfiles)-1 {
			nextIndex = math.MaxUint64
		} else {
			nextIndex = w.logfiles[i+1].index
		}
		return logindex < nextIndex
	})
	return fi
}

func (w *Wal) locateFile(logindex uint64) (*logFile, error) {
	i := w.locate(logindex)
	if i >= len(w.logfiles) {
		panic("could not find log file")
	}
	return w.get(w.logfiles[i])
}

func (w *Wal) saveEntry(ent *pb.Entry) error {
	prevIndex := w.lastIndex()
	if prevIndex != 0 {
		if prevIndex+1 != ent.Index {
			return fmt.Errorf("append discontinuous log. prev index: %d, current: %d", prevIndex, ent.Index)
		}
	}

	woffset := w.last.WriteOffset()
	if uint64(woffset)+uint64(recordSize(&Entry{*ent})) > uint64(logfileSize) {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	if err := w.last.Save(ent); err != nil {
		return err
	}

	return nil
}

func isErrInvalid(err error) bool {
	if err == os.ErrInvalid {
		return true
	}

	if syserr, ok := err.(*os.SyscallError); ok && syserr.Err == syscall.EINVAL {
		return true
	}

	if patherr, ok := err.(*os.PathError); ok && patherr.Err == syscall.EINVAL {
		return true
	}
	return false
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil && !isErrInvalid(err) {
		return err
	}
	return nil
}
