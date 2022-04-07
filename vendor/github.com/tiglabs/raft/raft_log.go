// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"math"

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage"
	"github.com/tiglabs/raft/util"
)

const noLimit = math.MaxUint64

// raftLog is responsible for the operation of the log.
type raftLog struct {
	unstable           unstable
	cache              unstable
	storage            storage.Storage
	committed, applied uint64
}

func newRaftLog(storage storage.Storage) (*raftLog, error) {
	log := &raftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return nil, err
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}

	log.unstable = newUnstable(lastIndex + 1)
	log.cache = newUnstable(lastIndex + 1)
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	return log, nil
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

func (l *raftLog) firstIndex() uint64 {
	index, err := l.storage.FirstIndex()
	if err != nil {
		errMsg := fmt.Sprintf("[raftLog->firstIndex]get firstindex from storage err:[%v].", err)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		errMsg := fmt.Sprintf("[raftLog->lastIndex]get lastIndex from storage err:[%v]", err)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	return i
}

func (l *raftLog) term(i uint64) (uint64, error) {
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		return 0, nil
	}
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	t, c, err := l.storage.Term(i)
	if c {
		return 0, ErrCompacted
	}
	if err == nil {
		return t, nil
	}

	errMsg := fmt.Sprintf("[raftLog->term]get term[%d] from storage err:[%v].", i, err)
	logger.Error(errMsg)
	panic(AppPanicError(errMsg))
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		errMsg := fmt.Sprintf("[raftLog->lastTerm]unexpected error when getting the last term (%v)", err)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	return t
}

func (l *raftLog) lastIndexAndTerm() (uint64, uint64) {
	li := l.lastIndex()
	t, err := l.term(li)
	if err != nil {
		errMsg := fmt.Sprintf("[raftLog->lastIndexAndTerm]unexpected error when getting the last term (%v)", err)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	return li, t
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) findConflict(ents []*proto.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() && logger.IsEnableDebug() {
				logger.Debug("[raftLog->findConflict]found conflict at index %d [existing term: %d, conflicting term: %d]", ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...*proto.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			errMsg := fmt.Sprintf("[raftLog->maybeAppend]entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
			logger.Error(errMsg)
			panic(AppPanicError(errMsg))

		default:
			l.append(ents[ci-(index+1):]...)
		}
		l.commitTo(util.Min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *raftLog) append(ents ...*proto.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		errMsg := fmt.Sprintf("[raftLog->append]after(%d) is out of range [committed(%d)]", after, l.committed)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	l.unstable.truncateAndAppend(ents,false)
	return l.lastIndex()
}

func (l *raftLog) unstableEntries() []*proto.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

func (l *raftLog) persist() (err error) {
	if entries := l.unstableEntries(); len(entries) > 0 {
		if err = l.storage.StoreEntries(entries); err != nil {
			return
		}
		for _,e:=range entries{
			proto.PutEntryToPool(e)
		}
		l.cache.truncateAndAppend(entries, true)
		l.stableTo(entries[len(entries)-1].Index, entries[len(entries)-1].Term)

	}
	return
}

func (l *raftLog) nextEnts(maxSize uint64) (ents []*proto.Entry) {
	off := util.Max(l.applied+1, l.firstIndex())
	hi := l.committed + 1
	if hi > off {
		ents, err := l.slice(off, hi, maxSize)
		if err != nil {
			errMsg := fmt.Sprintf("[raftLog->nextEnts]unexpected error when getting unapplied[%d,%d) entries (%v)", off, hi, err)
			logger.Error(errMsg)
			panic(AppPanicError(errMsg))
		}
		return ents
	}
	return nil
}

func (l *raftLog) entries(i uint64, maxsize uint64) ([]*proto.Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			errMsg := fmt.Sprintf("[raftLog->commitTo]tocommit(%d) is out of range [lastIndex(%d)]", tocommit, l.lastIndex())
			logger.Error(errMsg)
			panic(AppPanicError(errMsg))
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		errMsg := fmt.Sprintf("[raftLog->appliedTo]applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	l.applied = i

	if term, err := l.term(i); err == nil {
		l.cache.stableTo(i, term,true)
	}
}

func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t,false) }

func (l *raftLog) isUpToDate(lasti, term uint64, fpri, lpri uint16) bool {
	li, lt := l.lastIndexAndTerm()
	return term > lt || (term == lt && lasti > li) || (term == lt && lasti == li && fpri >= lpri)
}

func (l *raftLog) restore(index uint64) {
	if logger.IsEnableDebug() {
		logger.Debug("[raftLog->restore]log [%s] starts to restore snapshot [index: %d]", l.String(), index)
	}
	l.committed = index
	l.applied = index
	l.unstable.restore(index)
	l.cache.restore(index)
}

func (l *raftLog) slice(lo, hi uint64, maxSize uint64) ([]*proto.Entry, error) {
	if lo == hi {
		return nil, nil
	}
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}

	var ents []*proto.Entry
	if lo < l.cache.offset {
		storedhi := util.Min(hi, l.cache.offset)
		storedEnts, cmp, err := l.storage.Entries(lo, storedhi, maxSize)
		if cmp {
			return nil, ErrCompacted
		} else if err != nil {
			errMsg := fmt.Sprintf("[raftLog->slice]get entries[%d:%d) from storage err:[%v].", lo, storedhi, err)
			logger.Error(errMsg)
			panic(AppPanicError(errMsg))
		}
		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < storedhi-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}

	if cachedHi, sure := l.cache.maybeLastIndex(); sure && hi > l.cache.offset {
		readLo := util.Max(lo, l.cache.offset)
		readHi := util.Min(hi, cachedHi+1)
		if readLo < readHi {
			if cachedEntries := l.cache.slice(readLo, readHi); len(cachedEntries) > 0 {
				if len(ents) > 0 {
					ents = append([]*proto.Entry{}, ents...)
					ents = append(ents, cachedEntries...)
				} else {
					ents = cachedEntries
				}
			}
		}

	}

	if hi > l.unstable.offset {
		if unstable := l.unstable.slice(util.Max(lo, l.unstable.offset), hi); len(unstable) > 0 {
			if len(ents) > 0 {
				ents = append([]*proto.Entry{}, ents...)
				ents = append(ents, unstable...)
			} else {
				ents = unstable
			}
		}

	}

	return limitSize(ents, maxSize), nil

	//if lo < l.unstable.offset {
	//	storedhi := util.Min(hi, l.unstable.offset)
	//	storedEnts, cmp, err := l.storage.Entries(lo, storedhi, maxSize)
	//	if cmp {
	//		return nil, ErrCompacted
	//	} else if err != nil {
	//		errMsg := fmt.Sprintf("[raftLog->slice]get entries[%d:%d) from storage err:[%v].", lo, storedhi, err)
	//		logger.Error(errMsg)
	//		panic(AppPanicError(errMsg))
	//	}
	//	// check if ents has reached the size limitation
	//	if uint64(len(storedEnts)) < storedhi-lo {
	//		return storedEnts, nil
	//	}
	//	ents = storedEnts
	//}
	//if hi > l.unstable.offset {
	//	unstable := l.unstable.slice(util.Max(lo, l.unstable.offset), hi)
	//	if len(ents) > 0 {
	//		ents = append([]*proto.Entry{}, ents...)
	//		ents = append(ents, unstable...)
	//	} else {
	//		ents = unstable
	//	}
	//}
	//if maxSize == noLimit {
	//	return ents, nil
	//}
	//return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		errMsg := fmt.Sprintf("[raftLog->mustCheckOutOfBounds]invalid slice %d > %d", lo, hi)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}
	li := l.lastIndex()
	length := li - fi + 1
	if lo < fi || hi > fi+length {
		errMsg := fmt.Sprintf("[raftLog->mustCheckOutOfBounds]slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, li)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	errMsg := fmt.Sprintf("[raftLog->zeroTermOnErrCompacted]unexpected error (%v)", err)
	logger.Error(errMsg)
	panic(AppPanicError(errMsg))
}

func (l *raftLog) allEntries() []*proto.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	errMsg := fmt.Sprintf("[log->allEntries]get all entries err:[%v]", err)
	logger.Error(errMsg)
	panic(AppPanicError(errMsg))
}

func limitSize(ents []*proto.Entry, maxSize uint64) []*proto.Entry {
	if len(ents) == 0 || maxSize == noLimit {
		return ents
	}

	size := ents[0].Size()
	limit := 1
	for l := len(ents); limit < l; limit++ {
		size += ents[limit].Size()
		if size > maxSize {
			break
		}
	}
	return ents[:limit]
}
