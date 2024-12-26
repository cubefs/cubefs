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

package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	uninitializedIndex = math.MaxUint64
)

var (
	groupPrefix       = []byte("g")
	hardStateInfix    = []byte("h")
	snapshotMetaInfix = []byte("s")
	logIndexInfix     = []byte("i")
)

type storageConfig struct {
	id                uint64
	maxCachedEntryNum uint64
	maxSnapshotNum    int
	snapshotTimeout   time.Duration
	members           []Member
	raw               Storage
	sm                StateMachine
}

func newStorage(cfg storageConfig) (*storage, error) {
	storage := &storage{
		id:               cfg.id,
		firstIndex:       uninitializedIndex,
		rawStg:           cfg.raw,
		stateMachine:     cfg.sm,
		snapshotRecorder: newSnapshotRecorder(cfg.maxSnapshotNum, cfg.snapshotTimeout),
		caches:           newEntryCache(cfg.maxCachedEntryNum),
	}

	value, err := cfg.raw.Get(EncodeHardStateKey(cfg.id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if value != nil {
		defer value.Close()
	}
	if value != nil {
		hs := raftpb.HardState{}
		if err := hs.Unmarshal(value.Value()); err != nil {
			return nil, err
		}
		storage.hardState = hs
	}

	snapMetaValue, err := cfg.raw.Get(encodeSnapshotMetaKey(cfg.id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if snapMetaValue != nil {
		defer snapMetaValue.Close()
	}
	if snapMetaValue != nil {
		snapMeta := raftpb.SnapshotMetadata{}
		if err := snapMeta.Unmarshal(snapMetaValue.Value()); err != nil {
			return nil, err
		}
		storage.snapshotMeta = snapMeta
	}

	members := make(map[uint64]Member)
	for i := range cfg.members {
		members[cfg.members[i].NodeID] = cfg.members[i]
	}
	storage.membersMu.members = members
	storage.updateConfState()

	return storage, nil
}

type storage struct {
	id           uint64
	firstIndex   uint64
	lastIndex    uint64
	appliedIndex uint64
	hardState    raftpb.HardState
	snapshotMeta raftpb.SnapshotMetadata
	membersMu    struct {
		sync.RWMutex
		members   map[uint64]Member
		confState raftpb.ConfState
	}
	// snapshotMu avoiding create snapshot and truncate log running currently
	snapshotMu sync.RWMutex
	// caches hold recent log entries of group
	caches *entryCache

	rawStg           Storage
	stateMachine     StateMachine
	snapshotRecorder *snapshotRecorder
}

// InitialState returns the saved HardState and ConfState information.
func (s *storage) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	return s.hardState, s.membersMu.confState, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	// get from caches firstly
	entries := s.caches.getFrom(lo, hi)
	if entries != nil {
		// log.Printf("get from caches: %d-%d, enties: %+v, len: %d\n", lo, hi, entries, len(entries))
		return entries, nil
	}

	// get from kv storage
	prefix := encodeIndexLogKeyPrefix(s.id)
	iter := s.rawStg.Iter(prefix)
	iter.SeekTo(EncodeIndexLogKey(s.id, lo))
	defer iter.Close()

	ret := make([]raftpb.Entry, 0)
	for {
		keyGetter, valGetter, err := iter.ReadNext()
		if err != nil {
			return nil, err
		}

		if keyGetter == nil || valGetter == nil {
			break
		}
		if !validForPrefix(keyGetter.Key(), prefix) {
			keyGetter.Close()
			valGetter.Close()
			break
		}
		if _, index := decodeIndexLogKey(keyGetter.Key()); index >= hi {
			keyGetter.Close()
			valGetter.Close()
			break
		}

		entry := &raftpb.Entry{}
		if err = entry.Unmarshal(valGetter.Value()); err != nil {
			keyGetter.Close()
			valGetter.Close()
			return nil, err
		}
		keyGetter.Close()
		valGetter.Close()
		ret = append(ret, *entry)

		if uint64(len(ret)) == maxSize {
			break
		}
	}

	return ret, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (s *storage) Term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}

	// the first index log may not be found after apply snapshot,
	// so return snapshot term index first when i is equal to snapshot meta index
	if s.snapshotMeta.Index == i {
		return s.snapshotMeta.Term, nil
	}

	// check first index if log compaction
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return 0, err
	}
	if firstIndex > i {
		return 0, raft.ErrCompacted
	}

	// get from cache firstly
	if entry := s.caches.get(i); entry.Index > 0 {
		// log.Debugf("get term form entry: %+v", entry)
		return entry.Term, nil
	}

	value, err := s.rawStg.Get(EncodeIndexLogKey(s.id, i))
	if err == nil {
		entry := &raftpb.Entry{}
		if err := entry.Unmarshal(value.Value()); err != nil {
			return 0, err
		}
		value.Close()
		return entry.Term, nil
	}

	return 0, err
}

// LastIndex returns the index of the last entry in the log.
func (s *storage) LastIndex() (uint64, error) {
	span, _ := trace.StartSpanFromContext(context.Background(), "")
	if lastIndex := atomic.LoadUint64(&s.lastIndex); lastIndex > 0 {
		return lastIndex, nil
	}

	iterator := s.rawStg.Iter(encodeIndexLogKeyPrefix(s.id))
	defer iterator.Close()

	if err := iterator.SeekForPrev(EncodeIndexLogKey(s.id, math.MaxUint64)); err != nil {
		span.Errorf("storage seek prev failed, err: %v", err)
		return 0, err
	}
	keyGetter, valGetter, err := iterator.ReadPrev()
	if err != nil {
		span.Errorf("storage read prev failed, err: %v", err)
		return 0, err
	}
	if valGetter == nil {
		return s.snapshotMeta.Index, nil
		// return 0, nil
	}
	defer func() {
		keyGetter.Close()
		valGetter.Close()
	}()

	if !validForPrefix(keyGetter.Key(), encodeIndexLogKeyPrefix(s.id)) {
		return s.snapshotMeta.Index, nil
	}

	entry := &raftpb.Entry{}
	if err := entry.Unmarshal(valGetter.Value()); err != nil {
		return 0, err
	}

	atomic.CompareAndSwapUint64(&s.lastIndex, 0, entry.Index)
	return entry.Index, nil
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (s *storage) FirstIndex() (uint64, error) {
	if firstIndex := atomic.LoadUint64(&s.firstIndex); firstIndex != uninitializedIndex {
		return firstIndex, nil
	}

	iterator := s.rawStg.Iter(encodeIndexLogKeyPrefix(s.id))
	defer iterator.Close()

	_, valGetter, err := iterator.ReadNext()
	if err != nil {
		return 0, err
	}
	// initial index log should return at least 1
	if valGetter == nil {
		// store the initialized value for first index when not found
		// avoiding iterator call frequently
		// atomic.CompareAndSwapUint64(&s.firstIndex, uninitializedIndex, 1)
		return s.snapshotMeta.Index + 1, nil
		// return 1, nil
	}

	defer valGetter.Close()
	entry := &raftpb.Entry{}
	if err := entry.Unmarshal(valGetter.Value()); err != nil {
		return 0, err
	}

	atomic.CompareAndSwapUint64(&s.firstIndex, uninitializedIndex, entry.Index)
	return entry.Index, nil
}

// Snapshot returns the most recent outgoingSnapshot.
// If outgoingSnapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// outgoingSnapshot and call Snapshot later.
func (s *storage) Snapshot() (raftpb.Snapshot, error) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	s.membersMu.RLock()
	cs := s.membersMu.confState
	members := make([]Member, 0, len(s.membersMu.members))
	for i := range s.membersMu.members {
		members = append(members, s.membersMu.members[i])
	}
	s.membersMu.RUnlock()

	smSnap, err := s.stateMachine.Snapshot()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	success := false
	defer func() {
		if !success {
			smSnap.Close()
		}
	}()
	appliedIndex := s.AppliedIndex()

	smSnapIndex := smSnap.Index()
	if smSnapIndex > appliedIndex {
		return raftpb.Snapshot{}, fmt.Errorf("state machine outgoingSnapshot index[%d] greater than applied index[%d]", smSnapIndex, appliedIndex)
	}
	/*firstIndex, err := s.FirstIndex()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	if smSnapIndex < firstIndex {
		return raftpb.Snapshot{}, fmt.Errorf("state machine outgoingSnapshot index[%d] less than first log index[%d]", smSnapIndex, firstIndex)
	}*/

	term, err := s.Term(smSnapIndex)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	snapID := uuid.New().String()

	snapshot := raftpb.Snapshot{
		Data: []byte(snapID),
		Metadata: raftpb.SnapshotMetadata{
			ConfState: cs,
			Index:     smSnapIndex,
			Term:      term,
		},
	}

	outgoingSnap := newOutgoingSnapshot(snapID, smSnap, members)
	// as the snapshot may be used in different follower's snapshot transmitting
	// we set finalizer for every snapshot and do close after gc recycle
	runtime.SetFinalizer(outgoingSnap, func(snap *outgoingSnapshot) {
		snap.Close()
	})
	s.snapshotRecorder.Set(outgoingSnap)
	success = true

	return snapshot, nil
}

func (s *storage) AppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *storage) SetAppliedIndex(index uint64) {
	atomic.StoreUint64(&s.appliedIndex, index)
}

func (s *storage) ResetLastIndex() {
	atomic.StoreUint64(&s.lastIndex, 0)
}

// SaveHardStateAndEntries is called by one worker only
func (s *storage) SaveHardStateAndEntries(hs raftpb.HardState, entries []raftpb.Entry) error {
	batch := s.rawStg.NewBatch()
	defer batch.Close()

	if !raft.IsEmptyHardState(hs) {
		value, err := hs.Marshal()
		if err != nil {
			return err
		}
		batch.Put(EncodeHardStateKey(s.id), value)
	}

	lastIndex := uint64(0)
	for i := range entries {
		key := EncodeIndexLogKey(s.id, entries[i].Index)
		value, err := entries[i].Marshal()
		if err != nil {
			return err
		}
		batch.Put(key, value)
		if lastIndex < entries[i].Index {
			lastIndex = entries[i].Index
		}
	}
	if err := s.rawStg.Write(batch); err != nil {
		return err
	}

	// update last index after save new entries
	if lastIndex > 0 {
		atomic.StoreUint64(&s.lastIndex, lastIndex)
	}

	if !raft.IsEmptyHardState(hs) {
		s.hardState = hs
	}

	// update entry cache
	if len(entries) > 0 {
		// log.Debugf("cache put entries: %+v", entries)
		s.caches.put(entries)
	}

	return nil
}

// SaveSnapshotMetaAndHardState is called by one worker only
func (s *storage) SaveSnapshotMetaAndHardState(snapMeta raftpb.SnapshotMetadata, hs raftpb.HardState) error {
	batch := s.rawStg.NewBatch()
	defer batch.Close()

	value, err := snapMeta.Marshal()
	if err != nil {
		return err
	}
	batch.Put(encodeSnapshotMetaKey(s.id), value)

	if !raft.IsEmptyHardState(hs) {
		value, err := hs.Marshal()
		if err != nil {
			return err
		}
		batch.Put(EncodeHardStateKey(s.id), value)
	}

	if err := s.rawStg.Write(batch); err != nil {
		return err
	}

	s.snapshotMeta = snapMeta
	if !raft.IsEmptyHardState(hs) {
		s.hardState = hs
	}

	return nil
}

// Truncate may be called by top level application concurrently
func (s *storage) Truncate(index uint64) error {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()

	oldestSnap := s.snapshotRecorder.Pop()
	if oldestSnap != nil && index > oldestSnap.st.Index() {
		return fmt.Errorf("can not truncate log index[%d] which large than the alive outgoingSnapshot's index[%d]", index, oldestSnap.st.Index())
	}

	if appliedIndex := s.AppliedIndex(); index > appliedIndex {
		return fmt.Errorf("truncate index[%d] large than applied index[%d]", index, appliedIndex)
	}

	batch := s.rawStg.NewBatch()
	defer batch.Close()

	batch.DeleteRange(EncodeIndexLogKey(s.id, 0), EncodeIndexLogKey(s.id, index+1))
	if err := s.rawStg.Write(batch); err != nil {
		return err
	}

	// update first index after truncate wal log
	for {
		firstIndex := atomic.LoadUint64(&s.firstIndex)
		if firstIndex > index {
			return nil
		}
		log.Infof("group: %d truncate, firstIndex: %d, index: %d", s.id, s.firstIndex, index)
		if atomic.CompareAndSwapUint64(&s.firstIndex, firstIndex, index+1) {
			return nil
		}
	}
}

func (s *storage) GetSnapshot(id string) *outgoingSnapshot {
	return s.snapshotRecorder.Get(id)
}

func (s *storage) DeleteSnapshot(id string) {
	s.snapshotRecorder.Delete(id)
}

func (s *storage) NewBatch() Batch {
	return s.rawStg.NewBatch()
}

func (s *storage) MemberChange(member *Member) {
	s.membersMu.Lock()
	defer s.membersMu.Unlock()

	switch member.Type {
	case MemberChangeType_AddMember:
		s.membersMu.members[member.NodeID] = *member
	case MemberChangeType_RemoveMember:
		delete(s.membersMu.members, member.NodeID)
	default:

	}

	s.updateConfState()
}

func (s *storage) GetMember(id uint64) (Member, bool) {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()
	m, hit := s.membersMu.members[id]
	return m, hit
}

// Clear remove all raft log and hard state of the group
func (s *storage) Clear() error {
	batch := s.rawStg.NewBatch()
	defer batch.Close()

	batch.DeleteRange(EncodeIndexLogKey(s.id, 0), EncodeIndexLogKey(s.id, math.MaxUint64))
	batch.Delete(EncodeHardStateKey(s.id))
	batch.Delete(encodeSnapshotMetaKey(s.id))
	if err := s.rawStg.Write(batch); err != nil {
		return err
	}
	return nil
}

func (s *storage) Close() {
	s.snapshotRecorder.Close()
}

func (s *storage) updateConfState() {
	learners := make([]uint64, 0)
	voters := make([]uint64, 0)

	for _, m := range s.membersMu.members {
		if m.Learner {
			learners = append(learners, m.NodeID)
		} else {
			voters = append(voters, m.NodeID)
		}
	}
	s.membersMu.confState.Learners = learners
	s.membersMu.confState.Voters = voters
}

type entryCache struct {
	data     []raftpb.Entry
	head     uint64
	tail     uint64
	nextTail uint64
	cap      uint64
	usedCap  uint64
}

// TODO: use btree instead
func newEntryCache(cap uint64) *entryCache {
	ring := &entryCache{
		data: make([]raftpb.Entry, cap),
		cap:  cap,
	}
	return ring
}

func (r *entryCache) put(entries []raftpb.Entry) {
	for i := range entries {
		if r.data[r.tail].Index >= entries[i].Index {
			continue
		}
		r.data[r.nextTail] = entries[i]
		r.tail = r.nextTail
		if r.cap == r.usedCap {
			r.head++
			r.head = r.head % r.cap
			r.nextTail++
			r.nextTail = r.nextTail % r.cap
		} else {
			r.nextTail++
			r.nextTail = r.nextTail % r.cap
			r.usedCap++
		}
	}
}

func (r *entryCache) get(index uint64) (entry raftpb.Entry) {
	if r.head == r.tail {
		return
	}
	if r.min() > index {
		return
	}

	if r.max() < index {
		return
	}

	i := r.head
	for {
		if r.data[i].Index == index {
			entry = r.data[i]
			return
		}
		i = (i + 1) % r.cap
		if i == r.nextTail {
			break
		}
	}
	return
}

func (r *entryCache) getFrom(lo, hi uint64) (ret []raftpb.Entry) {
	if r.head == r.tail {
		return nil
	}
	min := r.min()
	if min > hi || lo < min {
		return nil
	}
	if r.max() < lo {
		return nil
	}

	i := r.head
	for {
		if r.data[i].Index < hi && r.data[i].Index >= lo {
			ret = append(ret, r.data[i])
		}
		if r.data[i].Index == hi-1 {
			break
		}
		i = (i + 1) % r.cap
		if i == r.nextTail {
			break
		}
	}
	return ret
}

func (r *entryCache) min() uint64 {
	return r.data[r.head].Index
}

func (r *entryCache) max() uint64 {
	return r.data[r.tail].Index
}

func EncodeIndexLogKey(id uint64, index uint64) []byte {
	b := make([]byte, 8+8+len(groupPrefix)+len(logIndexInfix))
	copy(b, groupPrefix)
	binary.BigEndian.PutUint64(b[len(groupPrefix):], id)
	copy(b[8+len(groupPrefix):], logIndexInfix)
	binary.BigEndian.PutUint64(b[8+len(groupPrefix)+len(logIndexInfix):], index)

	return b
}

func decodeIndexLogKey(b []byte) (id uint64, index uint64) {
	id = binary.BigEndian.Uint64(b[len(groupPrefix):])
	index = binary.BigEndian.Uint64(b[len(groupPrefix)+8+len(logIndexInfix):])
	return
}

func encodeIndexLogKeyPrefix(id uint64) []byte {
	b := make([]byte, 8+len(groupPrefix)+len(logIndexInfix))
	copy(b, groupPrefix)
	binary.BigEndian.PutUint64(b[len(groupPrefix):], id)
	copy(b[8+len(groupPrefix):], logIndexInfix)

	return b
}

func EncodeHardStateKey(id uint64) []byte {
	b := make([]byte, 8+len(groupPrefix)+len(hardStateInfix))
	copy(b, groupPrefix)
	binary.BigEndian.PutUint64(b[len(groupPrefix):], id)
	copy(b[8+len(groupPrefix):], hardStateInfix)

	return b
}

func decodeHardStateKey(b []byte) (uint64, []byte) {
	id := binary.BigEndian.Uint64(b[len(groupPrefix):])
	infix := b[len(groupPrefix)+8:]

	return id, infix
}

func encodeSnapshotMetaKey(id uint64) []byte {
	b := make([]byte, 8+len(groupPrefix)+len(snapshotMetaInfix))
	copy(b, groupPrefix)
	binary.BigEndian.PutUint64(b[len(groupPrefix):], id)
	copy(b[8+len(groupPrefix):], snapshotMetaInfix)

	return b
}

func validForPrefix(key []byte, prefix []byte) bool {
	return bytes.HasPrefix(key, prefix)
}

func EncodeIndexLogKeyPrefix(id uint64) []byte {
	return encodeIndexLogKeyPrefix(id)
}

func EncodeSnapshotMetaKey(id uint64) []byte {
	return encodeSnapshotMetaKey(id)
}
