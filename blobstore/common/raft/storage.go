package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/google/uuid"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	uninitializedIndex = math.MaxUint64
)

var (
	groupPrefix    = []byte("g")
	hardStateInfix = []byte("h")
	logIndexInfix  = []byte("i")
)

type storageConfig struct {
	id              uint64
	maxSnapshotNum  int
	snapshotTimeout time.Duration
	members         []Member
	raw             Storage
	sm              StateMachine
}

func newStorage(cfg storageConfig) (*storage, error) {
	storage := &storage{
		id:               cfg.id,
		firstIndex:       uninitializedIndex,
		rawStg:           cfg.raw,
		stateMachine:     cfg.sm,
		snapshotRecorder: newSnapshotRecorder(cfg.maxSnapshotNum, cfg.snapshotTimeout),
	}

	value, err := cfg.raw.Get(encodeHardStateKey(cfg.id))
	if err != nil && err != kvstore.ErrNotFound {
		return nil, err
	}
	if value != nil {
		defer value.Close()
	}

	if value != nil {
		hs := &raftpb.HardState{}
		if err := hs.Unmarshal(value.Value()); err != nil {
			return nil, err
		}
		storage.hardState = *hs
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
	membersMu    struct {
		sync.RWMutex
		members   map[uint64]Member
		confState raftpb.ConfState
	}
	// snapshotMu avoiding create snapshot and truncate log running currently
	snapshotMu sync.RWMutex

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
	// iter := s.rawStg.Iter(encodeIndexLogKey(s.id, lo))
	prefix := encodeIndexLogKeyPrefix(s.id)
	iter := s.rawStg.Iter(prefix)
	iter.SeekTo(encodeIndexLogKey(s.id, lo))
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
			break
		}
		if _, index := decodeIndexLogKey(keyGetter.Key()); index >= hi {
			break
		}

		entry := &raftpb.Entry{}
		if err = entry.Unmarshal(valGetter.Value()); err != nil {
			valGetter.Close()
			return nil, err
		}
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
	// so return hard state commit first when i is equal to hard state's commit
	if s.hardState.Commit == i {
		return s.hardState.Term, nil
	}
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return 0, err
	}
	if firstIndex > i {
		return 0, raft.ErrCompacted
	}

	value, err := s.rawStg.Get(encodeIndexLogKey(s.id, i))
	if err != nil {
		return 0, err
	}

	entry := &raftpb.Entry{}
	if err := entry.Unmarshal(value.Value()); err != nil {
		return 0, err
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (s *storage) LastIndex() (uint64, error) {
	if lastIndex := atomic.LoadUint64(&s.lastIndex); lastIndex > 0 {
		return lastIndex, nil
	}

	iterator := s.rawStg.Iter(nil)
	defer iterator.Close()

	if err := iterator.SeekForPrev(encodeIndexLogKey(s.id, math.MaxUint64)); err != nil {
		return 0, err
	}
	keyGetter, valGetter, err := iterator.ReadPrev()
	if err != nil {
		return 0, err
	}
	if valGetter == nil {
		return 0, nil
	}
	defer func() {
		keyGetter.Close()
		valGetter.Close()
	}()

	if !validForPrefix(keyGetter.Key(), encodeIndexLogKeyPrefix(s.id)) {
		return 0, nil
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
		atomic.CompareAndSwapUint64(&s.firstIndex, uninitializedIndex, 1)
		return 1, nil
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
	s.membersMu.RUnlock()

	smSnap := s.stateMachine.Snapshot()
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
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	if smSnapIndex < firstIndex {
		return raftpb.Snapshot{}, fmt.Errorf("state machine outgoingSnapshot index[%d] less than first log index[%d]", smSnapIndex, firstIndex)
	}

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

	outgoingSnap := newOutgoingSnapshot(snapID, smSnap)
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

// SaveHardStateAndEntries is called by one worker only
func (s *storage) SaveHardStateAndEntries(hs raftpb.HardState, entries []raftpb.Entry) error {
	batch := s.rawStg.NewBatch()
	defer batch.Close()

	if !raft.IsEmptyHardState(hs) {
		value, err := hs.Marshal()
		if err != nil {
			return err
		}
		batch.Put(encodeHardStateKey(s.id), value)
	}

	lastIndex := uint64(0)
	for i := range entries {
		key := encodeIndexLogKey(s.id, entries[i].Index)
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

	batch.DeleteRange(encodeIndexLogKey(s.id, 0), encodeIndexLogKey(s.id, index))
	if err := s.rawStg.Write(batch); err != nil {
		return err
	}

	// update first index after truncate wal log
	for {
		firstIndex := atomic.LoadUint64(&s.firstIndex)
		if firstIndex > index {
			return nil
		}
		if atomic.CompareAndSwapUint64(&s.firstIndex, firstIndex, index) {
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
	}

	s.updateConfState()
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

func encodeIndexLogKey(id uint64, index uint64) []byte {
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

func encodeHardStateKey(id uint64) []byte {
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

func validForPrefix(key []byte, prefix []byte) bool {
	return bytes.HasPrefix(key, prefix)
}
