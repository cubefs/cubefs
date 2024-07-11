package raft

import (
	"container/list"
	"fmt"
	"io"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
)

func newOutgoingSnapshot(id string, st Snapshot) *outgoingSnapshot {
	return &outgoingSnapshot{
		id: id,
		st: st,
	}
}

// RaftSnapshotHeader
type outgoingSnapshot struct {
	id     string
	st     Snapshot
	expire time.Time
}

// BatchData return snapshot batch data
// Return io.EOF error when no more new batch data
func (s *outgoingSnapshot) BatchData() (Batch, error) {
	batch, err := s.st.ReadBatch()
	return batch, err
}

func (s *outgoingSnapshot) Close() {
	s.st.Close()
}

func newIncomingSnapshot(header *RaftSnapshotHeader, storage incomingSnapshotStorage, stream SnapshotResponseStream) *incomingSnapshot {
	return &incomingSnapshot{
		RaftSnapshotHeader: header,
		storage:            storage,
		stream:             stream,
		seq:                1,
	}
}

type incomingSnapshotStorage interface {
	NewBatch() Batch
}

// incomingSnapshot held the incoming snapshot and implements the Snapshot interface
// it will be used for state machine apply snapshot
type incomingSnapshot struct {
	*RaftSnapshotHeader

	final   bool
	seq     uint32
	storage incomingSnapshotStorage
	stream  SnapshotResponseStream
}

func (i *incomingSnapshot) ReadBatch() (Batch, error) {
	if i.final {
		return nil, io.EOF
	}

	req, err := i.stream.Recv()
	if err != nil {
		return nil, err
	}
	if req.Seq != i.seq {
		return nil, fmt.Errorf("unexpected snapshot request sequence: %d, expected: %d", req.Seq, i.seq)
	}

	var batch Batch
	if len(req.Data) > 0 {
		batch = i.storage.NewBatch()
		batch.From(req.Data)
	}

	i.final = req.Final
	i.seq++

	return batch, nil
}

func (i *incomingSnapshot) Index() uint64 {
	message := i.RaftMessageRequest.Message
	return message.Index
}

func (i *incomingSnapshot) Term() uint64 {
	message := i.RaftMessageRequest.Message
	return message.Term
}

func (i *incomingSnapshot) Close() error {
	return nil
}

func newSnapshotRecorder(maxSnapshot int, timeout time.Duration) *snapshotRecorder {
	sr := &snapshotRecorder{
		maxSnapshot: maxSnapshot,
		timeout:     timeout,
		evictList:   list.New(),
		snaps:       make(map[string]*list.Element),
	}

	return sr
}

type snapshotRecorder struct {
	sync.RWMutex

	maxSnapshot int
	timeout     time.Duration
	evictList   *list.List
	snaps       map[string]*list.Element
}

func (s *snapshotRecorder) Set(st *outgoingSnapshot) error {
	s.Lock()
	defer s.Unlock()

	if s.evictList.Len() >= s.maxSnapshot {
		elem := s.evictList.Front()
		snap := elem.Value.(*outgoingSnapshot)
		if time.Since(snap.expire) < 0 {
			return raft.ErrSnapshotTemporarilyUnavailable
		}
		s.evictList.Remove(elem)
		elem.Value.(*outgoingSnapshot).Close()
		delete(s.snaps, snap.id)
	}
	if _, hit := s.snaps[st.id]; hit {
		return fmt.Errorf("outgoingSnapshot(%s) exist", st.id)
	}
	st.expire = time.Now().Add(s.timeout)
	s.snaps[st.id] = s.evictList.PushBack(st)
	return nil
}

func (s *snapshotRecorder) Pop() *outgoingSnapshot {
	s.RLock()
	defer s.RUnlock()

	if s.evictList.Len() == 0 {
		return nil
	}
	elem := s.evictList.Front()
	snap := elem.Value.(*outgoingSnapshot)
	return snap
}

func (s *snapshotRecorder) Get(key string) *outgoingSnapshot {
	s.Lock()
	defer s.Unlock()

	if v, ok := s.snaps[key]; ok {
		snap := v.Value.(*outgoingSnapshot)
		snap.expire = time.Now().Add(s.timeout)
		s.evictList.MoveToBack(v)
		return snap
	}
	return nil
}

func (s *snapshotRecorder) Delete(key string) {
	s.Lock()
	defer s.Unlock()

	if v, ok := s.snaps[key]; ok {
		delete(s.snaps, key)
		// as the snapshot may be used in different follower's snapshot transmitting
		// we can't close the snapshot directly after recorder delete
		// v.Value.(*outgoingSnapshot).Close()
		s.evictList.Remove(v)
	}
}

func (s *snapshotRecorder) Close() {
	s.Lock()
	defer s.Unlock()

	for key, val := range s.snaps {
		delete(s.snaps, key)
		s.evictList.Remove(val)
	}
}
