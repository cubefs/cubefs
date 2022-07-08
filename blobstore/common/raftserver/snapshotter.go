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

package raftserver

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
)

type snapshotter struct {
	sync.Mutex
	maxSnapshot int
	timeout     time.Duration
	evictList   *list.List
	snaps       map[string]*list.Element
	stopc       chan struct{}
}

func newSnapshotter(maxSnapshot int, timeout time.Duration) *snapshotter {
	shotter := &snapshotter{
		maxSnapshot: maxSnapshot,
		timeout:     timeout,
		evictList:   list.New(),
		snaps:       make(map[string]*list.Element),
		stopc:       make(chan struct{}),
	}

	go shotter.evict()
	return shotter
}

func (s *snapshotter) evict() {
	ticker := time.NewTicker(s.timeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.Lock()
			elem := s.evictList.Front()
			if elem != nil {
				snap := elem.Value.(*snapshot)
				if time.Since(snap.expire) >= 0 {
					s.evictList.Remove(elem)
					delete(s.snaps, snap.st.Name())
				}
			}
			s.Unlock()
		case <-s.stopc:
			s.deleteAll()
			return
		}
	}
}

func (s *snapshotter) Set(st *snapshot) error {
	s.Lock()
	defer s.Unlock()
	if s.evictList.Len() >= s.maxSnapshot {
		elem := s.evictList.Front()
		snap := elem.Value.(*snapshot)
		if time.Since(snap.expire) < 0 {
			return raft.ErrSnapshotTemporarilyUnavailable
		}
		s.evictList.Remove(elem)
		delete(s.snaps, snap.st.Name())
	}
	if _, hit := s.snaps[st.Name()]; hit {
		return fmt.Errorf("snapshot(%s) exist", st.Name())
	}
	st.expire = time.Now().Add(s.timeout)
	s.snaps[st.Name()] = s.evictList.PushBack(st)
	return nil
}

func (s *snapshotter) Get(key string) *snapshot {
	s.Lock()
	defer s.Unlock()

	if v, ok := s.snaps[key]; ok {
		snap := v.Value.(*snapshot)
		snap.expire = time.Now().Add(s.timeout)
		s.evictList.MoveToBack(v)
		return snap
	}
	return nil
}

func (s *snapshotter) Delete(key string) {
	s.Lock()
	defer s.Unlock()
	if v, ok := s.snaps[key]; ok {
		delete(s.snaps, key)
		s.evictList.Remove(v)
	}
}

func (s *snapshotter) deleteAll() {
	s.Lock()
	defer s.Unlock()
	for key, val := range s.snaps {
		delete(s.snaps, key)
		s.evictList.Remove(val)
	}
}

func (s *snapshotter) Stop() {
	close(s.stopc)
}
