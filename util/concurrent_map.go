package util

import (
	"sync"
)

type ConcurrentMap struct {
	sync.RWMutex
	m map[string]struct{}
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		m: make(map[string]struct{}),
	}
}

func (s *ConcurrentMap) Add(key string) {
	s.Lock()
	defer s.Unlock()
	s.m[key] = struct{}{}
}

func (s *ConcurrentMap) Remove(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, key)
}

func (s *ConcurrentMap) Contains(key string) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *ConcurrentMap) List() []string {
	list := make([]string, 0)
	s.RLock()
	defer s.RUnlock()
	for key := range s.m {
		list = append(list, key)
	}
	return list
}

func (s *ConcurrentMap) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.m)
}
