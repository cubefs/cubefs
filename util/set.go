package util

import "sync"

type Null struct {
}
type Set struct {
	sync.RWMutex
	m map[string]Null
}

func NewSet() *Set {
	return &Set{
		m: map[string]Null{},
	}
}

func (s *Set) Add(val string) {
	s.Lock()
	defer s.Unlock()
	s.m[val] = Null{}
}
func (s *Set) Remove(val string) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, val)
}

func (s *Set) Has(key string) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *Set) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.m)
}

func (s *Set) Clear() {
	s.Lock()
	defer s.Unlock()
	s.m = make(map[string]Null)
}
