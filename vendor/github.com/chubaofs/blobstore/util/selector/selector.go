package selector

import (
	"errors"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Selector select hosts or something from somewhere.
type Selector interface {
	GetHashN(n int, key []byte) []string
	GetRandomN(n int) []string
	GetRoundRobinN(n int) []string
}

type selector struct {
	sync.RWMutex

	interval     int64
	lastUpdate   int64
	nextIndex    int
	cachedValues []string
	getter       func() ([]string, error)
}

// NewSelector implements Selector
func NewSelector(intervalMs int64, getter func() ([]string, error)) (Selector, error) {
	values, err := getter()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, errors.New("not found initial values from getter")
	}

	s := &selector{
		interval:     int64(time.Millisecond) * intervalMs,
		lastUpdate:   time.Now().UnixNano(),
		nextIndex:    0,
		cachedValues: values,
		getter:       getter,
	}
	return s, nil
}

func (s *selector) GetHashN(n int, key []byte) []string {
	if n <= 0 {
		return nil
	}
	values := s.getValues(n, false)
	if len(values) <= 1 {
		return values
	}

	idx := int(keyHash(key) % uint64(len(values)))
	values[0], values[idx] = values[idx], values[0]

	hashed := values[1:]
	rand.Shuffle(len(hashed), func(i, j int) {
		hashed[i], hashed[j] = hashed[j], hashed[i]
	})

	if n <= len(values) {
		values = values[:n]
	}
	return values
}

func (s *selector) GetRandomN(n int) []string {
	if n <= 0 {
		return nil
	}
	values := s.getValues(n, false)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	if n <= len(values) {
		values = values[:n]
	}
	return values
}

func (s *selector) GetRoundRobinN(n int) []string {
	if n <= 0 {
		return nil
	}
	values := s.getValues(n, true)
	if n <= len(values) {
		values = values[:n]
	}
	return values
}

func (s *selector) sync() {
	values, err := s.getter()
	if err != nil {
		return
	}
	if len(values) == 0 {
		return
	}
	sort.Strings(values)

	s.Lock()
	if s.nextIndex >= len(values) {
		s.nextIndex = 0
	}
	s.cachedValues = values
	s.lastUpdate = time.Now().UnixNano()
	s.Unlock()
}

func (s *selector) getValues(n int, rr bool) []string {
	s.RLock()
	interval := s.interval
	lastUpdate := s.lastUpdate
	idx := s.nextIndex
	values := make([]string, len(s.cachedValues))
	copy(values, s.cachedValues)
	s.RUnlock()

	if rr {
		if n > len(values) {
			n = len(values)
		}

		s.Lock()
		s.nextIndex = (s.nextIndex + n) % len(s.cachedValues)
		s.Unlock()

		values = append(values[idx:], values[0:idx]...)
	}

	if time.Now().UnixNano() >= lastUpdate+interval {
		go s.sync()
	}

	return values
}

func keyHash(key []byte) uint64 {
	f := fnv.New64()
	f.Write(key)
	return f.Sum64()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
