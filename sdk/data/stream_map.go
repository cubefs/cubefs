package data

import (
	"sync"
)

// concurrent map and lock
const (
	DefaultSegmentCount = 256
)

type ConcurrentStreamerMap []*ConcurrentStreamerMapSegment

type ConcurrentStreamerMapSegment struct {
	sync.RWMutex
	streamers map[uint64]*Streamer
}

func InitConcurrentStreamerMap(setSegCount int64) (m ConcurrentStreamerMap) {
	SegmentCount := DefaultSegmentCount
	if setSegCount > 0 {
		SegmentCount = int(setSegCount)
	}
	m = make([]*ConcurrentStreamerMapSegment, SegmentCount)
	for i := 0; i < SegmentCount; i++ {
		m[i] = &ConcurrentStreamerMapSegment{
			streamers: make(map[uint64]*Streamer),
		}
	}
	return m
}

func (m ConcurrentStreamerMap) GetMapSegment(inode uint64) *ConcurrentStreamerMapSegment {
	index := inode % uint64(len(m))
	return m[index]
}

func (m ConcurrentStreamerMap) Keys() []uint64 {
	inodes := make([]uint64, 0)
	for i := 0; i < len(m); i++ {
		mapSeg := m[i]
		mapSeg.Lock()
		for inode, _ := range mapSeg.streamers {
			inodes = append(inodes, inode)
		}
		mapSeg.Unlock()
	}
	return inodes
}
