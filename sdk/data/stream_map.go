package data

import (
	"sync"
)

// concurrent map and lock
const SegmentCount = 256

type ConcurrentStreamerMap []*ConcurrentStreamerMapSegment

type ConcurrentStreamerMapSegment struct {
	sync.RWMutex
	streamers map[uint64]*Streamer
}

func InitConcurrentStreamerMap() (m ConcurrentStreamerMap) {
	m = make([]*ConcurrentStreamerMapSegment, SegmentCount)
	for i := 0; i < SegmentCount; i++ {
		m[i] = &ConcurrentStreamerMapSegment{
			streamers: make(map[uint64]*Streamer),
		}
	}
	return m
}

func (m ConcurrentStreamerMap) GetMapSegment(inode uint64) *ConcurrentStreamerMapSegment {
	index := inode % SegmentCount
	return m[index]
}

func (m ConcurrentStreamerMap) Length() int {
	length := 0
	for i := 0; i < SegmentCount; i++ {
		mapSeg := m[i]
		mapSeg.Lock()
		length += len(mapSeg.streamers)
		mapSeg.Unlock()
	}
	return length
}

func (m ConcurrentStreamerMap) Keys() []uint64 {
	inodes := make([]uint64, 0)
	for i := 0; i < SegmentCount; i++ {
		mapSeg := m[i]
		mapSeg.Lock()
		for inode, _ := range mapSeg.streamers {
			inodes = append(inodes, inode)
		}
		mapSeg.Unlock()
	}
	return inodes
}
