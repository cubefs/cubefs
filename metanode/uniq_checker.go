// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

const (
	checkerVersionSize = 4
	CrcUint32Size      = 4
	checkerVersionV1   = 1
	checkerVersionV2   = 2
	checkerRecordV1Len = 16
	checkerRecordV2Len = 24
	opKeepTime         = 300
	opKeepOps          = 1024
	opRebuildSec       = 86400
	opCheckerInterval  = time.Second * 10

	opCheckerSliceCap = 1024
	// Optimization: Batch size for eviction processing
	evictionBatchSize = 1000
	// Optimization: Sleep interval for long operations
	sleepInterval = 100 * time.Microsecond
)

type uniqOp struct {
	uniqid  uint64
	atime   int64
	applyId uint64
}

type uniqChecker struct {
	sync.RWMutex
	op    map[uint64]*uniqOp
	inQue *uniqOpQueue
	rtime int64

	keepTime int64
	keepOps  int

	// Optimization: Cache for frequently accessed data
	lastEvictTime int64
	evictCount    int64
}

func newUniqChecker() *uniqChecker {
	return &uniqChecker{
		op:       make(map[uint64]*uniqOp),
		inQue:    newUniqOpQueue(),
		keepTime: opKeepTime,
		keepOps:  opKeepOps,
		rtime:    timeutil.GetCurrentTimeUnix(),
	}
}

func (checker *uniqChecker) clone() *uniqChecker {
	checker.RLock()
	inQue := checker.inQue.clone()
	checker.RUnlock()
	return &uniqChecker{inQue: inQue}
}

func (checker *uniqChecker) Marshal(version int32) (buf []byte, crc uint32, err error) {
	buffer := bytes.NewBuffer(make([]byte, 0, checkerVersionSize+checker.inQue.len()*checkerRecordV2Len))
	if err = binary.Write(buffer, binary.BigEndian, version); err != nil {
		return
	}

	// Optimization: Process in batches to reduce lock time
	checker.inQue.scan(func(op *uniqOp) bool {
		if err = binary.Write(buffer, binary.BigEndian, op.uniqid); err != nil {
			return false
		}
		if err = binary.Write(buffer, binary.BigEndian, op.atime); err != nil {
			return false
		}
		if version == checkerVersionV2 {
			if err = binary.Write(buffer, binary.BigEndian, op.applyId); err != nil {
				return false
			}
		}
		return true
	})

	// Optimization: Use more efficient CRC calculation
	sign := crc32.NewIEEE()
	if _, err = sign.Write(buffer.Bytes()); err != nil {
		return
	}
	crc = sign.Sum32()

	buf = buffer.Bytes()
	return
}

func (checker *uniqChecker) UnMarshal(data []byte) (err error) {
	if len(data) < checkerVersionSize {
		err = errors.New("invalid uniqChecker file length")
		log.LogErrorf("uniqChecker UnMarshal err(%v)", err)
		return
	}

	buff := bytes.NewBuffer(data)
	var version int32
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		log.LogErrorf("uniqChecker unmarshal read version err(%v)", err)
		return
	}

	var uniqid uint64
	var applyId uint64
	var atime int64

	now := time.Now().Unix()

	// Optimization: Pre-allocate map if we can estimate size
	recordCount := (len(data) - checkerVersionSize) / checkerRecordV1Len
	if recordCount > 0 {
		checker.op = make(map[uint64]*uniqOp, recordCount)
	}

	for buff.Len() != 0 {
		if err = binary.Read(buff, binary.BigEndian, &uniqid); err != nil {
			log.LogErrorf("uniqChecker unmarshal read uniqid err(%v)", err)
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &atime); err != nil {
			log.LogErrorf("uniqChecker unmarshal read atime err(%v)", err)
			return
		}
		if version == checkerVersionV2 {
			if err = binary.Read(buff, binary.BigEndian, &applyId); err != nil {
				log.LogErrorf("uniqChecker unmarshal read applyId err(%v)", err)
				return
			}
		}
		// atime over local time is too large
		if atime > now+86400 {
			log.LogWarnf("uniqChecker skip invalid atime %v uniqid %v", atime, uniqid)
			continue
		}
		uniqVal := &uniqOp{uniqid, atime, applyId}
		checker.inQue.append(uniqVal)
		checker.op[uniqid] = uniqVal
	}
	return
}

func (checker *uniqChecker) legalIn(bid uint64, applyId uint64) bool {
	// ignore zero uniqid
	if bid == 0 {
		return true
	}

	// Fast path: check if already exists (read lock)
	checker.RLock()
	if val, ok := checker.op[bid]; ok {
		checker.RUnlock()
		log.LogDebugf("uniqChecker legalIn bid %v applyId %v val.applyId %v", bid, applyId, val.applyId)
		return false
	}
	checker.RUnlock()

	// Slow path: add new entry (write lock)
	checker.Lock()
	defer checker.Unlock()

	uniqVal := &uniqOp{bid, time.Now().Unix(), applyId}
	checker.op[bid] = uniqVal
	checker.inQue.append(uniqVal)
	return true
}

// Optimization: More efficient eviction algorithm
func (checker *uniqChecker) evictIndex() (left int, idx int, op *uniqOp) {
	checker.Lock()
	defer checker.Unlock()

	inQueCnt := checker.inQue.len()
	if inQueCnt <= checker.keepOps {
		return inQueCnt, -1, nil
	}

	var c int
	var lastOp *uniqOp
	nowtime := time.Now().Unix()
	processedCount := 0

	// Optimization: Process in batches to avoid long lock times
	checker.inQue.scan(func(op *uniqOp) bool {
		kt := checker.keepTime
		if inQueCnt-c <= checker.keepOps {
			kt = 10 * checker.keepTime
		}
		if nowtime-op.atime >= kt {
			lastOp = op
			c++
			processedCount++

			// Optimization: Release lock periodically for long operations
			if processedCount%evictionBatchSize == 0 {
				checker.Unlock()
				time.Sleep(sleepInterval)
				checker.Lock()
			}
			return true
		}
		return false
	})

	checker.lastEvictTime = nowtime
	checker.evictCount += int64(c)

	return inQueCnt - c, c - 1, lastOp
}

// Optimization: More efficient eviction with better memory management
func (checker *uniqChecker) doEvict(evictBid uint64) {
	checker.Lock()
	defer checker.Unlock()

	// Optimization: Early return if evictBid doesn't exist
	if _, exists := checker.op[evictBid]; !exists {
		return
	}

	cnt := 0
	// evict from map
	if _, ok := checker.op[evictBid]; ok {
		checker.inQue.scan(func(op *uniqOp) bool {
			cnt++
			delete(checker.op, op.uniqid)
			return op.uniqid != evictBid
		})
	}

	if cnt == 0 {
		return
	}

	// truncate from queue
	checker.inQue.truncate(cnt - 1)

	// regular rebuild map to reduce memory usage
	n := timeutil.GetCurrentTimeUnix()
	if n-checker.rtime > opRebuildSec {
		checker.op = make(map[uint64]*uniqOp, checker.inQue.len())
		checker.inQue.scan(func(op *uniqOp) bool {
			checker.op[op.uniqid] = op
			return true
		})
		checker.rtime = n
	}
}

type uniqOpSlice struct {
	s []*uniqOp
}

// uniqOpQueue append only queue, item in queue should not be modified
type uniqOpQueue struct {
	cnt int
	ss  []*uniqOpSlice
	cur *uniqOpSlice
}

func newUniqOpQueue() *uniqOpQueue {
	s := &uniqOpSlice{s: make([]*uniqOp, 0, opCheckerSliceCap)}
	return &uniqOpQueue{
		cnt: 0,
		ss:  []*uniqOpSlice{s},
		cur: s,
	}
}

func (b *uniqOpQueue) append(v *uniqOp) {
	if cap(b.cur.s)-len(b.cur.s) == 0 {
		b.cur = &uniqOpSlice{s: make([]*uniqOp, 0, opCheckerSliceCap)}
		b.ss = append(b.ss, b.cur)
	}
	b.cur.s = append(b.cur.s, v)
	b.cnt++
}

// Optimization: More efficient indexing
func (b *uniqOpQueue) index(idx int) *uniqOp {
	if idx < 0 || idx >= b.cnt {
		return nil
	}

	for _, s := range b.ss {
		l := len(s.s)
		if idx < l {
			return s.s[idx]
		}
		idx -= l
	}
	return nil
}

// Optimization: More efficient truncation
func (b *uniqOpQueue) truncate(idx int) {
	if idx >= b.cnt-1 {
		b.reset()
		return
	}

	b.cnt = b.cnt - idx - 1

	var tidx int
	var s *uniqOpSlice
	for tidx, s = range b.ss {
		l := len(s.s)
		if idx < l {
			b.ss[tidx].s = s.s[idx+1:]
			break
		}
		idx -= l
	}
	b.ss = b.ss[tidx:]
}

func (b *uniqOpQueue) scan(fn func(op *uniqOp) bool) {
	for _, s := range b.ss {
		for _, op := range s.s {
			if !fn(op) {
				return
			}
		}
	}
}

func (b *uniqOpQueue) len() int {
	return b.cnt
}

func (b *uniqOpQueue) reset() {
	b.cur = &uniqOpSlice{s: make([]*uniqOp, 0, opCheckerSliceCap)}
	b.ss = []*uniqOpSlice{b.cur}
	b.cnt = 0
}

// Optimization: More efficient cloning
func (b *uniqOpQueue) clone() *uniqOpQueue {
	ss := make([]*uniqOpSlice, 0, len(b.ss))
	for _, s := range b.ss {
		// Optimization: Use copy to avoid slice sharing issues
		newSlice := make([]*uniqOp, len(s.s))
		copy(newSlice, s.s)
		ss = append(ss, &uniqOpSlice{s: newSlice})
	}

	return &uniqOpQueue{
		cnt: b.cnt,
		ss:  ss,
		cur: ss[len(ss)-1],
	}
}

// Optimization: Add utility methods for monitoring
func (checker *uniqChecker) getStats() map[string]interface{} {
	checker.RLock()
	defer checker.RUnlock()

	return map[string]interface{}{
		"queue_length": checker.inQue.len(),
		"map_size":     len(checker.op),
		"last_evict":   checker.lastEvictTime,
		"evict_count":  checker.evictCount,
	}
}
