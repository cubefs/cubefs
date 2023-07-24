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
)

const (
	checkerVersionSize = 4
	CrcUint32Size      = 4
	checkerVersion     = 1
	checkerRecordV1Len = 16
	opKeepTime         = 300
	opKeepOps          = 1024
	opRebuildSec       = 86400
	opCheckerInterval  = time.Second * 10

	opCheckerSliceCap = 1024
)

type uniqOp struct {
	uniqid uint64
	atime  int64
}

type uniqChecker struct {
	sync.Mutex
	op    map[uint64]struct{}
	inQue *uniqOpQueue
	rtime int64

	keepTime int64
	keepOps  int
}

func newUniqChecker() *uniqChecker {
	return &uniqChecker{
		op:       make(map[uint64]struct{}),
		inQue:    newUniqOpQueue(),
		keepTime: opKeepTime,
		keepOps:  opKeepOps,
		rtime:    Now.GetCurrentTime().Unix(),
	}
}

func (checker *uniqChecker) clone() *uniqChecker {
	checker.Lock()
	inQue := checker.inQue.clone()
	checker.Unlock()
	return &uniqChecker{inQue: inQue}
}

func (checker *uniqChecker) Marshal() (buf []byte, crc uint32, err error) {
	buffer := bytes.NewBuffer(make([]byte, 0, checkerVersionSize+checker.inQue.len()*checkerRecordV1Len))
	if err = binary.Write(buffer, binary.BigEndian, int32(checkerVersion)); err != nil {
		return
	}

	checker.inQue.scan(func(op *uniqOp) bool {
		if err = binary.Write(buffer, binary.BigEndian, op.uniqid); err != nil {
			return false
		}
		if err = binary.Write(buffer, binary.BigEndian, op.atime); err != nil {
			return false
		}
		return true
	})

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
	var atime int64
	now := time.Now().Unix()
	for buff.Len() != 0 {
		if err = binary.Read(buff, binary.BigEndian, &uniqid); err != nil {
			log.LogErrorf("uniqChecker unmarshal read uniqid err(%v)", err)
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &atime); err != nil {
			log.LogErrorf("uniqChecker unmarshal read atime err(%v)", err)
			return
		}
		// atime over local time is too large
		if atime > now+86400 {
			log.LogWarnf("uniqChecker skip invalid atime %v uniqid %v", atime, uniqid)
			continue
		}
		checker.inQue.append(&uniqOp{uniqid, atime})
		checker.op[uniqid] = struct{}{}
	}
	return
}

func (checker *uniqChecker) legalIn(bid uint64) bool {
	// ignore zero uniqid
	if bid == 0 {
		return true
	}

	checker.Lock()
	defer checker.Unlock()

	if _, ok := checker.op[bid]; ok {
		return false
	} else {
		checker.op[bid] = struct{}{}
		checker.inQue.append(&uniqOp{bid, time.Now().Unix()})
	}

	return true
}

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

	checker.inQue.scan(func(op *uniqOp) bool {
		kt := checker.keepTime
		if inQueCnt-c <= checker.keepOps {
			kt = 10 * checker.keepTime
		}
		if nowtime-op.atime >= kt {
			lastOp = op
			c++
			if c%10000 == 0 {
				checker.Unlock()
				time.Sleep(100 * time.Microsecond)
				checker.Lock()
			}
			return true
		}
		return false
	})

	return inQueCnt - c, c - 1, lastOp
}

func (checker *uniqChecker) doEvict(evictBid uint64) {
	checker.Lock()
	defer checker.Unlock()

	cnt := 0
	//evict from map
	if _, ok := checker.op[evictBid]; ok {
		checker.inQue.scan(func(op *uniqOp) bool {
			cnt++
			delete(checker.op, op.uniqid)
			if op.uniqid == evictBid {
				return false
			}
			return true
		})
	}

	if cnt == 0 {
		return
	}

	//truncate from queue
	checker.inQue.truncate(cnt - 1)

	//regular rebuild map to reduce memory usage
	n := Now.GetCurrentTime().Unix()
	if n-checker.rtime > opRebuildSec {
		checker.op = make(map[uint64]struct{}, checker.inQue.len())
		checker.inQue.scan(func(op *uniqOp) bool {
			checker.op[op.uniqid] = struct{}{}
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

func (b *uniqOpQueue) index(idx int) *uniqOp {
	for _, s := range b.ss {
		l := len(s.s)
		if idx >= l {
			idx = idx - l
		} else {
			return s.s[idx]
		}
	}
	return nil
}

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
		if idx >= l {
			idx = idx - l
		} else {
			b.ss[tidx].s = s.s[idx+1:]
			break
		}
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

func (b *uniqOpQueue) clone() *uniqOpQueue {
	ss := make([]*uniqOpSlice, 0, len(b.ss))
	for _, s := range b.ss {
		ss = append(ss, &uniqOpSlice{s.s[:]})
	}

	return &uniqOpQueue{
		cnt: b.cnt,
		ss:  ss,
		cur: ss[len(ss)-1],
	}
}
