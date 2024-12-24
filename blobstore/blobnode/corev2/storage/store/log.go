// Copyright 2024 The CubeFS Authors.
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

package store

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
)

var errLogArenaWriteFull = errors.New("log arena write full")

type (
	logHandler interface {
		Submit(lm logEntry) error
		GetHeader() logHeader
		UpdateHeader(h logHeader) error
	}
	logEntry interface {
		Size() uint16
		MarshalTo(raw []byte) error
		Unmarshal(raw []byte) error
		NotifyError(err error)
		Error() error
	}

	submitRet struct {
		idx        uint32
		checkpoint bool
	}
)

type logMgr struct {
	idx                uint32
	lhs                []logHandler
	latestLogHeaderVer logHeaderVer

	sf singleflight.Group
}

func (l *logMgr) Submit(lm logEntry) (ret submitRet, err error) {
AGAIN:
	idx := atomic.LoadUint32(&l.idx)
	lh := l.lhs[idx]
	err = lh.Submit(lm)
	if err == nil {
		return
	}

	if !errors.Is(err, errLogArenaWriteFull) {
		return
	}

	_, err, _ = l.sf.Do("switch", func() (interface{}, error) {
		// the first winner will raise checkpoint
		ret.idx = idx
		ret.checkpoint = true

		// use new log arena if available
		backupIdx := (idx + 1) % 2
		backup := l.lhs[backupIdx]
		if !backup.GetHeader().flag {
			return nil, err
		}

		// update backup log header
		if _err := backup.UpdateHeader(logHeader{
			ver:  l.latestLogHeaderVer,
			flag: false,
		}); err != nil {
			return nil, _err
		}

		l.latestLogHeaderVer++
		atomic.StoreUint32(&l.idx, backupIdx)

		return nil, nil
	})
	if err != nil {
		return
	}

	goto AGAIN
}

func (l *logMgr) CheckpointDone(logArenaIdx uint32) error {
	lh := l.lhs[logArenaIdx]

	header := lh.GetHeader()
	return lh.UpdateHeader(logHeader{
		ver:  header.ver,
		flag: true,
	})
}

type log struct {
	header       logHeader
	queue        logQueue
	logArenaSize uint64
	// log arena start offset
	startOffset uint64
	// current log record index, it will be reset when update log header
	currentLogRecordIndex uint64
	// max log record index, calculated by logArenaSize/logRecordSize
	maxLogRecordIndex uint64
	// logRecordSize
	logRecordSize uint64
	logHeaderSize uint64
	logHeaderBuff []byte
	ioEngine      iouring.Engine
}

func (l *log) Submit(lm logEntry) error {
	// check if log arena write full
	if atomic.LoadUint64(&l.currentLogRecordIndex) == l.maxLogRecordIndex {
		return errLogArenaWriteFull
	}

	queueIdx, done := l.queue.add(lm)
	// the first submitter will raise write batch loop
	if queueIdx == 1 {
		go l.writeBatch()
	}

	// wait for done
	<-done
	return lm.Error()
}

func (l *log) GetHeader() logHeader {
	return l.header
}

func (l *log) UpdateHeader(h logHeader) error {
	err := h.MarshalTo(l.logHeaderBuff)
	if err != nil {
		return err
	}

	return l.ioEngine.Read(l.logHeaderBuff, l.startOffset-l.logHeaderSize, len(l.logHeaderBuff))
}

func (l *log) Replay(fn func(le logEntry) bool) error {
	// no need to reuse as replay will be happened on process start
	buff := make([]byte, 1<<20)
	lr := &logRecord{}

	end := (l.logArenaSize + uint64(len(buff)) - 1) / uint64(len(buff))
	for i := 0; i < int(end); i++ {
		if err := l.ioEngine.Read(buff, l.startOffset+uint64(i*len(buff)), len(buff)); err != nil {
			return err
		}
		for {
			lr.raw = buff[:l.logRecordSize]
			// goto the end
			if lr.Ver() != l.header.ver {
				return nil
			}

			payload := lr.Payload()
			switch lr.RecordType() {
			case logRecordTypeSliceMeta:
				// todo: use map to get logEntry interface implement
				lsm := &logSliceMeta{}
				size := lsm.Size()
				for {
					if err := lsm.Unmarshal(payload[:size]); err != nil {
						return err
					}
					if !fn(lsm) {
						return nil
					}
					if len(payload) < int(size) {
						break
					}
					payload = payload[size:]
				}
			default:
				return errors.New("unsupported record type")
			}

			// move forward to next log record
			buff = buff[l.logRecordSize:]
		}
	}

	return nil
}

func (l *log) writeBatch() {
AGAIN:
	lms, done, ok := l.queue.drain()
	if !ok {
		return
	}

	lr := allocLogRecord()
	payload := lr.Payload()
	startIdx := 0

	for i, lm := range lms {
		size := lm.Size()
		// start to write when write full of one log record
		if size > uint16(len(payload)) {
			currentLogRecordIndex := atomic.LoadUint64(&l.currentLogRecordIndex)
			offset := l.startOffset + l.logRecordSize*currentLogRecordIndex
			err := l.ioEngine.Write(lr.Raw(), offset, int(l.logRecordSize))
			if err != nil {
				// notify all log entry waiter
				for j := startIdx; j < i; j++ {
					lms[j].NotifyError(err)
				}
			}
			// move forward to next cursor of lms slice
			startIdx = i
			// move to next log record index
			atomic.AddUint64(&l.currentLogRecordIndex, 1)

			// reset log record struct and reset payload buffer
			lr.Reset()
			payload = lr.Payload()
		}

		if err := lm.MarshalTo(payload); err != nil {
			lm.NotifyError(err)
			continue
		}
		payload = payload[size:]
	}

	freeLogRecord(lr)
	l.queue.recycle(lms)
	close(done)

	goto AGAIN
}

type logQueue struct {
	currentQueueIdx int
	queues          [2]struct {
		written bool
		done    chan struct{}
		lms     []logEntry
	}
	sync.Mutex
}

func (q *logQueue) add(lm logEntry) (queueIdx int, done <-chan struct{}) {
	q.Lock()
	defer q.Unlock()

	done = q.queues[q.currentQueueIdx].done
	q.queues[q.currentQueueIdx].lms = append(q.queues[q.currentQueueIdx].lms, lm)

	lastQueueIdx := (q.currentQueueIdx + 1) % 2
	queueIdx = math.MaxInt
	// last queue has been written, then start new write operation if first add
	if q.queues[lastQueueIdx].written {
		queueIdx = len(q.queues[q.currentQueueIdx].lms)
	}

	return
}

func (q *logQueue) drain() ([]logEntry, chan struct{}, bool) {
	q.Lock()
	defer q.Unlock()

	if len(q.queues[q.currentQueueIdx].lms) == 0 {
		return nil, nil, false
	}

	lms := q.queues[q.currentQueueIdx].lms
	q.queues[q.currentQueueIdx].lms = nil
	q.queues[q.currentQueueIdx].written = false

	q.currentQueueIdx = (q.currentQueueIdx + 1) % 2

	return lms, q.queues[q.currentQueueIdx].done, true
}

func (q *logQueue) recycle(processed []logEntry) {
	q.Lock()
	defer q.Unlock()

	lastQueueIdx := (q.currentQueueIdx + 1) % 2
	q.queues[lastQueueIdx].written = true
	q.queues[lastQueueIdx].lms = processed[:0]
	// todo: how to reuse
	q.queues[lastQueueIdx].done = make(chan struct{})
}

type logHeader struct {
	// auto increment field, the latest version log arena hold latest meta data which need to be replayed
	ver logHeaderVer `json:"version"`
	// flag 0 means this log arena is not checkpoint, flag 1 means this log arena is checkpoint already
	flag bool   `json:"flag"`
	crc  uint32 `json:"crc"`
}

// Marshal encode logHeader into []byte with 4096 align and padding
func (l *logHeader) Marshal() ([]byte, error) {
	// todo: calculate checksum automatically

	return nil, nil
}

// MarshalTo encode logHeader into []byte with 4096 align and padding
func (l *logHeader) MarshalTo(raw []byte) error {
	// todo: calculate checksum automatically

	return nil
}

func (l *logHeader) Unmarshal(raw []byte) error {
	return nil
}

type logRecord struct {
	// raw hold fields below, we keep the raw bytes to save decode memory copy
	// ver     logHeaderVer
	// typ     logRecordType
	// size    uint16
	// payload []byte
	// crc     uint32
	raw           []byte
	crcCalculated bool
}

func (lr *logRecord) Reset() {
	// todo: reset size and crc
}

func (lr *logRecord) SetVer(ver logHeaderVer) {
}

func (lr *logRecord) SetSize(size uint16) {
}

func (lr *logRecord) Raw() []byte {
	// check if crc calculate
	if !lr.crcCalculated {
		// todo: calculate crc checksum
	}
	return lr.raw
}

func (lr *logRecord) Ver() logHeaderVer {
	return 0
}

func (lr *logRecord) RecordType() logRecordType {
	return 0
}

func (lr *logRecord) Size() uint16 {
	return 0
}

func (lr *logRecord) Payload() []byte {
	return lr.raw[14:]
}

func (lr *logRecord) CRC() uint32 {
	return 0
}

var logRecordPool = sync.Pool{New: func() interface{} {
	return &logRecord{raw: make([]byte, rawStoreFormatV1Layout.logRecordSize)}
}}

func allocLogRecord() *logRecord {
	return logRecordPool.Get().(*logRecord)
}

func freeLogRecord(lr *logRecord) {
	logRecordPool.Put(lr)
}
