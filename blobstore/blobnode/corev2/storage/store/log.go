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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	errLogArenaWriteFull = errors.New("log arena write full")
	zeroed               = make([]byte, 8)
)

type (
	logHandler interface {
		Submit(lm logEntry) error
		Replay(fn func(le logEntry) error) error
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

type logMgrConfig struct {
	logConfigs []logConfig
}

func newLogMgr(cfg logMgrConfig) (*logMgr, error) {
	lm := &logMgr{}

	lm.latestLogHeaderVer = initLogHeaderVer
	for i := range cfg.logConfigs {
		lh, err := newLog(cfg.logConfigs[i])
		if err != nil {
			return nil, err
		}
		lm.lhs[i] = lh
		if h := lh.GetHeader(); h.ver > lm.latestLogHeaderVer {
			lm.latestLogHeaderVer = h.ver
		}
	}

	return lm, nil
}

type logMgr struct {
	idx                uint32
	lhs                [2]logHandler
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
		if backup.GetHeader().flag == logHeaderFlagUnCheckpoint {
			return nil, err
		}

		// update backup log header
		l.latestLogHeaderVer++
		if _err := backup.UpdateHeader(logHeader{
			ver:  l.latestLogHeaderVer,
			flag: logHeaderFlagUnCheckpoint,
		}); err != nil {
			return nil, _err
		}

		atomic.StoreUint32(&l.idx, backupIdx)

		return nil, nil
	})
	if err != nil {
		return
	}

	goto AGAIN
}

func (l *logMgr) Replay(fn func(le logEntry) error) error {
	h1 := l.lhs[0].GetHeader()
	h2 := l.lhs[1].GetHeader()

	latestIdx := 0
	if h1.ver < h2.ver {
		latestIdx = 1
	}

	oldestIdx := (latestIdx + 1) % 2
	if l.lhs[oldestIdx].GetHeader().flag == logHeaderFlagUnCheckpoint {
		if err := l.lhs[oldestIdx].Replay(fn); err != nil {
			return err
		}
	}
	if l.lhs[latestIdx].GetHeader().flag == logHeaderFlagUnCheckpoint {
		if err := l.lhs[latestIdx].Replay(fn); err != nil {
			return err
		}
	}

	return nil
}

func (l *logMgr) CheckpointDone(logArenaIdx uint32) error {
	lh := l.lhs[logArenaIdx]

	header := lh.GetHeader()
	return lh.UpdateHeader(logHeader{
		ver:  header.ver,
		flag: logHeaderFlagCheckpoint,
	})
}

type logConfig struct {
	logArenaSize uint64
	// log arena start offset
	startOffset   uint64
	logHeaderSize uint64
	logRecordSize uint64
	ioEngine      iouring.Engine
}

func newLog(cfg logConfig) (*log, error) {
	logHeaderBuff := util.AllocAlignedBlock(int(cfg.logHeaderSize), deviceSectorSize)

	if err := cfg.ioEngine.Read(logHeaderBuff, cfg.startOffset, len(logHeaderBuff)); err != nil {
		return nil, errors.Info(err, "read log header failed")
	}
	lh := logHeader{}
	if err := lh.Unmarshal(logHeaderBuff); err != nil {
		return nil, errors.Info(err, "unmarshal from log header buffer failed: ", err, " raw： ", logHeaderBuff)
	}

	return &log{
		header:            lh,
		queue:             newLogQueue(),
		maxLogRecordIndex: cfg.logArenaSize / cfg.logRecordSize,
		logHeaderBuff:     logHeaderBuff,
		cfg:               cfg,
	}, nil
}

type log struct {
	header logHeader
	queue  *logQueue
	// current log record index, it will be reset when update log header
	currentLogRecordIndex uint64
	// max log record index, calculated by logArenaSize/logRecordSize
	maxLogRecordIndex uint64
	logHeaderBuff     []byte

	cfg logConfig
}

func (l *log) Submit(lm logEntry) error {
	// check if log arena write full
	if atomic.LoadUint64(&l.currentLogRecordIndex) == l.maxLogRecordIndex {
		return errLogArenaWriteFull
	}

	queueIdx := l.queue.add(lm)
	// the first submitter will raise write batch loop
	if queueIdx == 1 {
		go l.writeBatch()
	}

	// wait for done
	// <-done
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

	return l.cfg.ioEngine.Write(l.logHeaderBuff, l.cfg.startOffset, len(l.logHeaderBuff))
}

func (l *log) Replay(fn func(le logEntry) error) error {
	// no need to reuse as replay will be happened on process start
	readBuff := util.AllocAlignedBlock(1<<20, deviceSectorSize)
	lr := &logRecord{}

	end := (l.cfg.logArenaSize + uint64(len(readBuff)) - 1) / uint64(len(readBuff))
	for i := 0; i < int(end); i++ {
		if err := l.cfg.ioEngine.Read(readBuff, l.cfg.startOffset+l.cfg.logHeaderSize+uint64(i*len(readBuff)), len(readBuff)); err != nil {
			return err
		}

		buff := readBuff
		for {
			// todo: deal with the Init error return
			if err := lr.Init(buff[:l.cfg.logRecordSize]); err != nil {
				return nil
			}
			// goto the end
			if lr.Ver() != l.header.ver {
				return nil
			}

			payload := lr.ActualPayload()
			switch lr.RecordType() {
			case logRecordTypeSliceMeta:
				// todo: use map to get logEntry interface implement
				lsm := &logSliceMeta{}
				size := lsm.Size()
				for {
					if err := lsm.Unmarshal(payload[:size]); err != nil {
						return err
					}
					if err := fn(lsm); err != nil {
						return err
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
			buff = buff[l.cfg.logRecordSize:]
			if len(buff) == 0 {
				break
			}
		}
	}

	return nil
}

func (l *log) writeBatch() {
AGAIN:
	lms, ok := l.queue.drain()
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
			offset := l.cfg.startOffset + l.cfg.logHeaderSize + l.cfg.logRecordSize*currentLogRecordIndex
			err := l.cfg.ioEngine.Write(lr.Raw(), offset, int(l.cfg.logRecordSize))
			// notify all log entry waiter
			for j := startIdx; j < i; j++ {
				lms[j].NotifyError(err)
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

	// the rest write
	if startIdx < len(lms) {
		currentLogRecordIndex := atomic.LoadUint64(&l.currentLogRecordIndex)
		offset := l.cfg.startOffset + l.cfg.logHeaderSize + l.cfg.logRecordSize*currentLogRecordIndex
		err := l.cfg.ioEngine.Write(lr.Raw(), offset, int(l.cfg.logRecordSize))
		// notify all log entry waiter
		for j := startIdx; j < len(lms); j++ {
			lms[j].NotifyError(err)
		}
		// move to next log record index
		atomic.AddUint64(&l.currentLogRecordIndex, 1)
	}

	freeLogRecord(lr)
	l.queue.recycle(lms)
	// close(done)

	goto AGAIN
}

func newLogQueue() *logQueue {
	return &logQueue{
		queues: [2]struct {
			written bool
			lms     []logEntry
		}{
			{lms: make([]logEntry, 0, 128), written: true},
			{lms: make([]logEntry, 0, 128), written: true},
		},
	}
}

type logQueue struct {
	currentQueueIdx int
	queues          [2]struct {
		written bool
		// done    chan struct{}
		lms []logEntry
	}
	sync.Mutex
}

func (q *logQueue) add(lm logEntry) (queueIdx int /*done <-chan struct{}*/) {
	q.Lock()

	// done = q.queues[q.currentQueueIdx].done
	q.queues[q.currentQueueIdx].lms = append(q.queues[q.currentQueueIdx].lms, lm)

	lastQueueIdx := (q.currentQueueIdx + 1) % 2
	queueIdx = math.MaxInt
	// last queue has been written, then start new write operation if first add
	if q.queues[lastQueueIdx].written {
		queueIdx = len(q.queues[q.currentQueueIdx].lms)
	}

	q.Unlock()
	return
}

func (q *logQueue) drain() ([]logEntry /*chan struct{},*/, bool) {
	q.Lock()
	defer q.Unlock()

	if len(q.queues[q.currentQueueIdx].lms) == 0 {
		return nil /*nil, */, false
	}

	lms := q.queues[q.currentQueueIdx].lms
	q.queues[q.currentQueueIdx].lms = nil
	q.queues[q.currentQueueIdx].written = false

	q.currentQueueIdx = (q.currentQueueIdx + 1) % 2

	return lms /*q.queues[q.currentQueueIdx].done,*/, true
}

func (q *logQueue) recycle(processed []logEntry) {
	q.Lock()
	lastQueueIdx := (q.currentQueueIdx + 1) % 2
	q.queues[lastQueueIdx].written = true
	q.queues[lastQueueIdx].lms = processed[:0]
	// q.queues[lastQueueIdx].done = make(chan struct{})
	q.Unlock()
}

type logHeader struct {
	// auto increment field, the latest version log arena hold latest meta data which need to be replayed
	ver logHeaderVer `json:"version"`
	// flag 0 means this log arena is not checkpoint, flag 1 means this log arena is checkpoint already
	flag logHeaderFlag `json:"flag"`
	crc  uint32        `json:"crc"`
}

// Marshal encode logHeader into []byte with 4096 align and padding
func (l *logHeader) Marshal() ([]byte, error) {
	raw := util.AllocAlignedBlock(4<<10, deviceSectorSize)
	if err := l.MarshalTo(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// MarshalTo encode logHeader into []byte with 4096 align and padding
func (l *logHeader) MarshalTo(raw []byte) error {
	copy(raw, _superBlockMagic[:])
	raw = raw[_superBlockMagicSize:]

	// calculate checksum automatically
	w := crc32.NewIEEE()
	binary.BigEndian.PutUint64(raw, uint64(l.ver))
	raw[8] = byte(l.flag)

	if _, err := w.Write(raw[:9]); err != nil {
		return err
	}
	crc := w.Sum32()
	binary.BigEndian.PutUint32(raw[9:], crc)
	l.crc = crc

	return nil
}

func (l *logHeader) Unmarshal(raw []byte) error {
	if !bytes.Equal(raw[:_superBlockMagicSize], _superBlockMagic[:]) {
		return nil
	}
	raw = raw[_superBlockMagicSize:]

	l.ver = logHeaderVer(binary.BigEndian.Uint64(raw))
	l.flag = logHeaderFlag(raw[8])
	l.crc = binary.BigEndian.Uint32(raw[9:])
	crc := crc32.ChecksumIEEE(raw[:9])
	if l.crc != crc {
		return fmt.Errorf("crc validate failed: %d-%d", l.crc, crc)
	}

	return nil
}

// todo: log arena need to be init to be 0

type logRecord struct {
	// raw hold fields below, we keep the raw bytes to save decode memory copy
	// crc     uint32
	// ver     logHeaderVer
	// typ     logRecordType
	// size    uint16
	// payload []byte
	raw           []byte
	crcCalculated bool
}

func (lr *logRecord) Init(raw []byte) error {
	// check crc
	crc := binary.BigEndian.Uint32(raw[:4])
	size := binary.BigEndian.Uint16(raw[13:])

	validatedCrc := crc32.ChecksumIEEE(raw[4 : 11+size])
	// todo: how to distinguish the crc checksum error or empty log record?
	if validatedCrc != crc {
		return fmt.Errorf("log record validate crc failed: %d-%d", crc, validatedCrc)
	}

	lr.raw = raw
	return nil
}

func (lr *logRecord) Reset() {
	// reset size and crc
	copy(lr.raw[:4], zeroed)
	copy(lr.raw[13:15], zeroed)
	lr.crcCalculated = false
}

func (lr *logRecord) SetVer(ver logHeaderVer) {
	binary.BigEndian.PutUint64(lr.raw[4:], uint64(ver))
}

func (lr *logRecord) SetSize(size uint16) {
	binary.BigEndian.PutUint16(lr.raw[13:], size)
}

func (lr *logRecord) Raw() []byte {
	// check if crc calculate
	if !lr.crcCalculated {
		// calculate crc checksum
		binary.BigEndian.PutUint32(lr.raw, crc32.ChecksumIEEE(lr.raw[4:11+lr.Size()]))
		lr.crcCalculated = true
	}
	return lr.raw
}

func (lr *logRecord) Ver() logHeaderVer {
	return logHeaderVer(binary.BigEndian.Uint64(lr.raw[4:]))
}

func (lr *logRecord) RecordType() logRecordType {
	return logRecordType(lr.raw[12])
}

func (lr *logRecord) Size() uint16 {
	return binary.BigEndian.Uint16(lr.raw[13:])
}

func (lr *logRecord) ActualPayload() []byte {
	return lr.raw[15:][:lr.Size()]
}

func (lr *logRecord) Payload() []byte {
	return lr.raw[15:]
}

func (lr *logRecord) CRC() uint32 {
	return binary.BigEndian.Uint32(lr.raw[:4])
}

var logRecordPool = sync.Pool{New: func() interface{} {
	return &logRecord{raw: util.AllocAlignedBlock(int(rawStoreFormatV1Layout.logRecordSize), deviceSectorSize)}
}}

func allocLogRecord() *logRecord {
	return logRecordPool.Get().(*logRecord)
}

func freeLogRecord(lr *logRecord) {
	logRecordPool.Put(lr)
}
