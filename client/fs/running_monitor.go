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

package fs

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

const (
	IdxTotal  = 3
	PidOffset = 32
	PidMask   = 0xffffffff
)

var opNameMap = map[string]uint8{
	"create":          1,
	"mkdir":           2,
	"remove":          3,
	"lookup":          4,
	"readdir":         5,
	"rename":          6,
	"setattr":         7,
	"mknod":           8,
	"symlink":         9,
	"link":            10,
	"getxattr":        11,
	"listxattr":       12,
	"setxattr":        13,
	"removexattr":     14,
	"fileopen":        15,
	"filerelease":     16,
	"fileread":        17,
	"filewrite":       18,
	"filesync":        19,
	"filefsnyc":       20,
	"filesetattr":     21,
	"filereadlink":    22,
	"filegetxattr":    23,
	"filelistxattr":   24,
	"filesetxattr":    25,
	"fileremovexattr": 26,
}

type RunningStat struct {
	startTime int64
	pid       uint32
	opIdx     uint8
	index     int
}

type RunningMonitor struct {
	enable                bool
	currentIndex          int
	clientOpRunningCntMap [IdxTotal]*sync.Map
	clientOpTimeOut       int64
	stopC                 chan struct{}
	mapBuffer             chan *sync.Map
	statPool              sync.Pool
}

func NewRunningMonitor(clientOpTimeOut int64) (rm *RunningMonitor) {
	rm = new(RunningMonitor)
	if clientOpTimeOut <= 0 {
		return
	}

	for i := 0; i < IdxTotal; i++ {
		rm.clientOpRunningCntMap[i] = new(sync.Map)
	}
	rm.currentIndex = 0
	rm.enable = true
	rm.clientOpTimeOut = clientOpTimeOut
	rm.stopC = make(chan struct{})
	rm.mapBuffer = make(chan *sync.Map, IdxTotal+1)
	rm.mapBuffer <- new(sync.Map)
	rm.statPool.New = func() interface{} {
		return new(RunningStat)
	}

	return
}

func (rm *RunningMonitor) Start() {
	if !rm.enable {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(rm.clientOpTimeOut) * time.Second)
		defer ticker.Stop()

		log.LogInfof("action[RunningMonitor#start] start")
		for {
			select {
			case <-rm.stopC:
				log.LogInfof("action[RunningMonitor#start] stop")
				return
			case <-ticker.C:
				rm.checkClientOpRunningCnt()
			}
		}
	}()
}

func (rm *RunningMonitor) Stop() {
	if rm.stopC != nil {
		close(rm.stopC)
	}
}

func (rm *RunningMonitor) checkClientOpRunningCnt() {
	checkIndex := (rm.currentIndex + 1) % IdxTotal

	if log.EnableDebug() {
		log.LogDebugf("action[RunningMonitor#checkClientOpRunningCnt] start, checkIndex[%d]", checkIndex)

		start := timeutil.GetCurrentTime()
		defer func() {
			elapsed := time.Since(start)
			log.LogDebugf("action[RunningMonitor#checkClientOpRunningCnt] checkIndex[%d] elapsed[%d]ms", checkIndex, elapsed.Milliseconds())
		}()
	}

	tmpMap := rm.clientOpRunningCntMap[checkIndex]
	rm.clientOpRunningCntMap[checkIndex] = rm.getMapFromBuffer()

	tmpMap.Range(func(key, value interface{}) bool {
		opIdx, pid := key2Index(key.(uint64))
		opName := getOpName(opIdx)

		runningCnt := atomic.LoadInt32(value.(*int32))
		if runningCnt != 0 {
			// report error by log
			log.LogWarnf("action[RunningMonitor#checkClientOpRunningCnt] pid[%d] op[%s] count[%d] run time out, clientOpTimeOut[%d]s",
				pid, opName, runningCnt, rm.clientOpTimeOut)
			runTimeOutCntGaugeVec := exporter.NewGaugeVecFromMap("op_run_time_out_count", "", []string{"op"})
			if runTimeOutCntGaugeVec != nil {
				runTimeOutCntGaugeVec.AddWithLabelValues(float64(runningCnt), opName)
			}
		}

		return true
	})

	rm.putMapIntoBuffer(tmpMap)
	rm.currentIndex = checkIndex
}

func getOpIdx(name string) (opIdx uint8) {
	var ok bool
	opIdx, ok = opNameMap[name]
	if !ok {
		opIdx = 0
	}
	return
}

func getOpName(opIdx uint8) (name string) {
	for k, v := range opNameMap {
		if v == opIdx {
			name = k
			return
		}
	}
	return "undefinedOp"
}

func getOpNum() (opNum int) {
	opNum = len(opNameMap)
	return
}

func (rm *RunningMonitor) AddClientOp(name string, pid uint32) (runningStat *RunningStat) {
	if !rm.enable {
		return
	}

	runningStat = rm.getStatFromBuffer()
	runningStat.startTime = timeutil.GetCurrentTimeUnix()
	runningStat.pid = pid
	runningStat.opIdx = getOpIdx(name)
	runningStat.index = rm.currentIndex

	key := getMapKey(runningStat.opIdx, pid)
	store, _ := rm.clientOpRunningCntMap[runningStat.index].LoadOrStore(key, new(int32))
	runningCnt := store.(*int32)
	atomic.AddInt32(runningCnt, 1)

	return
}

func (rm *RunningMonitor) SubClientOp(runningStat *RunningStat, err error) {
	if !rm.enable {
		return
	}

	defer rm.putStatIntoBuffer(runningStat)

	if err != nil && err != io.EOF {
		parsedError := ParseError(err)
		runFailGaugeVec := exporter.NewGaugeVecFromMap("op_run_fail_count", "", []string{"op", "err"})
		if runFailGaugeVec != nil {
			runFailGaugeVec.AddWithLabelValues(1, getOpName(runningStat.opIdx), parsedError.ErrnoName())
		}
	}

	now := timeutil.GetCurrentTimeUnix()
	if (now - runningStat.startTime) < rm.clientOpTimeOut {
		idx := runningStat.index
		key := getMapKey(runningStat.opIdx, runningStat.pid)

		store, ok := rm.clientOpRunningCntMap[idx].Load(key)
		if !ok {
			log.LogWarnf("action[RunningMonitor#SubClientOp] not loaded. pid[%d] op[%d]",
				runningStat.pid, runningStat.opIdx)
			return
		}

		runningCnt := store.(*int32)
		atomic.AddInt32(runningCnt, -1)
	}
}

func (rm *RunningMonitor) getMapFromBuffer() *sync.Map {
	if !rm.enable {
		return nil
	}

	return <-rm.mapBuffer
}

func (rm *RunningMonitor) putMapIntoBuffer(m *sync.Map) {
	if !rm.enable {
		return
	}

	m.Range(func(key, value interface{}) bool {
		m.Delete(key)
		return true
	})

	rm.mapBuffer <- m
}

func (rm *RunningMonitor) getStatFromBuffer() *RunningStat {
	return rm.statPool.Get().(*RunningStat)
}

func (rm *RunningMonitor) putStatIntoBuffer(runningStat *RunningStat) {
	rm.statPool.Put(runningStat)
}

func getMapKey(opIdx uint8, pid uint32) (key uint64) {
	key = uint64(opIdx) << PidOffset
	key += uint64(pid)
	return
}

func key2Index(key uint64) (opIdx uint8, pid uint32) {
	opIdx = uint8(key >> PidOffset)
	pid = uint32(key & PidMask)
	return
}
