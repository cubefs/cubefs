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
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	IdxTotal = 3
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

type RunningMonitor struct {
	enable                bool
	curCheckIdx           int
	clientOpRunningCntMap [IdxTotal]*sync.Map
	clientOpTimeOut       int64
	mu                    [IdxTotal]sync.RWMutex
	stopC                 chan struct{}
}

type RunningStat struct {
	startTime *time.Time
	pid       uint32
	opIdx     uint8
}

func NewRunningMonitor(clientOpTimeOut int64) (rm *RunningMonitor) {
	rm = new(RunningMonitor)
	rm.enable = false
	for i := 0; i < IdxTotal; i++ {
		rm.clientOpRunningCntMap[i] = new(sync.Map)
	}
	if clientOpTimeOut > 0 {
		rm.enable = true
		rm.clientOpTimeOut = clientOpTimeOut
		rm.stopC = make(chan struct{})
	}
	return
}

func (rm *RunningMonitor) Start() {
	if !rm.enable {
		return
	}
	go func() {
		log.LogInfof("action[RunningMonitor#start] start")
		rm.curCheckIdx = calNextIdx(calCheckIdx(time.Now().Unix(), rm.clientOpTimeOut))
		for {
			select {
			case <-rm.stopC:
				goto end
			default:
				for {
					time.Sleep(time.Millisecond * time.Duration(rm.calTimeToCurCheckIdx()))
					if rm.curCheckIdx == calCheckIdx(time.Now().Unix(), rm.clientOpTimeOut) {
						break
					}
				}
				rm.checkClientOpRunningCnt()
				rm.curCheckIdx = calNextIdx(rm.curCheckIdx)
			}
		}
	end:
		log.LogInfof("action[RunningMonitor#start] stop")
	}()
}

func (rm *RunningMonitor) Stop() {
	if rm.stopC != nil {
		close(rm.stopC)
	}
}

func (rm *RunningMonitor) calTimeToCurCheckIdx() (waitTime int64) {
	now := time.Now().Unix() * 1000
	restfulTime := calRestfulTimeInTimeOut(now, rm.clientOpTimeOut)

	diffIdxCnt := ((rm.curCheckIdx + IdxTotal) - calCheckIdx(now+restfulTime, rm.clientOpTimeOut)) % IdxTotal
	diffIdxTime := int64(diffIdxCnt) * (rm.clientOpTimeOut * 1000)

	waitTime = restfulTime + diffIdxTime
	return
}

func calRestfulTimeInTimeOut(timeStampInMs int64, timeOut int64) (restfulTime int64) {
	restfulTime = timeOut*1000 - timeStampInMs%(timeOut*1000)
	return
}

func (rm *RunningMonitor) checkClientOpRunningCnt() {
	log.LogDebugf("action[RunningMonitor#checkClientOpRunningCnt] start, curCheckIdx[%v]", rm.curCheckIdx)
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.LogDebugf("action[RunningMonitor#checkClientOpRunningCnt] curCheckIdx[%v] elapsed[%v]ms", rm.curCheckIdx, elapsed.Milliseconds())
	}()
	tmpMap := rm.clientOpRunningCntMap[rm.curCheckIdx]
	rm.mu[rm.curCheckIdx].Lock()
	rm.clientOpRunningCntMap[rm.curCheckIdx] = new(sync.Map)
	rm.mu[rm.curCheckIdx].Unlock()
	tmpMap.Range(func(key, value interface{}) bool {
		opIdx := key.(uint8)
		opName := GetOpName(opIdx)
		pid2RunningCntMap := value.(*sync.Map)
		var runningTotal int32 = 0
		pid2RunningCntMap.Range(func(key, value interface{}) bool {
			pid := key.(uint32)
			runningCnt := atomic.LoadInt32(value.(*int32))
			if runningCnt > 0 {
				runningTotal += runningCnt
				// report error by log
				log.LogErrorf("action[RunningMonitor#checkClientOpRunningCnt] pid[%v] op[%v] count[%v] run time out, clientOpTimeOut[%v]", pid, opName, runningCnt, rm.clientOpTimeOut)
			}

			return true
		})
		runTimeOutCntGaugeVec := exporter.NewGaugeVecFromMap("op_run_time_out_count", "", []string{"op"})
		if runTimeOutCntGaugeVec != nil {
			runTimeOutCntGaugeVec.AddWithLabelValues(float64(runningTotal), opName)
		}
		return true
	})
}

func GetOpIdx(name string) (opIdx uint8) {
	var ok bool
	opIdx, ok = opNameMap[name]
	if !ok {
		opIdx = 0
	}
	return
}

func GetOpName(opIdx uint8) (name string) {
	for k, v := range opNameMap {
		if v == opIdx {
			name = k
			return
		}
	}
	return "undefinedOp"
}

func GetOpNum() (opNum int) {
	opNum = len(opNameMap)
	return
}

func (rm *RunningMonitor) AddClientOp(name string, pid uint32) (runningStat *RunningStat) {
	now := time.Now()
	runningStat = new(RunningStat)
	runningStat.startTime = &now
	runningStat.pid = pid
	runningStat.opIdx = GetOpIdx(name)

	if rm.enable {
		idx := calWorkIdx(runningStat.startTime.Unix(), rm.clientOpTimeOut)
		rm.mu[idx].RLock()
		value, _ := rm.clientOpRunningCntMap[idx].LoadOrStore(runningStat.opIdx, new(sync.Map))
		rm.mu[idx].RUnlock()
		pid2RunningCntMap := value.(*sync.Map)

		store, _ := pid2RunningCntMap.LoadOrStore(runningStat.pid, new(int32))
		runningCnt := store.(*int32)
		atomic.AddInt32(runningCnt, 1)
	}
	return
}

func (rm *RunningMonitor) SubClientOp(runningStat *RunningStat, err error) {
	now := time.Now()
	rm.addClientFailOp(runningStat, err)

	if rm.enable {
		if now.Unix()-runningStat.startTime.Unix() < rm.clientOpTimeOut {
			idx := calWorkIdx(runningStat.startTime.Unix(), rm.clientOpTimeOut)
			rm.mu[idx].RLock()
			value, _ := rm.clientOpRunningCntMap[idx].LoadOrStore(runningStat.opIdx, new(sync.Map))
			rm.mu[idx].RUnlock()
			pid2RunningCntMap := value.(*sync.Map)

			store, _ := pid2RunningCntMap.LoadOrStore(runningStat.pid, new(int32))
			runningCnt := store.(*int32)
			atomic.AddInt32(runningCnt, -1)
		}
	}
	return
}

func (rm *RunningMonitor) addClientFailOp(runningStat *RunningStat, err error) {
	if err != nil {
		if err == io.EOF {
			return
		}
		parsedError := ParseError(err)
		runFailGaugeVec := exporter.NewGaugeVecFromMap("op_run_fail_count", "", []string{"op", "err"})
		if runFailGaugeVec != nil {
			runFailGaugeVec.AddWithLabelValues(1, GetOpName(runningStat.opIdx), parsedError.ErrnoName())
		}
	}
}

func calWorkIdx(timeStampInSecond int64, timeOut int64) (idx int) {
	idxInTotal := timeStampInSecond % (timeOut * IdxTotal)
	idx = int(idxInTotal / timeOut)
	return
}

func calNextIdx(curIdx int) (nextIdx int) {
	nextIdx = (curIdx + 1) % IdxTotal
	return
}

func calCheckIdx(timeStampInSecond int64, timeOut int64) (checkIdx int) {
	checkIdx = calNextIdx(calWorkIdx(timeStampInSecond, timeOut))
	return
}
