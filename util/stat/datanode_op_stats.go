// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stat

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

var (
	DpStat   = new(OpLogger)
	DiskStat = new(OpLogger)
)

type Operation struct {
	Name  string
	Op    string
	Count int
}

type OpLogger struct {
	mu             sync.Mutex
	opCounts       map[string]int
	opCountsMaster map[string]int
	opCountsPrev   map[string]int
	maxOps         int
	logFile        string
	ticker         *time.Ticker
	done           chan bool
	recordFile     bool
	sendMaster     bool
	fileSize       int64
	reserveTime    time.Duration
	dir            string
	filename       string
}

const (
	DefaultMaxOps   = 50
	DefaultDuration = time.Minute
	defaultSep      = "+"
	oplogModule     = "oplogs"
)

func NewOpLogger(dir, filename string, maxOps int, duration time.Duration) (*OpLogger, error) {
	dir = path.Join(dir, oplogModule)
	fi, err := os.Stat(dir)
	if err != nil {
		os.MkdirAll(dir, 0o755)
	} else {
		if !fi.IsDir() {
			return new(OpLogger), errors.New(dir + " is not a directory")
		}
	}
	_ = os.Chmod(dir, 0o755)
	logger := &OpLogger{
		opCounts:       map[string]int{},
		opCountsMaster: map[string]int{},
		opCountsPrev:   map[string]int{},
		maxOps:         maxOps,
		logFile:        path.Join(dir, filename),
		ticker:         time.NewTicker(duration),
		done:           make(chan bool),
		fileSize:       DefaultStatLogSize,
		recordFile:     true,
		reserveTime:    MaxReservedDays,
		dir:            dir,
		filename:       filename,
	}
	go logger.startFlushing()
	return logger, nil
}

func (l *OpLogger) Record(name string) {
	l.RecordOp(name, "")
}

func (l *OpLogger) RecordOp(name, op string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ticker == nil {
		return
	}
	key := name + defaultSep + op
	if l.recordFile {
		l.opCounts[key]++
	}
	if l.sendMaster {
		l.opCountsMaster[key]++
	}
}

func (l *OpLogger) SetRecordFile(recordFile bool) {
	l.recordFile = recordFile
}

func (l *OpLogger) SetSendMaster(sendMaster bool) {
	l.sendMaster = sendMaster
}

func (l *OpLogger) SetFileSize(fileSize int64) {
	l.fileSize = fileSize
}

func (l *OpLogger) SetReserveTime(duration time.Duration) {
	l.reserveTime = duration
}

func (l *OpLogger) GetMasterOps() []*Operation {
	l.mu.Lock()
	defer l.mu.Unlock()
	ops := l.getOps(l.opCountsMaster)
	l.opCountsMaster = map[string]int{}
	return ops
}

func (l *OpLogger) GetPrevOps() []*Operation {
	return l.getAllOps(l.opCountsPrev)
}

func (l *OpLogger) startFlushing() {
	for {
		select {
		case <-l.ticker.C:
			l.flush()
		case <-l.done:
			return
		}
	}
}

func (l *OpLogger) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.recordFile {
		return
	}

	l.rotate()
	l.opCountsPrev = l.opCounts
	ops := l.getOps(l.opCounts)
	file, err := os.OpenFile(l.logFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		log.LogErrorf("os.OpenFile failed.filename:%s,err:%+v", l.logFile, err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	fmt.Fprintf(writer, "\n===============  Statistic in %.2fs, %s  =====================\n",
		DefaultDuration.Seconds(), time.Now().Format("2006-01-02 15:04:05"))
	for _, op := range ops {
		fmt.Fprintf(writer, "%-30s %-10s %d\n", op.Name, op.Op, op.Count)
	}
	writer.Flush()
	l.opCounts = map[string]int{}

	l.remove()
}

func (l *OpLogger) rotate() {
	fileInfo, _ := os.Stat(l.logFile)
	if fileInfo == nil {
		return
	}
	if fileInfo.IsDir() {
		return
	}
	if fileInfo.Size() > l.fileSize {
		os.Rename(l.logFile, l.logFile+"."+time.Now().Format(FileNameDateFormat)+ShiftedExtension)
	}
}

func (l *OpLogger) remove() {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		fileInfo, _ := entry.Info()
		if fileInfo == nil {
			continue
		}
		if fileInfo.IsDir() {
			continue
		}
		if !strings.HasPrefix(fileInfo.Name(), l.filename) {
			continue
		}
		if !strings.HasSuffix(fileInfo.Name(), ShiftedExtension) {
			continue
		}
		if time.Since(fileInfo.ModTime()) > l.reserveTime {
			os.Remove(path.Join(l.dir, fileInfo.Name()))
		}
	}
}

func (l *OpLogger) getOps(m map[string]int) []*Operation {
	ops := l.getAllOps(m)
	if l.maxOps > 0 && len(ops) > l.maxOps {
		ops = ops[:l.maxOps]
	}
	return ops
}

func (l *OpLogger) getAllOps(m map[string]int) []*Operation {
	opMap := m
	ops := make([]*Operation, 0, len(opMap))
	if len(opMap) == 0 {
		return ops
	}
	for key, count := range opMap {
		arr := strings.Split(key, defaultSep)
		if len(arr) != 2 {
			continue
		}
		ops = append(ops, &Operation{Name: arr[0], Op: arr[1], Count: count})
	}
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].Count > ops[j].Count
	})
	return ops
}

func (l *OpLogger) Close() {
	l.ticker.Stop()
	l.done <- true
	l.flush()
}
