// Copyright 2018 The Containerfs Authors.
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

package log

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Level uint8

const (
	DebugLevel  Level = 1
	InfoLevel         = DebugLevel<<1 + 1
	WarnLevel         = InfoLevel<<1 + 1
	ErrorLevel        = WarnLevel<<1 + 1
	FatalLevel        = ErrorLevel<<1 + 1
	ReadLevel         = InfoLevel
	UpdateLevel       = InfoLevel
)

const (
	FileNameDateFormat     = "20060102150405"
	FileOpt                = os.O_RDWR | os.O_CREATE | os.O_APPEND
	WriterBufferInitSize   = 4 * 1024 * 1024
	WriterBufferLenLimit   = 4 * 1024 * 1024
	DefaultRollingInterval = 5 * time.Minute
	RolledExtension        = ".old"
)

var levelPrefixes = []string{
	"[DEBUG]",
	"[INFO ]",
	"[WARN ]",
	"[ERROR]",
	"[FATAL]",
	"[READ ]",
	"[WRITE]",
}

type RolledFile []os.FileInfo

func (f RolledFile) Less(i, j int) bool {
	return f[i].ModTime().Before(f[j].ModTime())
}

func (f RolledFile) Len() int {
	return len(f)
}

func (f RolledFile) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

type asyncWriter struct {
	file        *os.File
	fileName    string
	logSize     int64
	rollingSize int64
	buffer      *bytes.Buffer
	flushTmp    []byte
	flushC      chan bool
	rotateDay   chan struct{}
	mu          sync.Mutex
}

func (writer *asyncWriter) flushScheduler() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			writer.flushToFile()
		case _, open := <-writer.flushC:
			writer.flushToFile()
			if !open {
				ticker.Stop()
				writer.file.Close()
				return
			}
		}
	}
}

func (writer *asyncWriter) Write(p []byte) (n int, err error) {
	writer.mu.Lock()
	writer.buffer.Write(p)
	writer.mu.Unlock()
	if writer.buffer.Len() > WriterBufferLenLimit {
		select {
		case writer.flushC <- true:
		default:
		}
	}
	return
}

func (writer *asyncWriter) Close() (err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	close(writer.flushC)
	return
}

func (writer *asyncWriter) Flush() {
	writer.flushToFile()
	writer.file.Sync()
}

func (writer *asyncWriter) flushToFile() {
	writer.mu.Lock()
	flushLength := writer.buffer.Len()
	if writer.flushTmp == nil || cap(writer.flushTmp) < flushLength {
		writer.flushTmp = make([]byte, flushLength)
	}
	copy(writer.flushTmp, writer.buffer.Bytes())
	writer.buffer.Reset()
	writer.mu.Unlock()
	isRotateDay := false
	select {
	case <-writer.rotateDay:
		isRotateDay = true
	default:
	}
	if (writer.logSize+int64(flushLength))/(1024*1024) >= writer.
		rollingSize || isRotateDay {
		oldFile := writer.fileName + "." + time.Now().Format(
			FileNameDateFormat) + RolledExtension
		if _, err := os.Lstat(oldFile); err != nil {
			if err := writer.rename(oldFile); err == nil {
				if fp, err := os.OpenFile(writer.fileName, FileOpt, 0666); err == nil {
					writer.file.Close()
					writer.file = fp
					writer.logSize = 0
				}
			}
		}
	}
	writer.logSize += int64(flushLength)
	writer.file.Write(writer.flushTmp[:flushLength])
}

func (writer *asyncWriter) rename(newName string) error {
	if err := os.Rename(writer.fileName, newName); err != nil {
		return err
	}
	return nil
}

func newAsyncWriter(fileName string, rollingSize int64) (*asyncWriter, error) {
	fp, err := os.OpenFile(fileName, FileOpt, 0666)
	if err != nil {
		return nil, err
	}
	fInfo, err := fp.Stat()
	if err != nil {
		return nil, err
	}
	w := &asyncWriter{
		file:        fp,
		fileName:    fileName,
		rollingSize: rollingSize,
		logSize:     fInfo.Size(),
		buffer:      bytes.NewBuffer(make([]byte, 0, WriterBufferInitSize)),
		flushC:      make(chan bool, 1000),
		rotateDay:   make(chan struct{}, 1),
	}
	go w.flushScheduler()
	return w, nil
}

type LogObject struct {
	*log.Logger
	object *asyncWriter
}

func (ob *LogObject) Flush() {
	if ob.object != nil {
		ob.object.Flush()
	}
}

func (ob *LogObject) SetRotateByDay() {
	ob.object.rotateDay <- struct{}{}
}

func newLogObject(writer *asyncWriter, prefix string, flag int) *LogObject {
	return &LogObject{
		Logger: log.New(writer, prefix, flag),
		object: writer,
	}
}

type Log struct {
	dir            string
	module         string
	errorLogger    *LogObject
	warnLogger     *LogObject
	debugLogger    *LogObject
	infoLogger     *LogObject
	readLogger     *LogObject
	updateLogger   *LogObject
	level          Level
	msgC           chan string
	rotate         *LogRotate
	lastRolledTime time.Time
}

var (
	ErrLogFileName    = "_error.log"
	WarnLogFileName   = "_warn.log"
	InfoLogFileName   = "_info.log"
	DebugLogFileName  = "_debug.log"
	ReadLogFileName   = "_read.log"
	UpdateLogFileName = "_write.log"
)

var gLog *Log = nil

func InitLog(dir, module string, level Level, rotate *LogRotate) (*Log, error) {
	l := new(Log)
	l.dir = dir
	l.module = module
	fi, err := os.Stat(dir)
	if err != nil {
		os.MkdirAll(dir, 0755)
	} else {
		if !fi.IsDir() {
			return nil, errors.New(dir + " is not a directory")
		}
	}
	if rotate == nil {
		rotate = NewLogRotate()
	}
	l.rotate = rotate
	err = l.initLog(dir, module, level)
	if err != nil {
		return nil, err
	}
	l.lastRolledTime = time.Now()
	go l.checkLogRotation(dir, module)

	gLog = l
	return l, nil
}

func (l *Log) initLog(logDir, module string, level Level) error {
	logOpt := log.LstdFlags | log.Lmicroseconds

	newLog := func(logFileName string) (newLogger *LogObject, err error) {
		logName := path.Join(logDir, module+logFileName)
		w, err := newAsyncWriter(logName, l.rotate.rollingSize)
		if err != nil {
			return
		}
		newLogger = newLogObject(w, "", logOpt)
		return
	}
	var err error
	logHandles := [...]**LogObject{&l.debugLogger, &l.infoLogger, &l.warnLogger, &l.errorLogger, &l.readLogger, &l.updateLogger}
	logNames := [...]string{DebugLogFileName, InfoLogFileName, WarnLogFileName, ErrLogFileName, ReadLogFileName, UpdateLogFileName}
	for i := range logHandles {
		if *logHandles[i], err = newLog(logNames[i]); err != nil {
			return err
		}
	}
	l.level = level
	return nil
}

func (l *Log) SetPrefix(s, level string) string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		line = 0
	}
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	return level + " " + file + ":" + strconv.Itoa(line) + ": " + s
}

func (l *Log) Flush() {
	loggers := []*LogObject{
		l.debugLogger,
		l.infoLogger,
		l.warnLogger,
		l.errorLogger,
		l.readLogger,
		l.updateLogger,
	}
	for _, logger := range loggers {
		if logger != nil {
			logger.Flush()
		}
	}
}

func LogWarn(v ...interface{}) {
	if gLog == nil {
		return
	}
	if WarnLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[2])
	gLog.warnLogger.Output(2, s)
}

func LogWarnf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if WarnLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[2])
	gLog.warnLogger.Output(2, s)
}

func LogInfo(v ...interface{}) {
	if gLog == nil {
		return
	}
	if InfoLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[1])
	gLog.infoLogger.Output(2, s)
}

func LogInfof(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if InfoLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[1])
	gLog.infoLogger.Output(2, s)
}

func LogError(v ...interface{}) {
	if gLog == nil {
		return
	}
	if ErrorLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[3])
	gLog.errorLogger.Output(2, s)
}

func LogErrorf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if ErrorLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[3])
	gLog.errorLogger.Print(s)
}

func LogDebug(v ...interface{}) {
	if gLog == nil {
		return
	}
	if DebugLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[0])
	gLog.debugLogger.Print(s)
}

func LogDebugf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if DebugLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[0])
	gLog.debugLogger.Output(2, s)
}

func LogFatal(v ...interface{}) {
	if gLog == nil {
		return
	}
	if FatalLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.errorLogger.Output(2, s)
	os.Exit(1)
}

func LogFatalf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if FatalLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.errorLogger.Output(2, s)
	os.Exit(1)
}

func LogRead(v ...interface{}) {
	if gLog == nil {
		return
	}
	if ReadLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[5])
	gLog.readLogger.Output(2, s)
}

func LogReadf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if ReadLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[5])
	gLog.readLogger.Output(2, s)
}

func LogWrite(v ...interface{}) {
	if gLog == nil {
		return
	}
	if UpdateLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[6])
	gLog.updateLogger.Output(2, s)
}

func LogWritef(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if UpdateLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[6])
	gLog.updateLogger.Output(2, s)
}

func LogFlush() {
	if gLog != nil {
		gLog.Flush()
	}
}

func (l *Log) checkLogRotation(logDir, module string) {
	var needDelFiles RolledFile
	for {
		needDelFiles = needDelFiles[:0]
		// check disk space
		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(logDir, &fs); err != nil {
			LogErrorf("check disk space: %s", err.Error())
			continue
		}
		diskSpaceLeft := int64(fs.Bavail * uint64(fs.Bsize))
		diskSpaceLeft -= l.rotate.headRoom * 1024 * 1024
		if diskSpaceLeft <= 0 {
			// collector free file list
			fp, err := os.Open(logDir)
			if err != nil {
				LogErrorf("error opening log directory: %s", err.Error())
				continue
			}

			fInfos, err := fp.Readdir(0)
			if err != nil {
				LogErrorf("error read log directory files: %s", err.Error())
				continue
			}
			for _, info := range fInfos {
				if info.Mode().IsRegular() && strings.HasSuffix(info.Name(),
					RolledExtension) {
					needDelFiles = append(needDelFiles, info)
				}
			}
			sort.Sort(needDelFiles)
			// delete old file
			for _, info := range needDelFiles {
				if err = os.Remove(path.Join(logDir, info.Name())); err == nil {
					diskSpaceLeft += info.Size()
					if diskSpaceLeft > 0 {
						break
					}
				} else {
					LogErrorf("failed delete log file %s", info.Name())
				}
			}
		}
		// check is day rotate
		now := time.Now()
		if now.Day() == l.lastRolledTime.Day() {
			time.Sleep(DefaultRollingInterval)
			continue
		}

		// Rotate log files
		l.debugLogger.SetRotateByDay()
		l.infoLogger.SetRotateByDay()
		l.warnLogger.SetRotateByDay()
		l.errorLogger.SetRotateByDay()
		l.readLogger.SetRotateByDay()
		l.updateLogger.SetRotateByDay()

		l.lastRolledTime = now
	}
}
