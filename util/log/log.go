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

package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	syslog "log"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	blog "github.com/cubefs/cubefs/blobstore/util/log"
)

type Level uint8

const (
	DebugLevel    Level = 1
	InfoLevel           = DebugLevel<<1 + 1
	WarnLevel           = InfoLevel<<1 + 1
	ErrorLevel          = WarnLevel<<1 + 1
	FatalLevel          = ErrorLevel<<1 + 1
	CriticalLevel       = FatalLevel << +1
	ReadLevel           = InfoLevel
	UpdateLevel         = InfoLevel
)

const (
	FileNameDateFormat    = "20060102150405"
	FileOpt               = os.O_RDWR | os.O_CREATE | os.O_APPEND
	WriterBufferInitSize  = 4 * 1024 * 1024
	WriterBufferLenLimit  = 4 * 1024 * 1024
	DefaultRotateInterval = 1 * time.Second
	RotatedExtension      = ".old"
	MaxReservedDays       = 7 * 24 * time.Hour
)

var levelPrefixes = []string{
	"[DEBUG]",
	"[INFO ]",
	"[WARN ]",
	"[ERROR]",
	"[FATAL]",
	"[READ ]",
	"[WRITE]",
	"[Critical]",
}

type RotatedFile []os.FileInfo

func (f RotatedFile) Less(i, j int) bool {
	return f[i].ModTime().Before(f[j].ModTime())
}

func (f RotatedFile) Len() int {
	return len(f)
}

func (f RotatedFile) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func setBlobLogLevel(loglevel Level) {
	blevel := blog.Lwarn
	switch loglevel {
	case DebugLevel:
		blevel = blog.Ldebug
	case InfoLevel:
		blevel = blog.Linfo
	case WarnLevel:
		blevel = blog.Lwarn
	case ErrorLevel:
		blevel = blog.Lerror
	default:
	}
	blog.SetOutputLevel(blevel)
}

func GetBlobLogLevel() blog.Level {
	blevel := blog.Lwarn
	if gLog == nil {
		return blevel
	}

	level := gLog.level
	switch level {
	case DebugLevel:
		blevel = blog.Ldebug
	case InfoLevel:
		blevel = blog.Linfo
	case WarnLevel:
		blevel = blog.Lwarn
	case ErrorLevel:
		blevel = blog.Lerror
	default:
		blevel = blog.Lwarn
	}

	return blevel
}

type asyncWriter struct {
	file       *os.File
	fileName   string
	logSize    int64
	rotateSize int64
	buffer     *bytes.Buffer
	flushTmp   *bytes.Buffer
	flushC     chan bool
	rotateDay  chan struct{} // TODO rotateTime?
	mu         sync.Mutex
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

				if writer.fileName != "" {
					// TODO Unhandled errors
					writer.file.Close()
				}
				return
			}
		}
	}
}

// Write writes the log.
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

// Close closes the writer.
func (writer *asyncWriter) Close() (err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	close(writer.flushC)
	return
}

// Flush flushes the write.
func (writer *asyncWriter) Flush() {
	writer.flushToFile()
	// TODO Unhandled errors
	if writer.fileName != "" {
		writer.file.Sync()
	}
}

func (writer *asyncWriter) flushToFile() {
	writer.mu.Lock()
	writer.buffer, writer.flushTmp = writer.flushTmp, writer.buffer

	isRotateDay := false
	select {
	case <-writer.rotateDay:
		isRotateDay = true
	default:
	}
	flushLength := writer.flushTmp.Len()

	if writer.fileName == "" {
		if _, err := writer.flushTmp.WriteTo(os.Stdout); err != nil {
			syslog.Printf("log write to stdout error: %v", err)
		}
	} else {
		if (writer.logSize+int64(flushLength)) >= writer.
			rotateSize || isRotateDay {
			oldFile := writer.fileName + "." + time.Now().Format(
				FileNameDateFormat) + RotatedExtension
			if _, err := os.Lstat(oldFile); err != nil {
				if err := writer.rename(oldFile); err == nil {
					if fp, err := os.OpenFile(writer.fileName, FileOpt, 0o666); err == nil {
						writer.file.Close()
						writer.file = fp
						writer.logSize = 0
						_ = os.Chmod(writer.fileName, 0o666)
					} else {
						syslog.Printf("log rotate: openFile %v error: %v", writer.fileName, err)
					}
				} else {
					syslog.Printf("log rotate: rename %v error: %v ", oldFile, err)
				}
			} else {
				syslog.Printf("log rotate: lstat error: %v already exists", oldFile)
			}
		}
		writer.logSize += int64(flushLength)
		// TODO Unhandled errors
		if _, err := writer.file.Write(writer.flushTmp.Bytes()); err != nil {
			syslog.Printf("log write to %v error: %v", writer.fileName, err)
		}
	}

	writer.flushTmp.Reset()
	writer.mu.Unlock()
}

func (writer *asyncWriter) rename(newName string) error {
	if err := os.Rename(writer.fileName, newName); err != nil {
		return err
	}
	return nil
}

func newAsyncWriter(fileName string, rotateSize int64) (*asyncWriter, error) {
	var fp *os.File
	var fInfo os.FileInfo
	var logSize int64
	var err error
	if fileName != "" {
		fp, err = os.OpenFile(fileName, FileOpt, 0o666)
		if err != nil {
			return nil, err
		}
		fInfo, err = fp.Stat()
		if err != nil {
			return nil, err
		}
		_ = os.Chmod(fileName, 0o666)
		logSize = fInfo.Size()
	}

	w := &asyncWriter{
		file:       fp,
		fileName:   fileName,
		rotateSize: rotateSize,
		logSize:    logSize,
		buffer:     bytes.NewBuffer(make([]byte, 0, WriterBufferInitSize)),
		flushTmp:   bytes.NewBuffer(make([]byte, 0, WriterBufferInitSize)),
		flushC:     make(chan bool, 1000),
		rotateDay:  make(chan struct{}, 1),
	}
	go w.flushScheduler()
	return w, nil
}

// LogObject defines the log object.
type LogObject struct {
	*log.Logger
	object *asyncWriter
}

// Flush flushes the log object.
func (ob *LogObject) Flush() {
	if ob.object != nil {
		ob.object.Flush()
	}
}

func (ob *LogObject) SetRotation() {
	ob.object.rotateDay <- struct{}{}
}

func newLogObject(writer *asyncWriter, prefix string, flag int) *LogObject {
	return &LogObject{
		Logger: log.New(writer, prefix, flag),
		object: writer,
	}
}

// Log defines the log struct.
type Log struct {
	dir            string
	errorLogger    *LogObject
	warnLogger     *LogObject
	debugLogger    *LogObject
	infoLogger     *LogObject
	readLogger     *LogObject
	updateLogger   *LogObject
	criticalLogger *LogObject
	qosLogger      *LogObject
	level          Level
	rotate         *LogRotate
	lastRolledTime time.Time
	printStderr    int32
}

var (
	ErrLogFileName      = "_error.log"
	WarnLogFileName     = "_warn.log"
	InfoLogFileName     = "_info.log"
	DebugLogFileName    = "_debug.log"
	ReadLogFileName     = "_read.log"
	UpdateLogFileName   = "_write.log"
	CriticalLogFileName = "_critical.log"
	QoSLogFileName      = "_qos.log"
)

var gLog *Log = nil

var LogDir string

func (l *Log) DisableStderrOutput() {
	atomic.StoreInt32(&l.printStderr, 0)
}

func (l *Log) outputStderr(calldepth int, s string) {
	if atomic.LoadInt32(&l.printStderr) != 0 {
		log.Output(calldepth+1, s)
	}
}

// InitLog initializes the log.
func InitLog(dir, module string, level Level, rotate *LogRotate, logLeftSpaceLimitRatio float64) (*Log, error) {
	l := new(Log)
	l.printStderr = 1
	if dir != "" {
		var err error
		dir = path.Join(dir, module)
		dir, err = filepath.Abs(dir)
		if err != nil {
			return nil, errors.New("get absolute file path failed, " + err.Error())
		}
		l.dir = dir
		LogDir = dir
		fi, err := os.Stat(dir)
		if err != nil {
			os.MkdirAll(dir, 0o755)
		} else {
			if !fi.IsDir() {
				return nil, errors.New(dir + " is not a directory")
			}
		}
		_ = os.Chmod(dir, 0o755)

		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(dir, &fs); err != nil {
			return nil, fmt.Errorf("[InitLog] stats disk space: %s", err.Error())
		}

		if rotate == nil {
			rotate = NewLogRotate()
		}

		if rotate.headRoom == 0 {
			minLogLeftSpaceLimit := float64(fs.Blocks*uint64(fs.Bsize)) * logLeftSpaceLimitRatio / 1024 / 1024

			rotate.SetHeadRoomMb(int64(math.Min(minLogLeftSpaceLimit, DefaultHeadRoom)))
		}

		if rotate.rotateSize == 0 {
			minRotateSize := int64(fs.Bavail * uint64(fs.Bsize) / uint64(len(levelPrefixes)))
			if minRotateSize < DefaultMinRotateSize {
				minRotateSize = DefaultMinRotateSize
			}
			rotate.SetRotateSizeMb(int64(math.Min(float64(minRotateSize), float64(DefaultRotateSize))))
		}
	} else {
		if rotate == nil {
			rotate = NewLogRotate()
		}
	}

	l.rotate = rotate
	err := l.initLog(dir, module, level)
	if err != nil {
		return nil, err
	}
	l.lastRolledTime = time.Now()
	if dir != "" {
		go l.checkLogRotation(dir, module)
	}

	gLog = l
	setBlobLogLevel(level)
	return l, nil
}

func TruncMsg(msg string) string {
	return TruncMsgWith(msg, 100)
}

func TruncMsgWith(msg string, size int) string {
	if len(msg) < size {
		return msg
	}

	return msg[0:size]
}

func OutputPid(logDir, role string) error {
	pidFile := path.Join(logDir, fmt.Sprintf("%s.pid", role))
	file, err := os.Create(pidFile)
	if err != nil {
		return fmt.Errorf("open pid file %s error %s", pidFile, err.Error())
	}

	pid := os.Getpid()
	_, err = file.Write([]byte(fmt.Sprintf("%d", pid)))
	if err != nil {
		return fmt.Errorf("write pid failed, pid %d, file %s, err %s", pid, pidFile, err.Error())
	}

	file.Close()
	return nil
}

func (l *Log) initLog(logDir, module string, level Level) error {
	logOpt := log.LstdFlags | log.Lmicroseconds

	newLog := func(logFileName string) (newLogger *LogObject, err error) {
		var logName string
		if logDir == "" {
			logName = ""
		} else {
			logName = path.Join(logDir, module+logFileName)
		}
		w, err := newAsyncWriter(logName, l.rotate.rotateSize)
		if err != nil {
			return
		}
		newLogger = newLogObject(w, "", logOpt)
		return
	}
	var err error
	logHandles := [...]**LogObject{&l.debugLogger, &l.infoLogger, &l.warnLogger, &l.errorLogger, &l.readLogger, &l.updateLogger, &l.criticalLogger, &l.qosLogger}
	logNames := [...]string{DebugLogFileName, InfoLogFileName, WarnLogFileName, ErrLogFileName, ReadLogFileName, UpdateLogFileName, CriticalLogFileName, QoSLogFileName}
	for i := range logHandles {
		if *logHandles[i], err = newLog(logNames[i]); err != nil {
			return err
		}
	}
	l.level = level
	return nil
}

// SetPrefix sets the log prefix.
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

// Flush flushes the log.
func (l *Log) Flush() {
	loggers := []*LogObject{
		l.debugLogger,
		l.infoLogger,
		l.warnLogger,
		l.errorLogger,
		l.readLogger,
		l.updateLogger,
		l.criticalLogger,
	}
	for _, logger := range loggers {
		if logger != nil {
			logger.Flush()
		}
	}
}

const (
	SetLogLevelPath = "/loglevel/set"
)

func SetLogLevel(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	levelStr := r.FormValue("level")
	var level Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = DebugLevel
	case "info", "read", "write":
		level = InfoLevel
	case "warn":
		level = WarnLevel
	case "error":
		level = ErrorLevel
	case "critical":
		level = CriticalLevel
	case "fatal":
		level = FatalLevel
	default:
		err = fmt.Errorf("level only can be set :debug,info,warn,error,critical,read,write,fatal")
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	gLog.level = Level(level)
	setBlobLogLevel(level)
	buildSuccessResp(w, "set log level success")
}

func buildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "")
}

func buildFailureResp(w http.ResponseWriter, code int, msg string) {
	buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

// LogWarn indicates the warnings.
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

// LogWarnf indicates the warnings with specific format.
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

// LogInfo indicates log the information. TODO explain
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

// LogInfo indicates log the information with specific format. TODO explain
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

func EnableInfo() bool {
	if gLog == nil {
		return false
	}
	return InfoLevel&gLog.level == gLog.level
}

// LogError logs the errors.
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

// LogErrorf logs the errors with the specified format.
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

// LogDebug logs the debug information.
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

// LogDebugf logs the debug information with specified format.
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

func EnableDebug() bool {
	if gLog == nil {
		return false
	}

	return DebugLevel&gLog.level == gLog.level
}

// LogFatal logs the fatal errors.
func LogFatal(v ...interface{}) {
	if gLog == nil {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.errorLogger.Output(2, s)
	gLog.Flush()
	os.Exit(1)
}

// LogFatalf logs the fatal errors with specified format.
func LogFatalf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.errorLogger.Output(2, s)
	gLog.Flush()
	os.Exit(1)
}

// LogFatal logs the fatal errors.
func LogCritical(v ...interface{}) {
	if gLog == nil {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.criticalLogger.Output(2, s)
	gLog.outputStderr(2, s)
}

// LogFatalf logs the fatal errors with specified format.
func LogCriticalf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.criticalLogger.Output(2, s)
	gLog.outputStderr(2, s)
}

// LogRead
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

// TODO not used?
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

// QosWrite
func QosWrite(v ...interface{}) {
	if gLog == nil {
		return
	}
	if UpdateLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[0])
	gLog.qosLogger.Output(2, s)
}

// QosWriteDebugf TODO not used
func QosWriteDebugf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if DebugLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[0])
	gLog.qosLogger.Output(2, s)
}

// LogWrite
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

// LogWritef TODO not used
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

// LogFlush flushes the log.
func LogFlush() {
	if gLog != nil {
		gLog.Flush()
	}
}

func LogDisableStderrOutput() {
	if gLog != nil {
		gLog.DisableStderrOutput()
	}
}

func (l *Log) checkLogRotation(logDir, module string) {
	var needDelFiles RotatedFile
	for {
		needDelFiles = needDelFiles[:0]
		// check disk space
		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(logDir, &fs); err != nil {
			LogErrorf("check disk space: %s", err.Error())
			time.Sleep(DefaultRotateInterval)
			continue
		}
		diskSpaceLeft := int64(fs.Bavail * uint64(fs.Bsize))
		diskSpaceLeft -= l.rotate.headRoom * 1024 * 1024
		if diskSpaceLeft <= 0 {
			LogDebugf("logLeftSpaceLimit has been reached, need to clear %v Mb of Space", (-diskSpaceLeft)/1024/1024)
		}
		err := l.removeLogFile(logDir, diskSpaceLeft, module)
		if err != nil {
			time.Sleep(DefaultRotateInterval)
			continue
		}
		// check if it is time to rotate
		now := time.Now()
		if now.Day() == l.lastRolledTime.Day() {
			time.Sleep(DefaultRotateInterval)
			continue
		}

		// rotate log files
		l.debugLogger.SetRotation()
		l.infoLogger.SetRotation()
		l.warnLogger.SetRotation()
		l.errorLogger.SetRotation()
		l.readLogger.SetRotation()
		l.updateLogger.SetRotation()
		l.criticalLogger.SetRotation()

		l.lastRolledTime = now
	}
}

func DeleteFileFilter(info os.FileInfo, diskSpaceLeft int64, module string) bool {
	if diskSpaceLeft <= 0 {
		return info.Mode().IsRegular() && strings.HasSuffix(info.Name(), RotatedExtension) && (strings.HasPrefix(info.Name(), module) || strings.HasPrefix(info.Name(), "audit"))
	}
	return time.Since(info.ModTime()) > MaxReservedDays && strings.HasSuffix(info.Name(), RotatedExtension) && strings.HasPrefix(info.Name(), module)
}

func (l *Log) removeLogFile(logDir string, diskSpaceLeft int64, module string) (err error) {
	// collect free file list
	fInfos, err := ioutil.ReadDir(logDir)
	if err != nil {
		LogErrorf("error read log directory files: %s", err.Error())
		return
	}
	var needDelFiles RotatedFile
	for _, info := range fInfos {
		if DeleteFileFilter(info, diskSpaceLeft, module) {
			LogDebugf("%v will be put into needDelFiles", info.Name())
			needDelFiles = append(needDelFiles, info)
		}
	}
	sort.Sort(needDelFiles)
	// delete old file
	for _, info := range needDelFiles {
		if err = os.Remove(path.Join(logDir, info.Name())); err != nil {
			LogErrorf("failed delete log file %s", info.Name())
			continue
		}
		diskSpaceLeft += info.Size()
		if diskSpaceLeft > 0 && time.Since(info.ModTime()) < MaxReservedDays {
			break
		}
	}
	err = nil
	return
}
