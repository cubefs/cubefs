// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"fmt"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/util/log"
)

// Logger encapsulation the log interface.
type Logger interface {
	IsEnableDebug() bool
	IsEnableInfo() bool
	IsEnableWarn() bool

	Debug(format string, v ...interface{})
	Info(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Error(format string, v ...interface{})
}

var (
	stdLogger  = NewDefaultLogger(0)
	raftLogger = Logger(stdLogger)
)

func SetLogger(l Logger) {
	raftLogger = l
}

func IsEnableDebug() bool {
	return raftLogger.IsEnableDebug()
}

func IsEnableInfo() bool {
	return raftLogger.IsEnableInfo()
}

func IsEnableWarn() bool {
	return raftLogger.IsEnableWarn()
}

func Debug(format string, v ...interface{}) {
	raftLogger.Debug(format, v...)
}

func Info(format string, v ...interface{}) {
	raftLogger.Info(format, v...)
}

func Warn(format string, v ...interface{}) {
	raftLogger.Warn(format, v...)
}

func Error(format string, v ...interface{}) {
	raftLogger.Error(format, v...)
}

// DefaultLogger is a default implementation of the Logger interface.
type DefaultLogger struct {
	*log.Log
	debugEnable bool
	infoEnable  bool
	warnEnable  bool
}

func NewDefaultLogger(level int) *DefaultLogger {
	logger, err := log.NewLog("", "raft", "DEBUG")
	if err != nil {
		panic(err)
	}
	return &DefaultLogger{
		Log:         logger,
		debugEnable: level <= log.DebugLevel,
		infoEnable:  level <= log.InfoLevel,
		warnEnable:  level <= log.WarnLevel,
	}
}

func (l *DefaultLogger) header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}

func (l *DefaultLogger) IsEnableDebug() bool {
	return l.debugEnable
}

func (l *DefaultLogger) Debug(format string, v ...interface{}) {
	l.Output(4, l.header("DEBUG", fmt.Sprintf(format, v...)), false)
}

func (l *DefaultLogger) IsEnableInfo() bool {
	return l.infoEnable
}

func (l *DefaultLogger) Info(format string, v ...interface{}) {
	l.Output(4, l.header("INFO", fmt.Sprintf(format, v...)), false)
}

func (l *DefaultLogger) IsEnableWarn() bool {
	return l.warnEnable
}

func (l *DefaultLogger) Warn(format string, v ...interface{}) {
	l.Output(4, l.header("WARN", fmt.Sprintf(format, v...)), false)
}

func (l *DefaultLogger) Error(format string, v ...interface{}) {
	l.Output(4, l.header("ERROR", fmt.Sprintf(format, v...)), false)
}

type FileLogger struct {
	*log.Log
	debugEnable bool
	infoEnable  bool
	warnEnable  bool
}

func NewFileLogger(logger *log.Log, level int) *FileLogger {
	return &FileLogger{
		Log:         logger,
		debugEnable: level <= log.DebugLevel,
		infoEnable:  level <= log.InfoLevel,
		warnEnable:  level <= log.WarnLevel,
	}
}

func (fl *FileLogger) IsEnableDebug() bool {
	return fl.debugEnable
}

func (fl *FileLogger) Debug(format string, v ...interface{}) {
	fl.Debug(fmt.Sprintf(format, v...))
}

func (fl *FileLogger) IsEnableInfo() bool {
	return fl.infoEnable
}

func (fl *FileLogger) Info(format string, v ...interface{}) {
	fl.Info(fmt.Sprintf(format, v...))
}

func (fl *FileLogger) IsEnableWarn() bool {
	return fl.warnEnable
}

func (fl *FileLogger) Warn(format string, v ...interface{}) {
	fl.Warn(fmt.Sprintf(format, v...))
}

func (fl *FileLogger) Error(format string, v ...interface{}) {
	fl.Error(fmt.Sprintf(format, v...))
}
