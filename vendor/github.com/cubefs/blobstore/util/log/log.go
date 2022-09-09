// Copyright 2022 The CubeFS Authors.
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
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

// defines log level
const (
	Ldebug = iota
	Linfo
	Lwarn
	Lerror
	Lpanic
	Lfatal
	maxLevel
)

var levelToStrings = []string{
	"[DEBUG]",
	"[INFO]",
	"[WARN]",
	"[ERROR]",
	"[PANIC]",
	"[FATAL]",
}

// DefaultLogger default logger initial with os.Stderr.
var DefaultLogger Logger

// BaseLogger defines interface of application log apis.
type BaseLogger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Debugf(format string, v ...interface{})
	Debug(v ...interface{})
	Infof(format string, v ...interface{})
	Info(v ...interface{})
	Warnf(format string, v ...interface{})
	Warn(v ...interface{})
	Errorf(format string, v ...interface{})
	Error(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatal(v ...interface{})
	Panicf(format string, v ...interface{})
	Panic(v ...interface{})
}

// Logger a implemented logger should implements all these function.
type Logger interface {
	BaseLogger

	// atomically control log level
	GetOutputLevel() int
	SetOutputLevel(logLevel int)
	SetOutput(w io.Writer)
	Output(id string, lvl int, calldepth int, s string) error

	// implement raft Logger with these two function
	Warningf(format string, v ...interface{})
	Warning(v ...interface{})
}

func init() {
	DefaultLogger = New(os.Stderr, 3)
}

// ChangeDefaultLevelHandler returns http handler of default log level modify API
func ChangeDefaultLevelHandler() (string, http.HandlerFunc) {
	return "/log/level", func(resp http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			resp.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := req.ParseForm(); err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			return
		}
		level, err := strconv.Atoi(req.FormValue("level"))
		if err != nil || level < 0 || level >= maxLevel {
			resp.WriteHeader(http.StatusBadRequest)
			return
		}

		DefaultLogger.SetOutputLevel(level)
	}
}

func Printf(format string, v ...interface{}) { DefaultLogger.(*logger).outputf(Linfo, format, v) }
func Println(v ...interface{})               { DefaultLogger.(*logger).output(Linfo, v) }
func Debugf(format string, v ...interface{}) { DefaultLogger.(*logger).outputf(Ldebug, format, v) }
func Debug(v ...interface{})                 { DefaultLogger.(*logger).output(Ldebug, v) }
func Infof(format string, v ...interface{})  { DefaultLogger.(*logger).outputf(Linfo, format, v) }
func Info(v ...interface{})                  { DefaultLogger.(*logger).output(Linfo, v) }
func Warnf(format string, v ...interface{})  { DefaultLogger.(*logger).outputf(Lwarn, format, v) }
func Warn(v ...interface{})                  { DefaultLogger.(*logger).output(Lwarn, v) }
func Errorf(format string, v ...interface{}) { DefaultLogger.(*logger).outputf(Lerror, format, v) }
func Error(v ...interface{})                 { DefaultLogger.(*logger).output(Lerror, v) }
func Fatalf(format string, v ...interface{}) {
	DefaultLogger.(*logger).outputf(Lfatal, format, v)
	os.Exit(1)
}

func Fatal(v ...interface{}) {
	DefaultLogger.(*logger).output(Lfatal, v)
	os.Exit(1)
}

func Panicf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	DefaultLogger.(*logger).outputf(Lpanic, format, v)
	panic(s)
}

func Panic(v ...interface{}) {
	s := fmt.Sprintln(v...)
	DefaultLogger.(*logger).output(Lpanic, v)
	panic(s)
}

func GetOutputLevel() int    { return DefaultLogger.GetOutputLevel() }
func SetOutputLevel(lvl int) { DefaultLogger.SetOutputLevel(lvl) }
func SetOutput(w io.Writer)  { DefaultLogger.SetOutput(w) }
