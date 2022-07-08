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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// defines log level
const (
	Ldebug Level = iota
	Linfo
	Lwarn
	Lerror
	Lpanic
	Lfatal
	maxLevel
)

// Level type log level
type Level int

// UnmarshalJSON unserialize log level with json.
// Try compatible digit firstly then string.
func (l *Level) UnmarshalJSON(data []byte) error {
	if lvl, err := strconv.Atoi(string(data)); err == nil {
		*l = Level(lvl)
		return nil
	}

	var lvlName string
	json.Unmarshal(data, &lvlName)
	lvl, exist := levelMapping[strings.ToLower(lvlName)]
	if !exist {
		return fmt.Errorf("invalid log level: %s", string(data))
	}
	*l = lvl
	return nil
}

// UnmarshalYAML unserialize log level with yaml.
func (l *Level) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var lvlName string
	unmarshal(&lvlName)
	lvl, exist := levelMapping[strings.ToLower(lvlName)]
	if !exist {
		return fmt.Errorf("invalid log level: %s", lvlName)
	}
	*l = lvl
	return nil
}

var levelMapping = map[string]Level{
	"debug": Ldebug,
	"info":  Linfo,
	"warn":  Lwarn,
	"error": Lerror,
	"panic": Lpanic,
	"fatal": Lfatal,
}

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
	GetOutputLevel() Level
	SetOutputLevel(logLevel Level)
	SetOutput(w io.Writer)
	Output(id string, lvl Level, calldepth int, s string) error

	// implement raft Logger with these two function
	Warningf(format string, v ...interface{})
	Warning(v ...interface{})
}

func init() {
	DefaultLogger = New(os.Stderr, 3)
}

// ChangeDefaultLevelHandler returns http handler of default log level modify API
func ChangeDefaultLevelHandler() (string, http.HandlerFunc) {
	return "/log/level", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			level := DefaultLogger.GetOutputLevel()
			w.Write([]byte(fmt.Sprintf("{\"level\": \"%s\"}", levelToStrings[level])))
		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			level, err := strconv.Atoi(r.FormValue("level"))
			if err != nil || level < 0 || level >= int(maxLevel) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			DefaultLogger.SetOutputLevel(Level(level))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
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

func GetOutputLevel() Level    { return DefaultLogger.GetOutputLevel() }
func SetOutputLevel(lvl Level) { DefaultLogger.SetOutputLevel(lvl) }
func SetOutput(w io.Writer)    { DefaultLogger.SetOutput(w) }
