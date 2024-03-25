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

package log

import (
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

// alias of blobstore logger.

const (
	Ldebug = log.Ldebug
	Linfo  = log.Linfo
	Lwarn  = log.Lwarn
	Lerror = log.Lerror
	Lpanic = log.Lpanic
	Lfatal = log.Lfatal
)

var (
	Printf  = log.Printf
	Println = log.Println
	Debugf  = log.Debugf
	Debug   = log.Debug
	Infof   = log.Infof
	Info    = log.Info
	Warnf   = log.Warnf
	Warn    = log.Warn
	Errorf  = log.Errorf
	Error   = log.Error
	Fatalf  = log.Fatalf
	Fatal   = log.Fatal
	Panicf  = log.Panicf
	Panic   = log.Panic

	GetOutputLevel = log.GetOutputLevel
	SetOutputLevel = log.SetOutputLevel
	SetOutput      = log.SetOutput

	ChangeDefaultLevelHandler = log.ChangeDefaultLevelHandler
)

func ParseLevel(levelName string, defaultLevel log.Level) log.Level {
	switch strings.ToLower(levelName) {
	case "debug":
		return log.Ldebug
	case "info":
		return log.Linfo
	case "warn", "warning":
		return log.Lwarn
	case "error":
		return log.Lerror
	case "panic", "critical":
		return log.Lpanic
	case "fatal":
		return log.Lfatal
	default:
		return defaultLevel
	}
}
