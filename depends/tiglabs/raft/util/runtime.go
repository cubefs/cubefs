// Copyright 2018 The tiglabs raft Authors.
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

package util

import (
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/logger"
)

func HandleCrash(handlers ...func(interface{})) {
	if r := recover(); r != nil {
		debug.PrintStack()
		logPanic(r)
		for _, fn := range handlers {
			fn(r)
		}
	}
}

func logPanic(r interface{}) {
	callers := ""
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		callers = callers + fmt.Sprintf("%v:%v\n", file, line)
	}
	logger.Error("Recovered from panic: %#v (%v)\n%v", r, r, callers)
}

func RunWorker(f func(), handlers ...func(interface{})) {
	go func() {
		defer HandleCrash(handlers...)

		f()
	}()
}

func RunWorkerUtilStop(f func(), stopCh <-chan struct{}, handlers ...func(interface{})) {
	go func() {
		for {
			select {
			case <-stopCh:
				return

			default:
				func() {
					defer HandleCrash(handlers...)
					f()
				}()
			}
		}
	}()
}
