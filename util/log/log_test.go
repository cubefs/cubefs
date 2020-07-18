// Copyright 2018 The Chubao Authors.
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

// These tests are too simple.

import (
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	go func() {
		http.ListenAndServe(":10000", nil)
	}()
	InitLog("/tmp/cfs", "cfs", DebugLevel, nil)
	for i := 0; i < 10; i++ {
		LogDebugf("[debug] current time %v.", time.Now())
		LogWarnf("[warn] current time %v.", time.Now())
		LogErrorf("[error] current time %v.", time.Now())
		LogInfof("[info] current time %v.", time.Now())
		time.Sleep(2 * time.Millisecond)
	}
}
