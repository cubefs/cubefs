// Copyright 2018 The ChuBao Authors.
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
	NewLog("/tmp/bdfs", "bdfs", DebugLevel)
	for {
		LogDebugf("action[TestLog] current time %v.", time.Now())
		time.Sleep(200 * time.Millisecond)
	}
}
