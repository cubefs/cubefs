// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package log

// These tests are too simple.

import (
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	NewLog("/tmp/bdfs", "bdfs", DebugLevel)
	for {
		LogDebugf("action[TestLog] current time %v.", time.Now())
		time.Sleep(200 * time.Millisecond)
	}
}
