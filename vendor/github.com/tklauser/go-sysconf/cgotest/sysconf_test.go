// Copyright 2018 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sysconf_cgotest

import "testing"

// The actual test function is in sysconf_cgotest.go so that it can use cgo.
func TestSysconfCgoMatch(t *testing.T) { testSysconfCgoMatch(t) }
