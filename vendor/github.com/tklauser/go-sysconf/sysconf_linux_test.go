// Copyright 2018 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sysconf

import (
	"os"
	"testing"
)

// TestGetNproc tests that sysfs and /proc/stat report the same number of online
// CPUs.
func TestGetNproc(t *testing.T) {
	if _, err := os.Stat("/sys/devices/system/cpu/online"); err != nil {
		t.Skipf("sysfs not mounted, skipping")
	}

	nprocSysfs, err := getNprocsSysfs()
	if err != nil {
		t.Fatalf("getNprocsSysfs: %v", err)
	}

	nprocProcStat, err := getNprocsProcStat()
	if err != nil {
		t.Fatalf("getNprocsProcStat: %v", err)
	}

	if nprocSysfs != nprocProcStat {
		t.Errorf("Number of online CPUs not matching. getNprocsSysfs returned %v, getNprocsProcStat returned %v",
			nprocSysfs, nprocProcStat)
	}
}
