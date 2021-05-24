// Copyright 2018 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package sysconf_test

import (
	"bytes"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/tklauser/go-sysconf"
)

func TestSysconf(t *testing.T) {
	// Just test the basic functionality here. The comparison tests against
	// C.sysconf are in the test directory.
	val, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	if err != nil {
		t.Errorf("Sysconf(SC_CLK_TCK): %v", err)
	}
	t.Logf("clock ticks = %v", val)

	_, err = sysconf.Sysconf(-1)
	if err == nil {
		t.Errorf("Sysconf(-1) returned %v, want non-nil", err)
	}
}

func TestOpenMax(t *testing.T) {
	openMax, err := sysconf.Sysconf(sysconf.SC_OPEN_MAX)
	if err != nil {
		t.Fatalf("Sysconf(SC_OPEN_MAX): %v", err)
	}

	// from https://pubs.opengroup.org/onlinepubs/009695399/basedefs/limits.h.html
	_POSIX_OPEN_MAX := int64(20)

	// according to sysconf(3), OPEN_MAX must be ≥ _POSIX_OPEN_MAX
	if openMax < _POSIX_OPEN_MAX {
		t.Errorf("Sysconf(SC_OPEN_MAX) (%d) expected to be greater or equal _POSIX_OPEN_MAX (%d)",
			openMax, _POSIX_OPEN_MAX)
	}
}

func TestGetconf(t *testing.T) {
	testCases := []struct {
		goVar int
		name  string
	}{
		{sysconf.SC_CLK_TCK, "CLK_TCK"},
		{sysconf.SC_HOST_NAME_MAX, "HOST_NAME_MAX"},
		{sysconf.SC_OPEN_MAX, "OPEN_MAX"},
		{sysconf.SC_PAGE_SIZE, "PAGE_SIZE"},
	}

	getconf, err := exec.LookPath("getconf")
	if err != nil {
		t.Skipf("getconf not found in PATH: %v", err)
	}

	for _, tc := range testCases {
		cmd := exec.Command(getconf, tc.name)
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			// Ignore getconf errors and skip the test
			t.Skipf("failed to invoke getconf: %v", err)
			break
		}
		want, err := strconv.ParseInt(strings.TrimSpace(out.String()), 10, 64)
		if err != nil {
			t.Errorf("strconv.ParseInt: %v", err)
		}

		got, err := sysconf.Sysconf(tc.goVar)
		if err != nil {
			t.Errorf("Sysconf(%s/%d): %v", tc.name, tc.goVar, err)
		}

		if got != want {
			t.Errorf("Sysconf(%v/%d) returned %v, want %v", tc.name, tc.goVar, got, want)
		}
	}
}
