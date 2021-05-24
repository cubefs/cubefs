// Copyright 2018 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package sysconf_test

import (
	"fmt"
	"os"

	"github.com/tklauser/go-sysconf"
)

func ExampleSysconf_clktck() {
	clktck, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Sysconf: %v\n", err)
	}
	fmt.Printf("sysconf(SC_CLK_TCK) = %v\n", clktck)
}

func ExampleSysconf_invalidParameter() {
	_, err := sysconf.Sysconf(-1)
	fmt.Print(err)

	// Output: invalid parameter value
}
