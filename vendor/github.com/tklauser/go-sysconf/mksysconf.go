// Copyright 2018 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"go/format"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
)

func gensysconf(in, out string) error {
	if _, err := os.Stat(in); err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}

	cmd := exec.Command("go", "tool", "cgo", "-godefs", in)
	defer os.RemoveAll("_obj")
	b, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintln(os.Stderr, string(b))
		return err
	}
	b, err = format.Source(b)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(out, b, 0644); err != nil {
		return err
	}
	return nil
}

func main() {
	goos := runtime.GOOS
	if goos == "illumos" {
		goos = "solaris"
	}
	defs := fmt.Sprintf("sysconf_defs_%s.go", goos)
	if err := gensysconf(defs, "z"+defs); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	vals := fmt.Sprintf("sysconf_values_%s.go", runtime.GOOS)
	// sysconf variable values are GOARCH-specific, thus write per GOARCH
	zvals := fmt.Sprintf("zsysconf_values_%s_%s.go", runtime.GOOS, runtime.GOARCH)
	if err := gensysconf(vals, zvals); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
