// Copyright 2015 Google Inc. All Rights Reserved.
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

// A simple tool for mounting sample file systems, used by the tests in
// samples/.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"syscall"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/samples/flushfs"
)

var fType = flag.String("type", "", "The name of the samples/ sub-dir.")
var fMountPoint = flag.String("mount_point", "", "Path to mount point.")
var fReadyFile = flag.Uint64("ready_file", 0, "FD to signal when ready.")

var fFlushesFile = flag.Uint64("flushfs.flushes_file", 0, "")
var fFsyncsFile = flag.Uint64("flushfs.fsyncs_file", 0, "")
var fFlushError = flag.Int("flushfs.flush_error", 0, "")
var fFsyncError = flag.Int("flushfs.fsync_error", 0, "")

var fReadOnly = flag.Bool("read_only", false, "Mount in read-only mode.")
var fDebug = flag.Bool("debug", false, "Enable debug logging.")

func makeFlushFS() (server fuse.Server, err error) {
	// Check the flags.
	if *fFlushesFile == 0 || *fFsyncsFile == 0 {
		err = fmt.Errorf("You must set the flushfs flags.")
		return
	}

	// Set up the files.
	flushes := os.NewFile(uintptr(*fFlushesFile), "(flushes file)")
	fsyncs := os.NewFile(uintptr(*fFsyncsFile), "(fsyncs file)")

	// Set up errors.
	var flushErr error
	var fsyncErr error

	if *fFlushError != 0 {
		flushErr = syscall.Errno(*fFlushError)
	}

	if *fFsyncError != 0 {
		fsyncErr = syscall.Errno(*fFsyncError)
	}

	// Report flushes and fsyncs by writing the contents followed by a newline.
	report := func(f *os.File, outErr error) func(string) error {
		return func(s string) (err error) {
			buf := []byte(s)
			buf = append(buf, '\n')

			_, err = f.Write(buf)
			if err != nil {
				err = fmt.Errorf("Write: %v", err)
				return
			}

			err = outErr
			return
		}
	}

	reportFlush := report(flushes, flushErr)
	reportFsync := report(fsyncs, fsyncErr)

	// Create the file system.
	server, err = flushfs.NewFileSystem(reportFlush, reportFsync)

	return
}

func makeFS() (server fuse.Server, err error) {
	switch *fType {
	default:
		err = fmt.Errorf("Unknown FS type: %v", *fType)

	case "flushfs":
		server, err = makeFlushFS()
	}

	return
}

func getReadyFile() (f *os.File, err error) {
	if *fReadyFile == 0 {
		err = errors.New("You must set --ready_file.")
		return
	}

	f = os.NewFile(uintptr(*fReadyFile), "(ready file)")
	return
}

func main() {
	flag.Parse()

	// Allow parallelism in the file system implementation, to help flush out
	// bugs like https://github.com/jacobsa/fuse/issues/4.
	runtime.GOMAXPROCS(2)

	// Grab the file to signal when ready.
	readyFile, err := getReadyFile()
	if err != nil {
		log.Fatalf("getReadyFile:", err)
	}

	// Create an appropriate file system.
	server, err := makeFS()
	if err != nil {
		log.Fatalf("makeFS: %v", err)
	}

	// Mount the file system.
	if *fMountPoint == "" {
		log.Fatalf("You must set --mount_point.")
	}

	cfg := &fuse.MountConfig{
		ReadOnly: *fReadOnly,
	}

	if *fDebug {
		cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)
	}

	mfs, err := fuse.Mount(*fMountPoint, server, cfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}

	// Signal that it is ready.
	_, err = readyFile.Write([]byte("x"))
	if err != nil {
		log.Fatalf("readyFile.Write: %v", err)
	}

	// Wait for it to be unmounted.
	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}
}
