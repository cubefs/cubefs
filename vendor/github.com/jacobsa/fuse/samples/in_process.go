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

package samples

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/ogletest"
	"github.com/jacobsa/timeutil"
)

// A struct that implements common behavior needed by tests in the samples/
// directory. Use it as an embedded field in your test fixture, calling its
// SetUp method from your SetUp method after setting the Server field.
type SampleTest struct {
	// The server under test and the configuration with which it should be
	// mounted. These must be set by the user of this type before calling SetUp;
	// all the other fields below are set by SetUp itself.
	Server      fuse.Server
	MountConfig fuse.MountConfig

	// A context object that can be used for long-running operations.
	Ctx context.Context

	// A clock with a fixed initial time. The test's set up method may use this
	// to wire the server with a clock, if desired.
	Clock timeutil.SimulatedClock

	// The directory at which the file system is mounted.
	Dir string

	// Anothing non-nil in this slice will be closed by TearDown. The test will
	// fail if closing fails.
	ToClose []io.Closer

	mfs *fuse.MountedFileSystem
}

// Mount t.Server and initialize the other exported fields of the struct.
// Panics on error.
//
// REQUIRES: t.Server has been set.
func (t *SampleTest) SetUp(ti *ogletest.TestInfo) {
	cfg := t.MountConfig
	if *fDebug {
		cfg.DebugLogger = log.New(os.Stderr, "fuse: ", 0)
	}

	err := t.initialize(ti.Ctx, t.Server, &cfg)
	if err != nil {
		panic(err)
	}
}

// Like SetUp, but doens't panic.
func (t *SampleTest) initialize(
	ctx context.Context,
	server fuse.Server,
	config *fuse.MountConfig) (err error) {
	// Initialize the context used by the test.
	t.Ctx = ctx

	// Make the server share that context, if the test hasn't already set some
	// other one.
	if config.OpContext == nil {
		config.OpContext = ctx
	}

	// Initialize the clock.
	t.Clock.SetTime(time.Date(2012, 8, 15, 22, 56, 0, 0, time.Local))

	// Set up a temporary directory.
	t.Dir, err = ioutil.TempDir("", "sample_test")
	if err != nil {
		err = fmt.Errorf("TempDir: %v", err)
		return
	}

	// Mount the file system.
	t.mfs, err = fuse.Mount(t.Dir, server, config)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	return
}

// Unmount the file system and clean up. Panics on error.
func (t *SampleTest) TearDown() {
	err := t.destroy()
	if err != nil {
		panic(err)
	}
}

// Like TearDown, but doesn't panic.
func (t *SampleTest) destroy() (err error) {
	// Close what is necessary.
	for _, c := range t.ToClose {
		if c == nil {
			continue
		}

		ogletest.ExpectEq(nil, c.Close())
	}

	// Was the file system mounted?
	if t.mfs == nil {
		return
	}

	// Unmount the file system.
	err = unmount(t.Dir)
	if err != nil {
		err = fmt.Errorf("unmount: %v", err)
		return
	}

	// Unlink the mount point.
	if err = os.Remove(t.Dir); err != nil {
		err = fmt.Errorf("Unlinking mount point: %v", err)
		return
	}

	// Join the file system.
	err = t.mfs.Join(t.Ctx)
	if err != nil {
		err = fmt.Errorf("mfs.Join: %v", err)
		return
	}

	return
}
