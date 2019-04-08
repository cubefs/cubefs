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

package interruptfs_test

import (
	"bytes"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/fuse/samples"
	"github.com/jacobsa/fuse/samples/interruptfs"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestInterruptFS(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type InterruptFSTest struct {
	samples.SampleTest
	fs *interruptfs.InterruptFS
}

func init() { RegisterTestSuite(&InterruptFSTest{}) }

var _ SetUpInterface = &InterruptFSTest{}
var _ TearDownInterface = &InterruptFSTest{}

func (t *InterruptFSTest) SetUp(ti *TestInfo) {
	var err error

	// Create the file system.
	t.fs = interruptfs.New()
	AssertEq(nil, err)

	t.Server = fuseutil.NewFileSystemServer(t.fs)

	// Mount it.
	t.SampleTest.SetUp(ti)
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *InterruptFSTest) StatFoo() {
	fi, err := os.Stat(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	ExpectEq("foo", fi.Name())
	ExpectEq(0777, fi.Mode())
	ExpectFalse(fi.IsDir())
}

func (t *InterruptFSTest) InterruptedDuringRead() {
	var err error
	t.fs.EnableReadBlocking()

	// Start a sub-process that attempts to read the file.
	cmd := exec.Command("cat", path.Join(t.Dir, "foo"))

	var cmdOutput bytes.Buffer
	cmd.Stdout = &cmdOutput
	cmd.Stderr = &cmdOutput

	err = cmd.Start()
	AssertEq(nil, err)

	// Wait for the command in the background, writing to a channel when it is
	// finished.
	cmdErr := make(chan error)
	go func() {
		cmdErr <- cmd.Wait()
	}()

	// Wait for the read to make it to the file system.
	t.fs.WaitForFirstRead()

	// The command should be hanging on the read, and not yet have returned.
	select {
	case err = <-cmdErr:
		AddFailure("Command returned early with error: %v", err)
		AbortTest()

	case <-time.After(10 * time.Millisecond):
	}

	// Send SIGINT.
	cmd.Process.Signal(os.Interrupt)

	// Now the command should return, with an appropriate error.
	err = <-cmdErr
	ExpectThat(err, Error(HasSubstr("signal")))
	ExpectThat(err, Error(HasSubstr("interrupt")))
}

func (t *InterruptFSTest) InterruptedDuringFlush() {
	var err error
	t.fs.EnableFlushBlocking()

	// Start a sub-process that attempts to read the file.
	cmd := exec.Command("cat", path.Join(t.Dir, "foo"))

	var cmdOutput bytes.Buffer
	cmd.Stdout = &cmdOutput
	cmd.Stderr = &cmdOutput

	err = cmd.Start()
	AssertEq(nil, err)

	// Wait for the command in the background, writing to a channel when it is
	// finished.
	cmdErr := make(chan error)
	go func() {
		cmdErr <- cmd.Wait()
	}()

	// Wait for the flush to make it to the file system.
	t.fs.WaitForFirstFlush()

	// The command should be hanging on the flush, and not yet have returned.
	select {
	case err = <-cmdErr:
		AddFailure("Command returned early with error: %v", err)
		AbortTest()

	case <-time.After(10 * time.Millisecond):
	}

	// Send SIGINT.
	cmd.Process.Signal(os.Interrupt)

	// Now the command should return, with an appropriate error.
	err = <-cmdErr
	ExpectThat(err, Error(HasSubstr("signal")))
	ExpectThat(err, Error(HasSubstr("interrupt")))
}
