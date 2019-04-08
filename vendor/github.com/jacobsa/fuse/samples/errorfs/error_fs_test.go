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

package errorfs_test

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"syscall"
	"testing"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/fuse/samples"
	"github.com/jacobsa/fuse/samples/errorfs"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestErrorFS(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type ErrorFSTest struct {
	samples.SampleTest
	fs errorfs.FS
}

func init() { RegisterTestSuite(&ErrorFSTest{}) }

var _ SetUpInterface = &ErrorFSTest{}
var _ TearDownInterface = &ErrorFSTest{}

func (t *ErrorFSTest) SetUp(ti *TestInfo) {
	var err error

	// Create the file system.
	t.fs, err = errorfs.New()
	AssertEq(nil, err)

	t.Server = fuseutil.NewFileSystemServer(t.fs)

	// Mount it.
	t.SampleTest.SetUp(ti)
}

////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////

func (t *ErrorFSTest) OpenFile() {
	t.fs.SetError(reflect.TypeOf(&fuseops.OpenFileOp{}), syscall.EOWNERDEAD)

	f, err := os.Open(path.Join(t.Dir, "foo"))
	defer f.Close()
	ExpectThat(err, Error(MatchesRegexp("open.*: .*owner died")))
}

func (t *ErrorFSTest) ReadFile() {
	t.fs.SetError(reflect.TypeOf(&fuseops.ReadFileOp{}), syscall.EOWNERDEAD)

	// Open
	f, err := os.Open(path.Join(t.Dir, "foo"))
	defer f.Close()
	AssertEq(nil, err)

	// Read
	_, err = ioutil.ReadAll(f)
	ExpectThat(err, Error(MatchesRegexp("read.*: .*owner died")))
}

func (t *ErrorFSTest) OpenDir() {
	t.fs.SetError(reflect.TypeOf(&fuseops.OpenDirOp{}), syscall.EOWNERDEAD)

	f, err := os.Open(t.Dir)
	defer f.Close()
	ExpectThat(err, Error(MatchesRegexp("open.*: .*owner died")))
}

func (t *ErrorFSTest) ReadDir() {
	t.fs.SetError(reflect.TypeOf(&fuseops.ReadDirOp{}), syscall.EOWNERDEAD)

	// Open
	f, err := os.Open(t.Dir)
	defer f.Close()
	AssertEq(nil, err)

	// Read
	_, err = f.Readdirnames(1)
	ExpectThat(err, Error(MatchesRegexp("read.*: .*owner died")))
}
