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

package forgetfs_test

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/jacobsa/fuse/samples"
	"github.com/jacobsa/fuse/samples/forgetfs"
	. "github.com/jacobsa/ogletest"
)

func TestForgetFS(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type ForgetFSTest struct {
	samples.SampleTest
	fs *forgetfs.ForgetFS
}

func init() { RegisterTestSuite(&ForgetFSTest{}) }

func (t *ForgetFSTest) SetUp(ti *TestInfo) {
	t.fs = forgetfs.NewFileSystem()
	t.Server = t.fs
	t.SampleTest.SetUp(ti)
}

func (t *ForgetFSTest) TearDown() {
	// Unmount.
	t.SampleTest.TearDown()

	// Crash if anything is left.
	t.fs.Check()
}

////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////

func (t *ForgetFSTest) Open_Foo() {
	var err error

	f, err := os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	err = f.Close()
	AssertEq(nil, err)
}

func (t *ForgetFSTest) Open_Bar() {
	var err error

	f, err := os.Open(path.Join(t.Dir, "bar"))
	AssertEq(nil, err)

	err = f.Close()
	AssertEq(nil, err)
}

func (t *ForgetFSTest) Open_ManyTimes() {
	// Set up a slice of files that will be closed when we're done.
	var toClose []io.Closer
	defer func() {
		for _, c := range toClose {
			ExpectEq(nil, c.Close())
		}
	}()

	// Open foo many times.
	for i := 0; i < 100; i++ {
		f, err := os.Open(path.Join(t.Dir, "foo"))
		AssertEq(nil, err)
		toClose = append(toClose, f)
	}

	// Open bar many times.
	for i := 0; i < 100; i++ {
		f, err := os.Open(path.Join(t.Dir, "bar"))
		AssertEq(nil, err)
		toClose = append(toClose, f)
	}
}

func (t *ForgetFSTest) Stat_Foo() {
	var fi os.FileInfo
	var err error

	fi, err = os.Stat(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)
	AssertEq("foo", fi.Name())
	AssertEq(os.FileMode(0777), fi.Mode())
}

func (t *ForgetFSTest) Stat_Bar() {
	var fi os.FileInfo
	var err error

	fi, err = os.Stat(path.Join(t.Dir, "bar"))
	AssertEq(nil, err)
	AssertEq("bar", fi.Name())
	AssertEq(0777|os.ModeDir, fi.Mode())
}

func (t *ForgetFSTest) Stat_ManyTimes() {
	var err error

	// Stat foo many times.
	for i := 0; i < 100; i++ {
		_, err = os.Stat(path.Join(t.Dir, "foo"))
		AssertEq(nil, err)
	}

	// Stat bar many times.
	for i := 0; i < 100; i++ {
		_, err = os.Stat(path.Join(t.Dir, "bar"))
		AssertEq(nil, err)
	}
}

func (t *ForgetFSTest) CreateFile() {
	// Create and close many files within the root.
	for i := 0; i < 100; i++ {
		f, err := os.Create(path.Join(t.Dir, "blah"))
		AssertEq(nil, err)
		AssertEq(nil, f.Close())
	}

	// Create and close many files within the sub-directory.
	for i := 0; i < 100; i++ {
		f, err := os.Create(path.Join(t.Dir, "bar", "blah"))
		AssertEq(nil, err)
		AssertEq(nil, f.Close())
	}
}

func (t *ForgetFSTest) MkDir() {
	// Create many directories within the root.
	for i := 0; i < 100; i++ {
		err := os.Mkdir(path.Join(t.Dir, "blah"), 0777)
		AssertEq(nil, err)
	}

	// Create many directories within the sub-directory.
	for i := 0; i < 100; i++ {
		err := os.Mkdir(path.Join(t.Dir, "bar", "blah"), 0777)
		AssertEq(nil, err)
	}
}
