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

// Tests for the behavior of os.File objects on plain old posix file systems,
// for use in verifying the intended behavior of memfs.

package memfs_test

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/jacobsa/fuse/fusetesting"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestPosix(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func getFileOffset(f *os.File) (offset int64, err error) {
	const relativeToCurrent = 1
	offset, err = f.Seek(0, relativeToCurrent)
	return
}

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type PosixTest struct {
	ctx context.Context

	// A temporary directory.
	dir string

	// Files to close when tearing down. Nil entries are skipped.
	toClose []io.Closer
}

var _ SetUpInterface = &PosixTest{}
var _ TearDownInterface = &PosixTest{}

func init() { RegisterTestSuite(&PosixTest{}) }

func (t *PosixTest) SetUp(ti *TestInfo) {
	var err error

	t.ctx = ti.Ctx

	// Create a temporary directory.
	t.dir, err = ioutil.TempDir("", "posix_test")
	if err != nil {
		panic(err)
	}
}

func (t *PosixTest) TearDown() {
	// Close any files we opened.
	for _, c := range t.toClose {
		if c == nil {
			continue
		}

		err := c.Close()
		if err != nil {
			panic(err)
		}
	}

	// Remove the temporary directory.
	err := os.RemoveAll(t.dir)
	if err != nil {
		panic(err)
	}
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *PosixTest) WriteOverlapsEndOfFile() {
	var err error
	var n int

	// Create a file.
	f, err := os.Create(path.Join(t.dir, "foo"))
	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Make it 4 bytes long.
	err = f.Truncate(4)
	AssertEq(nil, err)

	// Write the range [2, 6).
	n, err = f.WriteAt([]byte("taco"), 2)
	AssertEq(nil, err)
	AssertEq(4, n)

	// Read the full contents of the file.
	contents, err := ioutil.ReadAll(f)
	AssertEq(nil, err)
	ExpectEq("\x00\x00taco", string(contents))
}

func (t *PosixTest) WriteStartsAtEndOfFile() {
	var err error
	var n int

	// Create a file.
	f, err := os.Create(path.Join(t.dir, "foo"))
	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Make it 2 bytes long.
	err = f.Truncate(2)
	AssertEq(nil, err)

	// Write the range [2, 6).
	n, err = f.WriteAt([]byte("taco"), 2)
	AssertEq(nil, err)
	AssertEq(4, n)

	// Read the full contents of the file.
	contents, err := ioutil.ReadAll(f)
	AssertEq(nil, err)
	ExpectEq("\x00\x00taco", string(contents))
}

func (t *PosixTest) WriteStartsPastEndOfFile() {
	var err error
	var n int

	// Create a file.
	f, err := os.Create(path.Join(t.dir, "foo"))
	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Write the range [2, 6).
	n, err = f.WriteAt([]byte("taco"), 2)
	AssertEq(nil, err)
	AssertEq(4, n)

	// Read the full contents of the file.
	contents, err := ioutil.ReadAll(f)
	AssertEq(nil, err)
	ExpectEq("\x00\x00taco", string(contents))
}

func (t *PosixTest) WriteStartsPastEndOfFile_AppendMode() {
	var err error
	var n int

	// Create a file.
	f, err := os.OpenFile(
		path.Join(t.dir, "foo"),
		os.O_RDWR|os.O_APPEND|os.O_CREATE,
		0600)

	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Write three bytes.
	n, err = f.Write([]byte("111"))
	AssertEq(nil, err)
	AssertEq(3, n)

	// Write at offset six.
	n, err = f.WriteAt([]byte("222"), 6)
	AssertEq(nil, err)
	AssertEq(3, n)

	// Read the full contents of the file.
	//
	// Linux's support for pwrite is buggy; the pwrite(2) man page says this:
	//
	//     POSIX requires that opening a file with the O_APPEND flag should have
	//     no affect on the location at which pwrite() writes data.  However, on
	//     Linux,  if  a  file  is opened with O_APPEND, pwrite() appends data to
	//     the end of the file, regardless of the value of offset.
	//
	contents, err := ioutil.ReadFile(f.Name())
	AssertEq(nil, err)

	if runtime.GOOS == "linux" {
		ExpectEq("111222", string(contents))
	} else {
		ExpectEq("111\x00\x00\x00222", string(contents))
	}
}

func (t *PosixTest) WriteAtDoesntChangeOffset_NotAppendMode() {
	var err error
	var n int

	// Create a file.
	f, err := os.Create(path.Join(t.dir, "foo"))
	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Make it 16 bytes long.
	err = f.Truncate(16)
	AssertEq(nil, err)

	// Seek to offset 4.
	_, err = f.Seek(4, 0)
	AssertEq(nil, err)

	// Write the range [10, 14).
	n, err = f.WriteAt([]byte("taco"), 2)
	AssertEq(nil, err)
	AssertEq(4, n)

	// We should still be at offset 4.
	offset, err := getFileOffset(f)
	AssertEq(nil, err)
	ExpectEq(4, offset)
}

func (t *PosixTest) WriteAtDoesntChangeOffset_AppendMode() {
	var err error
	var n int

	// Create a file in append mode.
	f, err := os.OpenFile(
		path.Join(t.dir, "foo"),
		os.O_RDWR|os.O_APPEND|os.O_CREATE,
		0600)

	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Make it 16 bytes long.
	err = f.Truncate(16)
	AssertEq(nil, err)

	// Seek to offset 4.
	_, err = f.Seek(4, 0)
	AssertEq(nil, err)

	// Write the range [10, 14).
	n, err = f.WriteAt([]byte("taco"), 2)
	AssertEq(nil, err)
	AssertEq(4, n)

	// We should still be at offset 4.
	offset, err := getFileOffset(f)
	AssertEq(nil, err)
	ExpectEq(4, offset)
}

func (t *PosixTest) AppendMode() {
	var err error
	var n int
	var off int64
	buf := make([]byte, 1024)

	// Create a file with some contents.
	fileName := path.Join(t.dir, "foo")
	err = ioutil.WriteFile(fileName, []byte("Jello, "), 0600)
	AssertEq(nil, err)

	// Open the file in append mode.
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND, 0600)
	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Seek to somewhere silly and then write.
	off, err = f.Seek(2, 0)
	AssertEq(nil, err)
	AssertEq(2, off)

	n, err = f.Write([]byte("world!"))
	AssertEq(nil, err)
	AssertEq(6, n)

	// The offset should have been updated to point at the end of the file.
	off, err = getFileOffset(f)
	AssertEq(nil, err)
	ExpectEq(13, off)

	// A random write should still work, without updating the offset.
	n, err = f.WriteAt([]byte("H"), 0)
	AssertEq(nil, err)
	AssertEq(1, n)

	off, err = getFileOffset(f)
	AssertEq(nil, err)
	ExpectEq(13, off)

	// Read back the contents of the file, which should be correct even though we
	// seeked to a silly place before writing the world part.
	//
	// Linux's support for pwrite is buggy; the pwrite(2) man page says this:
	//
	//     POSIX requires that opening a file with the O_APPEND flag should have
	//     no affect on the location at which pwrite() writes data.  However, on
	//     Linux,  if  a  file  is opened with O_APPEND, pwrite() appends data to
	//     the end of the file, regardless of the value of offset.
	//
	// So we allow either the POSIX result or the Linux result.
	n, err = f.ReadAt(buf, 0)
	AssertEq(io.EOF, err)

	if runtime.GOOS == "linux" {
		ExpectEq("Jello, world!H", string(buf[:n]))
	} else {
		ExpectEq("Hello, world!", string(buf[:n]))
	}
}

func (t *PosixTest) ReadsPastEndOfFile() {
	var err error
	var n int
	buf := make([]byte, 1024)

	// Create a file.
	f, err := os.Create(path.Join(t.dir, "foo"))
	t.toClose = append(t.toClose, f)
	AssertEq(nil, err)

	// Give it some contents.
	n, err = f.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// Read a range overlapping EOF.
	n, err = f.ReadAt(buf[:4], 2)
	AssertEq(io.EOF, err)
	ExpectEq(2, n)
	ExpectEq("co", string(buf[:n]))

	// Read a range starting at EOF.
	n, err = f.ReadAt(buf[:4], 4)
	AssertEq(io.EOF, err)
	ExpectEq(0, n)
	ExpectEq("", string(buf[:n]))

	// Read a range starting past EOF.
	n, err = f.ReadAt(buf[:4], 100)
	AssertEq(io.EOF, err)
	ExpectEq(0, n)
	ExpectEq("", string(buf[:n]))
}

func (t *PosixTest) HardLinkDirectory() {
	dirName := path.Join(t.dir, "dir")

	// Create a directory.
	err := os.Mkdir(dirName, 0700)
	AssertEq(nil, err)

	// Attempt to hard-link it to a new name.
	err = os.Link(dirName, path.Join(t.dir, "other"))

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("link")))
	ExpectThat(err, Error(HasSubstr("not permitted")))
}

func (t *PosixTest) RmdirWhileOpenedForReading() {
	var err error

	// Create a directory.
	err = os.Mkdir(path.Join(t.dir, "dir"), 0700)
	AssertEq(nil, err)

	// Open the directory for reading.
	f, err := os.Open(path.Join(t.dir, "dir"))
	defer func() {
		if f != nil {
			ExpectEq(nil, f.Close())
		}
	}()

	AssertEq(nil, err)

	// Remove the directory.
	err = os.Remove(path.Join(t.dir, "dir"))
	AssertEq(nil, err)

	// Create a new directory, with the same name even, and add some contents
	// within it.
	err = os.MkdirAll(path.Join(t.dir, "dir/foo"), 0700)
	AssertEq(nil, err)

	err = os.MkdirAll(path.Join(t.dir, "dir/bar"), 0700)
	AssertEq(nil, err)

	err = os.MkdirAll(path.Join(t.dir, "dir/baz"), 0700)
	AssertEq(nil, err)

	// We should still be able to stat the open file handle.
	fi, err := f.Stat()
	ExpectEq("dir", fi.Name())

	// Attempt to read from the directory. This shouldn't see any junk from the
	// new directory. It should either succeed with an empty result or should
	// return ENOENT.
	names, err := f.Readdirnames(0)

	if err != nil {
		ExpectThat(err, Error(HasSubstr("no such file")))
	} else {
		ExpectThat(names, ElementsAre())
	}
}

func (t *PosixTest) CreateInParallel_NoTruncate() {
	fusetesting.RunCreateInParallelTest_NoTruncate(t.ctx, t.dir)
}

func (t *PosixTest) CreateInParallel_Truncate() {
	fusetesting.RunCreateInParallelTest_Truncate(t.ctx, t.dir)
}

func (t *PosixTest) CreateInParallel_Exclusive() {
	fusetesting.RunCreateInParallelTest_Exclusive(t.ctx, t.dir)
}

func (t *PosixTest) MkdirInParallel() {
	fusetesting.RunMkdirInParallelTest(t.ctx, t.dir)
}

func (t *PosixTest) SymlinkInParallel() {
	fusetesting.RunSymlinkInParallelTest(t.ctx, t.dir)
}

func (t *PosixTest) HardlinkInParallel() {
	fusetesting.RunHardlinkInParallelTest(t.ctx, t.dir)
}
