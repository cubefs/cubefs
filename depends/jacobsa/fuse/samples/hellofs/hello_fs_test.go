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

package hellofs_test

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"syscall"
	"testing"

	"github.com/jacobsa/fuse/fusetesting"
	"github.com/jacobsa/fuse/samples"
	"github.com/jacobsa/fuse/samples/hellofs"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestHelloFS(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type HelloFSTest struct {
	samples.SampleTest
}

func init() { RegisterTestSuite(&HelloFSTest{}) }

func (t *HelloFSTest) SetUp(ti *TestInfo) {
	var err error

	t.Server, err = hellofs.NewHelloFS(&t.Clock)
	AssertEq(nil, err)

	t.SampleTest.SetUp(ti)
}

////////////////////////////////////////////////////////////////////////
// Test functions
////////////////////////////////////////////////////////////////////////

func (t *HelloFSTest) ReadDir_Root() {
	entries, err := fusetesting.ReadDirPicky(t.Dir)

	AssertEq(nil, err)
	AssertEq(2, len(entries))
	var fi os.FileInfo

	// dir
	fi = entries[0]
	ExpectEq("dir", fi.Name())
	ExpectEq(0, fi.Size())
	ExpectEq(os.ModeDir|0555, fi.Mode())
	ExpectEq(0, t.Clock.Now().Sub(fi.ModTime()), "ModTime: %v", fi.ModTime())
	ExpectTrue(fi.IsDir())

	// hello
	fi = entries[1]
	ExpectEq("hello", fi.Name())
	ExpectEq(len("Hello, world!"), fi.Size())
	ExpectEq(0444, fi.Mode())
	ExpectEq(0, t.Clock.Now().Sub(fi.ModTime()), "ModTime: %v", fi.ModTime())
	ExpectFalse(fi.IsDir())
}

func (t *HelloFSTest) ReadDir_Dir() {
	entries, err := fusetesting.ReadDirPicky(path.Join(t.Dir, "dir"))

	AssertEq(nil, err)
	AssertEq(1, len(entries))
	var fi os.FileInfo

	// world
	fi = entries[0]
	ExpectEq("world", fi.Name())
	ExpectEq(len("Hello, world!"), fi.Size())
	ExpectEq(0444, fi.Mode())
	ExpectEq(0, t.Clock.Now().Sub(fi.ModTime()), "ModTime: %v", fi.ModTime())
	ExpectFalse(fi.IsDir())
}

func (t *HelloFSTest) ReadDir_NonExistent() {
	_, err := fusetesting.ReadDirPicky(path.Join(t.Dir, "foobar"))

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("no such file")))
}

func (t *HelloFSTest) Stat_Hello() {
	fi, err := os.Stat(path.Join(t.Dir, "hello"))
	AssertEq(nil, err)

	ExpectEq("hello", fi.Name())
	ExpectEq(len("Hello, world!"), fi.Size())
	ExpectEq(0444, fi.Mode())
	ExpectEq(0, t.Clock.Now().Sub(fi.ModTime()), "ModTime: %v", fi.ModTime())
	ExpectFalse(fi.IsDir())
	ExpectEq(1, fi.Sys().(*syscall.Stat_t).Nlink)
}

func (t *HelloFSTest) Stat_Dir() {
	fi, err := os.Stat(path.Join(t.Dir, "dir"))
	AssertEq(nil, err)

	ExpectEq("dir", fi.Name())
	ExpectEq(0, fi.Size())
	ExpectEq(0555|os.ModeDir, fi.Mode())
	ExpectEq(0, t.Clock.Now().Sub(fi.ModTime()), "ModTime: %v", fi.ModTime())
	ExpectTrue(fi.IsDir())
	ExpectEq(1, fi.Sys().(*syscall.Stat_t).Nlink)
}

func (t *HelloFSTest) Stat_World() {
	fi, err := os.Stat(path.Join(t.Dir, "dir/world"))
	AssertEq(nil, err)

	ExpectEq("world", fi.Name())
	ExpectEq(len("Hello, world!"), fi.Size())
	ExpectEq(0444, fi.Mode())
	ExpectEq(0, t.Clock.Now().Sub(fi.ModTime()), "ModTime: %v", fi.ModTime())
	ExpectFalse(fi.IsDir())
	ExpectEq(1, fi.Sys().(*syscall.Stat_t).Nlink)
}

func (t *HelloFSTest) Stat_NonExistent() {
	_, err := os.Stat(path.Join(t.Dir, "foobar"))

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("no such file")))
}

func (t *HelloFSTest) ReadFile_Hello() {
	slice, err := ioutil.ReadFile(path.Join(t.Dir, "hello"))

	AssertEq(nil, err)
	ExpectEq("Hello, world!", string(slice))
}

func (t *HelloFSTest) ReadFile_Dir() {
	_, err := ioutil.ReadFile(path.Join(t.Dir, "dir"))

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("is a directory")))
}

func (t *HelloFSTest) ReadFile_World() {
	slice, err := ioutil.ReadFile(path.Join(t.Dir, "dir/world"))

	AssertEq(nil, err)
	ExpectEq("Hello, world!", string(slice))
}

func (t *HelloFSTest) OpenAndRead() {
	var buf []byte = make([]byte, 1024)
	var n int
	var off int64
	var err error

	// Open the file.
	f, err := os.Open(path.Join(t.Dir, "hello"))
	defer func() {
		if f != nil {
			ExpectEq(nil, f.Close())
		}
	}()

	AssertEq(nil, err)

	// Seeking shouldn't affect the random access reads below.
	_, err = f.Seek(7, 0)
	AssertEq(nil, err)

	// Random access reads
	n, err = f.ReadAt(buf[:2], 0)
	AssertEq(nil, err)
	ExpectEq(2, n)
	ExpectEq("He", string(buf[:n]))

	n, err = f.ReadAt(buf[:2], int64(len("Hel")))
	AssertEq(nil, err)
	ExpectEq(2, n)
	ExpectEq("lo", string(buf[:n]))

	n, err = f.ReadAt(buf[:3], int64(len("Hello, wo")))
	AssertEq(nil, err)
	ExpectEq(3, n)
	ExpectEq("rld", string(buf[:n]))

	// Read beyond end.
	n, err = f.ReadAt(buf[:3], int64(len("Hello, world")))
	AssertEq(io.EOF, err)
	ExpectEq(1, n)
	ExpectEq("!", string(buf[:n]))

	// Seek then read the rest.
	off, err = f.Seek(int64(len("Hel")), 0)
	AssertEq(nil, err)
	AssertEq(len("Hel"), off)

	n, err = io.ReadFull(f, buf[:len("lo, world!")])
	AssertEq(nil, err)
	ExpectEq(len("lo, world!"), n)
	ExpectEq("lo, world!", string(buf[:n]))
}

func (t *HelloFSTest) Open_NonExistent() {
	_, err := os.Open(path.Join(t.Dir, "foobar"))

	AssertNe(nil, err)
	ExpectThat(err, Error(HasSubstr("no such file")))
}
