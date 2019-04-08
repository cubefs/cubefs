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

package flushfs_test

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/jacobsa/fuse/fsutil"
	"github.com/jacobsa/fuse/fusetesting"
	"github.com/jacobsa/fuse/samples"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

func TestFlushFS(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type flushFSTest struct {
	samples.SubprocessTest

	// Files to which mount_sample is writing reported flushes and fsyncs.
	flushes *os.File
	fsyncs  *os.File

	// File handles that are closed in TearDown if non-nil.
	f1 *os.File
	f2 *os.File
}

func (t *flushFSTest) setUp(
	ti *TestInfo,
	flushErr syscall.Errno,
	fsyncErr syscall.Errno,
	readOnly bool) {
	var err error

	// Set up files to receive flush and fsync reports.
	t.flushes, err = fsutil.AnonymousFile("")
	AssertEq(nil, err)

	t.fsyncs, err = fsutil.AnonymousFile("")
	AssertEq(nil, err)

	// Set up test config.
	t.MountType = "flushfs"
	t.MountFlags = []string{
		"--flushfs.flush_error",
		fmt.Sprintf("%d", int(flushErr)),

		"--flushfs.fsync_error",
		fmt.Sprintf("%d", int(fsyncErr)),
	}

	if readOnly {
		t.MountFlags = append(t.MountFlags, "--read_only")
	}

	t.MountFiles = map[string]*os.File{
		"flushfs.flushes_file": t.flushes,
		"flushfs.fsyncs_file":  t.fsyncs,
	}

	t.SubprocessTest.SetUp(ti)
}

func (t *flushFSTest) TearDown() {
	// Unlink reporting files.
	os.Remove(t.flushes.Name())
	os.Remove(t.fsyncs.Name())

	// Close reporting files.
	t.flushes.Close()
	t.fsyncs.Close()

	// Close test files if non-nil.
	if t.f1 != nil {
		ExpectEq(nil, t.f1.Close())
	}

	if t.f2 != nil {
		ExpectEq(nil, t.f2.Close())
	}

	// Finish tearing down.
	t.SubprocessTest.TearDown()
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

var isDarwin = runtime.GOOS == "darwin"

func readReports(f *os.File) (reports []string, err error) {
	// Seek the file to the start.
	_, err = f.Seek(0, 0)
	if err != nil {
		err = fmt.Errorf("Seek: %v", err)
		return
	}

	// We expect reports to end in a newline (including the final one).
	reader := bufio.NewReader(f)
	for {
		var record []byte
		record, err = reader.ReadBytes('\n')
		if err == io.EOF {
			if len(record) != 0 {
				err = fmt.Errorf("Unexpected record:\n%s", hex.Dump(record))
				return
			}

			err = nil
			return
		}

		if err != nil {
			err = fmt.Errorf("ReadBytes: %v", err)
			return
		}

		// Strip the newline.
		reports = append(reports, string(record[:len(record)-1]))
	}
}

// Return a copy of the current contents of t.flushes.
func (t *flushFSTest) getFlushes() (p []string) {
	var err error
	if p, err = readReports(t.flushes); err != nil {
		panic(err)
	}

	return
}

// Return a copy of the current contents of t.fsyncs.
func (t *flushFSTest) getFsyncs() (p []string) {
	var err error
	if p, err = readReports(t.fsyncs); err != nil {
		panic(err)
	}

	return
}

// Like syscall.Dup2, but correctly annotates the syscall as blocking. See here
// for more info: https://github.com/golang/go/issues/10202
func dup2(oldfd int, newfd int) (err error) {
	_, _, e1 := syscall.Syscall(
		syscall.SYS_DUP2, uintptr(oldfd), uintptr(newfd), 0)

	if e1 != 0 {
		err = e1
	}

	return
}

// Call msync(2) with the MS_SYNC flag on a slice previously returned by
// mmap(2).
func msync(p []byte) (err error) {
	_, _, errno := unix.Syscall(
		unix.SYS_MSYNC,
		uintptr(unsafe.Pointer(&p[0])),
		uintptr(len(p)),
		unix.MS_SYNC)

	if errno != 0 {
		err = errno
		return
	}

	return
}

////////////////////////////////////////////////////////////////////////
// No errors
////////////////////////////////////////////////////////////////////////

type NoErrorsTest struct {
	flushFSTest
}

func init() { RegisterTestSuite(&NoErrorsTest{}) }

func (t *NoErrorsTest) SetUp(ti *TestInfo) {
	const noErr = 0
	t.flushFSTest.setUp(ti, noErr, noErr, false)
}

func (t *NoErrorsTest) Close_ReadWrite() {
	var n int
	var off int64
	var err error
	buf := make([]byte, 1024)

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// Seek and read them back.
	off, err = t.f1.Seek(0, 0)
	AssertEq(nil, err)
	AssertEq(0, off)

	n, err = t.f1.Read(buf)
	AssertThat(err, AnyOf(nil, io.EOF))
	AssertEq("taco", string(buf[:n]))

	// At this point, no flushes or fsyncs should have happened.
	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close the file.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	// Now we should have received the flush operation (but still no fsync).
	ExpectThat(t.getFlushes(), ElementsAre("taco"))
	ExpectThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Close_ReadOnly() {
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDONLY, 0)
	AssertEq(nil, err)

	// At this point, no flushes or fsyncs should have happened.
	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close the file.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	// Now we should have received the flush operation (but still no fsync).
	ExpectThat(t.getFlushes(), ElementsAre(""))
	ExpectThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Close_WriteOnly() {
	var n int
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// At this point, no flushes or fsyncs should have happened.
	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close the file.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	// Now we should have received the flush operation (but still no fsync).
	ExpectThat(t.getFlushes(), ElementsAre("taco"))
	ExpectThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Close_MultipleTimes_NonOverlappingFileHandles() {
	var n int
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// At this point, no flushes or fsyncs should have happened.
	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close the file.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	// Now we should have received the flush operation (but still no fsync).
	AssertThat(t.getFlushes(), ElementsAre("taco"))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Open the file again.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Write again; expect no further flushes.
	n, err = t.f1.Write([]byte("p"))
	AssertEq(nil, err)
	AssertEq(1, n)

	AssertThat(t.getFlushes(), ElementsAre("taco"))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close the file. Now the new contents should be flushed.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre("taco", "paco"))
	AssertThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Close_MultipleTimes_OverlappingFileHandles() {
	var n int
	var err error

	// Open the file with two handles.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	t.f2, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Write some contents with each handle.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	n, err = t.f2.Write([]byte("p"))
	AssertEq(nil, err)
	AssertEq(1, n)

	// At this point, no flushes or fsyncs should have happened.
	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close one handle. The current contents should be flushed.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre("paco"))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Write some more contents via the other handle. Again, no further flushes.
	n, err = t.f2.Write([]byte("orp"))
	AssertEq(nil, err)
	AssertEq(3, n)

	AssertThat(t.getFlushes(), ElementsAre("paco"))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close the handle. Now the new contents should be flushed.
	err = t.f2.Close()
	t.f2 = nil
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre("paco", "porp"))
	AssertThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Fsync() {
	var n int
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Fsync.
	err = t.f1.Sync()
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre("taco"))

	// Write some more contents.
	n, err = t.f1.Write([]byte("s"))
	AssertEq(nil, err)
	AssertEq(1, n)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre("taco"))

	// Fsync.
	err = t.f1.Sync()
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre("taco", "tacos"))
}

func (t *NoErrorsTest) Fdatasync() {
	var n int
	var err error

	if !fsutil.FdatasyncSupported {
		return
	}

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Fdatasync.
	err = fsutil.Fdatasync(t.f1)
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre("taco"))

	// Write some more contents.
	n, err = t.f1.Write([]byte("s"))
	AssertEq(nil, err)
	AssertEq(1, n)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre("taco"))

	// Fdatasync.
	err = fsutil.Fdatasync(t.f1)
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre("taco", "tacos"))
}

func (t *NoErrorsTest) Dup() {
	var n int
	var err error

	var expectedFlushes []interface{}

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	fd1 := t.f1.Fd()

	// Use dup(2) to get another copy.
	fd2, err := syscall.Dup(int(fd1))
	AssertEq(nil, err)

	t.f2 = os.NewFile(uintptr(fd2), t.f1.Name())

	// Write some contents with each handle.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	n, err = t.f2.Write([]byte("s"))
	AssertEq(nil, err)
	AssertEq(1, n)

	// At this point, no flushes or fsyncs should have happened.
	AssertThat(t.getFlushes(), ElementsAre())
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close one handle. On Linux the current contents should be flushed. On OS
	// X, where the semantics of handles are different, they apparently are not.
	// (Cf. https://github.com/osxfuse/osxfuse/issues/199)
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	if !isDarwin {
		expectedFlushes = append(expectedFlushes, "tacos")
	}

	AssertThat(t.getFlushes(), ElementsAre(expectedFlushes...))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Write some more contents via the other handle. Again, no further flushes.
	n, err = t.f2.Write([]byte("!"))
	AssertEq(nil, err)
	AssertEq(1, n)

	AssertThat(t.getFlushes(), ElementsAre(expectedFlushes...))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Close the handle. Now the new contents should be flushed.
	err = t.f2.Close()
	t.f2 = nil
	AssertEq(nil, err)

	expectedFlushes = append(expectedFlushes, "tacos!")
	ExpectThat(t.getFlushes(), ElementsAre(expectedFlushes...))
	ExpectThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Dup2() {
	var n int
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// Create some anonymous temporary file.
	t.f2, err = fsutil.AnonymousFile("")
	AssertEq(nil, err)

	// Duplicate the temporary file descriptor on top of the file from our file
	// system. We should see a flush.
	err = dup2(int(t.f2.Fd()), int(t.f1.Fd()))
	ExpectEq(nil, err)

	ExpectThat(t.getFlushes(), ElementsAre("taco"))
	ExpectThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Mmap_NoMsync_MunmapBeforeClose() {
	var n int
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// mmap the file.
	data, err := syscall.Mmap(
		int(t.f1.Fd()), 0, 4,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)

	AssertEq(nil, err)
	defer syscall.Munmap(data)

	AssertEq("taco", string(data))

	// Modify the contents.
	data[0] = 'p'

	// Unmap.
	err = syscall.Munmap(data)
	AssertEq(nil, err)

	// munmap does not cause a flush.
	ExpectThat(t.getFlushes(), ElementsAre())
	ExpectThat(t.getFsyncs(), ElementsAre())

	// Close the file. We should see a flush with up to date contents.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	ExpectThat(t.getFlushes(), ElementsAre("paco"))
	ExpectThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Mmap_NoMsync_CloseBeforeMunmap() {
	var n int
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// mmap the file.
	data, err := syscall.Mmap(
		int(t.f1.Fd()), 0, 4,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)

	AssertEq(nil, err)
	defer syscall.Munmap(data)

	AssertEq("taco", string(data))

	// Close the file. We should see a flush.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre("taco"))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Modify the contents.
	data[0] = 'p'

	// Unmap.
	err = syscall.Munmap(data)
	AssertEq(nil, err)

	// munmap does not cause a flush.
	ExpectThat(t.getFlushes(), ElementsAre("taco"))
	ExpectThat(t.getFsyncs(), ElementsAre())
}

func (t *NoErrorsTest) Mmap_WithMsync_MunmapBeforeClose() {
	var n int
	var err error

	var expectedFsyncs []interface{}

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// mmap the file.
	data, err := syscall.Mmap(
		int(t.f1.Fd()), 0, 4,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)

	AssertEq(nil, err)
	defer syscall.Munmap(data)

	AssertEq("taco", string(data))

	// Modify the contents.
	data[0] = 'p'

	// msync. This causes an fsync, except on OS X (cf.
	// https://github.com/osxfuse/osxfuse/issues/202).
	err = msync(data)
	ExpectEq(nil, err)

	if !isDarwin {
		expectedFsyncs = append(expectedFsyncs, "paco")
	}

	ExpectThat(t.getFlushes(), ElementsAre())
	ExpectThat(t.getFsyncs(), ElementsAre(expectedFsyncs...))

	// Unmap. This does not cause anything.
	err = syscall.Munmap(data)
	AssertEq(nil, err)

	ExpectThat(t.getFlushes(), ElementsAre())
	ExpectThat(t.getFsyncs(), ElementsAre(expectedFsyncs...))

	// Close the file. We should now see a flush with the modified contents, even
	// on OS X.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	ExpectThat(t.getFlushes(), ElementsAre("paco"))
	ExpectThat(t.getFsyncs(), ElementsAre(expectedFsyncs...))
}

func (t *NoErrorsTest) Mmap_WithMsync_CloseBeforeMunmap() {
	var n int
	var err error

	var expectedFsyncs []interface{}

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Write some contents to the file.
	n, err = t.f1.Write([]byte("taco"))
	AssertEq(nil, err)
	AssertEq(4, n)

	// mmap the file.
	data, err := syscall.Mmap(
		int(t.f1.Fd()), 0, 4,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)

	AssertEq(nil, err)
	defer syscall.Munmap(data)

	AssertEq("taco", string(data))

	// Close the file. We should see a flush.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	AssertThat(t.getFlushes(), ElementsAre("taco"))
	AssertThat(t.getFsyncs(), ElementsAre())

	// Modify the contents.
	data[0] = 'p'

	// msync. This causes an fsync, except on OS X (cf.
	// https://github.com/osxfuse/osxfuse/issues/202).
	err = msync(data)
	ExpectEq(nil, err)

	if !isDarwin {
		expectedFsyncs = append(expectedFsyncs, "paco")
	}

	ExpectThat(t.getFlushes(), ElementsAre("taco"))
	ExpectThat(t.getFsyncs(), ElementsAre(expectedFsyncs...))

	// Unmap. Again, this does not cause a flush.
	err = syscall.Munmap(data)
	AssertEq(nil, err)

	ExpectThat(t.getFlushes(), ElementsAre("taco"))
	ExpectThat(t.getFsyncs(), ElementsAre(expectedFsyncs...))
}

func (t *NoErrorsTest) Directory() {
	var err error

	// Open the directory.
	t.f1, err = os.Open(path.Join(t.Dir, "bar"))
	AssertEq(nil, err)

	// Sanity check: stat it.
	fi, err := t.f1.Stat()
	AssertEq(nil, err)
	AssertEq(0777|os.ModeDir, fi.Mode())

	// Sync it.
	err = t.f1.Sync()
	AssertEq(nil, err)

	// Close it.
	err = t.f1.Close()
	t.f1 = nil
	AssertEq(nil, err)

	// No flushes or fsync requests should have been received.
	ExpectThat(t.getFlushes(), ElementsAre())
	ExpectThat(t.getFsyncs(), ElementsAre())
}

////////////////////////////////////////////////////////////////////////
// Flush error
////////////////////////////////////////////////////////////////////////

type FlushErrorTest struct {
	flushFSTest
}

func init() { RegisterTestSuite(&FlushErrorTest{}) }

func (t *FlushErrorTest) SetUp(ti *TestInfo) {
	const noErr = 0
	t.flushFSTest.setUp(ti, syscall.ENOENT, noErr, false)
}

func (t *FlushErrorTest) Close() {
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Close the file.
	err = t.f1.Close()
	t.f1 = nil

	ExpectThat(err, Error(HasSubstr("no such file")))
}

func (t *FlushErrorTest) Dup() {
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	fd1 := t.f1.Fd()

	// Use dup(2) to get another copy.
	fd2, err := syscall.Dup(int(fd1))
	AssertEq(nil, err)

	t.f2 = os.NewFile(uintptr(fd2), t.f1.Name())

	// Close by the first handle. On OS X, where the semantics of file handles
	// are different (cf. https://github.com/osxfuse/osxfuse/issues/199), this
	// does not result in an error.
	err = t.f1.Close()
	t.f1 = nil

	if isDarwin {
		AssertEq(nil, err)
	} else {
		ExpectThat(err, Error(HasSubstr("no such file")))
	}

	// Close by the second handle.
	err = t.f2.Close()
	t.f2 = nil

	ExpectThat(err, Error(HasSubstr("no such file")))
}

func (t *FlushErrorTest) Dup2() {
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_WRONLY, 0)
	AssertEq(nil, err)

	// Create some anonymous temporary file.
	t.f2, err = fsutil.AnonymousFile("")
	AssertEq(nil, err)

	// Duplicate the temporary file descriptor on top of the file from our file
	// system. We shouldn't see the flush error.
	err = dup2(int(t.f2.Fd()), int(t.f1.Fd()))
	ExpectEq(nil, err)
}

////////////////////////////////////////////////////////////////////////
// Fsync error
////////////////////////////////////////////////////////////////////////

type FsyncErrorTest struct {
	flushFSTest
}

func init() { RegisterTestSuite(&FsyncErrorTest{}) }

func (t *FsyncErrorTest) SetUp(ti *TestInfo) {
	const noErr = 0
	t.flushFSTest.setUp(ti, noErr, syscall.ENOENT, false)
}

func (t *FsyncErrorTest) Fsync() {
	var err error

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Fsync.
	err = t.f1.Sync()

	ExpectThat(err, Error(HasSubstr("no such file")))
}

func (t *FsyncErrorTest) Fdatasync() {
	var err error

	if !fsutil.FdatasyncSupported {
		return
	}

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// Fdatasync.
	err = fsutil.Fdatasync(t.f1)

	ExpectThat(err, Error(HasSubstr("no such file")))
}

func (t *FsyncErrorTest) Msync() {
	var err error

	// On OS X, msync does not cause SyncFile.
	if isDarwin {
		return
	}

	// Open the file.
	t.f1, err = os.OpenFile(path.Join(t.Dir, "foo"), os.O_RDWR, 0)
	AssertEq(nil, err)

	// mmap the file.
	data, err := syscall.Mmap(
		int(t.f1.Fd()), 0, 4,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)

	AssertEq(nil, err)
	defer syscall.Munmap(data)

	// msync the mapping.
	err = msync(data)
	ExpectThat(err, Error(HasSubstr("no such file")))

	// Unmap.
	err = syscall.Munmap(data)
	AssertEq(nil, err)
}

////////////////////////////////////////////////////////////////////////
// Read-only mount
////////////////////////////////////////////////////////////////////////

type ReadOnlyTest struct {
	flushFSTest
}

func init() { RegisterTestSuite(&ReadOnlyTest{}) }

func (t *ReadOnlyTest) SetUp(ti *TestInfo) {
	const noErr = 0
	t.flushFSTest.setUp(ti, noErr, noErr, true)
}

func (t *ReadOnlyTest) ReadRoot() {
	var fi os.FileInfo

	// Read.
	entries, err := fusetesting.ReadDirPicky(t.Dir)
	AssertEq(nil, err)
	AssertEq(2, len(entries))

	// bar
	fi = entries[0]
	ExpectEq("bar", fi.Name())
	ExpectEq(os.FileMode(0777)|os.ModeDir, fi.Mode())

	// foo
	fi = entries[1]
	ExpectEq("foo", fi.Name())
	ExpectEq(os.FileMode(0777), fi.Mode())
}

func (t *ReadOnlyTest) StatFiles() {
	var fi os.FileInfo
	var err error

	// bar
	fi, err = os.Stat(path.Join(t.Dir, "bar"))
	AssertEq(nil, err)

	ExpectEq("bar", fi.Name())
	ExpectEq(os.FileMode(0777)|os.ModeDir, fi.Mode())

	// foo
	fi, err = os.Stat(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	ExpectEq("foo", fi.Name())
	ExpectEq(os.FileMode(0777), fi.Mode())
}

func (t *ReadOnlyTest) ReadFile() {
	_, err := ioutil.ReadFile(path.Join(t.Dir, "foo"))
	ExpectEq(nil, err)
}

func (t *ReadOnlyTest) ReadDir() {
	_, err := fusetesting.ReadDirPicky(path.Join(t.Dir, "bar"))
	ExpectEq(nil, err)
}

func (t *ReadOnlyTest) CreateFile() {
	err := ioutil.WriteFile(path.Join(t.Dir, "blah"), []byte{}, 0400)
	ExpectThat(err, Error(HasSubstr("read-only")))
}

func (t *ReadOnlyTest) Mkdir() {
	err := os.Mkdir(path.Join(t.Dir, "blah"), 0700)
	ExpectThat(err, Error(HasSubstr("read-only")))
}

func (t *ReadOnlyTest) OpenForWrite() {
	modes := []int{
		os.O_WRONLY,
		os.O_RDWR,
	}

	for _, mode := range modes {
		f, err := os.OpenFile(path.Join(t.Dir, "foo"), mode, 0700)
		f.Close()
		ExpectThat(err, Error(HasSubstr("read-only")), "mode: %v", mode)
	}
}

func (t *ReadOnlyTest) Chtimes() {
	err := os.Chtimes(path.Join(t.Dir, "foo"), time.Now(), time.Now())
	ExpectThat(err, Error(MatchesRegexp("read-only|not permitted")))
}

func (t *ReadOnlyTest) Chmod() {
	err := os.Chmod(path.Join(t.Dir, "foo"), 0700)
	ExpectThat(err, Error(HasSubstr("read-only")))
}
