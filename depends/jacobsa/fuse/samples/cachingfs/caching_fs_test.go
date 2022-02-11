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

package cachingfs_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/fuse/samples"
	"github.com/jacobsa/fuse/samples/cachingfs"
	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
	"github.com/jacobsa/timeutil"
)

func TestCachingFS(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type cachingFSTest struct {
	samples.SampleTest

	fs           cachingfs.CachingFS
	initialMtime time.Time
}

var _ TearDownInterface = &cachingFSTest{}

func (t *cachingFSTest) setUp(
	ti *TestInfo,
	lookupEntryTimeout time.Duration,
	getattrTimeout time.Duration) {
	var err error

	// We assert things about whether or not mtimes are cached, but writeback
	// caching causes them to always be cached. Turn it off.
	t.MountConfig.DisableWritebackCaching = true

	// Create the file system.
	t.fs, err = cachingfs.NewCachingFS(lookupEntryTimeout, getattrTimeout)
	AssertEq(nil, err)

	t.Server = fuseutil.NewFileSystemServer(t.fs)

	// Mount it.
	t.SampleTest.SetUp(ti)

	// Set up the mtime.
	t.initialMtime = time.Date(2012, 8, 15, 22, 56, 0, 0, time.Local)
	t.fs.SetMtime(t.initialMtime)
}

func (t *cachingFSTest) statAll() (foo, dir, bar os.FileInfo) {
	var err error

	foo, err = os.Stat(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	dir, err = os.Stat(path.Join(t.Dir, "dir"))
	AssertEq(nil, err)

	bar, err = os.Stat(path.Join(t.Dir, "dir/bar"))
	AssertEq(nil, err)

	return
}

func (t *cachingFSTest) openFiles() (foo, dir, bar *os.File) {
	var err error

	foo, err = os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	dir, err = os.Open(path.Join(t.Dir, "dir"))
	AssertEq(nil, err)

	bar, err = os.Open(path.Join(t.Dir, "dir/bar"))
	AssertEq(nil, err)

	return
}

func (t *cachingFSTest) statFiles(
	f, g, h *os.File) (foo, dir, bar os.FileInfo) {
	var err error

	foo, err = f.Stat()
	AssertEq(nil, err)

	dir, err = g.Stat()
	AssertEq(nil, err)

	bar, err = h.Stat()
	AssertEq(nil, err)

	return
}

func getInodeID(fi os.FileInfo) uint64 {
	return fi.Sys().(*syscall.Stat_t).Ino
}

////////////////////////////////////////////////////////////////////////
// Basics
////////////////////////////////////////////////////////////////////////

type BasicsTest struct {
	cachingFSTest
}

var _ SetUpInterface = &BasicsTest{}

func init() { RegisterTestSuite(&BasicsTest{}) }

func (t *BasicsTest) SetUp(ti *TestInfo) {
	const (
		lookupEntryTimeout = 0
		getattrTimeout     = 0
	)

	t.cachingFSTest.setUp(ti, lookupEntryTimeout, getattrTimeout)
}

func (t *BasicsTest) StatNonexistent() {
	names := []string{
		"blah",
		"bar",
		"dir/blah",
		"dir/dir",
		"dir/foo",
	}

	for _, n := range names {
		_, err := os.Stat(path.Join(t.Dir, n))

		AssertNe(nil, err)
		ExpectTrue(os.IsNotExist(err), "n: %s, err: %v", n, err)
	}
}

func (t *BasicsTest) StatFoo() {
	fi, err := os.Stat(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	ExpectEq("foo", fi.Name())
	ExpectEq(cachingfs.FooSize, fi.Size())
	ExpectEq(0777, fi.Mode())
	ExpectThat(fi.ModTime(), timeutil.TimeEq(t.initialMtime))
	ExpectFalse(fi.IsDir())
	ExpectEq(t.fs.FooID(), getInodeID(fi))
	ExpectEq(1, fi.Sys().(*syscall.Stat_t).Nlink)
}

func (t *BasicsTest) StatDir() {
	fi, err := os.Stat(path.Join(t.Dir, "dir"))
	AssertEq(nil, err)

	ExpectEq("dir", fi.Name())
	ExpectEq(os.ModeDir|0777, fi.Mode())
	ExpectThat(fi.ModTime(), timeutil.TimeEq(t.initialMtime))
	ExpectTrue(fi.IsDir())
	ExpectEq(t.fs.DirID(), getInodeID(fi))
	ExpectEq(1, fi.Sys().(*syscall.Stat_t).Nlink)
}

func (t *BasicsTest) StatBar() {
	fi, err := os.Stat(path.Join(t.Dir, "dir/bar"))
	AssertEq(nil, err)

	ExpectEq("bar", fi.Name())
	ExpectEq(cachingfs.BarSize, fi.Size())
	ExpectEq(0777, fi.Mode())
	ExpectThat(fi.ModTime(), timeutil.TimeEq(t.initialMtime))
	ExpectFalse(fi.IsDir())
	ExpectEq(t.fs.BarID(), getInodeID(fi))
	ExpectEq(1, fi.Sys().(*syscall.Stat_t).Nlink)
}

////////////////////////////////////////////////////////////////////////
// No caching
////////////////////////////////////////////////////////////////////////

type NoCachingTest struct {
	cachingFSTest
}

var _ SetUpInterface = &NoCachingTest{}

func init() { RegisterTestSuite(&NoCachingTest{}) }

func (t *NoCachingTest) SetUp(ti *TestInfo) {
	const (
		lookupEntryTimeout = 0
		getattrTimeout     = 0
	)

	t.cachingFSTest.setUp(ti, lookupEntryTimeout, getattrTimeout)
}

func (t *NoCachingTest) StatStat() {
	fooBefore, dirBefore, barBefore := t.statAll()
	fooAfter, dirAfter, barAfter := t.statAll()

	// Make sure everything matches.
	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(fooBefore.ModTime()))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(dirBefore.ModTime()))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(barBefore.ModTime()))

	ExpectEq(getInodeID(fooBefore), getInodeID(fooAfter))
	ExpectEq(getInodeID(dirBefore), getInodeID(dirAfter))
	ExpectEq(getInodeID(barBefore), getInodeID(barAfter))
}

func (t *NoCachingTest) StatRenumberStat() {
	t.statAll()
	t.fs.RenumberInodes()
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should see the new inode IDs, because the entries should not have been
	// cached.
	ExpectEq(t.fs.FooID(), getInodeID(fooAfter))
	ExpectEq(t.fs.DirID(), getInodeID(dirAfter))
	ExpectEq(t.fs.BarID(), getInodeID(barAfter))
}

func (t *NoCachingTest) StatMtimeStat() {
	newMtime := t.initialMtime.Add(time.Second)

	t.statAll()
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should see the new mtimes, because the attributes should not have been
	// cached.
	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))
}

func (t *NoCachingTest) StatRenumberMtimeStat() {
	newMtime := t.initialMtime.Add(time.Second)

	t.statAll()
	t.fs.RenumberInodes()
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should see the new inode IDs and mtimes, because nothing should have
	// been cached.
	ExpectEq(t.fs.FooID(), getInodeID(fooAfter))
	ExpectEq(t.fs.DirID(), getInodeID(dirAfter))
	ExpectEq(t.fs.BarID(), getInodeID(barAfter))

	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))
}

////////////////////////////////////////////////////////////////////////
// Entry caching
////////////////////////////////////////////////////////////////////////

type EntryCachingTest struct {
	cachingFSTest
	lookupEntryTimeout time.Duration
}

var _ SetUpInterface = &EntryCachingTest{}

func init() { RegisterTestSuite(&EntryCachingTest{}) }

func (t *EntryCachingTest) SetUp(ti *TestInfo) {
	t.lookupEntryTimeout = 250 * time.Millisecond
	t.SampleTest.MountConfig.EnableVnodeCaching = true

	t.cachingFSTest.setUp(ti, t.lookupEntryTimeout, 0)
}

func (t *EntryCachingTest) StatStat() {
	fooBefore, dirBefore, barBefore := t.statAll()
	fooAfter, dirAfter, barAfter := t.statAll()

	// Make sure everything matches.
	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(fooBefore.ModTime()))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(dirBefore.ModTime()))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(barBefore.ModTime()))

	ExpectEq(getInodeID(fooBefore), getInodeID(fooAfter))
	ExpectEq(getInodeID(dirBefore), getInodeID(dirAfter))
	ExpectEq(getInodeID(barBefore), getInodeID(barAfter))
}

func (t *EntryCachingTest) StatRenumberStat() {
	fooBefore, dirBefore, barBefore := t.statAll()
	t.fs.RenumberInodes()
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should still see the old inode IDs, because the inode entries should
	// have been cached.
	ExpectEq(getInodeID(fooBefore), getInodeID(fooAfter))
	ExpectEq(getInodeID(dirBefore), getInodeID(dirAfter))
	ExpectEq(getInodeID(barBefore), getInodeID(barAfter))

	// But after waiting for the entry cache to expire, we should see the new
	// IDs.
	//
	// Note that the cache is not guaranteed to expire on darwin. See notes on
	// fuse.MountConfig.EnableVnodeCaching.
	if runtime.GOOS != "darwin" {
		time.Sleep(2 * t.lookupEntryTimeout)
		fooAfter, dirAfter, barAfter = t.statAll()

		ExpectEq(t.fs.FooID(), getInodeID(fooAfter))
		ExpectEq(t.fs.DirID(), getInodeID(dirAfter))
		ExpectEq(t.fs.BarID(), getInodeID(barAfter))
	}
}

func (t *EntryCachingTest) StatMtimeStat() {
	newMtime := t.initialMtime.Add(time.Second)

	t.statAll()
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should see the new mtimes, because the attributes should not have been
	// cached.
	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))
}

func (t *EntryCachingTest) StatRenumberMtimeStat() {
	newMtime := t.initialMtime.Add(time.Second)

	fooBefore, dirBefore, barBefore := t.statAll()
	t.fs.RenumberInodes()
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should still see the old inode IDs, because the inode entries should
	// have been cached. But the attributes should not have been.
	ExpectEq(getInodeID(fooBefore), getInodeID(fooAfter))
	ExpectEq(getInodeID(dirBefore), getInodeID(dirAfter))
	ExpectEq(getInodeID(barBefore), getInodeID(barAfter))

	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))

	// After waiting for the entry cache to expire, we should see fresh
	// everything.
	//
	// Note that the cache is not guaranteed to expire on darwin. See notes on
	// fuse.MountConfig.EnableVnodeCaching.
	if runtime.GOOS != "darwin" {
		time.Sleep(2 * t.lookupEntryTimeout)
		fooAfter, dirAfter, barAfter = t.statAll()

		ExpectEq(t.fs.FooID(), getInodeID(fooAfter))
		ExpectEq(t.fs.DirID(), getInodeID(dirAfter))
		ExpectEq(t.fs.BarID(), getInodeID(barAfter))

		ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
		ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
		ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))
	}
}

////////////////////////////////////////////////////////////////////////
// Attribute caching
////////////////////////////////////////////////////////////////////////

type AttributeCachingTest struct {
	cachingFSTest
	getattrTimeout time.Duration
}

var _ SetUpInterface = &AttributeCachingTest{}

func init() { RegisterTestSuite(&AttributeCachingTest{}) }

func (t *AttributeCachingTest) SetUp(ti *TestInfo) {
	t.getattrTimeout = 250 * time.Millisecond
	t.cachingFSTest.setUp(ti, 0, t.getattrTimeout)
}

func (t *AttributeCachingTest) StatStat() {
	fooBefore, dirBefore, barBefore := t.statAll()
	fooAfter, dirAfter, barAfter := t.statAll()

	// Make sure everything matches.
	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(fooBefore.ModTime()))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(dirBefore.ModTime()))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(barBefore.ModTime()))

	ExpectEq(getInodeID(fooBefore), getInodeID(fooAfter))
	ExpectEq(getInodeID(dirBefore), getInodeID(dirAfter))
	ExpectEq(getInodeID(barBefore), getInodeID(barAfter))
}

func (t *AttributeCachingTest) StatRenumberStat() {
	t.statAll()
	t.fs.RenumberInodes()
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should see the new inode IDs, because the entries should not have been
	// cached.
	ExpectEq(t.fs.FooID(), getInodeID(fooAfter))
	ExpectEq(t.fs.DirID(), getInodeID(dirAfter))
	ExpectEq(t.fs.BarID(), getInodeID(barAfter))
}

func (t *AttributeCachingTest) StatMtimeStat_ViaPath() {
	newMtime := t.initialMtime.Add(time.Second)

	t.statAll()
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statAll()

	// Since we don't have entry caching enabled, the call above had to look up
	// the entry again. With the lookup we returned new attributes, so it's
	// possible that the mtime will be fresh. On Linux it appears to be, and on
	// OS X it appears to not be.
	m := AnyOf(timeutil.TimeEq(newMtime), timeutil.TimeEq(t.initialMtime))
	ExpectThat(fooAfter.ModTime(), m)
	ExpectThat(dirAfter.ModTime(), m)
	ExpectThat(barAfter.ModTime(), m)
}

func (t *AttributeCachingTest) StatMtimeStat_ViaFileDescriptor() {
	newMtime := t.initialMtime.Add(time.Second)

	// Open everything, fixing a particular inode number for each.
	foo, dir, bar := t.openFiles()
	defer func() {
		foo.Close()
		dir.Close()
		bar.Close()
	}()

	fooBefore, dirBefore, barBefore := t.statFiles(foo, dir, bar)
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statFiles(foo, dir, bar)

	// We should still see the old cached mtime.
	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(fooBefore.ModTime()))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(dirBefore.ModTime()))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(barBefore.ModTime()))

	// After waiting for the attribute cache to expire, we should see the fresh
	// mtime.
	time.Sleep(2 * t.getattrTimeout)
	fooAfter, dirAfter, barAfter = t.statFiles(foo, dir, bar)

	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))
}

func (t *AttributeCachingTest) StatRenumberMtimeStat_ViaPath() {
	newMtime := t.initialMtime.Add(time.Second)

	t.statAll()
	t.fs.RenumberInodes()
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statAll()

	// We should see new everything, because this is the first time the new
	// inodes have been encountered. Entries for the old ones should not have
	// been cached, because we have entry caching disabled.
	ExpectEq(t.fs.FooID(), getInodeID(fooAfter))
	ExpectEq(t.fs.DirID(), getInodeID(dirAfter))
	ExpectEq(t.fs.BarID(), getInodeID(barAfter))

	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))
}

func (t *AttributeCachingTest) StatRenumberMtimeStat_ViaFileDescriptor() {
	newMtime := t.initialMtime.Add(time.Second)

	// Open everything, fixing a particular inode number for each.
	foo, dir, bar := t.openFiles()
	defer func() {
		foo.Close()
		dir.Close()
		bar.Close()
	}()

	fooBefore, dirBefore, barBefore := t.statFiles(foo, dir, bar)
	t.fs.RenumberInodes()
	t.fs.SetMtime(newMtime)
	fooAfter, dirAfter, barAfter := t.statFiles(foo, dir, bar)

	// We should still see the old cached mtime with the old inode ID.
	ExpectEq(getInodeID(fooBefore), getInodeID(fooAfter))
	ExpectEq(getInodeID(dirBefore), getInodeID(dirAfter))
	ExpectEq(getInodeID(barBefore), getInodeID(barAfter))

	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(fooBefore.ModTime()))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(dirBefore.ModTime()))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(barBefore.ModTime()))

	// After waiting for the attribute cache to expire, we should see the fresh
	// mtime, still with the old inode ID.
	time.Sleep(2 * t.getattrTimeout)
	fooAfter, dirAfter, barAfter = t.statFiles(foo, dir, bar)

	ExpectEq(getInodeID(fooBefore), getInodeID(fooAfter))
	ExpectEq(getInodeID(dirBefore), getInodeID(dirAfter))
	ExpectEq(getInodeID(barBefore), getInodeID(barAfter))

	ExpectThat(fooAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(dirAfter.ModTime(), timeutil.TimeEq(newMtime))
	ExpectThat(barAfter.ModTime(), timeutil.TimeEq(newMtime))
}

////////////////////////////////////////////////////////////////////////
// Page cache
////////////////////////////////////////////////////////////////////////

type PageCacheTest struct {
	cachingFSTest
}

var _ SetUpInterface = &PageCacheTest{}

func init() { RegisterTestSuite(&PageCacheTest{}) }

func (t *PageCacheTest) SetUp(ti *TestInfo) {
	const (
		lookupEntryTimeout = 0
		getattrTimeout     = 0
	)

	t.cachingFSTest.setUp(ti, lookupEntryTimeout, getattrTimeout)
}

func (t *PageCacheTest) SingleFileHandle_NoKeepCache() {
	t.fs.SetKeepCache(false)

	// Open the file.
	f, err := os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	defer f.Close()

	// Read its contents once.
	f.Seek(0, 0)
	AssertEq(nil, err)

	c1, err := ioutil.ReadAll(f)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c1))

	// And again.
	f.Seek(0, 0)
	AssertEq(nil, err)

	c2, err := ioutil.ReadAll(f)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c2))

	// We should have seen the same contents each time.
	ExpectTrue(bytes.Equal(c1, c2))
}

func (t *PageCacheTest) SingleFileHandle_KeepCache() {
	t.fs.SetKeepCache(true)

	// Open the file.
	f, err := os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	defer f.Close()

	// Read its contents once.
	f.Seek(0, 0)
	AssertEq(nil, err)

	c1, err := ioutil.ReadAll(f)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c1))

	// And again.
	f.Seek(0, 0)
	AssertEq(nil, err)

	c2, err := ioutil.ReadAll(f)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c2))

	// We should have seen the same contents each time.
	ExpectTrue(bytes.Equal(c1, c2))
}

func (t *PageCacheTest) TwoFileHandles_NoKeepCache() {
	t.fs.SetKeepCache(false)

	// SetKeepCache(false) doesn't work on OS X. See the notes on
	// OpenFileOp.KeepPageCache.
	if runtime.GOOS == "darwin" {
		return
	}

	// Open the file.
	f1, err := os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	defer f1.Close()

	// Read its contents once.
	f1.Seek(0, 0)
	AssertEq(nil, err)

	c1, err := ioutil.ReadAll(f1)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c1))

	// Open a second handle.
	f2, err := os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	defer f2.Close()

	// We should see different contents if we read from that handle, due to the
	// cache being invalidated at the time of opening.
	f2.Seek(0, 0)
	AssertEq(nil, err)

	c2, err := ioutil.ReadAll(f2)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c2))

	ExpectFalse(bytes.Equal(c1, c2))

	// Another read from the second handle should give the same result as the
	// first one from that handle.
	f2.Seek(0, 0)
	AssertEq(nil, err)

	c3, err := ioutil.ReadAll(f2)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c3))

	ExpectTrue(bytes.Equal(c2, c3))

	// And another read from the first handle should give the same result yet
	// again.
	f1.Seek(0, 0)
	AssertEq(nil, err)

	c4, err := ioutil.ReadAll(f1)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c4))

	ExpectTrue(bytes.Equal(c2, c4))
}

func (t *PageCacheTest) TwoFileHandles_KeepCache() {
	t.fs.SetKeepCache(true)

	// Open the file.
	f1, err := os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	defer f1.Close()

	// Read its contents once.
	f1.Seek(0, 0)
	AssertEq(nil, err)

	c1, err := ioutil.ReadAll(f1)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c1))

	// Open a second handle.
	f2, err := os.Open(path.Join(t.Dir, "foo"))
	AssertEq(nil, err)

	defer f2.Close()

	// We should see the same contents when we read via the second handle.
	f2.Seek(0, 0)
	AssertEq(nil, err)

	c2, err := ioutil.ReadAll(f2)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c2))

	ExpectTrue(bytes.Equal(c1, c2))

	// Ditto if we read again from the first.
	f1.Seek(0, 0)
	AssertEq(nil, err)

	c3, err := ioutil.ReadAll(f1)
	AssertEq(nil, err)
	AssertEq(cachingfs.FooSize, len(c3))

	ExpectTrue(bytes.Equal(c1, c3))
}
