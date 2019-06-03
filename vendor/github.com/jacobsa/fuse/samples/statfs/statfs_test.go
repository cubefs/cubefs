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

package statfs_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/fuse/samples"
	"github.com/jacobsa/fuse/samples/statfs"
	. "github.com/jacobsa/ogletest"
)

func TestStatFS(t *testing.T) { RunTests(t) }

const fsName = "some_fs_name"
const volumeName = "Some volume"

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// Ask `df` for statistics about the file system's capacity and free space,
// useful for checking that our reading of statfs(2) output matches the
// system's. The output is not guaranteed to have resolution greater than 2^10
// (1 KiB).
func df(dir string) (capacity, used, available uint64, err error) {
	// Call df with a block size of 1024 and capture its output.
	cmd := exec.Command("df", dir)
	cmd.Env = []string{"BLOCKSIZE=1024"}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return
	}

	// Scrape it.
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		// Is this the line we're interested in?
		if !bytes.Contains(line, []byte(dir)) {
			continue
		}

		submatches := gDfOutputRegexp.FindSubmatch(line)
		if submatches == nil {
			err = fmt.Errorf("Unable to parse line: %q", line)
			return
		}

		capacity, err = strconv.ParseUint(string(submatches[1]), 10, 64)
		if err != nil {
			return
		}

		used, err = strconv.ParseUint(string(submatches[2]), 10, 64)
		if err != nil {
			return
		}

		available, err = strconv.ParseUint(string(submatches[3]), 10, 64)
		if err != nil {
			return
		}

		// Scale appropriately based on the BLOCKSIZE set above.
		capacity *= 1024
		used *= 1024
		available *= 1024

		return
	}

	err = fmt.Errorf("Unable to parse df output:\n%s", output)
	return
}

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type StatFSTest struct {
	samples.SampleTest
	fs statfs.FS

	// t.Dir, with symlinks resolved and redundant path components removed.
	canonicalDir string
}

var _ SetUpInterface = &StatFSTest{}
var _ TearDownInterface = &StatFSTest{}

func init() { RegisterTestSuite(&StatFSTest{}) }

func (t *StatFSTest) SetUp(ti *TestInfo) {
	var err error

	// Writeback caching can ruin our measurement of the write sizes the kernel
	// decides to give us, since it causes write acking to race against writes
	// being issued from the client.
	t.MountConfig.DisableWritebackCaching = true

	// Configure names.
	t.MountConfig.FSName = fsName
	t.MountConfig.VolumeName = volumeName

	// Create the file system.
	t.fs = statfs.New()
	t.Server = fuseutil.NewFileSystemServer(t.fs)

	// Mount it.
	t.SampleTest.SetUp(ti)

	// Canonicalize the mount point.
	t.canonicalDir, err = filepath.EvalSymlinks(t.Dir)
	AssertEq(nil, err)
	t.canonicalDir = path.Clean(t.canonicalDir)
}

////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////

func (t *StatFSTest) CapacityAndFreeSpace() {
	canned := fuseops.StatFSOp{
		Blocks:          1024,
		BlocksFree:      896,
		BlocksAvailable: 768,

		IoSize: 1024, // Shouldn't matter.
	}

	// Check that df agrees with us about a range of block sizes.
	for log2BlockSize := uint(9); log2BlockSize <= 17; log2BlockSize++ {
		bs := uint64(1) << log2BlockSize
		desc := fmt.Sprintf("block size: %d (2^%d)", bs, log2BlockSize)

		// Set up the canned response.
		canned.BlockSize = uint32(bs)
		t.fs.SetStatFSResponse(canned)

		// Call df.
		capacity, used, available, err := df(t.canonicalDir)
		AssertEq(nil, err)

		ExpectEq(bs*canned.Blocks, capacity, "%s", desc)
		ExpectEq(bs*(canned.Blocks-canned.BlocksFree), used, "%s", desc)
		ExpectEq(bs*canned.BlocksAvailable, available, "%s", desc)
	}
}

func (t *StatFSTest) WriteSize() {
	var err error

	// Set up a smallish block size.
	canned := fuseops.StatFSOp{
		BlockSize:       8192,
		IoSize:          16384,
		Blocks:          1234,
		BlocksFree:      1234,
		BlocksAvailable: 1234,
	}

	t.fs.SetStatFSResponse(canned)

	// Cause a large amount of date to be written.
	err = ioutil.WriteFile(
		path.Join(t.Dir, "foo"),
		bytes.Repeat([]byte{'x'}, 1<<22),
		0400)

	AssertEq(nil, err)

	// Despite the small block size, the OS shouldn't have given us pitifully
	// small chunks of data.
	switch runtime.GOOS {
	case "linux":
		ExpectEq(1<<17, t.fs.MostRecentWriteSize())

	case "darwin":
		ExpectEq(1<<20, t.fs.MostRecentWriteSize())

	default:
		AddFailure("Unhandled OS: %s", runtime.GOOS)
	}
}

func (t *StatFSTest) StatBlocks() {
	var err error
	var stat syscall.Stat_t
	const fileName = "foo"
	const size = 1 << 22

	err = ioutil.WriteFile(
		path.Join(t.Dir, fileName),
		bytes.Repeat([]byte{'x'}, size),
		0400)
	AssertEq(nil, err)

	t.fs.SetStatResponse(fuseops.InodeAttributes{
		Size: size,
	})

	err = syscall.Stat(path.Join(t.Dir, fileName), &stat)
	AssertEq(nil, err)
	ExpectEq(size/512, stat.Blocks)
}
