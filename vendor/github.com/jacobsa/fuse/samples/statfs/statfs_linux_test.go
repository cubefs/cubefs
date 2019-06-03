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
	"fmt"
	"math"
	"regexp"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	. "github.com/jacobsa/ogletest"
)

// Sample output:
//
//     Filesystem                  1K-blocks Used Available Use% Mounted on
//     some_fuse_file_system       512       64   384       15%  /tmp/sample_test001288095
//
var gDfOutputRegexp = regexp.MustCompile(`^\S+\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+%.*$`)

////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////

func (t *StatFSTest) Syscall_ZeroValues() {
	var err error
	var stat syscall.Statfs_t

	// Call without configuring a canned response, meaning the OS will see the
	// zero value for each field. The assertions below act as documentation for
	// the OS's behavior in this case.
	err = syscall.Statfs(t.Dir, &stat)
	AssertEq(nil, err)

	ExpectEq(0, stat.Bsize)
	ExpectEq(0, stat.Frsize)
	ExpectEq(0, stat.Blocks)
	ExpectEq(0, stat.Bfree)
	ExpectEq(0, stat.Bavail)
	ExpectEq(0, stat.Files)
	ExpectEq(0, stat.Ffree)
}

func (t *StatFSTest) Syscall_NonZeroValues() {
	var err error
	var stat syscall.Statfs_t

	// Set up the canned response.
	canned := fuseops.StatFSOp{
		BlockSize: 1 << 15,
		IoSize:    1 << 16,

		Blocks:          1<<51 + 3,
		BlocksFree:      1<<43 + 5,
		BlocksAvailable: 1<<41 + 7,

		Inodes:     1<<59 + 11,
		InodesFree: 1<<58 + 13,
	}

	t.fs.SetStatFSResponse(canned)

	// Stat.
	err = syscall.Statfs(t.Dir, &stat)
	AssertEq(nil, err)

	ExpectEq(canned.BlockSize, stat.Frsize)
	ExpectEq(canned.IoSize, stat.Bsize)
	ExpectEq(canned.Blocks, stat.Blocks)
	ExpectEq(canned.BlocksFree, stat.Bfree)
	ExpectEq(canned.BlocksAvailable, stat.Bavail)
	ExpectEq(canned.Inodes, stat.Files)
	ExpectEq(canned.InodesFree, stat.Ffree)
}

func (t *StatFSTest) BlockSizes() {
	var err error

	// Test a bunch of weird block sizes that OS X would be cranky about.
	blockSizes := []uint32{
		0,
		1,
		3,
		17,
		1<<20 - 1,
		1<<20 + 0,
		1<<20 + 1,
		math.MaxInt32,
		math.MaxInt32 + 1,
		math.MaxUint32,
	}

	for _, bs := range blockSizes {
		desc := fmt.Sprintf("block size %d", bs)

		// Set up.
		canned := fuseops.StatFSOp{
			BlockSize: bs,
			Blocks:    10,
		}

		t.fs.SetStatFSResponse(canned)

		// Check.
		var stat syscall.Statfs_t
		err = syscall.Statfs(t.Dir, &stat)
		AssertEq(nil, err)

		ExpectEq(bs, stat.Frsize, "%s", desc)
	}
}

func (t *StatFSTest) IoSizes() {
	var err error

	// Test a bunch of weird IO sizes that OS X would be cranky about.
	ioSizes := []uint32{
		0,
		1,
		3,
		17,
		1<<20 - 1,
		1<<20 + 0,
		1<<20 + 1,
		math.MaxInt32,
		math.MaxInt32 + 1,
		math.MaxUint32,
	}

	for _, bs := range ioSizes {
		desc := fmt.Sprintf("IO size %d", bs)

		// Set up.
		canned := fuseops.StatFSOp{
			IoSize: bs,
			Blocks: 10,
		}

		t.fs.SetStatFSResponse(canned)

		// Check.
		var stat syscall.Statfs_t
		err = syscall.Statfs(t.Dir, &stat)
		AssertEq(nil, err)

		ExpectEq(bs, stat.Bsize, "%s", desc)
	}
}
