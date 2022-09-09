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

package fsutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

// Create a temporary file with the same semantics as ioutil.TempFile, but
// ensure that it is unlinked before returning so that it does not persist
// after the process exits.
//
// Warning: this is not production-quality code, and should only be used for
// testing purposes. In particular, there is a race between creating and
// unlinking by name.
func AnonymousFile(dir string) (f *os.File, err error) {
	// Choose a prefix based on the binary name.
	prefix := path.Base(os.Args[0])

	// Create the file.
	f, err = ioutil.TempFile(dir, prefix)
	if err != nil {
		err = fmt.Errorf("TempFile: %v", err)
		return
	}

	// Unlink it.
	err = os.Remove(f.Name())
	if err != nil {
		err = fmt.Errorf("Remove: %v", err)
		return
	}

	return
}

// Call fdatasync on the supplied file.
//
// REQUIRES: FdatasyncSupported is true.
func Fdatasync(f *os.File) error {
	return fdatasync(f)
}
