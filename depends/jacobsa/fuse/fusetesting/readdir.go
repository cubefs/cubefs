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

package fusetesting

import (
	"fmt"
	"os"
	"path"
	"sort"
)

type sortedEntries []os.FileInfo

func (f sortedEntries) Len() int           { return len(f) }
func (f sortedEntries) Less(i, j int) bool { return f[i].Name() < f[j].Name() }
func (f sortedEntries) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

// Read the directory with the given name and return a list of directory
// entries, sorted by name.
//
// Unlike ioutil.ReadDir (cf. http://goo.gl/i0nNP4), this function does not
// silently ignore "file not found" errors when stat'ing the names read from
// the directory.
func ReadDirPicky(dirname string) (entries []os.FileInfo, err error) {
	// Open the directory.
	f, err := os.Open(dirname)
	if err != nil {
		err = fmt.Errorf("Open: %v", err)
		return
	}

	// Don't forget to close it later.
	defer func() {
		closeErr := f.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("Close: %v", closeErr)
		}
	}()

	// Read all of the names from the directory.
	names, err := f.Readdirnames(-1)
	if err != nil {
		err = fmt.Errorf("Readdirnames: %v", err)
		return
	}

	// Stat each one.
	for _, name := range names {
		var fi os.FileInfo

		fi, err = os.Lstat(path.Join(dirname, name))
		if err != nil {
			err = fmt.Errorf("Lstat(%s): %v", name, err)
			return
		}

		entries = append(entries, fi)
	}

	// Sort the entries by name.
	sort.Sort(sortedEntries(entries))

	return
}
