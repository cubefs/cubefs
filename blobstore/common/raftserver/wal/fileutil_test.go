// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package wal

import (
	"os"
	"path"
	"testing"
)

func TestFileutil(t *testing.T) {
	dir := "/tmp/raftwal-" + string(genRandomBytes(8))

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		err := InitPath(dir)
		if err != nil {
			t.Fatal(err)
		}
	}

	logfiles := []logName{
		{1, 1},
		{2, 1000},
		{3, 2000},
	}

	defer os.RemoveAll(dir)

	for _, name := range logfiles {
		file := path.Join(dir, name.String())
		f, err := os.Create(file)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	fnames, err := listLogFiles(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(fnames) != len(logfiles) {
		t.Fatal(len(fnames))
	}

	for i, name := range fnames {
		if name != logfiles[i] {
			t.Fatal(name)
		}
	}
}
