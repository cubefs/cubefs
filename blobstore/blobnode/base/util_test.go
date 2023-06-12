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

package base

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Example:
// "Range": "bytes=0-99"
// "Range": "bytes=100-200"
// "Range": "bytes=-50"
// "Range": "bytes=150-"
func TestParseOneRange(t *testing.T) {
	s1 := "bytes=0-99"
	s2 := "bytes=100-200"
	s3 := "bytes=-50"
	s4 := "bytes=150-"
	s5 := "bytes150"
	s6 := "bytes=150"
	s7 := "bytes=a-120"
	s8 := "bytes=120-a"

	//
	start, end, err := ParseHttpRangeStr(s1)
	require.NoError(t, err)
	require.Equal(t, 0, int(start))
	require.Equal(t, 99, int(end))

	from, to, err := FixHttpRange(start, end, 200)
	require.NoError(t, err)
	require.Equal(t, 0, int(from))
	require.Equal(t, 100, int(to))

	//
	start, end, err = ParseHttpRangeStr(s2)
	require.NoError(t, err)
	require.Equal(t, 100, int(start))
	require.Equal(t, 200, int(end))

	_, _, err = FixHttpRange(start, end, 200)
	require.Error(t, err)

	_, _, err = FixHttpRange(start, end, 201)
	require.NoError(t, err)

	//
	start, end, err = ParseHttpRangeStr(s3)
	require.NoError(t, err)
	require.Equal(t, -1, int(start))
	require.Equal(t, 50, int(end))

	from, to, err = FixHttpRange(start, end, 200)
	require.NoError(t, err)
	require.Equal(t, 150, int(from))
	require.Equal(t, 200, int(to))

	//
	start, end, err = ParseHttpRangeStr(s4)
	require.NoError(t, err)
	require.Equal(t, 150, int(start))
	require.Equal(t, -1, int(end))

	from, to, err = FixHttpRange(start, end, 200)
	require.NoError(t, err)
	require.Equal(t, 150, int(from))
	require.Equal(t, 200, int(to))

	_, _, err = ParseHttpRangeStr(s5)
	require.Error(t, err)

	_, _, err = ParseHttpRangeStr(s6)
	require.Error(t, err)

	_, _, err = ParseHttpRangeStr(s7)
	require.Error(t, err)

	_, _, err = ParseHttpRangeStr(s8)
	require.Error(t, err)

	from, to, err = FixHttpRange(-1, 200, 100)
	require.NoError(t, err)
	require.Equal(t, 0, int(from))
	require.Equal(t, 100, int(to))
}

func TestIsEmptyDisk(t *testing.T) {
	filename := "!!"
	flag, err := IsEmptyDisk(filename)
	require.Equal(t, false, flag)
	require.Error(t, err)

	testDir, err := os.MkdirTemp(os.TempDir(), "blobnode_util")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	flag, err = IsEmptyDisk(testDir)
	require.Equal(t, true, flag)
	require.NoError(t, err)

	path := filepath.Join(testDir, "lost+found")
	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	flag, err = IsEmptyDisk(testDir)
	require.Equal(t, true, flag)
	require.NoError(t, err)

	path = filepath.Join(testDir, "test")
	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	flag, err = IsEmptyDisk(testDir)
	require.Equal(t, false, flag)
	require.NoError(t, err)

	path = filepath.Join(testDir, "test")
	err = os.Remove(path)
	require.NoError(t, err)

	flag, err = IsEmptyDisk(testDir)
	require.Equal(t, true, flag)
	require.NoError(t, err)
}
