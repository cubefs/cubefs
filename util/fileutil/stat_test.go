// Copyright 2024 The CubeFS Authors.
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

package fileutil_test

import (
	"os"
	"path"
	"testing"

	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/stretchr/testify/require"
)

func TestStat(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.Remove(dir)
	ino, err := fileutil.Stat(dir)
	require.NoError(t, err)
	require.NotEqual(t, 0, ino)
}

const blkSize = 4096

func TestGetFilePhyscialSize(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	tmpFile := path.Join(dir, "tmp")
	err = os.WriteFile(tmpFile, []byte("Hello World"), 0o755)
	require.NoError(t, err)
	size, err := fileutil.GetFilePhysicalSize(tmpFile)
	require.NoError(t, err)
	require.EqualValues(t, blkSize, size)
}
