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

package sys

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStat(t *testing.T) {
	_, err := GetInfo("")
	require.Error(t, err)

	_, err = GetInfo(os.TempDir())
	require.NoError(t, err)
}

func TestIsMountPoint(t *testing.T) {
	b := IsMountPoint("")
	require.Equal(t, false, b)

	dir, err := ioutil.TempDir(os.TempDir(), "MountPointTestFile")
	require.NoError(t, err)

	err = os.MkdirAll(dir, 0o775)
	require.NoError(t, err)
	defer os.Remove(dir)

	linkDir := filepath.Join(os.TempDir(), "MountPointTestFileLink")
	defer os.Remove(linkDir)

	err = os.Symlink(dir, linkDir)
	require.NoError(t, err)

	b = IsMountPoint(linkDir)
	require.Equal(t, false, b)
}

func TestGetFSType(t *testing.T) {
	fs := getFSType(0x12345678)
	require.Equal(t, "UNKNOWN", fs)

	fs = getFSType(0x137d)
	require.Equal(t, "EXT", fs)
}
