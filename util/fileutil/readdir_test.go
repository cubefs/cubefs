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

func TestReaddir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	file1 := "f1"
	file2 := "f2"

	err = os.WriteFile(path.Join(tmpDir, file1), []byte(""), 0644)
	require.NoError(t, err)

	os.WriteFile(path.Join(tmpDir, file2), []byte(""), 0644)
	require.NoError(t, err)

	names, err := fileutil.ReadDir(tmpDir)
	require.NoError(t, err)

	fileMap := make(map[string]interface{})

	for _, v := range names {
		fileMap[v] = 1
	}

	require.EqualValues(t, 2, len(fileMap))

	_, file1Exist := fileMap[file1]
	_, file2Exist := fileMap[file2]

	require.True(t, file1Exist)
	require.True(t, file2Exist)
}
