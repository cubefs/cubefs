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

package strutil_test

import (
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/strutil"
	"github.com/stretchr/testify/require"
)

func TestParseSize(t *testing.T) {
	str1 := "1024"
	str2 := "1024kb"
	str3 := "1024KB"

	size1, err := strutil.ParseSize(str1)
	require.NoError(t, err)
	require.EqualValues(t, 1024, size1)

	size2, err := strutil.ParseSize(str2)
	require.NoError(t, err)
	require.EqualValues(t, 1024*util.KB, size2)

	size3, err := strutil.ParseSize(str3)
	require.NoError(t, err)
	require.EqualValues(t, 1024*util.KB, size3)
}

func TestFormatSize(t *testing.T) {
	size1 := 1023
	size2 := util.KB
	size3 := util.MB

	str1 := strutil.FormatSize(uint64(size1))
	require.Equal(t, "1023", str1)

	str2 := strutil.FormatSize(uint64(size2))
	require.Equal(t, "1KB", str2)

	str3 := strutil.FormatSize(uint64(size3))
	require.Equal(t, "1MB", str3)
}
