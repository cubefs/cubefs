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
	str4 := "0PB"

	size1, err := strutil.ParseSize(str1)
	require.NoError(t, err)
	require.EqualValues(t, 1024, size1)

	size2, err := strutil.ParseSize(str2)
	require.NoError(t, err)
	require.EqualValues(t, 1024*util.KB, size2)

	size3, err := strutil.ParseSize(str3)
	require.NoError(t, err)
	require.EqualValues(t, 1024*util.KB, size3)

	size4, err := strutil.ParseSize(str4)
	require.NoError(t, err)
	require.EqualValues(t, 0, size4)
}

func TestFormatSize(t *testing.T) {
	size1 := 1023
	size2 := util.KB
	size3 := util.MB
	size4 := 0

	str1 := strutil.FormatSize(uint64(size1))
	require.Equal(t, "1023", str1)

	str2 := strutil.FormatSize(uint64(size2))
	require.Equal(t, "1KB", str2)

	str3 := strutil.FormatSize(uint64(size3))
	require.Equal(t, "1MB", str3)

	str4 := strutil.FormatSize(uint64(size4))
	require.Equal(t, "0", str4)
}

func TestParsePercent(t *testing.T) {
	str1 := "1%"
	str2 := "1"
	str3 := "0"
	str4 := "0%"

	v1, err := strutil.ParsePercent(str1)
	require.NoError(t, err)
	require.EqualValues(t, 0.01, v1)

	v2, err := strutil.ParsePercent(str2)
	require.NoError(t, err)
	require.EqualValues(t, 1, v2)

	v3, err := strutil.ParsePercent(str3)
	require.NoError(t, err)
	require.EqualValues(t, 0, v3)

	v4, err := strutil.ParsePercent(str4)
	require.NoError(t, err)
	require.EqualValues(t, 0, v4)
}

func TestFormatPercent(t *testing.T) {
	v1 := 0.01
	v2 := 1
	v3 := 0

	str1 := strutil.FormatPercent(v1)
	require.EqualValues(t, "1%", str1)

	str2 := strutil.FormatPercent(float64(v2))
	require.EqualValues(t, "100%", str2)

	str3 := strutil.FormatPercent(float64(v3))
	require.EqualValues(t, "0%", str3)
}
