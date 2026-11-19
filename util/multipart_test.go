// Copyright 2023 The CubeFS Authors.
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

package util_test

import (
	"strings"
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

const testMpIDForMultiPart = 1

func TestMultiPartID(t *testing.T) {
	id := util.CreateMultipartID(testMpIDForMultiPart).String()
	pid, found := util.MultipartIDFromString(id).PartitionID()
	require.Equal(t, found, true)
	require.EqualValues(t, pid, testMpIDForMultiPart)
}

func TestMultiPartIDRoundTrip(t *testing.T) {
	for _, mpID := range []uint64{7, 42, 123456, 99999999} {
		id := util.CreateMultipartID(mpID).String()
		pid, found := util.MultipartIDFromString(id).PartitionID()
		require.True(t, found, "mpID %d", mpID)
		require.EqualValues(t, mpID, pid)
	}
}

// The multipart id comes from the uploadId of an S3 request, so a value
// that does not look like one we generated must be reported as "not found"
// (the caller then looks it up on all meta partitions), and must not panic.
func TestMultiPartIDMalformed(t *testing.T) {
	// the trailing 25 characters are: 'x', a 2 digit length, the partition id, padding
	head := strings.Repeat("a", 32)
	for name, id := range map[string]string{
		"empty":                 "",
		"shorter than trailer":  "abc",
		"no delimiter":          head + strings.Repeat("0", 25),
		"delimiter only":        head + "x" + strings.Repeat("0", 24),
		"zero length":           head + "x00" + strings.Repeat("1", 22),
		"length not a number":   head + "xab" + strings.Repeat("1", 22),
		"negative length":       head + "x-9" + strings.Repeat("1", 22),
		"length past the end":   head + "x23" + strings.Repeat("1", 22),
		"length far past":       head + "x99" + strings.Repeat("1", 22),
		"partition id not num":  head + "x05" + "abcde" + strings.Repeat("0", 17),
		"multi-byte characters": strings.Repeat("é", 25),
		"long multi-byte":       strings.Repeat("é", 30),
	} {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, found := util.MultipartIDFromString(id).PartitionID()
				require.False(t, found)
			})
		})
	}
}
