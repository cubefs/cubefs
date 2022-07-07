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

package cfmt_test

import (
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
)

func TestParseLocation(t *testing.T) {
	loc := access.Location{
		ClusterID: 1,
		CodeMode:  3,
		Size:      19213422425,
		BlobSize:  1 << 22,
		Crc:       1 << 31,
		Blobs: []access.SliceInfo{
			{MinBid: 0x199, Vid: 100020, Count: 0},
			{MinBid: 10, Vid: 20, Count: 300},
			{MinBid: 14225224, Vid: 0xffffffff, Count: 1 << 31},
		},
	}

	locx, err := cfmt.ParseLocation("")
	require.Error(t, err)

	_, err = cfmt.ParseLocation("xxxx")
	require.Error(t, err)

	locx, err = cfmt.ParseLocation("{}")
	require.NoError(t, err)
	require.Equal(t, access.Location{}, locx)

	locx, err = cfmt.ParseLocation(loc.ToString())
	require.NoError(t, err)
	require.Equal(t, loc, locx)

	locx, err = cfmt.ParseLocation(loc.Base64String())
	require.NoError(t, err)
	require.Equal(t, loc, locx)

	b, _ := common.Marshal(loc)
	locx, err = cfmt.ParseLocation(string(b))
	require.NoError(t, err)
	require.Equal(t, loc, locx)
}

func TestLocation(t *testing.T) {
	loc := access.Location{
		ClusterID: 1,
		CodeMode:  3,
		Size:      19213422425,
		BlobSize:  1 << 22,
		Crc:       1 << 31,
		Blobs: []access.SliceInfo{
			{MinBid: 0x199, Vid: 100020, Count: 0},
			{MinBid: 10, Vid: 20, Count: 300},
			{MinBid: 14225224, Vid: 0xffffffff, Count: 1 << 31},
		},
	}
	printLine()
	for _, line := range cfmt.LocationF(&loc) {
		fmt.Println(line)
	}
	printLine()
	fmt.Println(cfmt.LocationJoin(&loc, "\t-->\t"))
	printLine()
	fmt.Println(cfmt.LocationJoin(nil, "\t--> "))
	printLine()
}

func TestHashSumMap(t *testing.T) {
	hashes := make(access.HashSumMap)
	algs := access.HashAlgorithm(0xff - 1)
	for alg := range algs.ToHashSumMap() {
		hasher := alg.ToHasher()
		buf := make([]byte, 1024)
		rand.Read(buf)
		hasher.Write(buf)
		hashes[alg] = hasher.Sum(nil)
	}
	printLine()
	for _, printLine := range cfmt.HashSumMapF(hashes) {
		fmt.Println(printLine)
	}
	printLine()
	fmt.Println(cfmt.HashSumMapJoin(hashes, "xxx | --> "))
	printLine()
}

func printLine() {
	fmt.Println(strings.Repeat("-", 100))
}
