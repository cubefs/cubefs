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
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestChunkInfo(t *testing.T) {
	val := blobnode.ChunkInfo{
		Id:         blobnode.NewChunkId(4191518780817409),
		Vuid:       4191518780817409,
		DiskID:     0xff,
		Total:      1 << 35,
		Used:       1 << 34,
		Free:       100,
		Size:       1 << 32,
		Status:     blobnode.ChunkStatusReadOnly,
		Compacting: false,
	}
	printLine()
	for _, line := range cfmt.ChunkInfoF(&val) {
		fmt.Println(line)
	}
	printLine()
	fmt.Println(cfmt.ChunkInfoJoin(&val, "\t--> "))
	printLine()
	fmt.Println(cfmt.ChunkInfoJoin(nil, "\t--> "))
	printLine()
}

func TestDiskHeartBeatInfo(t *testing.T) {
	val := blobnode.DiskHeartBeatInfo{
		DiskID:       12342522,
		Used:         10220055555,
		Free:         9483500,
		Size:         10 * (1 << 40),
		MaxChunkCnt:  9999,
		FreeChunkCnt: 880,
		UsedChunkCnt: 82,
	}
	printLine()
	for _, line := range cfmt.DiskHeartBeatInfoF(&val) {
		fmt.Println(line)
	}
	printLine()
	fmt.Println(cfmt.DiskHeartBeatInfoJoin(&val, "\t--> "))
	printLine()
	fmt.Println(cfmt.DiskHeartBeatInfoJoin(nil, "\t--> "))
	printLine()
}

func TestDiskInfo(t *testing.T) {
	val := blobnode.DiskInfo{
		ClusterID:    0xf2,
		Idc:          "idc--xx",
		Rack:         "rack-10",
		Host:         "http://255.255.255.255:99999",
		Path:         "/path/to/disk",
		Status:       proto.DiskStatusRepaired,
		Readonly:     false,
		CreateAt:     time.Now().Add(-time.Hour * 1020),
		LastUpdateAt: time.Now().Add(-time.Hour * 224),
		DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{
			DiskID:       12342522,
			Used:         10220055555,
			Free:         9483500,
			Size:         10 * (1 << 40),
			MaxChunkCnt:  9999,
			FreeChunkCnt: 880,
			UsedChunkCnt: 82,
		},
	}
	printLine()
	for _, line := range cfmt.DiskInfoF(&val) {
		fmt.Println(line)
	}
	printLine()
	fmt.Println(cfmt.DiskInfoJoin(&val, "\t--> "))
	printLine()
	fmt.Println(cfmt.DiskInfoJoin(nil, "\t--> "))
	printLine()

	for _, line := range cfmt.DiskInfoFV(&val) {
		fmt.Println(line)
	}
	printLine()
	fmt.Println(cfmt.DiskInfoJoinV(&val, "\t--> "))
	printLine()
	fmt.Println(cfmt.DiskInfoJoinV(nil, "\t--> "))
	printLine()
}
