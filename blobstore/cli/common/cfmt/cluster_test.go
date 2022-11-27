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
	"math/rand"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestVolumeInfo(t *testing.T) {
	val := clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase{
			Vid:         101313,
			CodeMode:    2,
			Status:      2,
			HealthScore: -1,
			Total:       1 << 50,
			Free:        1 << 32,
			Used:        1 << 45,
		},
		Units: make([]clustermgr.Unit, 32),
	}
	for i := 0; i < len(val.Units); i++ {
		val.Units[i] = clustermgr.Unit{
			Vuid:   proto.Vuid(rand.Uint64()),
			DiskID: proto.DiskID(rand.Uint32()),
			Host:   fmt.Sprintf("host-%d", i),
		}
	}
	printLine()
	for _, line := range cfmt.VolumeInfoF(&val) {
		fmt.Println(line)
	}
	printLine()
	fmt.Println(cfmt.VolumeInfoJoin(&val, "\t--> "))
	printLine()
	fmt.Println(cfmt.VolumeInfoJoin(nil, "\t--> "))
	printLine()
}

func TestClusterInfo(t *testing.T) {
	val := clustermgr.ClusterInfo{
		Region:    "foo-region",
		ClusterID: 0xffff,
		Capacity:  1 << 40,
		Available: 1 << 38,
		Readonly:  false,
		Nodes:     []string{"node-1", "node-2", "node-xxx"},
	}
	printLine()
	for _, line := range cfmt.ClusterInfoF(&val) {
		fmt.Println(line)
	}
	printLine()
	fmt.Println(cfmt.ClusterInfoJoin(&val, "\t--> "))
	printLine()
	fmt.Println(cfmt.ClusterInfoJoin(nil, "\t--> "))
	printLine()
}
