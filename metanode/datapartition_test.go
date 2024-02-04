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

package metanode_test

import (
	"testing"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestVolView(t *testing.T) {
	vol := metanode.NewVol()
	require.NotNil(t, vol)
	dp := vol.GetPartition(1)
	require.Nil(t, dp)
	dp = &metanode.DataPartition{
		PartitionID:   1,
		Status:        proto.ReadWrite,
		ReplicaNum:    3,
		PartitionType: "",
		Hosts:         []string{"192.168.0.1", "192.168.0.2", "192.168.0.3"},
	}
	hosts := dp.GetAllAddrs()
	require.EqualValues(t, "192.168.0.2/192.168.0.3/", hosts)
	dpView := metanode.NewDataPartitionsView()
	dpView.DataPartitions = append(dpView.DataPartitions, dp)
	vol.UpdatePartitions(dpView)

	gotDp := vol.GetPartition(dp.PartitionID)
	require.EqualValues(t, gotDp, dp)
}
