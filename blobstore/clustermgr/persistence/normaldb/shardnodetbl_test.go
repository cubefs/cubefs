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

package normaldb

import (
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var snnr1 = ShardNodeInfoRecord{
	NodeInfoRecord: NodeInfoRecord{
		Version:   NodeInfoVersionNormal,
		NodeID:    proto.NodeID(1),
		ClusterID: proto.ClusterID(1),
		Idc:       "z0",
		Rack:      "rack1",
		Host:      "127.0.0.1",
		Status:    proto.NodeStatusNormal,
		Role:      proto.NodeRoleShardNode,
		DiskType:  proto.DiskTypeNVMeSSD,
	},
	RaftHost: "127.0.0.1:10011",
}

var snnr2 = ShardNodeInfoRecord{
	NodeInfoRecord: NodeInfoRecord{
		Version:   NodeInfoVersionNormal,
		NodeID:    proto.NodeID(2),
		ClusterID: proto.ClusterID(1),
		Idc:       "z0",
		Rack:      "rack2",
		Host:      "127.0.0.2",
		Status:    proto.NodeStatusNormal,
		Role:      proto.NodeRoleShardNode,
		DiskType:  proto.DiskTypeNVMeSSD,
	},
	RaftHost: "127.0.0.1:10012",
}

func TestShardNodeTbl(t *testing.T) {
	tmpDBPath := path.Join(os.TempDir(), "normaldb", uuid.NewString()) + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	nodeTbl, err := OpenShardNodeTable(db)
	require.NoError(t, err)

	// get all node/ add node / delete node
	nodeList, err := nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 0, len(nodeList))

	err = nodeTbl.UpdateNode(&snnr1)
	require.NoError(t, err)

	err = nodeTbl.UpdateNode(&snnr2)
	require.NoError(t, err)

	nodeList, err = nodeTbl.GetAllNodes()
	require.NoError(t, err)
	require.Equal(t, 2, len(nodeList))
}
