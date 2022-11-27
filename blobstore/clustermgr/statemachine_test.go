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

package clustermgr

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
)

func TestStateMachine(t *testing.T) {
	srcService, srcClean := initTestService(t)
	defer srcClean()
	srcClusterClient := initTestClusterClient(srcService)

	// change another listen port
	oldPort := testServiceCfg.RaftConfig.ServerConfig.ListenPort
	oldMembers := testServiceCfg.RaftConfig.ServerConfig.Members
	testServiceCfg.RaftConfig.ServerConfig.ListenPort = GetFreePort()
	testServiceCfg.RaftConfig.ServerConfig.Members = []raftserver.Member{
		{NodeID: 1, Host: "127.0.0.1:65342", Learner: false},
	}
	testServiceCfg.CodeModePolicies = []codemode.Policy{
		{
			ModeName:  codemode.EC15P12.Name(),
			MinSize:   1048577,
			MaxSize:   1073741824,
			SizeRatio: 0.8,
			Enable:    false,
		},
		{
			ModeName:  codemode.EC6P6.Name(),
			MinSize:   0,
			MaxSize:   1048576,
			SizeRatio: 0.2,
			Enable:    false,
		},
	}

	destService, destClean := initTestService(t)
	defer destClean()
	testServiceCfg.RaftConfig.ServerConfig.ListenPort = oldPort
	testServiceCfg.RaftConfig.ServerConfig.Members = oldMembers

	insertDiskInfos(t, srcClusterClient, 1, 10, "z0")

	// test snapshot
	{
		snapshot, err := srcService.Snapshot()
		require.NoError(t, err)
		err = destService.ApplySnapshot(raftserver.SnapshotMeta{Index: snapshot.Index()}, snapshot)
		require.NoError(t, err)
	}
}
