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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
)

func TestService(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	testServiceName := "testService"
	testHostPrefix := "http://127.0.0."
	// test register and get service
	{
		ret, err := testClusterClient.GetService(ctx, clustermgr.GetServiceArgs{Name: testServiceName})
		require.NoError(t, err)
		require.Equal(t, 0, len(ret.Nodes))

		for i := 0; i < 10; i++ {
			node := clustermgr.ServiceNode{
				ClusterID: 1,
				Name:      testServiceName,
				Host:      testHostPrefix + strconv.Itoa(i+1) + ":8080",
				Idc:       "z0",
			}
			err := testClusterClient.RegisterService(ctx, node, 1, 1, 2)
			require.NoError(t, err)
		}

		ret, err = testClusterClient.GetService(ctx, clustermgr.GetServiceArgs{Name: testServiceName})
		require.NoError(t, err)
		require.Equal(t, 10, len(ret.Nodes))

		// failed case ,idc not match
		node := clustermgr.ServiceNode{
			ClusterID: 1,
			Name:      testServiceName,
			Host:      testHostPrefix + strconv.Itoa(1),
			Idc:       "z9",
		}
		err = testClusterClient.RegisterService(ctx, node, 1, 1, 2)
		require.Error(t, err)

		// failed case cluster id not match
		node.Idc = "z0"
		node.ClusterID = 2
		err = testClusterClient.RegisterService(ctx, node, 1, 1, 2)
		require.Error(t, err)

		// failed case,host not invalid
		node.Idc = "z0"
		node.ClusterID = 1
		node.Host = "http://x.0.0.1:8080"
		err = testClusterClient.RegisterService(ctx, node, 1, 1, 2)
		require.Error(t, err)
	}

	// test unregister and get service
	{
		err := testClusterClient.UnregisterService(ctx, clustermgr.UnregisterArgs{Name: testServiceName, Host: testHostPrefix + "1:8080"})
		require.NoError(t, err)

		ret, err := testClusterClient.GetService(ctx, clustermgr.GetServiceArgs{Name: testServiceName})
		require.NoError(t, err)
		require.Equal(t, 9, len(ret.Nodes))

		// failed case
		err = testClusterClient.UnregisterService(ctx, clustermgr.UnregisterArgs{Name: testServiceName, Host: testHostPrefix + "111:8080"})
		require.Error(t, err)
	}

	// test heartbeat service
	{
		err := testClusterClient.UnregisterService(ctx, clustermgr.UnregisterArgs{Name: testServiceName, Host: testHostPrefix + "9:8080"})
		require.NoError(t, err)
	}
}
