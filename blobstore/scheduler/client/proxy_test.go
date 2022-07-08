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

package client

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestMQProxy(t *testing.T) {
	mqcli := mocks.NewMockProxyLbRpcClient(gomock.NewController(t))
	mqcli.EXPECT().SendShardRepairMsg(gomock.Any(), gomock.Any()).Return(nil)
	cli := &proxyClient{
		client:    mqcli,
		clusterID: 1,
	}
	err := cli.SendShardRepairMsg(context.Background(), 0, 0, []uint8{0})
	require.NoError(t, err)
}
