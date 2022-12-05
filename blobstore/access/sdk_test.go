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

package access

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

func TestAccessSDKNew(t *testing.T) {
	_, err := NewSDK(StreamConfig{})
	require.Error(t, err)
	_, err = NewSDK(StreamConfig{IDC: idc})
	require.Error(t, err)
	config := StreamConfig{IDC: idc}
	config.ClusterConfig.ConsulAgentAddr = "localhost:xxx"
	_, err = NewSDK(config)
	require.Error(t, err)
}

func TestAccessSDKPut(t *testing.T) {
	var cli SDK = &sdkStream{handler: newService().streamHandler}
	{
		_, err := cli.Put(ctx, nil, 0, nil)
		assertErrorCode(t, 500, err)
	}
	{
		body := bytes.NewReader(make([]byte, 1024))
		_, err := cli.Put(ctx, body, 1024, nil)
		require.NoError(t, err)
	}
}

func TestAccessSDKGet(t *testing.T) {
	var cli SDK = &sdkStream{handler: newService().streamHandler}
	w := bytes.NewBuffer(make([]byte, 2048))
	loc := location.Copy()
	{
		err := cli.Get(ctx, w, &loc, 10, 0)
		require.Error(t, err)
		require.Equal(t, 400, rpc.DetectStatusCode(err))
	}
	{
		loc.Size = 1023
		fillCrc(&loc)
		err := cli.Get(ctx, w, &loc, 1023, 0)
		require.Error(t, err)
		require.Equal(t, 500, rpc.DetectStatusCode(err))
	}
	{
		loc.Size = 1024
		fillCrc(&loc)
		err := cli.Get(ctx, w, &loc, 1024, 0)
		require.NoError(t, err)
		require.Equal(t, 200, rpc.DetectStatusCode(err))
	}
}

func TestAccessSDKDelete(t *testing.T) {
	var cli SDK = &sdkStream{handler: newService().streamHandler}
	loc := location.Copy()
	{
		err := cli.Delete(ctx, &access.Location{})
		require.Error(t, err)
		require.Equal(t, 400, rpc.DetectStatusCode(err))
	}
	{
		loc.Size = 1023
		fillCrc(&loc)
		err := cli.Delete(ctx, &loc)
		require.Error(t, err)
		require.Equal(t, 500, rpc.DetectStatusCode(err))
	}
	{
		loc.Size = 1024
		fillCrc(&loc)
		err := cli.Delete(ctx, &loc)
		require.NoError(t, err)
		require.Equal(t, 200, rpc.DetectStatusCode(err))
	}
}
