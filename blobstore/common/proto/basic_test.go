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

package proto_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestProtoDiskID(t *testing.T) {
	id := proto.DiskID(9)
	bytes := []byte{0x00, 0x00, 0x00, 0x09}
	require.Equal(t, uint32(9), uint32(id))
	require.Equal(t, bytes, id.Encode())
	require.Equal(t, "9", id.ToString())

	var dec proto.DiskID
	require.Equal(t, id, dec.Decode(bytes))
	require.Equal(t, id, dec)
}

func TestProtoVID(t *testing.T) {
	id := proto.Vid(123)
	require.Equal(t, "123", id.ToString())
}

func TestProtoClusterID(t *testing.T) {
	id := proto.ClusterID(10)
	require.Equal(t, "10", id.ToString())
}

func TestProtoToken(t *testing.T) {
	host := "127.0.0.1:80"
	vid := proto.Vid(123)
	token := "127.0.0.1:80;123"
	require.Equal(t, token, proto.EncodeToken(host, vid))
	{
		newHost, newVid, err := proto.DecodeToken(token)
		require.NoError(t, err)
		require.Equal(t, host, newHost)
		require.Equal(t, vid, newVid)
	}
	{
		_, _, err := proto.DecodeToken(host)
		require.Error(t, err)
	}
	{
		_, _, err := proto.DecodeToken(token + ";")
		require.Error(t, err)
	}
}
