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

package stream

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
)

func newReader(size int) io.Reader {
	buff := make([]byte, size)
	rand.Read(buff)
	return bytes.NewReader(buff)
}

func TestAccessStreamConfig(t *testing.T) {
	cfg := StreamConfig{
		IDC:                idcOther,
		MemPoolSizeClasses: map[int]int{1024: 1},
		CodeModesPutQuorums: map[codemode.CodeMode]int{
			codemode.EC15P12:  16,
			codemode.EC6P10L2: 18,
		},
		ClusterConfig: controller.ClusterConfig{
			ConsulAgentAddr: "http://127.0.0.1:8500",
		},
	}
	err := confCheck(&cfg)

	require.NoError(t, err)
	require.Equal(t, idcOther, cfg.IDC)
	require.Equal(t, map[int]int{1024: 1}, cfg.MemPoolSizeClasses)
	require.Equal(t, defaultDiskPunishIntervalS, cfg.DiskPunishIntervalS)

	cfg = StreamConfig{
		IDC:                idcOther,
		MemPoolSizeClasses: map[int]int{1024: 1},
		CodeModesPutQuorums: map[codemode.CodeMode]int{
			codemode.EC15P12:  16,
			codemode.EC6P10L2: 18,
		},
		ClusterConfig: controller.ClusterConfig{
			Clusters: []controller.Cluster{
				{ClusterID: 1, Hosts: []string{"host1"}},
				{ClusterID: 2, Hosts: []string{"host2"}},
			},
		},
	}
	err = confCheck(&cfg)
	require.NoError(t, err)
}

func TestAccessStreamNew(t *testing.T) {
	require.Equal(t, idc, streamer.IDC)

	_, err := NewStreamHandler(&StreamConfig{IDC: "idc"}, nil)
	require.NotNil(t, err)
}

func TestAccessStreamDelete(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamDelete")
	size := 1 << 18
	loc, err := streamer.Put(ctx(), newReader(size), int64(size), nil)
	require.NoError(t, err)

	err = streamer.Delete(ctx(), loc)
	require.NoError(t, err)

	dataShards.clean()
}

func TestAccessStreamAdmin(t *testing.T) {
	{
		handler := Handler{}
		sa := handler.Admin()
		require.NotNil(t, sa)

		admin := sa.(*StreamAdmin)
		require.Nil(t, admin.MemPool)
		require.Nil(t, admin.Controller)
	}
	{
		sa := streamer.Admin()
		require.NotNil(t, sa)

		admin := sa.(*StreamAdmin)
		require.NotNil(t, admin.MemPool)
		t.Log("mempool status:", admin.MemPool.Status())

		ctr := admin.Controller
		require.NotNil(t, ctr)
		t.Log("region:", ctr.Region())
		require.Error(t, ctr.ChangeChooseAlg(controller.AlgChoose(100)))
		require.NoError(t, ctr.ChangeChooseAlg(controller.AlgRandom))
		require.NoError(t, ctr.ChangeChooseAlg(controller.AlgAvailable))
	}
}
