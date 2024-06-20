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

func TestProtoDiskStatus(t *testing.T) {
	for st := proto.DiskStatusNormal; st < proto.DiskStatusMax; st++ {
		require.True(t, st.IsValid())
		t.Logf("disk st %d -> %s", st, st)
	}
	st := proto.DiskStatus(0xff)
	require.False(t, st.IsValid())
	t.Logf("disk st %d -> %s", st, st)
}

func TestProtoVolumeStatus(t *testing.T) {
	for st := proto.VolumeStatusIdle; st <= proto.VolumeStatusUnlocking; st++ {
		require.True(t, st.IsValid())
		t.Logf("volume st %d -> %s", st, st)
	}
	st := proto.VolumeStatus(0xff)
	require.False(t, st.IsValid())
	t.Logf("volume st %d -> %s", st, st)
}

func TestProtoDiskType(t *testing.T) {
	for diskType := proto.DiskTypeHDD; diskType < proto.DiskTypeMax; diskType++ {
		require.True(t, diskType.IsValid())
		t.Logf("disk type %d -> %s", diskType, diskType)
	}
	diskType := proto.DiskType(0xff)
	require.False(t, diskType.IsValid())
	t.Logf("disk type %d -> %s", diskType, diskType)
}

func TestProtoNodeRole(t *testing.T) {
	for nodeRole := proto.NodeRoleBlobNode; nodeRole < proto.NodeRoleMax; nodeRole++ {
		require.True(t, nodeRole.IsValid())
		t.Logf("node role %d -> %s", nodeRole, nodeRole)
	}
	nodeRole := proto.NodeRole(0xff)
	require.False(t, nodeRole.IsValid())
	t.Logf("node role %d -> %s", nodeRole, nodeRole)
}
