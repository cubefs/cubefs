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
