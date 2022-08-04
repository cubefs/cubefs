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

package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardRepairMsg_IsValid(t *testing.T) {
	for _, cs := range []struct {
		msg ShardRepairMsg
		ok  bool
	}{
		{ShardRepairMsg{1, 1, 1, []uint8{1}, 0, "access", ""}, true},
		{ShardRepairMsg{1, 0, 1, []uint8{1}, 0, "access", ""}, false},
		{ShardRepairMsg{1, 1, 0, []uint8{1}, 0, "access", ""}, false},
		{ShardRepairMsg{1, 1, 1, []uint8{}, 0, "access", ""}, false},
	} {
		require.Equal(t, cs.ok, cs.msg.IsValid())
	}
}

func TestDeleteMsg_IsValid(t *testing.T) {
	msg := DeleteMsg{ClusterID: 1, Bid: 1, Vid: 1}
	require.Equal(t, true, msg.IsValid())

	msg = DeleteMsg{ClusterID: 1, Vid: 1}
	require.Equal(t, false, msg.IsValid())

	msg = DeleteMsg{ClusterID: 1, Bid: 1}
	require.Equal(t, false, msg.IsValid())
}

func TestMsgMarshal(t *testing.T) {
	stags := BlobDeleteStage{}
	stags.SetStage(1, MarkDelStage)
	stags.SetStage(2, MarkDelStage)
	require.Equal(t, stags, stags.Copy())

	vuid, _ := NewVuid(1, 2, 1)
	sg, exist := stags.Stage(vuid)
	require.True(t, exist)
	require.Equal(t, MarkDelStage, sg)
	vuid, _ = NewVuid(1, 10, 1)
	_, exist = stags.Stage(vuid)
	require.False(t, exist)

	msg := DeleteMsg{ClusterID: 1, Bid: 1}
	msg.SetDeleteStage(stags)
	b, err := json.Marshal(msg)
	require.NoError(t, err)

	var delMsg DeleteMsg
	err = json.Unmarshal(b, &delMsg)
	require.NoError(t, err)
	require.Equal(t, msg, delMsg)
}
