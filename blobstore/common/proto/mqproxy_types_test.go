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
	msg := ShardRepairMsg{
		ClusterID: 1,
		Bid:       1,
		Vid:       1,
		BadIdx:    []uint8{1},
		Retry:     0,
		Reason:    "access",
	}
	require.Equal(t, true, msg.IsValid())

	msg = ShardRepairMsg{
		ClusterID: 1,
		Bid:       0,
		Vid:       1,
		BadIdx:    []uint8{1},
		Retry:     0,
		Reason:    "access",
	}
	require.Equal(t, false, msg.IsValid())

	msg = ShardRepairMsg{
		ClusterID: 1,
		Bid:       1,
		Vid:       0,
		BadIdx:    []uint8{1},
		Retry:     0,
		Reason:    "access",
	}
	require.Equal(t, false, msg.IsValid())

	msg = ShardRepairMsg{
		ClusterID: 1,
		Bid:       1,
		Vid:       1,
		BadIdx:    []uint8{},
		Retry:     0,
		Reason:    "access",
	}
	require.Equal(t, false, msg.IsValid())
}

func TestDeleteMsg_IsValid(t *testing.T) {
	msg := DeleteMsg{
		ClusterID: 1,
		Bid:       1,
		Vid:       1,
		Retry:     0,
		Time:      0,
	}
	require.Equal(t, true, msg.IsValid())

	msg = DeleteMsg{
		ClusterID: 1,
		Bid:       0,
		Vid:       1,
		Retry:     0,
		Time:      0,
	}
	require.Equal(t, false, msg.IsValid())

	msg = DeleteMsg{
		ClusterID: 1,
		Bid:       1,
		Vid:       0,
		Retry:     0,
		Time:      0,
	}
	require.Equal(t, false, msg.IsValid())
}

func TestMsgMarshal(t *testing.T) {
	stags := BlobDeleteStage{}
	stags.SetStage(1, 1)
	stags.SetStage(2, 1)
	msg := DeleteMsg{
		ClusterID:     1,
		Bid:           1,
		Vid:           0,
		Retry:         0,
		Time:          0,
		BlobDelStages: stags,
	}
	b, err := json.Marshal(msg)
	require.NoError(t, err)

	var delMsg DeleteMsg
	err = json.Unmarshal(b, &delMsg)
	require.NoError(t, err)
	t.Logf("del msg %+v", delMsg)
}
