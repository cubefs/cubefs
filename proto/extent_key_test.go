// Copyright 2024 The CubeFS Authors.
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
	"encoding/json"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestDelExtentParam(t *testing.T) {
	eks := make([]*proto.ExtentKey, 0)
	eks = append(eks, &proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
	})
	v, err := json.Marshal(eks)
	require.NoError(t, err)

	ekParams := make([]*proto.DelExtentParam, 0)
	err = json.Unmarshal(v, &ekParams)
	require.NoError(t, err)

	require.EqualValues(t, 1, len(ekParams))
	require.EqualValues(t, 1, ekParams[0].PartitionId)
	require.EqualValues(t, 1, ekParams[0].ExtentId)
	require.False(t, ekParams[0].IsSnapshotDeletion)

	ekParams[0].IsSnapshotDeletion = true

	v, err = json.Marshal(ekParams)
	require.NoError(t, err)

	err = json.Unmarshal(v, &eks)
	require.NoError(t, err)

	require.EqualValues(t, 1, len(eks))
	require.EqualValues(t, 1, eks[0].PartitionId)
	require.EqualValues(t, 1, eks[0].ExtentId)
}
