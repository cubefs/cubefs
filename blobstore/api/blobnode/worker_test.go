// Copyright 2023 The CubeFS Authors.
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
package blobnode

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardPartialRepairRet_Marshal(t *testing.T) {
	ret := &ShardPartialRepairRet{}
	ret.BadIdxes = []int{1, 2, 3}
	ret.Data = []byte("test data")
	marshal, _, err := ret.Marshal()
	require.NoError(t, err)
	require.Equal(t, 29, len(marshal))
	ret = &ShardPartialRepairRet{}
	err = ret.UnmarshalFrom(bytes.NewReader(marshal))
	require.NoError(t, err)
	require.EqualValues(t, []int{1, 2, 3}, ret.BadIdxes)
	require.EqualValues(t, []byte("test data"), ret.Data)

	ret = &ShardPartialRepairRet{}
	ret.Data = []byte("test data")
	marshal, _, err = ret.Marshal()
	require.NoError(t, err)
	require.Equal(t, 17, len(marshal))
	ret = &ShardPartialRepairRet{}
	err = ret.UnmarshalFrom(bytes.NewReader(marshal))
	require.NoError(t, err)
	require.EqualValues(t, []int{}, ret.BadIdxes)
	require.EqualValues(t, []byte("test data"), ret.Data)
}
