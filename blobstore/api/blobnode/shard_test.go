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

package blobnode

import (
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/stretchr/testify/require"

	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
)

func TestShardStatus(t *testing.T) {
	require.Equal(t, ShardStatusDefault, ShardStatus(0))
	require.Equal(t, ShardStatusNormal, ShardStatus(1))
	require.Equal(t, ShardStatusMarkDelete, ShardStatus(2))

	var err error
	require.Nil(t, convertEIO(err))

	err = syscall.EIO
	require.ErrorIs(t, convertEIO(err), bloberr.ErrDiskBroken)

	err = errors.New("input/output error")
	require.ErrorIs(t, convertEIO(err), bloberr.ErrDiskBroken)
}
