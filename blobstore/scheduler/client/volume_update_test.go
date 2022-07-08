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

package client

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestVolumeUpdate(t *testing.T) {
	any := gomock.Any()
	errMock := errors.New("fake error")
	cli := NewVolumeUpdater(&api.Config{}, "127.0.0.1:xxx").(*volumeUpdater)

	ctx := context.Background()
	schedulerCli := mocks.NewMockIScheduler(gomock.NewController(t))
	schedulerCli.EXPECT().UpdateVol(any, any, any).Return(nil)
	cli.Client = schedulerCli

	err := cli.UpdateLeaderVolumeCache(ctx, proto.Vid(1))
	require.NoError(t, err)

	schedulerCli.EXPECT().UpdateVol(any, any, any).Return(errMock)
	err = cli.UpdateFollowerVolumeCache(ctx, "127.0.0.1:xxx", proto.Vid(1))
	require.True(t, errors.Is(err, errMock))
}
