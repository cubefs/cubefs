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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestBlobnode(t *testing.T) {
	any := gomock.Any()
	ctx := context.Background()

	cli := NewBlobnodeClient(&api.Config{}).(*blobnodeClient)
	client := mocks.NewMockStorageAPI(gomock.NewController(t))
	client.EXPECT().MarkDeleteShard(any, any, any).Return(nil)
	client.EXPECT().DeleteShard(any, any, any).Return(nil)
	cli.client = client

	err := cli.MarkDelete(ctx, proto.VunitLocation{}, proto.BlobID(1))
	require.NoError(t, err)

	err = cli.Delete(ctx, proto.VunitLocation{}, proto.BlobID(1))
	require.NoError(t, err)
}
