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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetIoType(t *testing.T) {
	ctx := context.TODO()

	iotype := GetIoType(ctx)
	require.Equal(t, NormalIO, iotype)

	ctx0 := context.WithValue(ctx, _ioFlowStatKey, CompactIO)
	iotype = GetIoType(ctx0)
	require.Equal(t, CompactIO, iotype)

	ctx1 := context.WithValue(ctx0, _ioFlowStatKey, DiskRepairIO)
	iotype = GetIoType(ctx1)
	require.Equal(t, DiskRepairIO, iotype)
}
