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

package base

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

func TestSubtraction(t *testing.T) {
	fromCm := []proto.Vuid{1, 2, 3, 4, 5}
	fromDb := []proto.Vuid{1, 2, 3}
	remain := Subtraction(fromCm, fromDb)
	require.Equal(t, []proto.Vuid{4, 5}, remain)
}

func TestHumanRead(t *testing.T) {
	bytesCntFormat(0)
	bytesCntFormat((1 << 60) + 1)
}

func TestDataMountBytePrintEx(t *testing.T) {
	dataMountBytes := [counter.SLOT]int{
		(1 << 60) + 1,
		2445979449, 2363491431, 2122318836,
		4341106710, 3521119887, 3681901617,
		3697979790, 4244637672, 3424650849,
		3279947292, 2675967228, 3624579435,
		4855608246, 5161093533, 5064624495,
		4855608246, 4984233630, 512, 0,
	}
	DataMountFormat(dataMountBytes)
}

func TestLoopExecUntilSuccess(t *testing.T) {
	InsistOn(context.Background(), "test", func() error {
		return nil
	})
}

func TestShouldAllocAndRedo(t *testing.T) {
	err := errcode.ErrNewVuidNotMatch
	code := rpc.DetectStatusCode(err)
	redo := ShouldAllocAndRedo(code)
	require.Equal(t, true, redo)
}

func TestAllocVunitSafe(t *testing.T) {
	ctx := context.Background()
	errMock := errors.New("fake error")
	volumeAllocClient := NewMockAllocVunit(gomock.NewController(t))
	vuid1, _ := proto.NewVuid(1, 1, 1)
	volumeAllocClient.EXPECT().AllocVolumeUnit(gomock.Any(), gomock.Any()).Return(nil, errMock)
	_, err := AllocVunitSafe(ctx, volumeAllocClient, vuid1, nil)
	require.True(t, errors.Is(err, errMock))

	vuid2, _ := proto.NewVuid(1, 1, 2)
	vuid3, _ := proto.NewVuid(1, 2, 1)
	volumeAllocClient.EXPECT().AllocVolumeUnit(gomock.Any(), gomock.Any()).Return(&client.AllocVunitInfo{VunitLocation: proto.VunitLocation{Vuid: vuid2, DiskID: proto.DiskID(1)}}, nil)
	allocVunit, err := AllocVunitSafe(ctx, volumeAllocClient, vuid1, []proto.VunitLocation{{Vuid: vuid2, DiskID: proto.DiskID(1)}, {Vuid: vuid3, DiskID: proto.DiskID(2)}})
	require.NoError(t, err)
	require.Equal(t, vuid2, allocVunit.Vuid)

	volumeAllocClient.EXPECT().AllocVolumeUnit(gomock.Any(), gomock.Any()).Return(&client.AllocVunitInfo{VunitLocation: proto.VunitLocation{Vuid: vuid2, DiskID: proto.DiskID(2)}}, nil)
	require.Panics(t, func() {
		AllocVunitSafe(ctx, volumeAllocClient, vuid1, []proto.VunitLocation{{Vuid: vuid2, DiskID: proto.DiskID(1)}, {Vuid: vuid3, DiskID: proto.DiskID(2)}})
	})
}

func TestGenTaskID(t *testing.T) {
	prefix := "test-"
	id := GenTaskID(prefix, proto.Vid(1))
	require.True(t, strings.HasPrefix(id, prefix))
}
