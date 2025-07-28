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

	// nil ctx, should return IOTypeMax/invalid
	iotype := GetIoType(ctx)
	require.Equal(t, IOTypeMax, iotype)

	ctx0 := context.WithValue(ctx, _ioFlowStatKey, BackgroundIO)
	iotype = GetIoType(ctx0)
	require.Equal(t, BackgroundIO, iotype)

	ctx1 := SetIoType(ctx, ReadIO)
	iotype = GetIoType(ctx1)
	require.Equal(t, ReadIO, iotype)

	ctx = SetIoType(ctx, DeleteIO)
	iotype = GetIoType(ctx)
	require.Equal(t, DeleteIO, iotype)
}

func TestIOType_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		ioType   IOType
		expected bool
	}{
		{name: "WriteIO is valid", ioType: WriteIO, expected: true},
		{name: "BackgroundIO is valid", ioType: BackgroundIO, expected: true},
		{name: "ReadIO is valid", ioType: ReadIO, expected: true},
		{name: "DeleteIO is valid", ioType: DeleteIO, expected: true},
		{name: "IOTypeMax is invalid", ioType: IOTypeMax, expected: false},
		{name: "IOTypeMax+1 is invalid", ioType: IOTypeMax + 1, expected: false},
		{name: "IOTypeMax-1 is valid", ioType: IOTypeMax - 1, expected: true},
		{name: "Zero value is valid (WriteIO)", ioType: 0, expected: true},
		{name: "Large value is invalid", ioType: 100, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ioType.IsValid()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIOType_String(t *testing.T) {
	tests := []struct {
		name     string
		ioType   IOType
		expected string
	}{
		{name: "WriteIO string", ioType: WriteIO, expected: "write"},
		{name: "BackgroundIO string", ioType: BackgroundIO, expected: "background"},
		{name: "ReadIO string", ioType: ReadIO, expected: "read"},
		{name: "DeleteIO string", ioType: DeleteIO, expected: "delete"},
		{name: "IOTypeMax string (should panic or return empty)", ioType: IOTypeMax, expected: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.ioType >= IOTypeMax {
				// Test that it doesn't panic
				require.Panics(t, func() {
					_ = tt.ioType.String()
				})
			} else {
				result := tt.ioType.String()
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIOtypemap(t *testing.T) {
	t.Run("ioTypeArray contains correct values", func(t *testing.T) {
		require.Len(t, ioTypeArray, int(IOTypeMax))
		require.Equal(t, "write", ioTypeArray[WriteIO])
		require.Equal(t, "background", ioTypeArray[BackgroundIO])
		require.Equal(t, "read", ioTypeArray[ReadIO])
		require.Equal(t, "delete", ioTypeArray[DeleteIO])
	})

	t.Run("ioTypeArray index validation", func(t *testing.T) {
		// Test that all valid IO types have corresponding strings
		for i := WriteIO; i < IOTypeMax; i++ {
			require.NotEmpty(t, ioTypeArray[i])
		}
	})
}

func TestRevertIOtypeMap(t *testing.T) {
	t.Run("revertIOMap contains correct values", func(t *testing.T) {
		require.Len(t, revertIOMap, int(IOTypeMax))
		require.Equal(t, revertIOMap["write"], WriteIO)
		require.Equal(t, revertIOMap["background"], BackgroundIO)
		require.Equal(t, revertIOMap["read"], ReadIO)
		require.Equal(t, revertIOMap["delete"], DeleteIO)

		require.Equal(t, revertIOMap[WriteIO.String()], WriteIO)
		require.Equal(t, revertIOMap[BackgroundIO.String()], BackgroundIO)
		require.Equal(t, revertIOMap[ReadIO.String()], ReadIO)
		require.Equal(t, revertIOMap[DeleteIO.String()], DeleteIO)
	})
}
