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

package core

import (
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestFormatInfo(t *testing.T) {
	formatInfo := &FormatInfo{
		FormatInfoProtectedField: FormatInfoProtectedField{
			DiskID:  proto.DiskID(101),
			Version: 1,
			Format:  FormatMetaTypeV1,
			Ctime:   time.Now().UnixNano(),
		},
	}

	checkSum, err := formatInfo.CalCheckSum()
	require.NoError(t, err)

	formatInfo.CheckSum = checkSum

	ctx := context.Background()

	diskPath, err := ioutil.TempDir(os.TempDir(), "BlobNodeTestFormatInfo")
	require.NoError(t, err)
	defer os.RemoveAll(diskPath)

	err = SaveDiskFormatInfo(ctx, diskPath, formatInfo)
	require.NoError(t, err)

	info, err := ReadFormatInfo(ctx, diskPath)
	require.NoError(t, err)

	require.Equal(t, true, reflect.DeepEqual(*info, *formatInfo))

	formatInfo.Ctime = time.Now().UnixNano()
	err = SaveDiskFormatInfo(ctx, diskPath, formatInfo)
	require.NoError(t, err)

	_, err = ReadFormatInfo(ctx, diskPath)
	require.Error(t, err)
}

func TestEnsureDiskArea(t *testing.T) {
	diskPath := "!!"
	err := EnsureDiskArea(diskPath, "")
	require.Error(t, err)
}
