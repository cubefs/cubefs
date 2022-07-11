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

package workutils

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/recordlog"
)

func TestDroppedBidRecorder(t *testing.T) {
	r := DroppedBidRecorderInst()
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	cfg := recordlog.Config{
		Dir:       dir,
		ChunkBits: 29,
	}
	err = r.Init(&cfg, 1)
	require.NoError(t, err)
	r.Write(context.Background(), 1, 2, "test")

	err = r.Init(&cfg, 1)
	require.NoError(t, err)

	os.RemoveAll("./test/")
}
