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

package metanode_test

import (
	"testing"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func TestDeletedExtentKeyMarshal(t *testing.T) {
	ek := &proto.ExtentKey{
		PartitionId:  1,
		FileOffset:   0,
		ExtentId:     1,
		ExtentOffset: 1,
		Size:         util.MB,
	}
	dek := metanode.NewDeletedExtentKey(ek, 0, 1)
	buf, err := dek.Marshal()
	require.NoError(t, err)
	unmarshalDek := &metanode.DeletedExtentKey{}
	err = unmarshalDek.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, dek, unmarshalDek)
}

func TestDeletedObjExtentKeyMarshal(t *testing.T) {
	oek := &proto.ObjExtentKey{
		Cid:        1,
		FileOffset: 0,
		Size:       util.MB,
		Blobs:      make([]proto.Blob, 0),
	}
	doek := metanode.NewDeletedObjExtentKey(oek, 0, 1)
	buf, err := doek.Marshal()
	require.NoError(t, err)
	unmarshalDoek := &metanode.DeletedObjExtentKey{}
	err = unmarshalDoek.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, doek, unmarshalDoek)
}
