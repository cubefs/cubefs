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

package proto_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/stretchr/testify/require"
)

func TestDelExtentParam(t *testing.T) {
	eks := make([]*proto.ExtentKey, 0)
	eks = append(eks, &proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
	})
	v, err := json.Marshal(eks)
	require.NoError(t, err)

	ekParams := make([]*proto.DelExtentParam, 0)
	err = json.Unmarshal(v, &ekParams)
	require.NoError(t, err)

	require.EqualValues(t, 1, len(ekParams))
	require.EqualValues(t, 1, ekParams[0].PartitionId)
	require.EqualValues(t, 1, ekParams[0].ExtentId)
	require.False(t, ekParams[0].IsSnapshotDeletion)

	ekParams[0].IsSnapshotDeletion = true

	v, err = json.Marshal(ekParams)
	require.NoError(t, err)

	err = json.Unmarshal(v, &eks)
	require.NoError(t, err)

	require.EqualValues(t, 1, len(eks))
	require.EqualValues(t, 1, eks[0].PartitionId)
	require.EqualValues(t, 1, eks[0].ExtentId)
}

func TestEkMarshalBinary(t *testing.T) {
	ek := &proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
		Size:        1024,
		CRC:         1001,
		SnapInfo: &proto.ExtSnapInfo{
			IsSplit: true,
		},
	}

	tmpBuf := buf.NewByteBufEx(1024)
	ek.MarshalBinary(tmpBuf, true)

	ek2 := &proto.ExtentKey{}
	buf2 := buf.NewReadByteBuf()
	buf2.SetData(tmpBuf.Bytes())
	err := ek2.UnmarshalBinary(buf2, true)
	if err != nil || !reflect.DeepEqual(ek, ek2) {
		t.Fail()
	}
}

func BenchmarkExtentMarshalBinary(b *testing.B) {
	ek := &proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
	}
	b.ReportAllocs()

	buf := buf.NewByteBufEx(1024)
	for i := 0; i < b.N; i++ {
		ek.MarshalBinary(buf, false)
	}
}

func TestExtentKeyCompitable(t *testing.T) {
	// data marshal by version 3.5.0
	oldData := []byte{0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 1}

	ek := &proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
		Size:        1024,
		FileOffset:  2048,
		SnapInfo: &proto.ExtSnapInfo{
			IsSplit: true,
			VerSeq:  1024,
		},
	}

	buff := buf.NewByteBufEx(40)
	err := ek.MarshalBinary(buff, true)
	if err != nil || !bytes.Equal(oldData, buff.Bytes()) {
		t.Fail()
	}
}
