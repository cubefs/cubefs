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

package metanode

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestSortedObjExtentAppend(t *testing.T) {
	se := NewSortedObjExtents()
	err := se.Append(proto.ObjExtentKey{
		Cid:        0,
		FileOffset: 0,
		Size:       1000,
	})
	require.NoError(t, err)
	err = se.Append(proto.ObjExtentKey{
		Cid:        1,
		FileOffset: 1000,
		Size:       1000,
	})
	require.NoError(t, err)
	err = se.Append(proto.ObjExtentKey{
		Cid:        2,
		FileOffset: 2000,
		Size:       1000,
	})
	require.NoError(t, err)
	err = se.Append(proto.ObjExtentKey{
		Cid:        3,
		FileOffset: 3000,
		Size:       500,
	})
	require.NoError(t, err)
	t.Logf("eks: %v\n", se.eks)
	require.EqualValues(t, 3500, se.Size())
	require.EqualValues(t, 4, len(se.eks))
	require.EqualValues(t, 3, se.eks[3].Cid)

	err = se.Append(proto.ObjExtentKey{
		Cid:        4,
		FileOffset: 3000,
		Size:       500,
	})
	require.Error(t, err)
	t.Logf("eks: %v\n", se.eks)
	require.EqualValues(t, 3500, se.Size())
	require.EqualValues(t, 4, len(se.eks))
	require.EqualValues(t, 3, se.eks[3].Cid)
}

func TestSortedObjExtentsMarshal(t *testing.T) {
	se := NewSortedObjExtents()
	e1 := proto.ObjExtentKey{
		Cid:        0,
		FileOffset: 1,
		Size:       1010,
	}
	e2 := proto.ObjExtentKey{
		Cid:        1,
		FileOffset: 4,
		Size:       1030,
	}

	se.eks = append(se.eks, e1)
	se.eks = append(se.eks, e2)

	data, err := se.MarshalBinary()
	require.NoError(t, err)

	se2 := NewSortedObjExtents()
	err = se2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.EqualValues(t, len(se.eks), len(se2.eks))

	for idx := 0; idx < len(se.eks); idx++ {
		e1 := se.eks[idx]
		e2 := se2.eks[idx]
		require.EqualValues(t, e1.Cid, e2.Cid)
		require.EqualValues(t, e1.FileOffset, e2.FileOffset)
		require.EqualValues(t, e1.Size, e2.Size)
	}
}
