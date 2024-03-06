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

	"github.com/stretchr/testify/require"
)

func TestInodeMarshal(t *testing.T) {
	inode := NewInode(1, FileModeType)
	buf, err := inode.Marshal()
	require.NoError(t, err)
	unmarshalInode := &Inode{}
	err = unmarshalInode.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, inode, unmarshalInode)
}
