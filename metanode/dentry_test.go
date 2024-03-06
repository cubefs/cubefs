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
	"github.com/stretchr/testify/require"
)

func TestDentryMarshal(t *testing.T) {
	dentry := &metanode.Dentry{
		ParentId: 123,
		Name:     "dentry_test",
		Inode:    16797219,
		Type:     2147484141,
	}
	buf, err := dentry.Marshal()
	require.NoError(t, err)
	unmarshalDentry := &metanode.Dentry{}
	err = unmarshalDentry.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, dentry, unmarshalDentry)
}
