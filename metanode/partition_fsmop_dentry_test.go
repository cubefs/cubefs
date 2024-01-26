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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
)

func TestOpEntry_RemoveDupReq(t *testing.T) {

	os.RemoveAll(testPath)
	defer os.RemoveAll(testPath)

	mp := newMP()
	dentry := &Dentry{Inode: 1, ParentId: 1, Name: "test", Type: 0}
	requestInfo := newRequestInfo()
	resp := mp.fsmDeleteDentry(dentry, false, requestInfo)
	require.Equal(t, proto.OpNotExistErr, resp.Status)

	// write data
	requestInfo1 := newRequestInfo()
	requestInfo1.ReqID = 2234
	mp.fsmCreateInode(NewInode(1, 0))
	mp.fsmCreateDentry(dentry, true, requestInfo1)

	// delete again
	resp = mp.fsmDeleteDentry(dentry, false, requestInfo)
	require.Equal(t, proto.OpNotExistErr, resp.Status)

	// check data exist
	getDentry, u := mp.getDentry(dentry)
	require.NotNil(t, getDentry)
	require.Equal(t, proto.OpOk, u)
}
