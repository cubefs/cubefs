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
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestExtentsTruncateAuditDeferOnInodeNotExist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	mp.SetEnableAuditLog(true)

	p := &Packet{}
	err := mp.ExtentsTruncate(&ExtentsTruncateReq{
		PartitionID: mp.config.PartitionId,
		Inode:       99999,
		Size:        0,
	}, p, "127.0.0.1")
	require.Error(t, err)
	require.EqualValues(t, proto.OpErr, p.ResultCode)
}
