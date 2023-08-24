// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"github.com/stretchr/testify/require"
)

func TestSnapshotScanner(t *testing.T) {
	snapshotRoutineNumPerTask = 1
	scanCheckInterval = 1
	scanner := &SnapshotScanner{
		ID:     "test_id",
		Volume: "test_vol",
		mw:     NewMockMetaWrapper(),
		lcnode: &LcNode{},
		adminTask: &proto.AdminTask{
			Response: &proto.SnapshotVerDelTaskResponse{},
		},
		verDelReq: &proto.SnapshotVerDelTaskRequest{
			Task: &proto.SnapshotVerDelTask{
				VolVersionInfo: &proto.VolVersionInfo{
					Ver: 0,
				},
			},
		},
		inodeChan:   unboundedchan.NewUnboundedChan(10),
		rPoll:       routinepool.NewRoutinePool(snapshotRoutineNumPerTask),
		currentStat: &proto.SnapshotStatistics{},
		stopC:       make(chan bool, 0),
	}
	go scanner.Start()
	time.Sleep(time.Second * 5)
	require.Equal(t, true, scanner.DoneScanning())
}
