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

package scheduler

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
)

var testArchiveConfig = ArchiveStoreConfig{
	ArchiveIntervalMin: defaultArchiveIntervalMin,
	ArchiveDelayMin:    defaultArchiveDelayMin,
}

func TestArchiveStore(t *testing.T) {
	ctr := gomock.NewController(t)
	{
		archiveTable := NewMockArchiveTable(ctr)
		archiveTable.EXPECT().FindTask(any, any).AnyTimes().Return(nil, nil)
		archiveTable.EXPECT().Insert(any, any).AnyTimes().Return(nil)

		mgr := NewArchiveStoreMgr(archiveTable, testArchiveConfig)
		defer mgr.Close()

		balanceTable := NewMockMigrateTaskTable(ctr)
		balanceTable.EXPECT().Name().AnyTimes().Return(proto.BalanceTaskType)
		record1 := &proto.ArchiveRecord{TaskID: uuid.New().String(), TaskType: proto.BalanceTaskType, Content: proto.MigrateTask{}}
		balanceTable.EXPECT().QueryMarkDeleteTasks(any, any).AnyTimes().Return([]*proto.ArchiveRecord{record1}, nil)

		repairTaskTable := NewMockRepairTaskTable(ctr)
		repairTaskTable.EXPECT().Name().AnyTimes().Return(proto.RepairTaskType)
		record2 := &proto.ArchiveRecord{TaskID: uuid.New().String(), TaskType: proto.RepairTaskType, Content: proto.VolRepairTask{}}
		repairTaskTable.EXPECT().QueryMarkDeleteTasks(any, any).AnyTimes().Return([]*proto.ArchiveRecord{record2}, nil)

		mgr.RegisterTables(balanceTable, repairTaskTable)
		mgr.Run()
	}
	{
		// register twice
		archiveTable := NewMockArchiveTable(ctr)
		mgr := NewArchiveStoreMgr(archiveTable, testArchiveConfig)
		defer mgr.Close()

		repairTaskTable := NewMockRepairTaskTable(ctr)
		repairTaskTable.EXPECT().Name().AnyTimes().Return(proto.RepairTaskType)
		mgr.RegisterTables(repairTaskTable)
		mgr.RegisterTables(repairTaskTable)
	}
	{
		// QueryMarkDeleteTasks failed
		archiveTable := NewMockArchiveTable(ctr)
		mgr := NewArchiveStoreMgr(archiveTable, testArchiveConfig)
		defer mgr.Close()

		repairTaskTable := NewMockRepairTaskTable(ctr)
		repairTaskTable.EXPECT().Name().AnyTimes().Return(proto.RepairTaskType)
		repairTaskTable.EXPECT().QueryMarkDeleteTasks(any, any).AnyTimes().Return(nil, errMock)

		mgr.RegisterTables(repairTaskTable)
		mgr.storeRun()
	}
	{
		archiveTable := NewMockArchiveTable(ctr)
		mgr := NewArchiveStoreMgr(archiveTable, testArchiveConfig)
		defer mgr.Close()

		repairTaskTable := NewMockRepairTaskTable(ctr)
		repairTaskTable.EXPECT().Name().AnyTimes().Return(proto.RepairTaskType)
		id1 := uuid.New().String()
		id2 := uuid.New().String()
		task1 := proto.VolRepairTask{TaskID: id1}
		task2 := proto.VolRepairTask{TaskID: id2}
		record1 := &proto.ArchiveRecord{TaskID: id1, TaskType: proto.RepairTaskType, Content: task1} // task already archive and remove success
		record2 := &proto.ArchiveRecord{TaskID: id2, TaskType: proto.RepairTaskType, Content: task2} // archive success
		record3 := &proto.ArchiveRecord{}                                                            // find task failed
		record4 := &proto.ArchiveRecord{}                                                            // task already archive and remove source failed
		repairTaskTable.EXPECT().QueryMarkDeleteTasks(any, any).AnyTimes().Return([]*proto.ArchiveRecord{record1, record2, record3, record4}, nil)

		archiveTable.EXPECT().FindTask(any, any).Return(record1, nil)
		archiveTable.EXPECT().FindTask(any, any).Return(nil, base.ErrNoDocuments)
		archiveTable.EXPECT().FindTask(any, any).Return(nil, errMock)
		archiveTable.EXPECT().FindTask(any, any).Return(record4, nil)

		archiveTable.EXPECT().Insert(any, any).Return(errMock)
		archiveTable.EXPECT().FindTask(any, any).Return(record1, nil)
		archiveTable.EXPECT().FindTask(any, any).Return(record4, nil)
		repairTaskTable.EXPECT().RemoveMarkDelete(any, any).Return(nil)
		repairTaskTable.EXPECT().RemoveMarkDelete(any, any).Return(errMock)

		mgr.RegisterTables(repairTaskTable)
		mgr.storeRun()
	}
}
