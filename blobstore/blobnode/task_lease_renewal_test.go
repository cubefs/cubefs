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

package blobnode

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type MockReportCli struct {
	renewalFail   error
	failTaskIDMap map[string]bool
}

func (m *MockReportCli) RenewalTask(ctx context.Context, tasks *api.TaskRenewalArgs) (ret *api.TaskRenewalRet, err error) {
	result := api.TaskRenewalRet{Errors: make(map[proto.TaskType]map[string]string)}
	for typ, ids := range tasks.IDs {
		errors := make(map[string]string)
		for _, taskID := range ids {
			if _, ok := m.failTaskIDMap[taskID]; ok {
				errors[taskID] = "mock fail"
			}
		}
		result.Errors[typ] = errors
	}
	return &result, m.renewalFail
}

func TestWorkerTaskRenewal(t *testing.T) {
	idc := "Z0"

	// test renewal ok
	{
		tm := initTestTaskRunnerMgr(t, 20, proto.TaskTypeDiskDrop, proto.TaskTypeDiskRepair)
		reportCli := MockReportCli{}
		taskRenter := NewTaskRenter(idc, &reportCli, tm)
		taskRenter.renewalTask()
		tasks := tm.GetAliveTasks()
		require.Equal(t, 2, len(tasks))
		require.Equal(t, 20, len(tasks[proto.TaskTypeDiskDrop]))
		require.Equal(t, 20, len(tasks[proto.TaskTypeDiskRepair]))
	}

	// test renewal fail
	{
		tm := initTestTaskRunnerMgr(t, 11, proto.TaskTypeBalance, proto.TaskTypeDiskDrop,
			proto.TaskTypeDiskRepair, proto.TaskTypeManualMigrate)
		reportCli := MockReportCli{failTaskIDMap: make(map[string]bool)}
		taskRenter := NewTaskRenter(idc, &reportCli, tm)

		taskRenter.renewalTask()
		tasks := tm.GetAliveTasks()
		require.Equal(t, 4, len(tasks))
		require.Equal(t, 11, len(tasks[proto.TaskTypeBalance]))

		reportCli.failTaskIDMap[proto.TaskTypeBalance.String()+"_1"] = true
		reportCli.failTaskIDMap[proto.TaskTypeBalance.String()+"_7"] = true
		reportCli.failTaskIDMap[proto.TaskTypeDiskDrop.String()+"_1"] = true
		reportCli.failTaskIDMap[proto.TaskTypeDiskRepair.String()+"_3"] = true
		reportCli.failTaskIDMap[proto.TaskTypeManualMigrate.String()+"_10"] = true

		taskRenter.renewalTask()
		tasks = tm.GetAliveTasks()
		require.Equal(t, 4, len(tasks))
		require.Equal(t, 9, len(tasks[proto.TaskTypeBalance]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskDrop]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeDiskRepair]))
		require.Equal(t, 10, len(tasks[proto.TaskTypeManualMigrate]))
	}

	// test all renewal fail
	{
		tm := initTestTaskRunnerMgr(t, 11, proto.TaskTypeBalance, proto.TaskTypeDiskDrop,
			proto.TaskTypeDiskRepair, proto.TaskTypeManualMigrate)
		reportCli := MockReportCli{
			renewalFail:   errors.New("mock fail"),
			failTaskIDMap: make(map[string]bool),
		}
		taskRenter := NewTaskRenter(idc, &reportCli, tm)
		tasks := tm.GetAliveTasks()
		require.Equal(t, 4, len(tasks))

		taskRenter.renewalTask()
		tasks = tm.GetAliveTasks()
		require.Equal(t, 0, len(tasks))
	}
}
