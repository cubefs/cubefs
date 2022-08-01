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
	"fmt"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

const (
	_count          = "count"
	_directDownload = "direct_download"
)

func addCmdMigrateTask(cmd *grumble.Command) {
	migrateCommand := &grumble.Command{
		Name:     "migrate",
		Help:     "migrate tools",
		LongHelp: "migrate tools for scheduler",
	}
	cmd.AddCommand(migrateCommand)

	migrateCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get migrate task",
		Run:  cmdGetTask,
		Args: func(a *grumble.Args) {
			a.String(_taskType, "task_type, such as disk_repair, disk_drop, balance and manual_migrate")
			a.String(_taskID, _taskID)
			a.Int(_clusterID, _clusterID, grumble.Default(int(1)))
		},
	})
	migrateCommand.AddCommand(&grumble.Command{
		Name: "add",
		Help: "add manual migrate task",
		Run:  cmdAddTask,
		Args: func(a *grumble.Args) {
			args.VuidRegister(a)
			a.Bool(_directDownload, _directDownload, grumble.Default(true))
			a.Int(_clusterID, _clusterID, grumble.Default(1))
		},
	})
	migrateCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list migrate tasks",
		Run:  cmdListTask,
		Args: func(a *grumble.Args) {
			a.String(_taskType, "task_type, such as disk_repair, disk_drop, balance and manual_migrate")
			a.Int(_count, _count, grumble.Default(10))
			args.DiskIDRegister(a, grumble.Default(uint64(0)))
		},
	})
	migrateCommand.AddCommand(&grumble.Command{
		Name: "disk",
		Help: "get migrating disk",
		Run:  cmdGetMigratingDisk,
		Args: func(a *grumble.Args) {
			a.String(_taskType, "task_type, such as disk_repair, disk_drop, balance and manual_migrate")
			args.DiskIDRegister(a, grumble.Default(uint64(0)))
		},
	})
}

func cmdGetTask(c *grumble.Context) error {
	taskType := proto.TaskType(c.Args.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	key := c.Args.String(_taskID)
	clusterID := c.Args.Int(_clusterID)
	clusterMgrCli := newClusterMgrClient()
	cli := scheduler.New(&scheduler.Config{}, clusterMgrCli, proto.ClusterID(clusterID))

	task, err := cli.DetailMigrateTask(common.CmdContext(), &scheduler.MigrateTaskDetailArgs{
		Type: taskType,
		ID:   key,
	})
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(task))
	return nil
}

func cmdAddTask(c *grumble.Context) error {
	ctx := common.CmdContext()
	clusterID := c.Args.Int(_clusterID)
	directDownload := c.Args.Bool(_directDownload)
	vuid := args.Vuid(c.Args)
	fmt.Printf("add manual migrate task: vid[%d], vuid[%d]", vuid.Vid(), vuid)
	if !common.Confirm("?") {
		return nil
	}
	clusterMgrCli := newClusterMgrClient()
	cli := scheduler.New(&scheduler.Config{}, clusterMgrCli, proto.ClusterID(clusterID))
	err := cli.AddManualMigrateTask(ctx, &scheduler.AddManualMigrateArgs{
		Vuid:           vuid,
		DirectDownload: directDownload,
	})
	if err != nil {
		return err
	}
	fmt.Println("add manual migrate task successfully")
	return nil
}

func cmdListTask(c *grumble.Context) error {
	ctx := common.CmdContext()

	taskType := proto.TaskType(c.Args.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	count := c.Args.Int(_count)
	diskID := args.DiskID(c.Args)
	clusterMgrCli := newClusterMgrTaskClient()
	prefix := client.GenMigrateTaskPrefix(taskType)
	if diskID != proto.InvalidDiskID {
		prefix = client.GenMigrateTaskPrefixByDiskID(taskType, diskID)
	}
	listKvArgs := &clustermgr.ListKvOpts{
		Prefix: prefix,
		Count:  count,
	}
	for {
		tasks, marker, err := clusterMgrCli.ListMigrateTasks(ctx, taskType, listKvArgs)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			printMigrateTask(task)
		}
		if marker == "" {
			return nil
		}
		if !common.Confirm("for more?") {
			return nil
		}
		listKvArgs.Marker = marker
	}
}

func cmdGetMigratingDisk(c *grumble.Context) error {
	ctx := common.CmdContext()

	taskType := proto.TaskType(c.Args.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	diskID := args.DiskID(c.Args)

	clusterMgrCli := newClusterMgrTaskClient()

	if diskID == 0 {
		disks, err := clusterMgrCli.ListMigratingDisks(ctx, taskType)
		if err != nil {
			return err
		}
		if len(disks) == 0 {
			return nil
		}
		for _, disk := range disks {
			printMigratingDisk(disk)
		}
		return nil
	}

	disk, err := clusterMgrCli.GetMigratingDisk(ctx, taskType, diskID)
	if err != nil {
		return err
	}
	printMigratingDisk(disk)
	return nil
}

func printMigrateTask(task *proto.MigrateTask) {
	type MigrateTaskSimple struct {
		ID       string             `json:"id"`
		TaskType proto.TaskType     `json:"task_type"`
		State    proto.MigrateState `json:"state"`
	}

	taskSimple := MigrateTaskSimple{
		ID:       task.TaskID,
		TaskType: task.TaskType,
		State:    task.State,
	}
	fmt.Println(common.RawString(taskSimple))
}

func printMigratingDisk(disk *client.MigratingDiskMeta) {
	type MigratingDiskSimple struct {
		ID           string         `json:"id"`
		DiskID       proto.DiskID   `json:"disk_id"`
		TaskType     proto.TaskType `json:"task_type"`
		UsedChunkCnt int64          `json:"used_chunk_cnt"`
	}
	migratingDisk := MigratingDiskSimple{
		ID:           disk.ID(),
		DiskID:       disk.Disk.DiskID,
		TaskType:     disk.TaskType,
		UsedChunkCnt: disk.Disk.UsedChunkCnt,
	}
	fmt.Println(common.RawString(migratingDisk))
}
