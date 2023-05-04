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
	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
)

const (
	_count          = "count"
	_diskID         = "disk_id"
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
		Flags: func(f *grumble.Flags) {
			migrateFlags(f)
			f.StringL(_taskID, "", "set the task_id")
		},
	})
	migrateCommand.AddCommand(&grumble.Command{
		Name: "add",
		Help: "add manual migrate task",
		Run:  cmdAddTask,
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
			f.Uint64("", "vuid", 0, "set the vuid")
			f.Bool("", _directDownload, true, "whether download directly")
		},
	})
	migrateCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list migrate tasks",
		Run:  cmdListTask,
		Flags: func(f *grumble.Flags) {
			migrateFlags(f)
			f.Uint64L(_diskID, 0, "disk id for which disk")
			f.IntL(_count, 10, "the number you want to get")
		},
	})
	migrateCommand.AddCommand(&grumble.Command{
		Name: "disk",
		Help: "get migrating disk",
		Run:  cmdGetMigratingDisk,
		Flags: func(f *grumble.Flags) {
			migrateFlags(f)
			f.Uint64L(_diskID, 0, "disk id for which disk")
		},
	})
	migrateCommand.AddCommand(&grumble.Command{
		Name: "progress",
		Help: "show migrating progress",
		Run:  cmdGetMigratingProgress,
		Flags: func(f *grumble.Flags) {
			migrateFlags(f)
			f.Uint64L(_diskID, 0, "disk id for which disk")
		},
	})
}

func migrateFlags(f *grumble.Flags) {
	clusterFlags(f)
	f.StringL(_taskType, "", "task_type, such as disk_repair, disk_drop, balance and manual_migrate")
}

func cmdGetTask(c *grumble.Context) error {
	taskType := proto.TaskType(c.Flags.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	key := c.Flags.String(_taskID)
	clusterID := getClusterID(c.Flags)
	clusterMgrCli := newClusterMgrClient(clusterID)
	cli := scheduler.New(&scheduler.Config{}, clusterMgrCli, clusterID)

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
	clusterID := getClusterID(c.Flags)
	directDownload := c.Flags.Bool(_directDownload)
	vuid := proto.Vuid(c.Flags.Uint64("vuid"))
	if !common.Confirm(fmt.Sprintf("add manual migrate task: vid[%d], vuid[%d] ?", vuid.Vid(), vuid)) {
		return nil
	}
	clusterMgrCli := newClusterMgrClient(clusterID)
	cli := scheduler.New(&scheduler.Config{}, clusterMgrCli, clusterID)
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
	taskType := proto.TaskType(c.Flags.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	count := c.Flags.Int(_count)
	diskID := proto.DiskID(c.Flags.Uint64(_diskID))
	clusterMgrCli := newClusterMgrTaskClient(getClusterID(c.Flags))
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

	taskType := proto.TaskType(c.Flags.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	diskID := proto.DiskID(c.Flags.Uint64(_diskID))

	clusterMgrCli := newClusterMgrTaskClient(getClusterID(c.Flags))

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

func cmdGetMigratingProgress(c *grumble.Context) error {
	ctx := common.CmdContext()

	taskType := proto.TaskType(c.Flags.String(_taskType))
	if taskType != proto.TaskTypeDiskRepair && taskType != proto.TaskTypeDiskDrop {
		fmt.Println("task_type must be one of disk_repair and disk_drop")
		return errcode.ErrIllegalTaskType
	}
	diskID := proto.DiskID(c.Flags.Uint64(_diskID))
	if diskID == proto.InvalidDiskID {
		return errcode.ErrIllegalDiskID
	}

	clusterID := getClusterID(c.Flags)
	clusterMgrCli := newClusterMgrClient(clusterID)
	cli := scheduler.New(&scheduler.Config{}, clusterMgrCli, clusterID)

	progress, err := cli.DiskMigratingStats(ctx, &scheduler.DiskMigratingStatsArgs{
		TaskType: taskType,
		DiskID:   diskID,
	})
	if err != nil {
		return err
	}
	if progress == nil {
		return nil
	}
	if progress.TotalTasksCnt == 0 {
		fmt.Println("no migrating task")
		return nil
	}
	curr := progress.MigratedTasksCnt * 100 / progress.TotalTasksCnt
	fmt.Printf("[%s] MigratedTasksCnt: %d/TotalTasksCnt: %d\n", common.LineBar(curr, 50), progress.MigratedTasksCnt, progress.TotalTasksCnt)
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
