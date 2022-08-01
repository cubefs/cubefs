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

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	_topic     = "topic"
	_partition = "partition"
	_offset    = "offset"
)

func addCmdKafkaConsumer(cmd *grumble.Command) {
	kafkaCommand := &grumble.Command{
		Name:     "kafka",
		Help:     "kafka consume tools",
		LongHelp: "kafka consume tools for scheduler",
	}
	cmd.AddCommand(kafkaCommand)

	kafkaCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get kafka consume offset",
		Run:  cmdGetConsumeOffset,
		Args: func(a *grumble.Args) {
			a.String(_taskType, "task_type, such as shard_repair and blob_delete")
			a.String(_topic, _topic, grumble.Default(""))
			a.Int(_partition, _partition, grumble.Default(int(0)))
		},
	})
	kafkaCommand.AddCommand(&grumble.Command{
		Name: "set",
		Help: "set kafka consume offset",
		Run:  cmdSetConsumeOffset,
		Args: func(a *grumble.Args) {
			a.String(_taskType, "task_type, such as shard_repair and blob_delete")
			a.String(_topic, _topic, grumble.Default(""))
			a.Int(_partition, _partition, grumble.Default(int(0)))
			a.Int64(_offset, _offset, grumble.Default(int64(0)))
		},
	})
}

func cmdGetConsumeOffset(c *grumble.Context) error {
	taskType := proto.TaskType(c.Args.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	topic := c.Args.String(_topic)
	partition := c.Args.Int(_partition)
	clusterMgrCli := newClusterMgrTaskClient()
	offset, err := clusterMgrCli.GetConsumeOffset(taskType, topic, int32(partition))
	if err != nil {
		return err
	}
	fmt.Println(offset)
	return nil
}

func cmdSetConsumeOffset(c *grumble.Context) error {
	taskType := proto.TaskType(c.Args.String(_taskType))
	if !taskType.Valid() {
		return errcode.ErrIllegalTaskType
	}
	topic := c.Args.String(_topic)
	partition := c.Args.Int(_partition)
	offset := c.Args.Int64(_offset)
	clusterMgrCli := newClusterMgrTaskClient()
	err := clusterMgrCli.SetConsumeOffset(taskType, topic, int32(partition), offset)
	if err != nil {
		return err
	}
	fmt.Println("set consume offset successfully")
	return nil
}
