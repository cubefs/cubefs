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

package clustermgr

import (
	"encoding/binary"
	"encoding/json"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/raftdb"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
)

func addCmdUpdateRaftDB(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "raft",
		Help:     "raft db tools",
		LongHelp: "update members for raft db",
	}
	cmd.AddCommand(command)

	command.AddCommand(&grumble.Command{
		Name: "add",
		Help: "add raft member",
		Run:  addMember,
		Args: func(a *grumble.Args) {
			a.String("path", "the path of raft db")
			a.Uint64("id", "node id")
			a.String("host", "raft host")
			a.String("node_host", "service host")
			a.Uint64("type", "member type, 1: learner, 2: normal, others will return error")
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list raft info",
		Run:  listInfo,
		Args: func(a *grumble.Args) {
			a.String("path", "the path of raft db")
		},
	})
}

func addMember(c *grumble.Context) error {
	path := c.Args.String("path")
	memberType := c.Args.Uint64("type")
	nodeID := c.Args.Uint64("id")
	nHost := c.Args.String("node_host")
	host := c.Args.String("host")

	var memType bool
	if nodeID <= 0 {
		return fmt.Errorf("node ID[%d] is invalid", nodeID)
	}
	if memberType != 1 && memberType != 2 {
		return fmt.Errorf("member type is invalid")
	}
	memType = memberType == 1
	raftDb, err := raftdb.OpenRaftDB(path, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = false
	})
	if err != nil {
		return fmt.Errorf("open db failed, err: %v", err)
	}
	defer raftDb.Close()
	val, err := raftDb.Get(base.RaftMemberKey)
	if err != nil {
		return fmt.Errorf("get db failed, err: %v", err)
	}
	mbrs := &base.RaftMembers{}
	if len(val) != 0 {
		err = json.Unmarshal(val, mbrs)
		if err != nil {
			return fmt.Errorf("unmarshal members failed, err: %v", err)
		}
	}
	for _, member := range mbrs.Mbs {
		if member.ID == nodeID {
			return fmt.Errorf("member already exist, please change the ID[%d]", nodeID)
		}
	}
	if !common.Confirm("this is a dangerous action, confirm to add?") {
		return nil
	}

	mbrs.Mbs = append(mbrs.Mbs, base.RaftMember{ID: nodeID, Learner: memType, NodeHost: nHost, Host: host})

	val, err = json.Marshal(mbrs)
	if err != nil {
		return fmt.Errorf("marshal members err: %v", err)
	}
	if err = raftDb.Put(base.RaftMemberKey, val); err != nil {
		return fmt.Errorf("put member err: %v", err)
	}
	fmt.Println("add member success")
	return nil
}

func listInfo(c *grumble.Context) error {
	path := c.Args.String("path")

	raftDb, err := raftdb.OpenRaftDB(path, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = true
	})
	if err != nil {
		return fmt.Errorf("open db failed, err: %v", err)
	}
	defer raftDb.Close()
	// get members
	val, err := raftDb.Get(base.RaftMemberKey)
	if err != nil {
		return fmt.Errorf("get db failed, err: %v", err)
	}
	mbrs := &base.RaftMembers{}
	if len(val) != 0 {
		err = json.Unmarshal(val, mbrs)
		if err != nil {
			return fmt.Errorf("unmarshal members failed, err: %v", err)
		}
	}
	// get applyIndex
	rawOldestIndex, err := raftDb.Get(base.ApplyIndexKey)
	if err != nil {
		return fmt.Errorf("read apply index from oldest raft db failed: %s", err.Error())
	}
	applyIndex := binary.BigEndian.Uint64(rawOldestIndex)

	_, _ = fmt.Println("raft info: \n", fmt.Sprintf("members: %+v\n applyIndex: %d", mbrs, applyIndex))

	return nil
}
