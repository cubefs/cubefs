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

package shardnode

import (
	"github.com/desertbit/grumble"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func addCmdShard(cmd *grumble.Command) {
	shardCommand := &grumble.Command{
		Name:     "shard",
		Help:     "shard tools",
		LongHelp: "shard tools for shardnode",
	}
	cmd.AddCommand(shardCommand)

	// get shard stats
	shardCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get shard form shardnode",
		Args: func(a *grumble.Args) {
			args.DiskIDRegister(a)
			args.SuidRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			clusterFlags(f)
		},
		Run: cmdGetShard,
	})

	// raft state
	shardCommand.AddCommand(&grumble.Command{
		Name: "hardState",
		Help: "get hardState from storage, should stop shardnode first",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "raft storage path")
			f.UintL("shard_id", 0, "shardID")
		},
		Run: cmdRaftHardStat,
	})

	// raft log
	shardCommand.AddCommand(&grumble.Command{
		Name: "raftLog",
		Help: "get raft log from storage, should stop shardnode first",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "raft storage path")
			f.UintL("shard_id", 0, "shardID")
			f.UintL("index", 0, "log index")
		},
		Run: cmdRaftLog,
	})
}

func cmdGetShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)
	diskID := args.DiskID(c.Args)
	suid := args.Suid(c.Args)

	diskInfo, err := cmClient.ShardNodeDiskInfo(ctx, diskID)
	if err != nil {
		return errors.Info(err, "get shardnode disk info failed")
	}

	snClient := shardnode.New(rpc2.Client{})

	ret, err := snClient.GetShardStats(ctx, diskInfo.Host, shardnode.GetShardArgs{
		DiskID: diskID,
		Suid:   suid,
	})
	if err != nil {
		return err
	}

	fmt.Println(common.Readable(ret))
	return nil
}

func cmdRaftHardStat(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Flags.String("path")
	shard := c.Flags.Uint64("shard_id")

	store, err := kvstore.NewKVStore(ctx, path, kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{"raft-wal"}})
	if err != nil {
		return err
	}
	defer store.Close()
	key := raft.EncodeHardStateKey(uint64(shard))
	vg, err := store.Get(ctx, kvstore.CF("raft-wal"), key, nil)
	if err != nil {
		return err
	}
	hs := raftpb.HardState{}
	if vg != nil {
		if err := hs.Unmarshal(vg.Value()); err != nil {
			return err
		}
	}
	fmt.Println(common.Readable(hs))
	return nil
}

func cmdRaftLog(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Flags.String("path")
	shard := c.Flags.Uint64("shard_id")
	index := c.Flags.Uint64("index")

	store, err := kvstore.NewKVStore(ctx, path, kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{"raft-wal"}})
	if err != nil {
		return err
	}
	defer store.Close()
	entry := &raftpb.Entry{}
	vg, err := store.Get(ctx, kvstore.CF("raft-wal"), raft.EncodeIndexLogKey(shard, index), nil)
	if err == nil {
		if err := entry.Unmarshal(vg.Value()); err != nil {
			return err
		}
		fmt.Println(common.Readable(entry))
		vg.Close()
	}
	return nil
}
