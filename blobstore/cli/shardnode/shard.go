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
	"bytes"
	"encoding/json"
	"math"
	"os"
	"time"

	"github.com/cubefs/cubefs/blobstore/shardnode/storage"

	"github.com/desertbit/grumble"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func addCmdShard(cmd *grumble.Command) {
	shardCommand := &grumble.Command{
		Name:     "shard",
		Help:     "shard tools",
		LongHelp: "shard tools for shardNode",
	}
	cmd.AddCommand(shardCommand)

	// get shard stats
	shardCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get shard form shardNode",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			args.SuidRegister(a)
		},
		Run: cmdGetShard,
	})

	//  list shard
	shardCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list shards form shardNode",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			a.Uint64("shardID", "shardID")
			a.Uint64("count", "list shard count")
		},
		Run: cmdListShard,
	})

	// transfer shard leader
	shardCommand.AddCommand(&grumble.Command{
		Name: "transferLeader",
		Help: "transfer shard leader",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			args.SuidRegister(a)
			a.Uint("targetDiskID", "target leader diskID")
		},
		Run: cmdTransferShardLeader,
	})

	// add shard
	shardCommand.AddCommand(&grumble.Command{
		Name: "addShard",
		Help: "add shard",
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			a.String("json", "add shard args json")
		},
		Run: cmdAddShard,
	})

	// raft state
	shardCommand.AddCommand(&grumble.Command{
		Name: "hardState",
		Help: "get hardState",
		Args: func(a *grumble.Args) {
			a.String("path", "raft storage path")
			a.Uint64("shard", "shardID")
		},
		Run: cmdRaftHardStat,
	})

	// raft log
	shardCommand.AddCommand(&grumble.Command{
		Name: "raftLog",
		Help: "get raft log",
		Args: func(a *grumble.Args) {
			a.String("path", "raft storage path")
			a.Uint64("shard", "shardID")
			a.Uint64("index", "log index")
		},
		Run: cmdRaftLog,
	})

	// updateShardInfo
	shardCommand.AddCommand(&grumble.Command{
		Name: "updateShardInfo",
		Help: "get shard info from db",
		Args: func(a *grumble.Args) {
			a.String("path", "raft storage path")
			args.SuidRegister(a)
			a.String("json", "shardInfo json")
		},
		Run: cmdUpdateShardInfo,
	})

	// shard data backup
	shardCommand.AddCommand(&grumble.Command{
		Name: "backupShard",
		Help: "backup shard from db",
		Args: func(a *grumble.Args) {
			a.String("path", "origin storage path")
			args.SuidRegister(a)
		},
		Run: cmdShardDataBackUp,
	})

	// shard data backup
	shardCommand.AddCommand(&grumble.Command{
		Name: "recoverShardData",
		Help: "recover shard data from db",
		Args: func(a *grumble.Args) {
			a.String("path", "origin storage path")
			a.String("destPath", "dest storage path")
			args.SuidRegister(a)
		},
		Run: cmdShardDataRecover,
	})

	// shard data clear
	shardCommand.AddCommand(&grumble.Command{
		Name: "clearShardData",
		Help: "clear shard data from db",
		Args: func(a *grumble.Args) {
			a.String("path", "origin storage path")
			args.SuidRegister(a)
		},
		Run: cmdShardDataClear,
	})

	// recover disk shards
	shardCommand.AddCommand(&grumble.Command{
		Name: "recoverDiskShard",
		Help: "recover disk shard after clear shard data",
		Args: func(a *grumble.Args) {
			a.String("cm", "clusterMgr address")
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
		},
		Run: cmdRecoverDiskShard,
	})

	// recover single disk shard
	shardCommand.AddCommand(&grumble.Command{
		Name: "recoverSingleShard",
		Help: "recover single disk shard",
		Args: func(a *grumble.Args) {
			a.String("cm", "clusterMgr address")
			args.NodeHostRegister(a)
			args.DiskIDRegister(a)
			args.SuidRegister(a)
		},
		Run: cmdRecoverSingleShard,
	})
}

func cmdGetShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	suid := args.Suid(c.Args)

	cli := shardnode.New(rpc2.Client{})
	ret, err := cli.GetShardStats(ctx, host, shardnode.GetShardArgs{
		DiskID: diskID,
		Suid:   suid,
	})
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(ret))
	return nil
}

func cmdListShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	shardID := c.Args.Uint64("shardID")
	count := c.Args.Uint64("count")

	cli := shardnode.New(rpc2.Client{})
	ret, err := cli.ListShards(ctx, host, shardnode.ListShardArgs{
		DiskID:  diskID,
		ShardID: proto.ShardID(shardID),
		Count:   count,
	})
	if err != nil {
		return err
	}
	for _, shard := range ret.Shards {
		fmt.Println(common.Readable(shard))
	}
	return nil
}

func cmdTransferShardLeader(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	suid := args.Suid(c.Args)
	targetDiskID := c.Args.Uint("targetDiskID")

	cli := shardnode.New(rpc2.Client{})
	err := cli.TransferShardLeader(ctx, host, shardnode.TransferShardLeaderArgs{
		DiskID:     diskID,
		Suid:       suid,
		DestDiskID: proto.DiskID(targetDiskID),
	})
	if err != nil {
		return err
	}
	return nil
}

func cmdAddShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	host := args.NodeHost(c.Args)
	jsonStr := c.Args.String("json")

	req := shardnode.AddShardArgs{}
	err := json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		return err
	}

	cli := shardnode.New(rpc2.Client{})
	err = cli.AddShard(ctx, host, req)
	if err != nil {
		return err
	}
	return nil
}

func cmdRaftHardStat(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Args.String("path")
	shard := c.Args.Uint64("shard")

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
	path := c.Args.String("path")
	shard := c.Args.Uint64("shard")
	index := c.Args.Uint64("index")

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

func cmdRecoverDiskShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmHost := c.Args.String("cm")
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	cfg := &clustermgr.Config{LbConfig: rpc.LbConfig{Hosts: []string{cmHost}}}
	cmClient := clustermgr.New(cfg)
	ret, err := cmClient.ListShardUnit(ctx, &clustermgr.ListShardUnitArgs{
		DiskID: diskID,
	})
	if err != nil {
		return errors.Info(err, "list shard unit failed")
	}

	// fetch disk info
	disks, err := cmClient.ListShardNodeDisk(ctx, &clustermgr.ListOptionArgs{Count: 100})
	if err != nil {
		return err
	}
	diskHostMap := make(map[proto.DiskID]*clustermgr.ShardNodeDiskInfo)
	for _, info := range disks.Disks {
		diskHostMap[info.DiskID] = info
	}
	fmt.Println("load disk info success")

	snClient := shardnode.New(rpc2.Client{})
	for _, unit := range ret {
		shardInfo, err := cmClient.ListShard(ctx, &clustermgr.ListShardArgs{
			Marker: unit.Suid.ShardID() - 1,
			Count:  1,
		})
		if err != nil {
			return errors.Info(err, "list shard failed")
		}
		if len(shardInfo.Shards) != 1 {
			return errors.New(fmt.Sprintf("get shard failed, shards: %+v", shardInfo.Shards))
		}
		shard := shardInfo.Shards[0]
		if shard.ShardID != unit.Suid.ShardID() {
			return errors.New(fmt.Sprintf("get wrong shard: %+v", shard))
		}
		units := make([]clustermgr.ShardUnit, len(shard.Units))
		for i, u := range shard.Units {
			units[i] = clustermgr.ShardUnit{
				Suid:    u.Suid,
				DiskID:  u.DiskID,
				Host:    u.Host,
				Learner: u.Learner,
			}
			if u.Suid != unit.Suid {
				// remove from raft group
				info, ok := diskHostMap[u.DiskID]
				if !ok {
					return errors.New("load disk info in map failed")
				}
				if _err := snClient.UpdateShard(ctx, info.Host, shardnode.UpdateShardArgs{
					DiskID:          u.DiskID,
					Suid:            u.Suid,
					ShardUpdateType: proto.ShardUpdateTypeRemoveMember,
					Unit: clustermgr.ShardUnit{
						Suid:    unit.Suid,
						DiskID:  unit.DiskID,
						Learner: unit.Learner,
					},
				}); _err != nil {
					fmt.Println("remove shard failed")
					return _err
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		if err = snClient.AddShard(ctx, host, shardnode.AddShardArgs{
			DiskID:       unit.DiskID,
			Suid:         unit.Suid,
			Range:        unit.Range,
			Units:        units,
			RouteVersion: unit.RouteVersion,
		}); err != nil {
			return errors.Info(err, "add shard failed")
		}
		for _, u := range shard.Units {
			if u.Suid != unit.Suid {
				// add to raft group
				info, ok := diskHostMap[u.DiskID]
				if !ok {
					return errors.New("load disk info in map failed")
				}
				if _err := snClient.UpdateShard(ctx, info.Host, shardnode.UpdateShardArgs{
					DiskID:          u.DiskID,
					Suid:            u.Suid,
					ShardUpdateType: proto.ShardUpdateTypeAddMember,
					Unit: clustermgr.ShardUnit{
						Suid:   unit.Suid,
						DiskID: unit.DiskID,
					},
				}); _err != nil {
					fmt.Println("add shard failed")
					return _err
				}
			}
		}
		fmt.Println("add shard success, suid:", unit.Suid)
	}
	return nil
}

func cmdRecoverSingleShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmHost := c.Args.String("cm")
	host := args.NodeHost(c.Args)
	diskID := args.DiskID(c.Args)
	suid := args.Suid(c.Args)
	cfg := &clustermgr.Config{LbConfig: rpc.LbConfig{Hosts: []string{cmHost}}}
	cmClient := clustermgr.New(cfg)

	// fetch disk info
	disks, err := cmClient.ListShardNodeDisk(ctx, &clustermgr.ListOptionArgs{Count: 100})
	if err != nil {
		return err
	}
	diskHostMap := make(map[proto.DiskID]*clustermgr.ShardNodeDiskInfo)
	for _, info := range disks.Disks {
		diskHostMap[info.DiskID] = info
	}
	fmt.Println("load disk info success")

	shardInfo, err := cmClient.ListShard(ctx, &clustermgr.ListShardArgs{
		Marker: suid.ShardID() - 1,
		Count:  1,
	})
	if err != nil || len(shardInfo.Shards) != 1 {
		fmt.Println("get shard info failed")
		return err
	}
	shard := shardInfo.Shards[0]
	units := make([]clustermgr.ShardUnit, len(shard.Units)+1)
	snClient := shardnode.New(rpc2.Client{})
	for i, u := range shard.Units {
		// remove from raft group
		fmt.Println("remove form disk: %d", u.DiskID)
		info, ok := diskHostMap[u.DiskID]
		if !ok {
			return errors.New("load disk info in map failed")
		}
		if _err := snClient.UpdateShard(ctx, info.Host, shardnode.UpdateShardArgs{
			DiskID:          u.DiskID,
			Suid:            u.Suid,
			ShardUpdateType: proto.ShardUpdateTypeRemoveMember,
			Unit: clustermgr.ShardUnit{
				Suid:   suid,
				DiskID: diskID,
			},
		}); _err != nil {
			fmt.Println("remove shard failed")
			return _err
		}
		units[i] = clustermgr.ShardUnit{
			Suid:    u.Suid,
			DiskID:  u.DiskID,
			Host:    u.Host,
			Learner: u.Learner,
		}
	}
	units[len(shard.Units)] = clustermgr.ShardUnit{
		Suid:    suid,
		DiskID:  diskID,
		Host:    host,
		Learner: true,
	}
	time.Sleep(100 * time.Millisecond)
	if err = snClient.AddShard(ctx, host, shardnode.AddShardArgs{
		DiskID:       diskID,
		Suid:         suid,
		Range:        shard.Range,
		Units:        units,
		RouteVersion: shard.RouteVersion,
	}); err != nil {
		return errors.Info(err, "add shard failed")
	}
	for _, u := range shard.Units {
		fmt.Println("add to disk: %d", u.DiskID)
		// add to raft group
		info, ok := diskHostMap[u.DiskID]
		if !ok {
			return errors.New("load disk info in map failed")
		}
		if _err := snClient.UpdateShard(ctx, info.Host, shardnode.UpdateShardArgs{
			DiskID:          u.DiskID,
			Suid:            u.Suid,
			ShardUpdateType: proto.ShardUpdateTypeAddMember,
			Unit: clustermgr.ShardUnit{
				Suid:   suid,
				DiskID: diskID,
			},
		}); _err != nil {
			fmt.Println("add shard failed")
			return _err
		}
	}
	fmt.Println("add shard success, suid:", suid)
	return nil
}

func cmdUpdateShardInfo(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Args.String("path")
	suid := args.Suid(c.Args)
	jsonInfo := c.Args.String("json")
	col := kvstore.CF("data")

	store, err := kvstore.NewKVStore(ctx, path, kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{col}})
	if err != nil {
		return err
	}
	defer store.Close()

	g := storage.NewShardKeysGenerator(suid)
	infoKey := g.EncodeShardInfoKey()
	raw, err := store.GetRaw(ctx, col, infoKey)
	if err != nil && !errors.Is(err, kvstore.ErrNotFound) {
		return err
	}
	if err == nil {
		sd := clustermgr.Shard{}
		err = sd.Unmarshal(raw)
		if err != nil {
			return err
		}
		fmt.Println(common.Readable(sd))
	}

	// update
	if len(jsonInfo) == 0 {
		return nil
	}
	newInfo := &clustermgr.Shard{}
	if err = json.Unmarshal([]byte(jsonInfo), newInfo); err != nil {
		return err
	}
	fmt.Println("new info:")
	fmt.Println(common.Readable(newInfo))

	_raw, err := newInfo.Marshal()
	if err != nil {
		return err
	}
	err = store.SetRaw(ctx, col, infoKey, _raw)
	if err != nil {
		return err
	}
	store.FlushCF(ctx, col)
	return nil
}

func cmdShardDataBackUp(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Args.String("path")
	suid := args.Suid(c.Args)

	colData := kvstore.CF("data")
	colRaft := kvstore.CF("raft-wal")

	backPath := path + "/" + suid.ShardID().ToString()
	if err := os.Mkdir(backPath, 0o755); err != nil {
		return err
	}
	fmt.Println("create backup dir: ", backPath)

	backKVStore, err := kvstore.NewKVStore(ctx, backPath+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{
		ColumnFamily:    []kvstore.CF{colData},
		CreateIfMissing: true,
	})
	if err != nil {
		return err
	}
	defer backKVStore.Close()

	backRaftStore, err := kvstore.NewKVStore(ctx, backPath+"/raft", kvstore.RocksdbLsmKVType, &kvstore.Option{
		ColumnFamily:    []kvstore.CF{colRaft},
		CreateIfMissing: true,
	})
	if err != nil {
		return err
	}
	defer backRaftStore.Close()
	fmt.Println("backup store opened")

	fmt.Println("start backup data")
	originKVStore, err := kvstore.NewKVStore(ctx, path+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colData}})
	if err != nil {
		return err
	}
	defer originKVStore.Close()
	fmt.Println("origin store open")

	g := storage.NewShardKeysGenerator(suid)
	shardDataPrefix := g.EncodeShardDataPrefix()
	dataList := originKVStore.List(ctx, colData, shardDataPrefix, nil, nil)
	kvCount := 0
	for {
		kg, vg, err := dataList.ReadNext()
		if err != nil {
			return err
		}
		if kg == nil || vg == nil {
			fmt.Println("list to end")
			break
		}
		if !bytes.HasPrefix(kg.Key(), shardDataPrefix) {
			return errors.New("key prefix not match")
		}
		if err = backKVStore.SetRaw(ctx, colData, kg.Key(), vg.Value()); err != nil {
			return err
		}
		kg.Close()
		vg.Close()
		kvCount++
	}
	fmt.Println("shard kv data backup done, num: ", kvCount)

	originRaftStore, err := kvstore.NewKVStore(ctx, path+"/raft", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colRaft}})
	if err != nil {
		return err
	}
	defer originRaftStore.Close()

	raftLogPrefix := raft.EncodeIndexLogKeyPrefix(uint64(suid.ShardID()))
	raftLogList := originRaftStore.List(ctx, colRaft, raftLogPrefix, nil, nil)
	logCount := 0
	for {
		kg, vg, err := raftLogList.ReadNext()
		if err != nil {
			return err
		}
		if kg == nil || vg == nil {
			fmt.Println("list to end")
			break
		}
		if !bytes.HasPrefix(kg.Key(), raftLogPrefix) {
			return errors.New("key prefix not match")
		}
		if err = backRaftStore.SetRaw(ctx, colRaft, kg.Key(), vg.Value()); err != nil {
			return err
		}
		kg.Close()
		vg.Close()
		logCount++
	}
	fmt.Println("shard raft log backup done, num: ", logCount)

	hardStateKey := raft.EncodeHardStateKey(uint64(suid.ShardID()))
	hsRaw, err := originRaftStore.GetRaw(ctx, colRaft, hardStateKey)
	if err != nil {
		return err
	}
	if err = backRaftStore.SetRaw(ctx, colRaft, hardStateKey, hsRaw); err != nil {
		return err
	}
	fmt.Println("hardState backup done")
	return nil
}

func cmdShardDataRecover(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Args.String("path")
	destPath := c.Args.String("destPath")
	suid := args.Suid(c.Args)

	colData := kvstore.CF("data")
	colRaft := kvstore.CF("raft-wal")

	kvStore, err := kvstore.NewKVStore(ctx, path+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colData}})
	if err != nil {
		return err
	}
	defer kvStore.Close()

	destKVStore, err := kvstore.NewKVStore(ctx, destPath+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colData}})
	if err != nil {
		return err
	}
	defer destKVStore.Close()

	fmt.Println("start recover shard kv data")
	g := storage.NewShardKeysGenerator(suid)
	shardDataPrefix := g.EncodeShardDataPrefix()
	dataList := kvStore.List(ctx, colData, shardDataPrefix, nil, nil)
	kvCount := 0
	for {
		kg, vg, err := dataList.ReadNext()
		if err != nil {
			return err
		}
		if kg == nil || vg == nil {
			fmt.Println("list to end")
			break
		}
		if !bytes.HasPrefix(kg.Key(), shardDataPrefix) {
			return errors.New("key prefix not match")
		}
		if err = destKVStore.SetRaw(ctx, colData, kg.Key(), vg.Value()); err != nil {
			return err
		}
		kg.Close()
		vg.Close()
		kvCount++
	}
	fmt.Println("shard kv data recover done, num: ", kvCount)

	raftStore, err := kvstore.NewKVStore(ctx, path+"/raft", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colRaft}})
	if err != nil {
		return err
	}
	defer raftStore.Close()

	destRaftStore, err := kvstore.NewKVStore(ctx, destPath+"/raft", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colRaft}})
	if err != nil {
		return err
	}
	defer destRaftStore.Close()
	fmt.Println("start recover shard raft data")

	raftLogPrefix := raft.EncodeIndexLogKeyPrefix(uint64(suid.ShardID()))
	raftLogList := raftStore.List(ctx, colRaft, raftLogPrefix, nil, nil)
	logCount := 0
	for {
		kg, vg, err := raftLogList.ReadNext()
		if err != nil {
			return err
		}
		if kg == nil || vg == nil {
			fmt.Println("list to end")
			break
		}
		if !bytes.HasPrefix(kg.Key(), raftLogPrefix) {
			return errors.New("key prefix not match")
		}
		if err = destRaftStore.SetRaw(ctx, colRaft, kg.Key(), vg.Value()); err != nil {
			return err
		}
		kg.Close()
		vg.Close()
		logCount++
	}
	fmt.Println("shard raft log recover, num: ", logCount)

	hardStateKey := raft.EncodeHardStateKey(uint64(suid.ShardID()))
	hsRaw, err := raftStore.GetRaw(ctx, colRaft, hardStateKey)
	if err != nil {
		return err
	}
	if err = destRaftStore.SetRaw(ctx, colRaft, hardStateKey, hsRaw); err != nil {
		return err
	}
	fmt.Println("hardState recover done")
	return nil
}

func cmdShardDataClear(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Args.String("path")
	suid := args.Suid(c.Args)

	colData := kvstore.CF("data")
	colRaft := kvstore.CF("raft-wal")

	dataStore, err := kvstore.NewKVStore(ctx, path+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colData}})
	if err != nil {
		return err
	}
	defer dataStore.Close()
	g := storage.NewShardKeysGenerator(suid)
	shardInfoKey := g.EncodeShardInfoKey()
	if err = dataStore.Delete(ctx, colData, shardInfoKey); err != nil {
		return err
	}
	fmt.Println("shard info deleted")

	shardDataPrefix := g.EncodeShardDataPrefix()
	shardDataMaxPrefix := g.EncodeShardDataMaxPrefix()
	if err = dataStore.DeleteRange(ctx, colData, shardDataPrefix, shardDataMaxPrefix); err != nil {
		return err
	}
	dataStore.FlushCF(ctx, colData)
	fmt.Println("shard data deleted")

	raftStore, err := kvstore.NewKVStore(ctx, path+"/raft", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colRaft}})
	if err != nil {
		return err
	}

	hardStateKey := raft.EncodeHardStateKey(uint64(suid.ShardID()))
	if err = raftStore.Delete(ctx, colRaft, hardStateKey); err != nil {
		return err
	}
	fmt.Println("hardState deleted")

	snapShotMetaKey := raft.EncodeSnapshotMetaKey(uint64(suid.ShardID()))
	if err = raftStore.Delete(ctx, colRaft, snapShotMetaKey); err != nil {
		return err
	}
	fmt.Println("snapShot meta deleted")

	if err = raftStore.DeleteRange(ctx, colRaft,
		raft.EncodeIndexLogKey(uint64(suid.ShardID()), 0),
		raft.EncodeIndexLogKey(uint64(suid.ShardID()), math.MaxUint64)); err != nil {
		return err
	}
	raftStore.FlushCF(ctx, colRaft)
	fmt.Println("raft log deleted")
	return nil
}
