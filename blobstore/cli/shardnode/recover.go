// Copyright 2026 The CubeFS Authors.
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

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func addCmdRecover(cmd *grumble.Command) {
	recoverCommand := &grumble.Command{
		Name: "recover",
		Help: "recover shardnode, directly recover from db",
	}
	cmd.AddCommand(recoverCommand)

	// update shard info
	recoverCommand.AddCommand(&grumble.Command{
		Name: "updateShardInfo",
		Help: "update shard info in storage, should stop shardnode first",
		Args: func(a *grumble.Args) {
			args.SuidRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "raft storage path")
			f.StringL("json", "", "shardInfo json")
		},
		Run: cmdUpdateShardInfo,
	})

	// shard data backup
	recoverCommand.AddCommand(&grumble.Command{
		Name: "backupShardData",
		Help: "backup shard data from storage, should stop shardnode first",
		Args: func(a *grumble.Args) {
			args.SuidRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "origin storage path")
		},
		Run: cmdShardDataBackUp,
	})

	// shard data recover
	recoverCommand.AddCommand(&grumble.Command{
		Name: "recoverShardData",
		Help: "recover shard data from storage, should stop shardnode first",
		Args: func(a *grumble.Args) {
			args.SuidRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			f.StringL("source_path", "", "origin storage path")
			f.StringL("dest_path", "", "dest storage path")
		},
		Run: cmdShardDataRecover,
	})

	// shard data clear
	recoverCommand.AddCommand(&grumble.Command{
		Name: "clearShardData",
		Help: "clear shard data in storage",
		Args: func(a *grumble.Args) {
			args.SuidRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "origin storage path")
		},
		Run: cmdShardDataClear,
	})

	// recover disk shards: clear shardnode data and recover by diskID and shardID
	recoverCommand.AddCommand(&grumble.Command{
		Name: "recoverDiskShard",
		Help: "recover disk shard after clear shard data in storage",
		Args: func(a *grumble.Args) {
			args.DiskIDRegister(a)
		},
		Run: cmdRecoverDiskShard,
	})
}

func cmdUpdateShardInfo(c *grumble.Context) error {
	ctx := common.CmdContext()
	path := c.Flags.String("path")
	suid := args.Suid(c.Args)
	jsonInfo := c.Flags.String("json")
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
		fmt.Println("old shard info: ", common.Readable(sd))
	}

	// update
	if len(jsonInfo) == 0 {
		return nil
	}
	newInfo := &clustermgr.Shard{}
	if err = json.Unmarshal([]byte(jsonInfo), newInfo); err != nil {
		return err
	}
	fmt.Println("new shard info: ", common.Readable(newInfo))

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
	path := c.Flags.String("path")
	suid := args.Suid(c.Args)

	colData := kvstore.CF("data")
	colRaft := kvstore.CF("raft-wal")

	backPath := path + "/shard_" + suid.ShardID().ToString() + "_backup"
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
	fmt.Println("backup store opened successfully")

	fmt.Println("start backup data")
	originKVStore, err := kvstore.NewKVStore(ctx, path+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colData}})
	if err != nil {
		return err
	}
	defer originKVStore.Close()
	fmt.Println("origin store opened successfully")

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
	srcPath := c.Flags.String("source_path")
	destPath := c.Flags.String("dest_path")
	suid := args.Suid(c.Args)

	colData := kvstore.CF("data")
	colRaft := kvstore.CF("raft-wal")

	kvStore, err := kvstore.NewKVStore(ctx, srcPath+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colData}})
	if err != nil {
		return err
	}
	defer kvStore.Close()

	destKVStore, err := kvstore.NewKVStore(ctx, destPath+"/kv", kvstore.RocksdbLsmKVType, &kvstore.Option{
		ColumnFamily:    []kvstore.CF{colData},
		CreateIfMissing: true,
	})
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

	raftStore, err := kvstore.NewKVStore(ctx, srcPath+"/raft", kvstore.RocksdbLsmKVType, &kvstore.Option{ColumnFamily: []kvstore.CF{colRaft}})
	if err != nil {
		return err
	}
	defer raftStore.Close()

	destRaftStore, err := kvstore.NewKVStore(ctx, destPath+"/raft", kvstore.RocksdbLsmKVType, &kvstore.Option{
		ColumnFamily:    []kvstore.CF{colRaft},
		CreateIfMissing: true,
	})
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
	path := c.Flags.String("path")
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

func cmdRecoverDiskShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)
	diskID := args.DiskID(c.Args)
	shardID := args.ShardID(c.Args)
	var shards []clustermgr.ShardUnitInfo
	var err error
	if shardID != proto.InvalidShardID {
		info, err := cmClient.GetShardInfo(ctx, &clustermgr.GetShardArgs{
			ShardID: proto.ShardID(shardID),
		})
		if err != nil {
			return errors.Info(err, "get single shard failed")
		}
		shards = []clustermgr.ShardUnitInfo{
			{
				Suid:    info.Units[0].Suid,
				DiskID:  info.Units[0].DiskID,
				Host:    info.Units[0].Host,
				Learner: info.Units[0].Learner,
			},
		}
	} else {
		shards, err = cmClient.ListShardUnit(ctx, &clustermgr.ListShardUnitArgs{
			DiskID: diskID,
		})
		if err != nil {
			return errors.Info(err, "list shard unit failed")
		}
	}

	snClient := shardnode.New(rpc2.Client{})
	for _, sd := range shards {
		shard, err := cmClient.GetShardInfo(ctx, &clustermgr.GetShardArgs{
			ShardID: sd.Suid.ShardID(),
		})
		if err != nil {
			return errors.Info(err, "get shard failed")
		}

		var _err error
		var host string
		for _, u := range shard.Units {
			if u.Suid == sd.Suid {
				host = u.Host
				continue
			}
			// remove from raft group
			if _err = snClient.UpdateShard(ctx, u.Host, shardnode.UpdateShardArgs{
				DiskID:          u.DiskID,
				Suid:            u.Suid,
				ShardUpdateType: proto.ShardUpdateTypeRemoveMember,
				Unit: clustermgr.ShardUnit{
					Suid:    sd.Suid,
					DiskID:  sd.DiskID,
					Learner: sd.Learner,
				},
			}); _err != nil {
				continue
			}
			break
		}
		if _err != nil {
			return errors.Info(_err, "remove shard failed")
		}

		time.Sleep(5 * time.Second)
		if _err = snClient.AddShard(ctx, host, shardnode.AddShardArgs{
			DiskID:       sd.DiskID,
			Suid:         sd.Suid,
			Range:        sd.Range,
			Units:        shard.Units,
			RouteVersion: sd.RouteVersion,
		}); _err != nil {
			return errors.Info(err, "add shard failed")
		}
		for _, u := range shard.Units {
			if u.Suid == sd.Suid {
				continue
			}
			if _err = snClient.UpdateShard(ctx, u.Host, shardnode.UpdateShardArgs{
				DiskID:          u.DiskID,
				Suid:            u.Suid,
				ShardUpdateType: proto.ShardUpdateTypeAddMember,
				Unit: clustermgr.ShardUnit{
					Suid:   sd.Suid,
					DiskID: sd.DiskID,
				},
			}); _err != nil {
				continue
			}
			break
		}
		if _err != nil {
			return errors.Info(_err, "add shard failed")
		}
		fmt.Println("add shard success, suid:", sd.Suid)
	}
	return nil
}
