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

package toolbox

import (
	"io"
	"os"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdShard(cmd *grumble.Command) {
	shardCommand := &grumble.Command{
		Name: "shard",
		Help: "show shard information",
	}
	cmd.AddCommand(shardCommand)

	shardCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list shards in volume, write to file",
		Args: func(a *grumble.Args) {
			a.Uint64("vid", "volume id")
		},
		Flags: func(f *grumble.Flags) {
			f.Uint64L("vuid", 0, "list only the vuid")
			f.StringL("filepath", "", "save to file path, must not exist file")
		},
		Run: listShards,
	})
	shardCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "show shard in blobnode",
		Args: func(a *grumble.Args) {
			a.Uint64("vid", "volume id")
			a.Uint64("bid", "blob id")
		},
		Flags: func(f *grumble.Flags) {
			f.Uint64L("vuid", 0, "get only the vuid")
		},
		Run: getShard,
	})
}

func listShards(c *grumble.Context) error {
	ctx := common.CmdContext()
	vid := proto.Vid(c.Args.Uint64("vid"))
	vuid := proto.Vuid(c.Flags.Uint64("vuid"))

	cmCli := config.Cluster()
	volume, err := cmCli.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
	if err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if file := c.Flags.String("filepath"); file != "" {
		f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	bnCli := blobnode.New(&blobnode.Config{})
	for _, unit := range volume.Units {
		if vuid > 0 && unit.Vuid != vuid {
			continue
		}
		args := &blobnode.ListShardsArgs{DiskID: unit.DiskID, Vuid: unit.Vuid}
		for {
			shards, next, err := bnCli.ListShards(ctx, unit.Host, args)
			if err != nil {
				fmt.Fprintf(w, "error list %s %+v %s\n", unit.Host, args, err)
				break
			}
			for _, shard := range shards {
				fmt.Fprintln(w, common.RawString(shard))
			}

			if next == 0 {
				break
			}
			args.StartBid = next
		}
	}
	return nil
}

func getShard(c *grumble.Context) error {
	ctx := common.CmdContext()
	vid := proto.Vid(c.Args.Uint64("vid"))
	bid := proto.BlobID(c.Args.Uint64("bid"))
	vuid := proto.Vuid(c.Flags.Uint64("vuid"))

	cmCli := config.Cluster()
	volume, err := cmCli.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
	if err != nil {
		return err
	}

	bnCli := blobnode.New(&blobnode.Config{})
	for _, unit := range volume.Units {
		if vuid > 0 && unit.Vuid != vuid {
			continue
		}
		shard, err := bnCli.StatShard(ctx, unit.Host, &blobnode.StatShardArgs{
			DiskID: unit.DiskID,
			Vuid:   unit.Vuid,
			Bid:    bid,
		})
		fmt.Printf("stat host:%s disk:%d vuid:%d bid:%d\n",
			unit.Host, unit.DiskID, unit.Vuid, bid)
		if err != nil {
			fmt.Println("\tError:", err.Error())
		} else {
			fmt.Println("\tShard:", common.RawString(shard))
		}
	}
	return nil
}
