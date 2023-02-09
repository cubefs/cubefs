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

package proxy

import (
	"sort"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func addCmdAllocator(cmd *grumble.Command) {
	allocatorCommand := &grumble.Command{
		Name: "allocator",
		Help: "proxy allocator tools",
	}
	cmd.AddCommand(allocatorCommand)

	allocatorCommand.AddCommand(&grumble.Command{
		Name: "alloc",
		Help: "alloc volume, just testing for most of time",
		Flags: func(f *grumble.Flags) {
			proxyFlags(f)
			f.UintL("code_mode", 0, "codemode uint")
			f.Uint64L("fsize", 0, "file size")
			f.Uint64L("bid_count", 0, "bid count")
		},
		Run: allocVolume,
	})
	allocatorCommand.AddCommand(&grumble.Command{
		Name: "discard",
		Help: "discard volumes on this proxy with the codemode",
		Args: func(a *grumble.Args) {
			a.UintList("vids", "volume ids")
		},
		Flags: func(f *grumble.Flags) {
			proxyFlags(f)
			f.UintL("code_mode", 0, "codemode uint")
		},
		Run: discardVolumes,
	})
	allocatorCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list volumes, show top 10 hosts and disks",
		Flags: func(f *grumble.Flags) {
			proxyFlags(f)
			f.UintL("code_mode", 0, "codemode uint")
			f.IntL("top", 10, "show top")
		},
		Run: listVolumes,
	})
}

func allocVolume(c *grumble.Context) error {
	volumes, err := proxyCli.VolumeAlloc(common.CmdContext(), c.Flags.String(_host),
		&proxy.AllocVolsArgs{
			CodeMode: codemode.CodeMode(c.Flags.Uint("code_mode")),
			Fsize:    c.Flags.Uint64("fsize"),
			BidCount: c.Flags.Uint64("bid_count"),
		})
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(volumes))
	return nil
}

func discardVolumes(c *grumble.Context) error {
	var vids []proto.Vid
	for _, vid := range c.Args.UintList("vids") {
		vids = append(vids, proto.Vid(vid))
	}
	if len(vids) == 0 {
		return nil
	}
	_, err := proxyCli.VolumeAlloc(common.CmdContext(), c.Flags.String(_host),
		&proxy.AllocVolsArgs{
			CodeMode: codemode.CodeMode(c.Flags.Uint("code_mode")),
			Fsize:    1,
			BidCount: 1,
			Discards: vids,
		})
	if err != nil {
		return err
	}
	fmt.Println("Dicard:", vids, "OK")
	return nil
}

func listVolumes(c *grumble.Context) error {
	result, err := proxyCli.ListVolumes(common.CmdContext(), c.Flags.String(_host),
		&proxy.ListVolsArgs{CodeMode: codemode.CodeMode(c.Flags.Uint("code_mode"))})
	if err != nil {
		return err
	}

	show := struct {
		free  uint64
		hosts map[string][]proto.Vid
		disks map[proto.DiskID][]proto.Vid
	}{}
	show.hosts = make(map[string][]proto.Vid)
	show.disks = make(map[proto.DiskID][]proto.Vid)
	for _, vol := range result.Volumes {
		show.free += vol.Free
		for _, unit := range vol.Units {
			show.hosts[unit.Host] = append(show.hosts[unit.Host], vol.Vid)
			show.disks[unit.DiskID] = append(show.disks[unit.DiskID], vol.Vid)
		}
	}

	hosts := make([]struct {
		Host string
		N    int
		Vids []proto.Vid
	}, len(show.hosts))
	idx := 0
	for host, vids := range show.hosts {
		hosts[idx].Host = host
		hosts[idx].Vids = vids
		hosts[idx].N = len(vids)
		idx++
	}
	sort.Slice(hosts, func(i, j int) bool {
		return hosts[i].N < hosts[j].N
	})

	disks := make([]struct {
		Disk proto.DiskID
		N    int
		Vids []proto.Vid
	}, len(show.disks))
	idx = 0
	for disk, vids := range show.disks {
		disks[idx].Disk = disk
		disks[idx].Vids = vids
		disks[idx].N = len(vids)
		idx++
	}
	sort.Slice(disks, func(i, j int) bool {
		return disks[i].N < disks[j].N
	})

	top := c.Flags.Int("top")
	if len(hosts) > top {
		hosts = hosts[:top]
	}
	if len(disks) > top {
		disks = disks[:top]
	}

	fmt.Printf("List on proxy:%s codemode:%d volumes:%d top:%d\n",
		c.Flags.String(_host), c.Flags.Uint("code_mode"), len(result.Volumes), top)
	fmt.Println("\nhosts:\n", common.Readable(hosts))
	fmt.Println("\ndisks:\n", common.Readable(disks))
	return nil
}
