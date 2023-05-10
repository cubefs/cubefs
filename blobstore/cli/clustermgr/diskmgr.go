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
	"context"
	"encoding/json"
	"strings"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func addCmdDisk(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "disk",
		Help:     "disk tools",
		LongHelp: "disk tools for clustermgr",
	}
	cmd.AddCommand(command)

	command.AddCommand(&grumble.Command{
		Name: "get",
		Help: "show disk <diskid>",
		Run:  cmdGetDisk,
		Args: func(a *grumble.Args) {
			args.DiskIDRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "listDisk",
		Help: "show disks",
		Run:  cmdListDisks,
		Flags: func(f *grumble.Flags) {
			flags.VverboseRegister(f)
			flags.VerboseRegister(f)
			clusterFlags(f)

			f.UintL("status", 0, "list disk status")
			f.Int64L("marker", 0, "list disk marker")
			f.IntL("count", 0, "list disk count")
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "updateDisk",
		Help: "update disk info in db",
		Run:  cmdUpdateDisk,
		Args: func(a *grumble.Args) {
			args.DiskIDRegister(a)
			a.String("dbPath", "normal db path")
			a.String("diskInfo", "modify disk info data")
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	// offline disk
	command.AddCommand(&grumble.Command{
		Name: "offline",
		Help: "offline disk <diskid>",
		Run:  cmdOfflineDisk,
		Args: func(a *grumble.Args) {
			args.DiskIDRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	// offline all disks on the specified node
	command.AddCommand(&grumble.Command{
		Name: "offlineAll",
		Help: "offline all disks on the specified node",
		Run:  cmdOfflineAllDisks,
		Args: func(a *grumble.Args) {
			args.NodeHostRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})
}

func cmdGetDisk(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)
	disk, err := cmClient.DiskInfo(ctx, args.DiskID(c.Args))
	if err != nil {
		return err
	}

	if config.Verbose() || flags.Verbose(c.Flags) {
		fmt.Println(cfmt.DiskInfoJoinV(disk, ""))
	} else {
		fmt.Println(disk)
	}
	return nil
}

func cmdListDisks(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	listOptionArgs := &clustermgr.ListOptionArgs{
		Status: proto.DiskStatus(c.Flags.Uint("status")),
		Marker: proto.DiskID(c.Flags.Int64("marker")),
		Count:  c.Flags.Int("count"),
	}
	if listOptionArgs.Marker <= proto.InvalidDiskID {
		listOptionArgs.Marker = proto.DiskID(1)
	}

	verbose := config.Verbose() || flags.Verbose(c.Flags)
	vv := flags.Vverbose(c.Flags)
	next := true
	num := 0
	ac := common.NewAlternateColor(3)
	for next && listOptionArgs.Marker > proto.InvalidDiskID {
		disks, err := cmClient.ListDisk(ctx, listOptionArgs)
		if err != nil {
			return err
		}

		for _, disk := range disks.Disks {
			num++
			showDisk(disk, ac, num, verbose, vv)
		}

		if disks.Marker == proto.InvalidDiskID || len(disks.Disks) < listOptionArgs.Count {
			next = false
		} else {
			listOptionArgs.Marker = disks.Marker
			fmt.Println()
			next = common.Confirm("list next page?")
		}
	}
	return nil
}

func cmdUpdateDisk(c *grumble.Context) error {
	diskid := args.DiskID(c.Args)
	dbPath := c.Args.String("dbPath")
	data := c.Args.String("diskInfo")
	if diskid <= 0 || dbPath == "" || data == "" {
		return errors.New("invalid common args")
	}
	diskInfo := &blobnode.DiskInfo{}
	err := json.Unmarshal([]byte(data), diskInfo)
	if err != nil {
		return err
	}
	db, err := openNormalDB(dbPath, false)
	if err != nil {
		return err
	}
	defer db.Close()
	tbl, err := openDiskTable(db)
	if err != nil {
		return err
	}
	diskRec, err := tbl.GetDisk(proto.DiskID(diskid))
	if err != nil {
		return err
	}
	if diskInfo.MaxChunkCnt > 0 {
		diskRec.MaxChunkCnt = diskInfo.MaxChunkCnt
	}
	if diskInfo.FreeChunkCnt > 0 {
		diskRec.FreeChunkCnt = diskInfo.FreeChunkCnt
	}
	if diskInfo.UsedChunkCnt > 0 {
		diskRec.UsedChunkCnt = diskInfo.UsedChunkCnt
	}
	if diskInfo.Status > 0 {
		diskRec.Status = diskInfo.Status
	}

	if !common.Confirm("to change?\n") {
		return nil
	}

	return tbl.AddDisk(diskRec)
}

func openDiskTable(db *normaldb.NormalDB) (*normaldb.DiskTable, error) {
	tbl, err := normaldb.OpenDiskTable(db, true)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func cmdOfflineDisk(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)
	diskid := args.DiskID(c.Args)
	if diskid <= 0 {
		return errors.New("invalid common args")
	}

	if !common.Confirm(fmt.Sprintf("offline disk %d?\n", diskid)) {
		return nil
	}

	verbose := config.Verbose() || flags.Verbose(c.Flags)
	if verbose {
		fmt.Printf("set disk %d readonly\n", diskid)
	}
	if err := readonlyAndDropDisk(ctx, cmClient, diskid); err != nil {
		return err
	}
	fmt.Printf("start to offline disk %d in the background\n", diskid)

	return nil
}

func cmdOfflineAllDisks(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)
	nodeHost := args.NodeHost(c.Args)
	if len(nodeHost) <= 0 {
		return errors.New("invalid args, node host is empty")
	}

	disks := make([]*blobnode.DiskInfo, 0)

	// list all disks on the node
	listOptionArgs := &clustermgr.ListOptionArgs{
		Host:   nodeHost,
		Status: proto.DiskStatusNormal, // only list normal disks
		Marker: proto.DiskID(1),
	}
	for listOptionArgs.Marker > proto.InvalidDiskID {
		disksOneQuery, err := cmClient.ListDisk(ctx, listOptionArgs)
		if err != nil {
			return err
		}
		disks = append(disks, disksOneQuery.Disks...)
		listOptionArgs.Marker = disksOneQuery.Marker
	}

	// show all disks on the node and confirm
	verbose := config.Verbose() || flags.Verbose(c.Flags)
	vv := flags.Vverbose(c.Flags)
	fmt.Printf("disks on node <%s>:\n", nodeHost)
	ac := common.NewAlternateColor(3)
	for i, disk := range disks {
		showDisk(disk, ac, i+1, verbose, vv)
	}
	fmt.Println()
	if !common.Confirm(fmt.Sprintf("offline all disks on node <%s>?\n", nodeHost)) {
		return nil
	}

	// offline disks sequentially
	for _, disk := range disks {
		diskid := disk.DiskID
		if vv {
			fmt.Printf("set disk %d readonly\n", diskid)
		}
		if err := readonlyAndDropDisk(ctx, cmClient, diskid); err != nil {
			return fmt.Errorf("offline disk %d failed: %w", diskid, err)
		}
		if verbose {
			fmt.Printf("start to offline disk %d in the background\n", diskid)
		}
	}

	fmt.Printf("successfully initiate background offline operations on all disks on node <%s>\n", nodeHost)

	return nil
}

func showDisk(disk *blobnode.DiskInfo, ac *common.AlternateColor, num int, verbose, vv bool) {
	if verbose || vv {
		fmt.Printf("%4d. %s\n", num, strings.Repeat("- ", 60))
		if vv {
			ac.Next().Println(cfmt.DiskInfoJoinV(disk, "  "))
		} else {
			ac.Next().Println(cfmt.DiskInfoJoin(disk, "  "))
		}
	} else {
		ac.Next().Printf("%4d. %v\n", num, disk)
	}
}

func readonlyAndDropDisk(ctx context.Context, cmClient *clustermgr.Client, diskid proto.DiskID) error {
	err := cmClient.SetReadonlyDisk(ctx, diskid, true)
	if err != nil {
		return fmt.Errorf("set disk %d readonly failed: %w", diskid, err)
	}
	if err := cmClient.DropDisk(ctx, diskid); err != nil {
		return fmt.Errorf("drop disk %d failed: %w", diskid, err)
	}

	return nil
}
