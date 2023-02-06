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
	"encoding/json"
	"errors"
	"time"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/args"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/kvdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

func addCmdVolume(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "volume",
		Help:     "volume tools",
		LongHelp: "volume tools for clustermgr",
	}
	cmd.AddCommand(command)

	command.AddCommand(&grumble.Command{
		Name: "listVolumeUnits",
		Help: "show volume units",
		Run:  cmdListVolumeUnits,
		Args: func(a *grumble.Args) {
			args.VidRegister(a)
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "updateVolume",
		Help: "update volume info",
		Run:  cmdUpdateVolume,
		Args: func(a *grumble.Args) {
			args.VidRegister(a)
			a.String("dbPath", "volume db path")
			a.String("volumeInfo", "modify volume info, "+
				"only support for codemode/status/total/used/free")
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "updateVolumeUnit",
		Help: "update volume unit",
		Run:  cmdUpdateVolumeUnit,
		Args: func(a *grumble.Args) {
			args.VuidRegister(a)
			a.String("dbPath", "volume db path")
			a.String("unit", "modify volume unit, "+
				"only support for diskid/epoch/nexEpoch/compacting")
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "listVolumes",
		Help: "list volumes",
		Run:  cmdListVolumes,
		Args: func(a *grumble.Args) {
			a.Int("count", "number of volumes to list")
			a.Uint64("marker", "list volumes start from special Vid", grumble.Default(uint64(0)))
		},
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			clusterFlags(f)
		},
	})
}

func cmdListVolumes(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	count := c.Args.Int("count")
	marker := c.Args.Uint64("marker")
	listVolumeArgs := &clustermgr.ListVolumeArgs{Count: count, Marker: proto.Vid(marker)}

	volumes, err := cmClient.ListVolume(ctx, listVolumeArgs)
	if err != nil {
		return err
	}
	for _, vol := range volumes.Volumes {
		fmt.Printf("%d: %+v\n", vol.Vid, vol.VolumeInfoBase)
	}
	return nil
}

func cmdUpdateVolume(c *grumble.Context) error {
	vid := args.Vid(c.Args)
	dbPath := c.Args.String("dbPath")
	data := c.Args.String("volumeInfo")
	if vid == 0 || dbPath == "" || data == "" {
		return errors.New("invalid command arguments")
	}

	modifyInfo := &clustermgr.VolumeInfo{}
	err := json.Unmarshal([]byte(data), modifyInfo)
	if err != nil {
		return err
	}
	db, err := openVolumeDB(dbPath, false)
	if err != nil {
		return err
	}
	defer db.Close()
	tbl, err := openVolumeTable(db)
	if err != nil {
		return err
	}
	srcInfo, err := tbl.GetVolume(proto.Vid(vid))
	if err != nil {
		return err
	}

	if modifyInfo.Status.IsValid() {
		srcInfo.Status = modifyInfo.Status
	}
	if modifyInfo.CodeMode.IsValid() {
		srcInfo.CodeMode = modifyInfo.CodeMode
	}
	if modifyInfo.Total > 0 {
		srcInfo.Total = modifyInfo.Total
	}
	if modifyInfo.Used > 0 {
		srcInfo.Used = modifyInfo.Used
	}
	if modifyInfo.Free > 0 {
		srcInfo.Free = modifyInfo.Free
	}

	if !common.Confirm("to change?\n" + cfmt.VolumeInfoJoin(modifyInfo, "")) {
		return nil
	}
	return tbl.PutVolumeRecord(srcInfo)
}

func cmdListVolumeUnits(c *grumble.Context) error {
	ctx := common.CmdContext()
	cmClient := newCMClient(c.Flags)

	volumeInfo, err := cmClient.GetVolumeInfo(ctx,
		&clustermgr.GetVolumeArgs{Vid: args.Vid(c.Args)})
	if err != nil {
		return err
	}

	fmt.Println("volume info:")
	fmt.Println(cfmt.VolumeInfoJoin(volumeInfo, "\t"))

	verbose := flags.Verbose(c.Flags)

	dnConfig := blobnode.Config{}
	dnConfig.ClientTimeoutMs = 3000
	blobnodeCli := blobnode.New(&dnConfig)

	n := len(volumeInfo.Units)
	loader := common.Loader(n)

	taskArgs := make([]interface{}, n)
	for i, arg := range volumeInfo.Units {
		taskArgs[i] = arg
	}
	chunkInfos := make([]string, n)
	task.C(func(i int, taskArgs interface{}) {
		unit := taskArgs.(clustermgr.Unit)
		info, err := blobnodeCli.StatChunk(ctx, unit.Host,
			&blobnode.StatChunkArgs{
				DiskID: unit.DiskID,
				Vuid:   unit.Vuid,
			})
		if err != nil {
			chunkInfos[i] = fmt.Sprintf("ERROR: %s %d %s", unit.Host, unit.Vuid, err.Error())
		} else if verbose {
			chunkInfos[i] = cfmt.ChunkInfoJoin(info, "\t")
		} else {
			chunkInfos[i] = fmt.Sprint(info)
		}
		loader <- 1
	}, taskArgs)
	time.Sleep(100 * time.Millisecond)

	fmt.Println("chunks:")
	for i, info := range chunkInfos {
		if verbose {
			fmt.Println("chunk info:", i)
		}
		fmt.Println(info)
	}

	return nil
}

func cmdUpdateVolumeUnit(c *grumble.Context) error {
	vuid := args.Vuid(c.Args)
	dbPath := c.Args.String("dbPath")
	data := c.Args.String("unitInfo")
	if vuid == 0 || dbPath == "" || data == "" {
		return errors.New("invalid command arguments")
	}

	modifyInfo := &clustermgr.AdminUpdateUnitArgs{}
	err := json.Unmarshal([]byte(data), modifyInfo)
	if err != nil {
		return err
	}
	db, err := openVolumeDB(dbPath, false)
	if err != nil {
		return err
	}
	defer db.Close()
	tbl, err := openVolumeTable(db)
	if err != nil {
		return err
	}
	srcInfo, err := tbl.GetVolumeUnit(vuid.VuidPrefix())
	if err != nil {
		return err
	}
	if modifyInfo.DiskID > 0 {
		srcInfo.DiskID = modifyInfo.DiskID
	}
	if proto.IsValidEpoch(modifyInfo.Epoch) {
		srcInfo.Epoch = modifyInfo.Epoch
	}
	if proto.IsValidEpoch(modifyInfo.NextEpoch) {
		srcInfo.NextEpoch = modifyInfo.NextEpoch
	}
	srcInfo.Compacting = modifyInfo.Compacting

	return tbl.PutVolumeUnit(vuid.VuidPrefix(), srcInfo)
}

func openVolumeDB(path string, readonly bool) (*volumedb.VolumeDB, error) {
	db, err := volumedb.Open(path, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = readonly
	})
	if err != nil {
		return nil, fmt.Errorf("open db failed, err: %s", err.Error())
	}
	return db, nil
}

func openVolumeTable(db *volumedb.VolumeDB) (*volumedb.VolumeTable, error) {
	tbl, err := volumedb.OpenVolumeTable(db)
	if err != nil {
		return nil, fmt.Errorf("open volume table failed, err: %s", err.Error())
	}
	return tbl, nil
}

func openKvDB(path string, readonly bool) (*kvdb.KvDB, error) {
	db, err := kvdb.Open(path, false, func(option *kvstore.RocksDBOption) {
		option.ReadOnly = readonly
	})
	if err != nil {
		return nil, fmt.Errorf("open db failed, err: %s", err.Error())
	}
	return db, nil
}
