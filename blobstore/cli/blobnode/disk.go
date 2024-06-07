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

package blobnode

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
)

func addCmdDisk(cmd *grumble.Command) {
	diskCommand := &grumble.Command{
		Name:     "disk",
		Help:     "disk tools",
		LongHelp: "disk tools for blobnode",
	}
	cmd.AddCommand(diskCommand)

	diskCommand.AddCommand(&grumble.Command{
		Name: "all",
		Help: "show all register disks",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			stat, err := cli.Stat(common.CmdContext(), host)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})

	diskCommand.AddCommand(&grumble.Command{
		Name: "stat",
		Help: "show disk info",
		Flags: func(f *grumble.Flags) {
			blobnodeFlags(f)
			f.UintL("diskid", 1, "disk id")
		},
		Run: func(c *grumble.Context) error {
			cli := blobnode.New(&blobnode.Config{})
			host := c.Flags.String("host")
			args := blobnode.DiskStatArgs{DiskID: proto.DiskID(c.Flags.Uint("diskid"))}
			stat, err := cli.DiskInfo(common.CmdContext(), host, &args)
			if err != nil {
				return err
			}
			fmt.Println(common.Readable(stat))
			return nil
		},
	})

	addCmdDiskDrop(diskCommand)
}

func addCmdDiskDrop(diskCommand *grumble.Command) {
	diskDropCommand := &grumble.Command{
		Name: "drop_stat",
		Help: "check dropped disk status",
		Flags: func(f *grumble.Flags) {
			f.StringL("cm_hosts", "", "required: e.g. [cm_hosts=ip1:9998,xxx] (multi or single)")
			f.StringL("node_host", "", "required: local blobnode host (to get disk_ids from cm)")
			f.BoolL("need_db", false, "not required: read local db, specific check chunk. default(false)")
		},
		Run: dropStatCheck,
	}

	diskCommand.AddCommand(diskDropCommand)
}

func dropStatCheck(c *grumble.Context) error {
	diskInfos, err := checkDiskDropConf(c)
	if err != nil {
		return err
	}

	vuidCmCnt := 0 // remain cm vuid count, the chunk which is not migration or cleanup
	vuidBnCnt := 0
	ctx := context.Background()
	cmCli := newCmClient(c)
	fmt.Printf("start time: " + time.Now().Format("2006-01-02 15:04:05") + "\n")
	printDiskID(diskInfos)

	// check cm
	if vuidCmCnt, err = getVuidFromCm(ctx, cmCli, diskInfos); err != nil {
		fmt.Printf("check clusterMgr err:%+v \n", err)
		return err
	}
	fmt.Printf("done get vuid from cm. diskCnt=%d, remain vuid=%d\n", len(diskInfos), vuidCmCnt)

	// check local blobnode
	if vuidBnCnt, err = getVuidFromBn(ctx, cmCli, diskInfos, c); err != nil {
		fmt.Printf("check blobnode err:%+v \n", err)
		return err
	}
	fmt.Printf("done get vuid from bn. diskCnt=%d, remain vuid=%d\n", len(diskInfos), vuidBnCnt)

	return nil
}

func checkDiskDropConf(c *grumble.Context) ([]*blobnode.DiskInfo, error) {
	cmHosts := strings.Split(c.Flags.String("cm_hosts"), ",")
	if len(cmHosts) == 0 {
		return nil, fmt.Errorf("invalid cm hosts")
	}

	nodeHost := c.Flags.String("node_host")
	re := regexp.MustCompile(`\b(\d{1,3}\.){3}\d{1,3}:\d{1,6}\b`)
	if nodeHost == "" || !re.MatchString(nodeHost) {
		return nil, fmt.Errorf("--node_host is required")
	}

	return parseAllLocalDiskIdsByCm(c)
}

func getVuidFromCm(ctx context.Context, cmCli *clustermgr.Client, dInfos []*blobnode.DiskInfo) (int, error) {
	vuidCnt := 0
	for _, dInfo := range dInfos {
		// is not repaired or dropped
		if dInfo.Status < proto.DiskStatusRepaired || dInfo.Status > proto.DiskStatusDropped {
			fmt.Printf("diskID=%d\tstatus=%d\tpath=%s\tchunk=%d\n", dInfo.DiskID, dInfo.Status, dInfo.Path, dInfo.UsedChunkCnt)
			vuidCnt += int(dInfo.UsedChunkCnt)
			continue
		}

		units, err := cmCli.ListVolumeUnit(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: dInfo.DiskID})
		if err != nil {
			return 0, err
		}

		if len(units) != 0 {
			vuidCnt += len(units)
			fmt.Printf("diskID=%d\tstatus=%d\tpath=%s\tchunk=%d\n", dInfo.DiskID, dInfo.Status, dInfo.Path, len(units))
		}

		for _, info := range units {
			fmt.Printf("  vuid=%d\n", info.Vuid)
		}
	}
	return vuidCnt, nil
}

func getVuidFromBn(ctx context.Context, cmCli *clustermgr.Client, dInfos []*blobnode.DiskInfo, c *grumble.Context) (int, error) {
	readDb := c.Flags.Bool("need_db")
	vuidCnt := 0

	for _, dInfo := range dInfos {
		cnt, err := walkSingleDisk(ctx, cmCli, dInfo, readDb)
		if err != nil {
			return 0, err
		}

		vuidCnt += cnt
	}

	return vuidCnt, nil
}

func walkSingleDisk(ctx context.Context, cmCli *clustermgr.Client, dh *blobnode.DiskInfo, readDb bool) (int, error) {
	const dataDir = "data"
	files, err := os.ReadDir(filepath.Join(dh.Path, dataDir))
	if err != nil {
		return 0, err
	}

	var db kvstore.KVStore
	if len(files) != 0 {
		fmt.Printf("diskID=%d\tpath=%s\n", dh.DiskID, dh.Path)

		if readDb {
			dbPath := filepath.Join(dh.Path, "meta/superblock")
			db, err = kvstore.OpenDB(dbPath, kvstore.WithReadonly(true))
			if err != nil {
				fmt.Printf("Err: Fail to load SuperBlock, err:%+v \n", err)
				return 0, err
			}
		}
	}

	vuidCnt := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		chunkId, err := parseChunkNameStr(file.Name()) // decode chunk
		if err != nil {
			fmt.Printf("---ERROR--- file:%s, error:%+v \n", file.Name(), err)
			continue
		}

		vuidCnt++
		vuid := chunkId.VolumeUnitId()
		newDisk := getNewDiskInfo(ctx, cmCli, vuid)
		vm := getChunkMeta(db, chunkId)
		fmt.Printf("  vuid=%d    status=%s    newDisk=%+v\n", vuid, vm.Status.String(), newDisk)
	}
	return vuidCnt, nil
}

func getChunkMeta(db kvstore.KVStore, chunkId blobnode.ChunkId) (vm core.VuidMeta) {
	if db == nil {
		return core.VuidMeta{}
	}

	key := []byte("chunks/" + chunkId.String())
	data, err := db.Get(key)
	if err != nil {
		fmt.Printf("Err: Fail to get chunk db, err:%+v \n", err)
		return core.VuidMeta{}
	}

	err = json.Unmarshal(data, &vm)
	if err != nil {
		fmt.Printf("Failed unmarshal, err:%+v \n", err)
		return core.VuidMeta{}
	}
	// fmt.Printf("chunk meta:%+v \n", vm)
	return vm
}

func parseChunkNameStr(name string) (chunkId blobnode.ChunkId, err error) {
	const chunkFileLen = 33 // 16+1+16

	if len(name) != chunkFileLen {
		return chunkId, fmt.Errorf("chunk file name length not match, file:%s", name)
	}

	if err = chunkId.Unmarshal([]byte(name)); err != nil {
		return chunkId, err
	}

	return chunkId, nil
}

// vuid -> vid -> volume info -> new disk
func getNewDiskInfo(ctx context.Context, cmCli *clustermgr.Client, vuid proto.Vuid) clustermgr.Unit {
	vid := vuid.Vid()

	volume, err := cmCli.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
	if err != nil {
		fmt.Printf("Err: Fail to get volume, err:%+v \n", err)
		return clustermgr.Unit{}
	}

	idx := int(vuid.Index())
	if idx < 0 || idx >= len(volume.Units) {
		fmt.Printf("Err: invalid index:%d, volume:%+v err:%+v \n", idx, volume)
		return clustermgr.Unit{}
	}

	return volume.Units[idx]
}

func newCmClient(c *grumble.Context) *clustermgr.Client {
	const prefix = "http://"

	cmHosts := strings.Split(c.Flags.String("cm_hosts"), ",")
	for i, host := range cmHosts {
		cmHosts[i] = prefix + host
	}

	secret := config.ClusterMgrSecret()
	cfg := &clustermgr.Config{}
	cfg.LbConfig.Hosts = cmHosts
	cfg.LbConfig.Config.Tc.Auth = auth.Config{EnableAuth: secret != "", Secret: secret}
	return clustermgr.New(cfg)
}

func parseAllLocalDiskIdsByCm(c *grumble.Context) (diskInfos []*blobnode.DiskInfo, err error) {
	const prefix = "http://"
	host := c.Flags.String("node_host")

	cmCli := newCmClient(c)
	ret, err := cmCli.ListDisk(context.Background(), &clustermgr.ListOptionArgs{Host: prefix + host})
	if err != nil {
		return nil, err
	}

	diskInfos = ret.Disks
	if len(diskInfos) == 0 {
		return nil, fmt.Errorf("error: empty, invalid disk ids")
	}
	return diskInfos, nil
}

func printDiskID(dInfos []*blobnode.DiskInfo) {
	diskIDs := make([]proto.DiskID, len(dInfos))
	for i, dInfo := range dInfos {
		diskIDs[i] = dInfo.DiskID
	}
	sort.SliceStable(diskIDs, func(i, j int) bool {
		return diskIDs[i] < diskIDs[j]
	})

	fmt.Printf("check dropped disk. diskCnt=%d, disk ids=%v\n", len(diskIDs), diskIDs)
}
