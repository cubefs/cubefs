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
	"strconv"
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
	auth_proto "github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
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
			f.StringL("disk_ids", "", "is empty: means all disk; else, we will check specify disk [disk_ids=1,2,xxx](multi or single)")
			f.BoolL("need_db", false, "not required: read local db, specific check chunk. default(false)")
			f.UintL("log_level", 2, "0:debug; 1:info; 2:warn; 3:error")
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
	ctx := common.CmdContext()
	cmCli := newCmClient(c)
	fmt.Printf("start time: " + time.Now().Format("2006-01-02 15:04:05") + "\n")
	printDiskID(diskInfos)
	log.SetOutputLevel(log.Level(c.Flags.Uint("log_level")))

	// check cm
	if vuidCmCnt, err = getVuidFromCm(ctx, cmCli, diskInfos); err != nil {
		fmt.Printf("check clusterMgr err:%+v \n", err)
		return err
	}
	fmt.Printf("done get vuid from cm. diskCnt=%d, remain vuid=%d\n", len(diskInfos), vuidCmCnt)

	// check local blobnode
	if vuidBnCnt, err = getVuidFromBn(ctx, c, cmCli, diskInfos); err != nil {
		fmt.Printf("check blobnode err:%+v \n", err)
		return err
	}
	specify := getSpecifyDiskIDs(c)
	fmt.Printf("done get vuid from bn. diskCnt=%d, specifyDisk=%v, remain vuid=%d\n", len(diskInfos), specify, vuidBnCnt)

	return nil
}

func checkDiskDropConf(c *grumble.Context) ([]*clustermgr.BlobNodeDiskInfo, error) {
	ctx := common.CmdContext()

	cmHosts := strings.Split(c.Flags.String("cm_hosts"), ",")
	if len(cmHosts) == 0 {
		return nil, fmt.Errorf("invalid cm hosts")
	}

	nodeHost := c.Flags.String("node_host")
	re := regexp.MustCompile(`\b(\d{1,3}\.){3}\d{1,3}:\d{1,6}\b`)
	if nodeHost == "" || !re.MatchString(nodeHost) {
		return nil, fmt.Errorf("--node_host is required")
	}

	return parseAllLocalDiskIdsByCm(ctx, c)
}

func getSpecifyDiskIDs(c *grumble.Context) map[proto.DiskID]struct{} {
	ret := make(map[proto.DiskID]struct{})

	diskStr := c.Flags.String("disk_ids")
	if diskStr == "" {
		return ret
	}

	disks := strings.Split(diskStr, ",")
	if len(disks) == 0 {
		return ret
	}

	for _, disk := range disks {
		diskID, err := strconv.ParseUint(disk, 10, 32)
		if err != nil {
			fmt.Printf("fail to convert string to int, str:%s, err:%+v\n", disk, err)
			return map[proto.DiskID]struct{}{}
		}
		ret[proto.DiskID(diskID)] = struct{}{}
	}

	return ret
}

func getVuidFromCm(ctx context.Context, cmCli *clustermgr.Client, dInfos []*clustermgr.BlobNodeDiskInfo) (int, error) {
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

		// The blobnode has performed disk swapping, so the disks count pulled from CM may be greater than disks count on the blobnode. A prompt is needed.
		if dInfo.Status != proto.DiskStatusDropped {
			fmt.Printf("diskID=%d\tstatus=%d\tpath=%s\n", dInfo.DiskID, dInfo.Status, dInfo.Path)
		}
	}
	return vuidCnt, nil
}

func getVuidFromBn(ctx context.Context, c *grumble.Context, cmCli *clustermgr.Client, dInfos []*clustermgr.BlobNodeDiskInfo) (int, error) {
	readDb := c.Flags.Bool("need_db")
	vuidCnt := 0

	specify := getSpecifyDiskIDs(c)

	for _, dInfo := range dInfos {
		if len(specify) != 0 {
			_, exist := specify[dInfo.DiskID]
			if !exist {
				continue
			}
		}

		cnt, err := walkSingleDisk(ctx, cmCli, dInfo, readDb)
		if err != nil {
			return 0, err
		}

		vuidCnt += cnt
	}

	return vuidCnt, nil
}

func walkSingleDisk(ctx context.Context, cmCli *clustermgr.Client, dh *clustermgr.BlobNodeDiskInfo, readDb bool) (int, error) {
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
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	allVuid := getDiskVuid(ctx, cmCli, dh.DiskID)
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
		_, exist := allVuid[vuid]
		fmt.Printf("  vuid=%d    status=%s    newDisk=%+v    existInCm=%t\n", vuid, vm.Status.String(), newDisk, exist)
	}
	return vuidCnt, nil
}

func getChunkMeta(db kvstore.KVStore, chunkId clustermgr.ChunkID) (vm core.VuidMeta) {
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

func parseChunkNameStr(name string) (chunkId clustermgr.ChunkID, err error) {
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

type diskVuid struct {
	diskID proto.DiskID
	vuid   proto.Vuid
}

func getDiskVuid(ctx context.Context, cmCli *clustermgr.Client, diskID proto.DiskID) map[proto.Vuid]diskVuid {
	ret, err := cmCli.ListVolumeUnit(ctx, &clustermgr.ListVolumeUnitArgs{DiskID: diskID})
	if err != nil {
		fmt.Printf("Err: Fail to list volume unit, err:%+v \n", err)
		return map[proto.Vuid]diskVuid{}
	}

	allVuid := make(map[proto.Vuid]diskVuid, len(ret))
	for i := range ret {
		allVuid[ret[i].Vuid] = diskVuid{diskID: ret[i].DiskID, vuid: ret[i].Vuid}
	}

	return allVuid
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
	cfg.LbConfig.Config.Tc.Auth = auth_proto.Config{EnableAuth: secret != "", Secret: secret}
	return clustermgr.New(cfg)
}

func parseAllLocalDiskIdsByCm(ctx context.Context, c *grumble.Context) (diskInfos []*clustermgr.BlobNodeDiskInfo, err error) {
	const prefix = "http://"
	const maxCnt = 100
	host := c.Flags.String("node_host")

	cmCli := newCmClient(c)
	marker := proto.DiskID(0)
	ret := clustermgr.ListDiskRet{}
	allDisk := make(map[proto.DiskID]*clustermgr.BlobNodeDiskInfo)
	for {
		ret, err = cmCli.ListDisk(ctx, &clustermgr.ListOptionArgs{Host: prefix + host, Count: maxCnt, Marker: marker})
		if err != nil {
			return nil, err
		}

		// there may be previously expired diskID
		for _, disk := range ret.Disks {
			allDisk[disk.DiskID] = disk
		}

		if ret.Marker == proto.InvalidDiskID {
			break
		}
		marker = ret.Marker
	}

	diskInfos = removeRedundantDiskID(allDisk)
	if len(diskInfos) == 0 {
		return nil, fmt.Errorf("error: empty, invalid disk ids")
	}
	return diskInfos, nil
}

func removeRedundantDiskID(allDisks map[proto.DiskID]*clustermgr.BlobNodeDiskInfo) []*clustermgr.BlobNodeDiskInfo {
	uniq := make(map[string]proto.DiskID)
	for _, disk := range allDisks {
		id, exist := uniq[disk.Path]
		// this id is monotonically increasing, so we take the latest(maximum) diskID in the same path
		if !exist || id < disk.DiskID {
			uniq[disk.Path] = disk.DiskID
		}
	}

	disks := make([]*clustermgr.BlobNodeDiskInfo, 0, len(uniq))
	for _, id := range uniq {
		disks = append(disks, allDisks[id])
	}
	return disks
}

func printDiskID(dInfos []*clustermgr.BlobNodeDiskInfo) {
	diskIDs := make([]proto.DiskID, len(dInfos))
	for i, dInfo := range dInfos {
		diskIDs[i] = dInfo.DiskID
	}
	sort.SliceStable(diskIDs, func(i, j int) bool {
		return diskIDs[i] < diskIDs[j]
	})

	fmt.Printf("check dropped disk. diskCnt=%d, disk ids=%v\n", len(diskIDs), diskIDs)
}
