package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/metanode"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	sdk "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/spf13/cobra"
	"github.com/tiglabs/raft"
)

const (
	cmdExtentUse                = "extent [command]"
	cmdExtentShort              = "Check extent consistency"
	cmdExtentInfo               = "info [partition] [extent]"
	cmdExtentInfoShort          = "show extent info"
	cmdCheckReplicaUse          = "check-replica volumeName"
	cmdCheckReplicaShort        = "Check replica consistency"
	cmdCheckLengthUse           = "check-length volumeName"
	cmdCheckLengthShort         = "Check extent length"
	cmdCheckExtentCrcUse        = "check-crc volumeName"
	cmdCheckExtentShort         = "Check extent crc"
	cmdCheckEkUse               = "check-ek volumeName"
	cmdCheckEkShort             = "Check inode extent key"
	cmdCheckNlinkUse            = "check-nlink volumeName"
	cmdCheckNlinkShort          = "Check inode nlink"
	cmdSearchExtentUse          = "search volumeName"
	cmdSearchExtentShort        = "Search extent key"
	cmdCheckGarbageUse          = "check-garbage volumeName"
	cmdCheckGarbageShort        = "Check garbage extents"
	cmdCheckTinyExtentHoleUse   = "check-tiny-hole"
	cmdCheckTinyExtentHoleShort = "Check tiny extents hole"
	cmdExtentRepair             = "repair [partition] [extent] [host]"
	cmdExtentRepairShort        = "repair extent"

)

const (
	checkTypeExtentReplica = 0
	checkTypeExtentLength  = 1
	checkTypeExtentCrc     = 2
	checkTypeInodeEk       = 3
	checkTypeInodeNlink    = 4
)

var client *sdk.MasterClient

type ExtentMd5 struct {
	PartitionID uint64 `json:"PartitionID"`
	ExtentID    uint64 `json:"ExtentID"`
	Md5         string `json:"md5"`
}

type DataPartition struct {
	VolName              string                    `json:"volName"`
	ID                   uint64                    `json:"id"`
	Size                 int                       `json:"size"`
	Used                 int                       `json:"used"`
	Status               int                       `json:"status"`
	Path                 string                    `json:"path"`
	Files                []storage.ExtentInfoBlock `json:"extents"`
	FileCount            int                       `json:"fileCount"`
	Replicas             []string                  `json:"replicas"`
	Peers                []proto.Peer              `json:"peers"`
	TinyDeleteRecordSize int64                     `json:"tinyDeleteRecordSize"`
	RaftStatus           *raft.Status              `json:"raftStatus"`
}

type Inode struct {
	Inode      uint64
	Type       uint32
	Size       uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	NLink      uint32

	Dens  []*Dentry
	Valid bool
}

type Dentry struct {
	ParentId uint64
	Name     string
	Inode    uint64
	Type     uint32

	Valid bool
}

type DataPartitionExtentCrcInfo struct {
	PartitionID       uint64
	ExtentCrcInfos    []ExtentCrcInfo
	LackReplicaExtent map[uint64][]string
	FailedExtent      map[uint64]error
}

type ExtentCrcInfo struct {
	FileID           uint64
	ExtentNum        int
	OffsetCrcAddrMap map[uint64]map[uint32][]string // offset:(crc:addrs)
}

func newExtentCmd(mc *sdk.MasterClient) *cobra.Command {
	client = mc
	var cmd = &cobra.Command{
		Use:   cmdExtentUse,
		Short: cmdExtentShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newExtentCheckCmd(checkTypeExtentReplica),
		newExtentCheckCmd(checkTypeExtentLength),
		newExtentCheckCmd(checkTypeExtentCrc),
		newExtentCheckCmd(checkTypeInodeEk),
		newExtentCheckCmd(checkTypeInodeNlink),
		newExtentSearchCmd(),
		newExtentGarbageCheckCmd(),
		newTinyExtentCheckHoleCmd(),
		newExtentGetCmd(),
		newExtentRepairCmd(),
	)
	return cmd
}

func newExtentGetCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdExtentInfo,
		Short: cmdExtentInfoShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			extentID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			dp, err := client.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				return
			}
			fmt.Printf("%-30v: %v\n", "Volume", dp.VolName)
			fmt.Printf("%-30v: %v\n", "Data Partition", partitionID)
			fmt.Printf("%-30v: %v\n", "Extent", extentID)
			fmt.Printf("%-30v: %v\n", "Hosts", strings.Join(dp.Hosts, ","))
			fmt.Println()
			if storage.IsTinyExtent(extentID) {
				stdout("%v\n", formatTinyExtentTableHeader())
			} else {
				stdout("%v\n", formatNormalExtentTableHeader())
			}

			for _, r := range dp.Replicas {
				dHost := fmt.Sprintf("%v:%v", strings.Split(r.Addr, ":")[0], client.DataNodeProfPort)
				dataClient := data.NewDataHttpClient(dHost, false)
				extent, err1 := dataClient.GetExtentInfo(partitionID, extentID)
				if err1 != nil {
					continue
				}
				if storage.IsTinyExtent(extentID) {
					extentHoles, _ := dataClient.GetExtentHoles(partitionID, extentID)
					stdout("%v\n", formatTinyExtent(r, extent, extentHoles))
				} else {
					stdout("%v\n", formatNormalExtent(r, extent))
				}

			}
		},
	}
	return cmd

}

func newExtentRepairCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdExtentRepair,
		Short: cmdExtentRepairShort,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			extentID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			host := args[2]
			dp, err := client.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				return
			}
			var exist bool
			for _, h := range dp.Hosts {
				if h == host {
					exist = true
					break
				}
			}
			if !exist {
				err = fmt.Errorf("host[%v] not exist in hosts[%v]", host, dp.Hosts)
				return
			}
			dHost := fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], client.DataNodeProfPort)
			dataClient := data.NewDataHttpClient(dHost, false)
			partition, err := dataClient.GetPartitionFromNode(partitionID)
			if err != nil {
				return
			}
			err = dataClient.RepairExtent(extentID, partition.Path, partitionID)
			if err != nil {
				return
			}
			fmt.Printf("repair success")
		},
	}
	return cmd
}

func newExtentCheckCmd(checkType int) *cobra.Command {
	var (
		use                string
		short              string
		path               string
		inodeStr           string
		metaPartitionId    uint64
		tinyOnly           bool
		tinyInUse          bool
		mpConcurrency      uint64
		inodeConcurrency   uint64
		extentConcurrency  uint64
		modifyTimeMin      string
		modifyTimeMax      string
		modifyTimestampMin int64
		modifyTimestampMax int64
	)
	if checkType == checkTypeExtentReplica {
		use = cmdCheckReplicaUse
		short = cmdCheckReplicaShort
	} else if checkType == checkTypeExtentLength {
		use = cmdCheckLengthUse
		short = cmdCheckLengthShort
	} else if checkType == checkTypeExtentCrc {
		use = cmdCheckExtentCrcUse
		short = cmdCheckExtentShort
	} else if checkType == checkTypeInodeEk {
		use = cmdCheckEkUse
		short = cmdCheckEkShort
	} else if checkType == checkTypeInodeNlink {
		use = cmdCheckNlinkUse
		short = cmdCheckNlinkShort
	}

	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				vol    = args[0]
				inodes []uint64
			)
			if modifyTimeMin != "" {
				minParsedTime, err := time.Parse("2006-01-02 15:04:05", modifyTimeMin)
				if err != nil {
					fmt.Println(err)
					return
				}
				modifyTimestampMin = minParsedTime.Unix()
			}
			if modifyTimeMax != "" {
				maxParsedTime, err := time.Parse("2006-01-02 15:04:05", modifyTimeMax)
				if err != nil {
					fmt.Println(err)
					return
				}
				modifyTimestampMax = maxParsedTime.Unix()
			}
			if len(inodeStr) > 0 {
				inodeSlice := strings.Split(inodeStr, ",")
				for _, inode := range inodeSlice {
					ino, err := strconv.Atoi(inode)
					if err != nil {
						continue
					}
					inodes = append(inodes, uint64(ino))
				}
			}

			switch checkType {
			case checkTypeExtentReplica, checkTypeExtentLength, checkTypeInodeEk, checkTypeInodeNlink:
				checkVol(vol, path, inodes, metaPartitionId, tinyOnly, tinyInUse, mpConcurrency, inodeConcurrency, extentConcurrency, checkType, modifyTimestampMin, modifyTimestampMax)
			case checkTypeExtentCrc:
				checkVolExtentCrc(vol, tinyOnly, util.MB*5)
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}

	cmd.Flags().StringVar(&path, "path", "", "path")
	cmd.Flags().StringVar(&inodeStr, "inode", "", "comma separated inodes")
	cmd.Flags().Uint64Var(&metaPartitionId, "mp", 0, "meta partition id")
	cmd.Flags().BoolVar(&tinyOnly, "tinyOnly", false, "check tiny extents only")
	cmd.Flags().BoolVar(&tinyInUse, "tinyInUse", false, "check tiny extents in use")
	cmd.Flags().Uint64Var(&mpConcurrency, "mpConcurrency", 1, "max concurrent checking meta partitions")
	cmd.Flags().Uint64Var(&inodeConcurrency, "inodeConcurrency", 1, "max concurrent checking inodes")
	cmd.Flags().Uint64Var(&extentConcurrency, "extentConcurrency", 1, "max concurrent checking extents")
	cmd.Flags().StringVar(&modifyTimeMin, "modifyTimeMin", "", "min modify time for inode")
	cmd.Flags().StringVar(&modifyTimeMax, "modifyTimeMax", "", "max modify time for inode")
	return cmd
}

func newTinyExtentCheckHoleCmd() *cobra.Command {
	var (
		use        = cmdCheckTinyExtentHoleUse
		short      = cmdCheckTinyExtentHoleShort
		scanLimit  uint64
		volumeStr  string
		autoRepair bool
		dpid       uint64
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Run: func(cmd *cobra.Command, args []string) {
			if scanLimit > 150 {
				stdout("scanLimit too high: %d\n", scanLimit)
				return
			}

			rServer := newRepairServer(autoRepair)
			log.LogInfof("fix tiny extent for master: %v", client.Leader())

			vols := loadSpecifiedVolumes()
			ids := loadSpecifiedPartitions()

			if dpid > 0 {
				ids = []uint64{dpid}
			}
			if volumeStr != "" {
				vols = []string{volumeStr}
			}

			log.LogInfo("check start")
			rServer.start()
			defer rServer.stop()

			rangeAllDataPartitions(scanLimit, vols, ids, func(vol *proto.SimpleVolView) {
				rServer.holeNumFd.Sync()
				rServer.holeSizeFd.Sync()
				rServer.availSizeFd.Sync()
				rServer.failedGetExtFd.Sync()
			}, rServer.checkAndRepairTinyExtents)
			log.LogInfo("check end")
			return
		},
	}
	cmd.Flags().Uint64Var(&scanLimit, "limit", 10, "limit rate")
	cmd.Flags().StringVar(&volumeStr, "volume", "", "fix by volume name")
	cmd.Flags().Uint64Var(&dpid, "partition", 0, "fix by data partition id")
	cmd.Flags().BoolVar(&autoRepair, "auto-repair", false, "true:scan bad tiny extent and send repair cmd to datanode automatically; false:only scan and record result, do not repair it")
	return cmd
}

func newExtentSearchCmd() *cobra.Command {
	var (
		use          = cmdSearchExtentUse
		short        = cmdSearchExtentShort
		concurrency  uint64
		dpStr        string
		extentStr    string
		extentOffset uint
		size         uint
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				vol     = args[0]
				dps     []uint64
				extents []uint64
			)
			if len(dpStr) > 0 {
				for _, v := range strings.Split(dpStr, ",") {
					dp, err := strconv.Atoi(v)
					if err != nil {
						continue
					}
					dps = append(dps, uint64(dp))
				}
			}
			if len(extentStr) > 0 {
				for _, v := range strings.Split(extentStr, ",") {
					extentRange := strings.Split(v, "-")
					if len(extentRange) == 2 {
						begin, err := strconv.Atoi(extentRange[0])
						if err != nil {
							continue
						}
						end, err := strconv.Atoi(extentRange[1])
						if err != nil {
							continue
						}
						for i := begin; i <= end; i++ {
							extents = append(extents, uint64(i))
						}
						continue
					}
					extent, err := strconv.Atoi(v)
					if err != nil {
						continue
					}
					extents = append(extents, uint64(extent))
				}
			}
			if len(dps) == 0 || (len(dps) > 1 && len(dps) != len(extents)) {
				stdout("invalid parameters.\n")
				return
			}
			searchExtent(vol, dps, extents, extentOffset, size, concurrency)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&dpStr, "dps", "", "comma separated data partitions")
	cmd.Flags().StringVar(&extentStr, "extents", "", "comma separated extents")
	cmd.Flags().UintVar(&extentOffset, "extentOffset", 0, "")
	cmd.Flags().UintVar(&size, "size", 0, "")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 1, "max concurrent searching inodes")
	return cmd
}

func searchExtent(vol string, dps []uint64, extents []uint64, extentOffset uint, size uint, concurrency uint64) {
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	inodes, _ := getFileInodesByMp(mps, 0, concurrency, 0, 0)
	extentMap := make(map[string]bool)
	var dp uint64
	for i := 0; i < len(extents); i++ {
		if len(dps) == 1 {
			dp = dps[0]
		} else {
			dp = dps[i]
		}
		extentMap[fmt.Sprintf("%d-%d", dp, extents[i])] = true
	}

	var wg sync.WaitGroup
	wg.Add(len(inodes))
	for i := 0; i < int(concurrency); i++ {
		go func(i int) {
			idx := 0
			for {
				if idx*int(concurrency)+i >= len(inodes) {
					break
				}
				inode := inodes[idx*int(concurrency)+i]
				extentsResp, err := getExtentsByInode(vol, inode, mps)
				if err != nil {
					stdout("get extents error: %v, inode: %d\n", err, inode)
					wg.Done()
					continue
				}
				for _, ek := range extentsResp.Extents {
					_, ok := extentMap[fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)]
					if ok {
						if size == 0 ||
							(ek.ExtentOffset >= uint64(extentOffset) && ek.ExtentOffset < uint64(extentOffset+size)) ||
							(ek.ExtentOffset+uint64(ek.Size) >= uint64(extentOffset) && ek.ExtentOffset+uint64(ek.Size) < uint64(extentOffset+size)) {
							stdout("inode: %d, ek: %s\n", inode, ek)
						}
					}
				}
				wg.Done()
				idx++
			}
		}(i)
	}
	wg.Wait()
}

func newExtentGarbageCheckCmd() *cobra.Command {
	var (
		use              = cmdCheckGarbageUse
		short            = cmdCheckGarbageShort
		all              bool
		active           bool
		dir              string
		clean            bool
		dpConcurrency    uint64
		mpConcurrency    uint64
		inodeConcurrency uint64
	)
	var cmd = &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				vol = args[0]
			)
			garbageCheck(vol, all, active, dir, clean, dpConcurrency, mpConcurrency, inodeConcurrency)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "Check all garbage extents (only for extents modified in last 7 days by default)")
	cmd.Flags().BoolVar(&active, "active", false, "Check garbage extents using active inodes of user file system (all inodes of metanode by default)")
	cmd.Flags().StringVar(&dir, "dir", ".", "Output file dir")
	cmd.Flags().BoolVar(&clean, "clean", false, "Clean garbage extents")
	cmd.Flags().Uint64Var(&dpConcurrency, "dpConcurrency", 1, "max concurrent checking data partitions")
	cmd.Flags().Uint64Var(&mpConcurrency, "mpConcurrency", 1, "max concurrent checking meta partitions")
	cmd.Flags().Uint64Var(&inodeConcurrency, "inodeConcurrency", 1, "max concurrent checking extents")
	return cmd
}

func garbageCheck(vol string, all bool, active bool, dir string, clean bool, dpConcurrency uint64, mpConcurrency uint64, inodeConcurrency uint64) {
	var (
		// map[dp][extent]size
		dataExtentMap = make(map[uint64]map[uint64]uint64)
		view          *proto.DataPartitionsView
		err           error
		wg            sync.WaitGroup
		ch            = make(chan uint64, 1000)
		mu            sync.Mutex
	)
	// get all extents from datanode, MUST get extents from datanode first in case of newly added extents being deleted
	view, err = client.ClientAPI().GetDataPartitions(vol)
	if err != nil {
		stdout("get data partitions error: %v\n", err)
		return
	}
	year, month, day := time.Now().Date()
	today := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
	wg.Add(len(view.DataPartitions))
	go func() {
		for _, dp := range view.DataPartitions {
			ch <- dp.PartitionID
		}
		close(ch)
	}()
	for i := 0; i < int(dpConcurrency); i++ {
		go func() {
			var dpInfo *DataPartition
			for dp := range ch {
				dpInfo, err = getExtentsByDp(dp, "")
				if err != nil {
					stdout("get extents error: %v, dp: %d\n", err, dp)
					os.Exit(0)
				}
				mu.Lock()
				_, ok := dataExtentMap[dp]
				if !ok {
					dataExtentMap[dp] = make(map[uint64]uint64)
				}
				for _, extent := range dpInfo.Files {
					if (all || today.Unix()-int64(extent[storage.ModifyTime]) >= 604800) && extent[storage.FileID] > storage.MinExtentID {
						dataExtentMap[dp][extent[storage.FileID]] = extent[storage.Size]
					}
				}
				mu.Unlock()
				wg.Done()
			}
			dpInfo = nil
		}()
	}
	wg.Wait()
	view = nil

	// get all extents from metanode
	var inodes []uint64
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	if active {
		inodes, err = getAllInodesByPath(vol, "")
	} else {
		inodes, err = getFileInodesByMp(mps, 0, mpConcurrency, 0, 0)
	}
	if err != nil {
		stdout("get all inodes error: %v\n", err)
		return
	}

	metaExtentMap := make(map[uint64]map[uint64]bool)
	extents, err := getExtentsByInodes(vol, inodes, inodeConcurrency, mps)
	inodes, mps = nil, nil
	if err != nil {
		stdout("get extents error: %v\n", err)
		return
	}
	for _, ek := range extents {
		_, ok := metaExtentMap[ek.PartitionId]
		if !ok {
			metaExtentMap[ek.PartitionId] = make(map[uint64]bool)
		}
		metaExtentMap[ek.PartitionId][ek.ExtentId] = true
	}
	extents = nil

	garbage := make(map[uint64][]uint64)
	var total uint64
	for dp := range dataExtentMap {
		for extent, size := range dataExtentMap[dp] {
			_, ok := metaExtentMap[dp]
			if ok {
				_, ok = metaExtentMap[dp][extent]
			}
			if !ok {
				garbage[dp] = append(garbage[dp], extent)
				total += size
			}
		}
	}

	stdout("garbageCheck, vol: %s, garbage size: %d\n", vol, total)
	os.Mkdir(fmt.Sprintf("%s/%s", dir, vol), os.ModePerm)
	for dp := range garbage {
		sort.Slice(garbage[dp], func(i, j int) bool { return garbage[dp][i] < garbage[dp][j] })
		strSlice := make([]string, len(garbage[dp]))
		for i, extent := range garbage[dp] {
			strSlice[i] = fmt.Sprintf("%d", extent)
		}
		ioutil.WriteFile(fmt.Sprintf("%s/%s/%d", dir, vol, dp), []byte(strings.Join(strSlice, "\n")), 0666)
		if clean {
			batchDeleteExtent(dp, garbage[dp])
		}
	}
}

func batchDeleteExtent(partitionId uint64, extents []uint64) (err error) {
	if len(extents) == 0 {
		return
	}
	stdout("start delete extent, partitionId: %d, extents len: %d\n", partitionId, len(extents))
	partition, err := client.AdminAPI().GetDataPartition("", partitionId)
	if err != nil {
		stdout("GetDataPartition error: %v, PartitionId: %v\n", err, partitionId)
		return
	}
	var gConnPool = util.NewConnectPool()
	conn, err := gConnPool.GetConnect(partition.Hosts[0])
	defer func() {
		if err != nil {
			gConnPool.PutConnect(conn, true)
		} else {
			gConnPool.PutConnect(conn, false)
		}
	}()

	if err != nil {
		stdout("get conn from pool error: %v, partitionId: %d\n", err, partitionId)
		return
	}
	dp := &metanode.DataPartition{
		PartitionID: partitionId,
		Hosts:       partition.Hosts,
	}
	eks := make([]*proto.ExtentKey, len(extents))
	for i := 0; i < len(extents); i++ {
		eks[i] = &proto.ExtentKey{
			PartitionId: partitionId,
			ExtentId:    extents[i],
		}
	}
	packet := metanode.NewPacketToBatchDeleteExtent(context.Background(), dp, eks)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		stdout("write to dataNode error: %v, logId: %s\n", err, packet.GetUniqueLogId())
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		stdout("read response from dataNode error: %s, logId: %s\n", err, packet.GetUniqueLogId())
		return
	}
	if packet.ResultCode != proto.OpOk {
		stdout("batch delete extent response: %s, logId: %s\n", packet.GetResultMsg(), packet.GetUniqueLogId())
	}
	stdout("finish delete extent, partitionId: %d, extents len: %v\n", partitionId, len(extents))
	return
}

func checkVol(vol string, path string, inodes []uint64, metaPartitionId uint64, tinyInUse bool, tinyOnly bool, mpConcurrency uint64, inodeConcurrency uint64, extentConcurrency uint64, checkType int, modifyTimeMin int64, modifyTimeMax int64) {
	defer func() {
		msg := fmt.Sprintf("checkVol, vol:%s, path%s", vol, path)
		if r := recover(); r != nil {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			stdout("%s%s\n", msg, stack)
		}
	}()
	stdout("begin check, vol:%s\n", vol)
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	if len(inodes) == 0 && path != "" {
		inodes, _ = getAllInodesByPath(vol, path)
	}
	if len(inodes) > 0 {
		checkInodes(vol, mps, inodes, tinyOnly, tinyInUse, inodeConcurrency, extentConcurrency, checkType)
		stdout("finish check, vol:%s\n", vol)
		return
	}

	var wg sync.WaitGroup
	mpCh := make(chan uint64, 1000)
	wg.Add(len(mps))
	go func() {
		for _, mp := range mps {
			mpCh <- mp.PartitionID
		}
		close(mpCh)
	}()

	for i := 0; i < int(mpConcurrency); i++ {
		go func() {
			for mp := range mpCh {
				if metaPartitionId > 0 && mp != metaPartitionId {
					wg.Done()
					continue
				}
				stdout("begin check, vol:%s, mpId: %d\n", vol, mp)
				if checkType == checkTypeInodeNlink {
					checkVolNlink(mps, mp, modifyTimeMin, modifyTimeMax)
				} else {
					inodes, _ = getFileInodesByMp(mps, mp, 1, modifyTimeMin, modifyTimeMax)
					checkInodes(vol, mps, inodes, tinyOnly, tinyInUse, inodeConcurrency, extentConcurrency, checkType)
				}
				stdout("finish check, vol:%s, mpId: %d\n", vol, mp)
				wg.Done()
			}
		}()
	}
	wg.Wait()
	stdout("finish check, vol:%s\n", vol)
}

func checkVolNlink(mps []*proto.MetaPartitionView, metaPartitionId uint64, modifyTimeMin int64, modifyTimeMax int64) {
	for _, mp := range mps {
		if metaPartitionId > 0 && mp.PartitionID != metaPartitionId {
			continue
		}
		var preMap map[uint64]uint32
		for i, host := range mp.Members {
			inodes, err := getInodesByMp(mp.PartitionID, host)
			if err != nil {
				stdout(err.Error())
				return
			}
			nlinkMap := make(map[uint64]uint32)
			for _, inode := range inodes {
				if modifyTimeMin > 0 && inode.ModifyTime < modifyTimeMin {
					continue
				}
				if modifyTimeMax > 0 && inode.ModifyTime > modifyTimeMax {
					continue
				}
				nlinkMap[inode.Inode] = inode.NLink
			}
			if i == 0 {
				preMap = nlinkMap
				continue
			}

			for ino, nlink := range nlinkMap {
				preNlink, ok := preMap[ino]
				if !ok {
					stdout("checkVolNlink ERROR, mpId: %d, ino %d of %s not exist in %s\n", mp.PartitionID, ino, mp.Members[i], mp.Members[i-1])
				} else if nlink != preNlink {
					stdout("checkVolNlink ERROR, mpId: %d, ino: %d, nlink %d of %s not equals to nlink %d of %s\n", mp.PartitionID, ino, nlink, mp.Members[i], preNlink, mp.Members[i-1])
				}
			}
			for preIno := range preMap {
				_, ok := nlinkMap[preIno]
				if !ok {
					stdout("checkVolNlink ERROR, mpId: %d, ino %d of %s not exist in %s\n", mp.PartitionID, preIno, mp.Members[i-1], mp.Members[i])
				}
			}
			preMap = nlinkMap
		}
	}
}

func checkInodes(vol string, mps []*proto.MetaPartitionView, inodes []uint64, tinyOnly bool, tinyInUse bool, inodeConcurrency uint64, extentConcurrency uint64, checkType int) {
	var (
		checkedExtent sync.Map
		wg            sync.WaitGroup
	)
	inoCh := make(chan uint64, 1000*1000)
	wg.Add(len(inodes))
	go func() {
		for _, ino := range inodes {
			inoCh <- ino
		}
		close(inoCh)
	}()

	for i := 0; i < int(inodeConcurrency); i++ {
		go func() {
			for ino := range inoCh {
				if checkType == checkTypeInodeEk {
					checkInodeEk(vol, ino, mps)
				} else {
					checkInode(vol, ino, checkedExtent, tinyOnly, tinyInUse, extentConcurrency, checkType, mps)
				}
				wg.Done()
			}
		}()
	}
	wg.Wait()
}

func getAllInodesByPath(vol string, path string) (inodes []uint64, err error) {
	ctx := context.Background()
	var mw *meta.MetaWrapper
	mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        vol,
		Masters:       client.Nodes(),
		ValidateOwner: false,
		InfiniteRetry: true,
	})
	if err != nil {
		stdout("NewMetaWrapper fails, err:%v\n", err)
		return
	}
	var ino uint64
	ino, err = mw.LookupPath(ctx, path)
	if err != nil {
		stdout("LookupPath fails, err:%v\n", err)
		return
	}
	return getChildInodesByParent(mw, vol, ino)
}

func getChildInodesByParent(mw *meta.MetaWrapper, vol string, parent uint64) (inodes []uint64, err error) {
	ctx := context.Background()
	var dentries []proto.Dentry
	dentries, err = mw.ReadDir_ll(ctx, parent)
	if err != nil {
		stdout("ReadDir_ll fails, err:%v\n", err)
		return
	}
	var newInodes []uint64
	for _, dentry := range dentries {
		if proto.IsRegular(dentry.Type) {
			inodes = append(inodes, dentry.Inode)
		} else if proto.IsDir(dentry.Type) {
			newInodes, err = getChildInodesByParent(mw, vol, dentry.Inode)
			if err != nil {
				return
			}
			inodes = append(inodes, newInodes...)
		}
	}
	return
}

func getFileInodesByMp(mps []*proto.MetaPartitionView, metaPartitionId uint64, concurrency uint64, modifyTimeMin int64, modifyTimeMax int64) (inodes []uint64, err error) {
	var (
		inos    []*Inode
		mpCount uint64
		wg      sync.WaitGroup
		mu      sync.Mutex
		ch      = make(chan *proto.MetaPartitionView, 1000)
	)
	for _, mp := range mps {
		if metaPartitionId > 0 && mp.PartitionID != metaPartitionId {
			continue
		}
		mpCount++
	}
	if mpCount == 0 {
		return
	}

	wg.Add(int(mpCount))
	go func() {
		for _, mp := range mps {
			if metaPartitionId > 0 && mp.PartitionID != metaPartitionId {
				continue
			}
			ch <- mp
		}
		close(ch)
	}()

	for i := 0; i < int(concurrency); i++ {
		go func() {
			for mp := range ch {
				inos, err = getInodesByMp(mp.PartitionID, mp.LeaderAddr)
				if err != nil {
					stdout("get inodes error: %v, mp: %d\n", err, mp.PartitionID)
					os.Exit(0)
				}
				mu.Lock()
				for _, ino := range inos {
					if !proto.IsRegular(ino.Type) {
						continue
					}
					if modifyTimeMin > 0 && ino.ModifyTime < modifyTimeMin {
						continue
					}
					if modifyTimeMax > 0 && ino.ModifyTime > modifyTimeMax {
						continue
					}
					inodes = append(inodes, ino.Inode)
				}
				mu.Unlock()
				wg.Done()
			}
		}()
	}
	wg.Wait()
	return
}

func checkInode(vol string, inode uint64, checkedExtent sync.Map, tinyOnly bool, tinyInUse bool, concurrency uint64, checkType int, mps []*proto.MetaPartitionView) {
	var err error
	var (
		extentsResp *proto.GetExtentsResponse
		errCount    int = 0
		wg          sync.WaitGroup
	)
	extentsResp, err = getExtentsByInode(vol, inode, mps)
	if err != nil {
		return
	}

	stdout("begin check, vol:%s, inode: %d, extent count: %d\n", vol, inode, len(extentsResp.Extents))
	ekCh := make(chan proto.ExtentKey)
	var length int
	for _, ek := range extentsResp.Extents {
		if !tinyOnly || storage.IsTinyExtent(ek.ExtentId) {
			length += 1
		}
	}
	wg.Add(length)
	go func() {
		for _, ek := range extentsResp.Extents {
			if !tinyOnly || storage.IsTinyExtent(ek.ExtentId) {
				ekCh <- ek
			}
		}
		close(ekCh)
	}()
	var idx int32
	for i := 0; i < int(concurrency); i++ {
		go func(client *sdk.MasterClient, checkedExtent sync.Map) {
			for ek := range ekCh {
				if checkType == checkTypeExtentReplica {
					checkExtentReplica(&ek, tinyInUse, checkedExtent)
				} else if checkType == checkTypeExtentLength {
					checkExtentLength(&ek, checkedExtent)
				}
				atomic.AddInt32(&idx, 1)
				if idx%100 == 0 {
					stdout("%d extents checked\n", idx)
				}
				wg.Done()
			}
		}(client, checkedExtent)
	}
	wg.Wait()
	stdout("finish check, vol:%s, inode: %d, err count: %d\n", vol, inode, errCount)
}

func checkInodeEk(vol string, inode uint64, mps []*proto.MetaPartitionView) {
	var hosts []string
	var mpId uint64
	for _, mp := range mps {
		if inode >= mp.Start && inode < mp.End {
			hosts = mp.Members
			mpId = mp.PartitionID
			break
		}
	}
	eks := make([]*proto.GetExtentsResponse, len(hosts))
	for i, host := range hosts {
		extents, err := getExtentsByInodeAndAddr(mpId, inode, host)
		if err != nil {
			return
		}
		eks[i] = extents
	}
	for i := 1; i < len(hosts); i++ {
		if len(eks[i].Extents) != len(eks[i-1].Extents) {
			stdout("checkInodeEk ERROR, inode: %d, host: %s, eks len: %d, host: %s, eks len: %d", inode, hosts[i-1], len(eks[i-1].Extents), hosts[i], len(eks[i].Extents))
		}
	}
}

func checkExtentReplica(ek *proto.ExtentKey, tinyInUse bool, checkedExtent sync.Map) (same bool, err error) {
	var (
		ok        bool
		ekStr     string = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
		partition *proto.DataPartitionInfo
	)
	if tinyInUse {
		ekStr = fmt.Sprintf("%s-%d-%d", ekStr, ek.ExtentOffset, ek.Size)
	}
	if _, ok = checkedExtent.LoadOrStore(ekStr, true); ok {
		return true, nil
	}
	partition, err = client.AdminAPI().GetDataPartition("", ek.PartitionId)
	if err != nil {
		stdout("GetDataPartition PartitionId(%v) err(%v)\n", ek.PartitionId, err)
		return
	}

	var (
		replicas = make([]struct {
			partitionId  uint64
			extentId     uint64
			extentOffset uint64
			size         uint32
			datanode     string
			md5          string
		}, len(partition.Replicas))
		md5Map       = make(map[string]int)
		extentMd5    *ExtentMd5
		extentOffset uint64
		size         uint32
	)
	for idx, replica := range partition.Replicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], client.DataNodeProfPort)
		if tinyInUse {
			extentOffset = ek.ExtentOffset
			size = ek.Size
		}
		extentMd5, err = getExtentMd5(datanode, ek.PartitionId, ek.ExtentId, extentOffset, size)
		if err != nil {
			stdout("getExtentMd5 datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
			return
		}
		replicas[idx].partitionId = ek.PartitionId
		replicas[idx].extentId = ek.ExtentId
		replicas[idx].extentOffset = ek.ExtentOffset
		replicas[idx].size = ek.Size
		replicas[idx].datanode = datanode
		replicas[idx].md5 = extentMd5.Md5
		if _, ok = md5Map[replicas[idx].md5]; ok {
			md5Map[replicas[idx].md5]++
		} else {
			md5Map[replicas[idx].md5] = 1
		}
	}
	if len(md5Map) == 1 {
		return true, nil
	}
	for _, r := range replicas {
		msg := fmt.Sprintf("dp: %d, extent: %d, extentOffset:%d, size:%d, datanode: %s, md5: %s\n", r.partitionId, r.extentId, r.extentOffset, r.size, r.datanode, r.md5)
		if _, ok = md5Map[r.md5]; ok && md5Map[r.md5] > len(partition.Replicas)/2 {
			stdout(msg)
		} else {
			stdout("ERROR %s", msg)
		}
	}
	return
}

func checkExtentLength(ek *proto.ExtentKey, checkedExtent sync.Map) (same bool, err error) {
	var (
		ok        bool
		ekStr     string = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
		partition *proto.DataPartitionInfo
		extent    storage.ExtentInfoBlock
	)
	if _, ok = checkedExtent.LoadOrStore(ekStr, true); ok {
		return true, nil
	}
	partition, err = client.AdminAPI().GetDataPartition("", ek.PartitionId)
	if err != nil {
		stdout("GetDataPartition ERROR: %v, PartitionId: %d\n", err, ek.PartitionId)
		return
	}

	datanode := fmt.Sprintf("%s:%d", strings.Split(partition.Replicas[0].Addr, ":")[0], client.DataNodeProfPort)
	extent, err = getExtent(ek.PartitionId, ek.ExtentId)
	if err != nil {
		stdout("getExtentFromData ERROR: %v, datanode: %v, PartitionId: %v, ExtentId: %v\n", err, datanode, ek.PartitionId, ek.ExtentId)
		return
	}
	if ek.ExtentOffset+uint64(ek.Size) > extent[storage.Size] {
		stdout("ERROR ek:%v, extent:%v\n", ek, extent)
		return false, nil
	}
	return true, nil
}

func checkVolExtentCrc(vol string, tiny bool, validateStep uint64) {
	defer func() {
		msg := fmt.Sprintf("checkVolExtentCrc, vol:%s ", vol)
		if r := recover(); r != nil {
			var stack string
			if r != nil {
				stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			}
			stdout("%s%s\n", msg, stack)
		}
	}()
	stdout("begin check, vol:%s\n", vol)
	dataPartitionsView, err := client.ClientAPI().GetDataPartitions(vol)
	if err != nil {
		stdout("not exist, vol:%s\n", vol)
		return
	}
	stdout("vol:%s dp count:%v\n", vol, len(dataPartitionsView.DataPartitions))
	data.StreamConnPool = util.NewConnectPoolWithTimeoutAndCap(0, 10, 30, int64(1*time.Second))
	wg := new(sync.WaitGroup)
	for _, dataPartition := range dataPartitionsView.DataPartitions {
		wg.Add(1)
		go func(dp *proto.DataPartitionResponse) {
			defer wg.Done()
			dpTinyExtentCrcInfo, err1 := validateDataPartitionTinyExtentCrc(dp, validateStep)
			if err1 != nil {
				stdoutRed(fmt.Sprintf("dp:%v err:%v \n", dp.PartitionID, err1))
			}
			if dpTinyExtentCrcInfo == nil {
				return
			}
			if len(dpTinyExtentCrcInfo.ExtentCrcInfos) != 0 {
				stdout("dp:%v diff tiny ExtentCrcInfo Count:%v \n", dp.PartitionID, len(dpTinyExtentCrcInfo.ExtentCrcInfos))
				for _, extentCrcInfo := range dpTinyExtentCrcInfo.ExtentCrcInfos {
					stdout("dp:%v tinyExtentID:%v detail[%v] \n", dp.PartitionID, extentCrcInfo.FileID, extentCrcInfo)
				}
			}
			if len(dpTinyExtentCrcInfo.LackReplicaExtent) != 0 {
				stdout("dp:%v LackReplicaExtent:%v \n", dp.PartitionID, dpTinyExtentCrcInfo.LackReplicaExtent)
			}
			if len(dpTinyExtentCrcInfo.FailedExtent) != 0 {
				stdout("dp:%v FailedExtent:%v \n", dp.PartitionID, dpTinyExtentCrcInfo.FailedExtent)
			}
		}(dataPartition)
	}
	wg.Wait()
	stdout("finish check, vol:%s\n", vol)
}

func validateDataPartitionTinyExtentCrc(dataPartition *proto.DataPartitionResponse, validateStep uint64) (dpTinyExtentCrcInfo *DataPartitionExtentCrcInfo, err error) {
	if dataPartition == nil {
		return nil, fmt.Errorf("action[validateDataPartitionTinyExtentCrc] dataPartition is nil")
	}
	if validateStep < util.MB {
		validateStep = util.MB
	}
	dpReplicaInfos, err := getDataPartitionReplicaInfos(dataPartition)
	if err != nil {
		return
	}
	// map[uint64]map[string]uint64 --> extentID:(host:extent size)
	extentReplicaHostSizeMap := make(map[uint64]map[string]uint64, 0)
	for replicaHost, partition := range dpReplicaInfos {
		for _, extentInfo := range partition.Files {
			if !storage.IsTinyExtent(extentInfo[storage.FileID]) {
				continue
			}
			replicaSizeMap, ok := extentReplicaHostSizeMap[extentInfo[storage.FileID]]
			if !ok {
				replicaSizeMap = make(map[string]uint64)
			}
			replicaSizeMap[replicaHost] = extentInfo[storage.Size]
			extentReplicaHostSizeMap[extentInfo[storage.FileID]] = replicaSizeMap
		}
	}

	lackReplicaExtent := make(map[uint64][]string)
	failedExtent := make(map[uint64]error)
	extentCrcInfos := make([]ExtentCrcInfo, 0)
	for extentID, replicaSizeMap := range extentReplicaHostSizeMap {
		// record lack replica extent id
		if len(replicaSizeMap) != len(dpReplicaInfos) {
			for replicaHost := range dpReplicaInfos {
				_, ok := replicaSizeMap[replicaHost]
				if !ok {
					lackReplicaExtent[extentID] = append(lackReplicaExtent[extentID], replicaHost)
				}
			}
		}

		extentCrcInfo, err1 := validateTinyExtentCrc(dataPartition, extentID, replicaSizeMap, validateStep)
		if err1 != nil {
			failedExtent[extentID] = err1
			continue
		}
		if extentCrcInfo.OffsetCrcAddrMap != nil && len(extentCrcInfo.OffsetCrcAddrMap) != 0 {
			extentCrcInfos = append(extentCrcInfos, extentCrcInfo)
		}
	}

	dpTinyExtentCrcInfo = &DataPartitionExtentCrcInfo{
		PartitionID:       dataPartition.PartitionID,
		ExtentCrcInfos:    extentCrcInfos,
		LackReplicaExtent: lackReplicaExtent,
		FailedExtent:      failedExtent,
	}
	return
}

// 1.以最小size为基准
// 2.以1M(可配置，最小1M)步长，读取三个副本前4K的数据
// 3.分别计算CRC并比较
func validateTinyExtentCrc(dataPartition *proto.DataPartitionResponse, extentID uint64, replicaSizeMap map[string]uint64,
	validateStep uint64) (extentCrcInfo ExtentCrcInfo, err error) {
	if dataPartition == nil {
		err = fmt.Errorf("action[validateTinyExtentCrc] dataPartition is nil")
		return
	}
	if validateStep < util.MB {
		validateStep = util.MB
	}
	minSize := uint64(math.MaxUint64)
	for _, size := range replicaSizeMap {
		if minSize > size {
			minSize = size
		}
	}
	offsetCrcAddrMap := make(map[uint64]map[uint32][]string) // offset:(crc:addrs)
	offset := uint64(0)
	size := uint64(util.KB * 4)
	for {
		// minSize 可能因为4K对齐，实际上进行了补齐
		if offset+size >= minSize {
			break
		}
		// read calculate compare
		crcLocAddrMapTmp := make(map[uint32][]string)
		for addr := range replicaSizeMap {
			crcData := make([]byte, size)
			err1 := readExtent(dataPartition, addr, extentID, crcData, offset, int(size))
			if err1 != nil {
				err = fmt.Errorf("addr[%v] extentId[%v] offset[%v] size[%v] err:%v", addr, extentID, int(offset), int(size), err1)
				return
			}
			crc := crc32.ChecksumIEEE(crcData)
			crcLocAddrMapTmp[crc] = append(crcLocAddrMapTmp[crc], addr)
		}
		if len(crcLocAddrMapTmp) >= 2 {
			offsetCrcAddrMap[offset] = crcLocAddrMapTmp
		}
		offset += validateStep
	}
	extentCrcInfo = ExtentCrcInfo{
		FileID:           extentID,
		ExtentNum:        len(replicaSizeMap),
		OffsetCrcAddrMap: offsetCrcAddrMap,
	}
	return
}

func getInodesByMp(metaPartitionId uint64, addr string) (Inodes []*Inode, err error) {
	httpClient := http.Client{
		Timeout: 2 * time.Minute,
	}
	resp, err := httpClient.Get(fmt.Sprintf("http://%s:%d/getAllInodes?pid=%d", strings.Split(addr, ":")[0], client.MetaNodeProfPort, metaPartitionId))
	if err != nil {
		return nil, fmt.Errorf("Get all inode info failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get all inode info read all body failed: %v", err)
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(data, body); err != nil {
		return nil, fmt.Errorf("Unmarshal all inode info body failed: %v", err)
	}

	var Result []*Inode
	if err = json.Unmarshal(body.Data, &Result); err != nil {
		return nil, fmt.Errorf("Unmarshal all inode info failed: %v", err)
	}

	stdout("getInodesByMp, mp: %d, addr: %s, total: %d\n", metaPartitionId, addr, len(Result))
	return Result, err
}

func getExtentsByInodes(vol string, inodes []uint64, concurrency uint64, mps []*proto.MetaPartitionView) (extents []proto.ExtentKey, err error) {
	var wg sync.WaitGroup
	inoCh := make(chan uint64, 1024)
	wg.Add(len(inodes))
	go func() {
		for _, ino := range inodes {
			inoCh <- ino
		}
		close(inoCh)
	}()

	resultCh := make(chan *proto.GetExtentsResponse, 1024)
	for i := 0; i < int(concurrency); i++ {
		go func() {
			for ino := range inoCh {
				re, tmpErr := getExtentsByInode(vol, ino, mps)
				if tmpErr != nil {
					err = fmt.Errorf("get extents from inode err: %v, inode: %d", tmpErr, ino)
					resultCh <- nil
				} else {
					resultCh <- re
				}
				wg.Done()
			}
		}()
	}

	var wgResult sync.WaitGroup
	wgResult.Add(len(inodes))
	go func() {
		for re := range resultCh {
			if re == nil {
				wgResult.Done()
				continue
			}
			extents = append(extents, re.Extents...)
			wgResult.Done()
		}
	}()
	wg.Wait()
	close(resultCh)
	wgResult.Wait()
	if err != nil {
		extents = extents[:0]
	}
	return
}

func getExtentsByInode(vol string, inode uint64, mps []*proto.MetaPartitionView) (re *proto.GetExtentsResponse, err error) {
	var addr string
	var mpId uint64
	for _, mp := range mps {
		if inode >= mp.Start && inode < mp.End {
			addr = mp.LeaderAddr
			mpId = mp.PartitionID
			break
		}
	}
	return getExtentsByInodeAndAddr(mpId, inode, addr)
}

func getExtentsByInodeAndAddr(mpId uint64, inode uint64, addr string) (re *proto.GetExtentsResponse, err error) {
	url := fmt.Sprintf("http://%s:%d/getExtentsByInode?pid=%d&ino=%d", strings.Split(addr, ":")[0], client.MetaNodeProfPort, mpId, inode)
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var data []byte
	if data, err = parseResp(respData); err != nil {
		return
	}
	re = &proto.GetExtentsResponse{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func getExtentsByDp(partitionId uint64, replicaAddr string) (re *DataPartition, err error) {
	if replicaAddr == "" {
		partition, err := client.AdminAPI().GetDataPartition("", partitionId)
		if err != nil {
			return nil, err
		}
		replicaAddr = partition.Hosts[0]
	}
	addressInfo := strings.Split(replicaAddr, ":")
	datanode := fmt.Sprintf("%s:%d", addressInfo[0], client.DataNodeProfPort)
	url := fmt.Sprintf("http://%s/partition?id=%d", datanode, partitionId)
	httpClient := http.Client{
		Timeout: 2 * time.Minute,
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var data []byte
	if data, err = parseResp(respData); err != nil {
		return
	}
	re = &DataPartition{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
		return
	}
	stdout("getExtentsByDp, dp: %d, addr: %s, total: %d\n", partitionId, replicaAddr, re.FileCount)
	return
}

func getExtent(partitionId uint64, extentId uint64) (re storage.ExtentInfoBlock, err error) {
	partition, err := client.AdminAPI().GetDataPartition("", partitionId)
	datanode := partition.Hosts[0]
	addressInfo := strings.Split(datanode, ":")
	datanode = fmt.Sprintf("%s:%d", addressInfo[0], client.DataNodeProfPort)
	url := fmt.Sprintf("http://%s/extent?partitionID=%d&extentID=%d", datanode, partitionId, extentId)
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var data []byte
	if data, err = parseResp(respData); err != nil {
		return
	}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == storage.EmptyExtentBlock {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func getExtentMd5(datanode string, dpId uint64, extentId uint64, extentOffset uint64, size uint32) (re *ExtentMd5, err error) {
	var (
		resp *http.Response
		data []byte
		url  string = fmt.Sprintf("http://%s/computeExtentMd5?id=%d&extent=%d&offset=%d&size=%d", datanode, dpId, extentId, extentOffset, size)
	)
	if resp, err = http.Get(url); err != nil {
		return
	}
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if data, err = parseResp(data); err != nil {
		return
	}
	re = &ExtentMd5{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("Get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func readExtent(dp *proto.DataPartitionResponse, addr string, extentId uint64, d []byte, offset uint64, size int) (err error) {
	ctx := context.Background()
	ek := &proto.ExtentKey{PartitionId: dp.PartitionID, ExtentId: extentId}
	dataPartition := &data.DataPartition{
		ClientWrapper:         &data.Wrapper{},
		DataPartitionResponse: *dp,
	}
	dataPartition.ClientWrapper.SetConnConfig()
	sc := data.NewStreamConnWithAddr(dataPartition, addr)
	reqPacket := data.NewReadPacket(ctx, ek, int(offset), size, 0, offset, true)
	req := data.NewExtentRequest(0, 0, d, nil)
	_, _, _, err = dataPartition.SendReadCmdToDataPartition(sc, reqPacket, req)
	return
}

func getDataPartitionReplicaInfos(dataPartition *proto.DataPartitionResponse) (dpReplicaInfos map[string]*DataPartition, err error) {
	if dataPartition == nil {
		return nil, fmt.Errorf("action[getDataPartitionReplicaInfos] dataPartition is nil")
	}
	dpReplicaInfos = make(map[string]*DataPartition, len(dataPartition.Hosts))
	for _, replicaHost := range dataPartition.Hosts {
		extentsFromTargetDatanode, err1 := getExtentsByDp(dataPartition.PartitionID, replicaHost)
		if err1 != nil {
			err = fmt.Errorf("action[getExtentsByDpFromTargetDatanode] partitionId:%v replicaHost:%v err:%v", dataPartition.PartitionID, replicaHost, err1)
			return
		}
		dpReplicaInfos[replicaHost] = extentsFromTargetDatanode
	}
	return
}

func parseResp(resp []byte) (data []byte, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(resp, &body); err != nil {
		return
	}
	data = body.Data
	return
}
