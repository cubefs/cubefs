package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/sdk/http_client"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/sdk/data"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/spf13/cobra"
	"github.com/tiglabs/raft"
)

const (
	cmdExtentUse                = "extent [command]"
	cmdExtentShort              = "Check extent consistency"
	cmdExtentInfo               = "info [partition] [extent]"
	cmdExtentInfoShort          = "show extent info"
	cmdExtentRepair             = "repair [partition] [extent(split by `-`)] [host]"
	cmdExtentRepairShort        = "repair extent"
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
	cmdCheckTinyExtentHoleShort = "check tiny extent hole size and available size"
	cmdCheckExtentReplicaShort  = "check extent replica "
	cmdExtentDelParse           = "parse"
	cmdExtentDelParseShort      = "parse meta/data extent del file"
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
		newExtentCheckByIdCmd(mc),
		newExtentParseCmd(),
	)
	return cmd
}

func newExtentGetCmd() *cobra.Command {
	var checkRetry bool
	var getMd5 bool
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
			if proto.IsTinyExtent(extentID) {
				stdout("%v\n", formatTinyExtentTableHeader())
			} else {
				stdout("%v\n", formatNormalExtentTableHeader())
			}

			minSize := uint64(math.MaxUint64)
			for _, r := range dp.Replicas {
				dHost := fmt.Sprintf("%v:%v", strings.Split(r.Addr, ":")[0], client.DataNodeProfPort)
				dataClient := http_client.NewDataClient(dHost, false)

				extent, err1 := dataClient.GetExtentInfo(partitionID, extentID)
				if err1 != nil {
					continue
				}
				var md5Sum string
				if extent[proto.ExtentInfoSize] < 16*unit.GB {
					md5Sum, err = dataClient.ComputeExtentMd5(partitionID, extentID, 0, extent[proto.ExtentInfoSize]-uint64(proto.PageSize))
					if err != nil {
						md5Sum = "null"
					}
				} else {
					md5Sum = "null"
				}
				if proto.IsTinyExtent(extentID) {
					extentHoles, _ := dataClient.GetExtentHoles(partitionID, extentID)
					stdout("%v\n", formatTinyExtent(r, extent, extentHoles, md5Sum))
				} else {
					stdout("%v\n", formatNormalExtent(r, extent, md5Sum))
				}
				if extent == nil {
					continue
				}
				if minSize > extent[proto.ExtentInfoSize] {
					minSize = extent[proto.ExtentInfoSize]
				}
			}
			blockSize := 128
			var wrongBlocks []int
			if !proto.IsTinyExtent(extentID) {
				stdout("wrongBlocks:\n")
				if wrongBlocks, err = checkExtentBlockCrc(dp.Replicas, client, partitionID, extentID); err != nil {
					stdout("err: %v", err)
					return
				}
				stdout("found: %v blocks at first scan(block size: %vKB), wrong block: %v\n", len(wrongBlocks), blockSize, wrongBlocks)
				stdout("wrong offsets:")
				for _, b := range wrongBlocks {
					stdout("%v-%v,", b*128*1024, b*128*1024+128*1024)
				}
				stdout("\n")
				if len(wrongBlocks) == 0 {
					return
				}
				if !checkRetry {
					return
				}
				if minSize == math.MaxUint64 {
					return
				}
				stdout("begin retry check:\n")
				ek := proto.ExtentKey{
					Size:        uint32(minSize),
					PartitionId: partitionID,
					ExtentId:    extentID,
				}
				if wrongBlocks, err = retryCheckBlockMd5(dp.Replicas, client, &ek, 0, 4, uint64(blockSize), wrongBlocks); err != nil {
					return
				}
				if len(wrongBlocks) == 0 {
					return
				}
				stdout("found: %v blocks at retry scan(block size: %vKB), wrong block index: %v\n", len(wrongBlocks), blockSize, wrongBlocks)
				stdout("\n")
				blockSize4K := 4
				stdout("begin check %v block:\n", blockSize4K)
				wrong4KBlocks := make([]int, 0)
				for _, b := range wrongBlocks {
					for i := 0; i < blockSize/blockSize4K; i++ {
						wrong4KBlocks = append(wrong4KBlocks, b*blockSize/blockSize4K+i)
					}
				}
				if wrong4KBlocks, err = retryCheckBlockMd5(dp.Replicas, client, &ek, 0, 4, uint64(blockSize4K), wrong4KBlocks); err != nil {
					return
				}
				if len(wrong4KBlocks) == 0 {
					return
				}
				stdout("found: %v blocks at retry scan(block size: %vKB), wrong block index: %v\n", len(wrong4KBlocks), blockSize4K, wrong4KBlocks)
				for _, b := range wrong4KBlocks {
					var output string
					if _, output, _, err = checkExtentReplicaByBlock(dp.Replicas, client, &ek, uint32(b), 0, uint64(blockSize4K)); err != nil {
						stdout("err: %v", err)
						return
					}
					stdout(output)
				}
			}
		},
	}
	cmd.Flags().BoolVar(&checkRetry, "check-retry", false, "check extent more times for accuracy")
	cmd.Flags().BoolVar(&getMd5, "check-md5", false, "get extent md5 info")
	return cmd
}

func newExtentRepairCmd() *cobra.Command {
	var fromFile bool
	var cmd = &cobra.Command{
		Use:   cmdExtentRepair,
		Short: cmdExtentRepairShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			extentsMap := make(map[string]map[uint64]map[uint64]bool, 0)
			if fromFile {
				extentsMap = loadRepairExtents()
			} else {
				var partitionID uint64
				if len(args) < 3 {
					stdout("arguments not enough, must be 3")
				}
				partitionID, err = strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return
				}
				extentIDs := make(map[uint64]bool, 0)
				extentStrs := strings.Split(args[1], "-")
				for _, idStr := range extentStrs {
					if eid, err1 := strconv.ParseUint(idStr, 10, 64); err1 != nil {
						stdout("invalid extent id", err1)
						return
					} else {
						extentIDs[eid] = true
					}
				}
				extentsMap[args[2]] = make(map[uint64]map[uint64]bool, 0)
				extentsMap[args[2]][partitionID] = extentIDs
			}
			for host, dpExtents := range extentsMap {
				for pid, extents := range dpExtents {
					exts := make([]uint64, 0)
					for k := range extents {
						exts = append(exts, k)
					}
					if len(exts) == 0 {
						continue
					}
					repairExtents(host, pid, exts)
				}
			}
			fmt.Println("extent repair finished")
		},
	}
	cmd.Flags().BoolVar(&fromFile, "from-file", false, "specify extents file name to repair, file name is repair_extents, format:`partitionID extentID host`")
	return cmd
}

func repairExtents(host string, partitionID uint64, extentIDs []uint64) {
	if partitionID < 0 || len(extentIDs) == 0 {
		return
	}
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
	dataClient := http_client.NewDataClient(dHost, false)
	partition, err := dataClient.GetPartitionFromNode(partitionID)
	if err != nil {
		fmt.Printf("repair failed: %v %v %v\n", partitionID, extentIDs, host)
		return
	}
	partitionPath := fmt.Sprintf("datapartition_%v_%v", partitionID, dp.Replicas[0].Total)
	if len(extentIDs) == 1 {
		err = dataClient.RepairExtent(extentIDs[0], partition.Path, partitionID)
		if err != nil {
			fmt.Printf("repair failed: %v %v %v %v\n", partitionID, extentIDs[0], host, partition.Path)
			if _, err = dataClient.GetPartitionFromNode(partitionID); err == nil {
				return
			}
			for i := 0; i < 3; i++ {
				if err = dataClient.ReLoadPartition(partitionPath, strings.Split(partition.Path, "/datapartition")[0]); err == nil {
					break
				}
			}
			return
		}
		fmt.Printf("repair success: %v %v %v %v\n", partitionID, extentIDs[0], host, partition.Path)
	} else {
		var extMap map[uint64]string
		extentsStrs := make([]string, 0)
		for _, e := range extentIDs {
			extentsStrs = append(extentsStrs, strconv.FormatUint(e, 10))
		}
		extMap, err = dataClient.RepairExtentBatch(strings.Join(extentsStrs, "-"), partition.Path, partitionID)
		if err != nil {
			fmt.Printf("repair failed: %v %v %v %v\n", partitionID, extentsStrs, host, partition.Path)
			if _, err = dataClient.GetPartitionFromNode(partitionID); err == nil {
				return
			}
			for i := 0; i < 3; i++ {
				if err = dataClient.ReLoadPartition(partitionPath, strings.Split(partition.Path, "/datapartition")[0]); err == nil {
					break
				}
			}
			return
		}
		fmt.Printf("repair success: %v %v %v %v\n", partitionID, extentsStrs, host, partition.Path)
		fmt.Printf("repair result: %v\n", extMap)
	}
}

func newExtentCheckCmd(checkType int) *cobra.Command {
	var (
		use               string
		short             string
		specifyPath       string
		inodeStr          string
		metaPartitionId   uint64
		tinyOnly          bool
		tinyInUse         bool
		mpConcurrency     uint64
		inodeConcurrency  uint64
		extentConcurrency uint64
		modifyTimeMin     string
		modifyTimeMax     string
		profPort          uint64
		fromFile          bool
		volFilter         string
		volExcludeFilter  string
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
				err           error
				para          *CheckParam
				vol           = args[0]
				specifyInodes []uint64
			)
			if len(inodeStr) > 0 {
				inodeSlice := strings.Split(inodeStr, ",")
				for _, inode := range inodeSlice {
					ino, err1 := strconv.Atoi(inode)
					if err1 != nil {
						continue
					}
					specifyInodes = append(specifyInodes, uint64(ino))
				}
			}

			ids := loadSpecifiedPartitions()
			vols := make([]string, 0)
			if fromFile {
				vols = loadSpecifiedVolumes(volFilter, volExcludeFilter)
			} else {
				vols = append(vols, vol)
			}
			para, err = NewCheckParam(tinyOnly, tinyInUse, mpConcurrency, inodeConcurrency, extentConcurrency, checkTypeExtentReplica, modifyTimeMin, modifyTimeMax, metaPartitionId, specifyInodes, ids)
			if err != nil {
				return
			}
			CheckVols(vols, client, para, specifyPath, func() bool {
				return false
			})
			fmt.Printf("results saved in local file")
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}

	cmd.Flags().StringVar(&specifyPath, "path", "", "path")
	cmd.Flags().StringVar(&inodeStr, "inode", "", "comma separated inodes")
	cmd.Flags().Uint64Var(&metaPartitionId, "mp", 0, "meta partition id")
	cmd.Flags().BoolVar(&tinyOnly, "tinyOnly", false, "check tiny extents only")
	cmd.Flags().BoolVar(&tinyInUse, "tinyInUse", false, "check tiny extents in use")
	cmd.Flags().Uint64Var(&mpConcurrency, "mpConcurrency", 1, "max concurrent checking meta partitions")
	cmd.Flags().Uint64Var(&inodeConcurrency, "inodeConcurrency", 1, "max concurrent checking inodes")
	cmd.Flags().Uint64Var(&extentConcurrency, "extentConcurrency", 1, "max concurrent checking extents")
	cmd.Flags().StringVar(&modifyTimeMin, "modifyTimeMin", "", "min modify time for inode")
	cmd.Flags().StringVar(&modifyTimeMax, "modifyTimeMax", "", "max modify time for inode")
	cmd.Flags().Uint64Var(&profPort, "profPort", 6007, "go pprof port")
	cmd.Flags().BoolVar(&fromFile, "from-file", false, "repair vols from file[filename:vols]")
	cmd.Flags().StringVar(&volFilter, "vol-filter", "", "check volume by filter")
	cmd.Flags().StringVar(&volExcludeFilter, "vol-exclude-filter", "", "exclude volume by filter")
	return cmd
}

func CheckVols(vols []string, c *sdk.MasterClient, para *CheckParam, path string, stopFunc func() bool) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("CheckVols error: %v", err)
		}
	}()

	rp := NewRepairPersist(c.Nodes()[0])
	go rp.PersistResult()
	defer rp.Close()
	for _, v := range vols {
		if stopFunc() {
			break
		}
		switch para.checkType {
		case checkTypeExtentReplica, checkTypeExtentLength, checkTypeInodeEk, checkTypeInodeNlink:
			CheckVol(v, c, path, para, rp.RCh, stopFunc)
		case checkTypeExtentCrc:
			checkVolExtentCrc(c, v, para.tinyOnly, unit.MB*5)
		}
	}
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
		Run: func(cmd *cobra.Command, args []string) {
			var (
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
			searchExtent(dps, extents, extentOffset, size, concurrency)
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

func searchExtent(dps []uint64, extents []uint64, extentOffset uint, size uint, concurrency uint64) {
	dataPartition, err := client.AdminAPI().GetDataPartition("", dps[0])
	if err != nil {
		return
	}
	vol := dataPartition.VolName
	mps, err := client.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	inodes, err := getFileInodesByMp(mps, 0, concurrency, 0, 0, client.MetaNodeProfPort, true)
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
				extentsResp, err := getExtentsByInode(inode, mps, client.MetaNodeProfPort)
				if err != nil {
					stdout("get extents error: %v, inode: %d\n", err, inode)
					wg.Done()
					idx++
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

			vols := loadSpecifiedVolumes("", "")
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
		inodes, err = getAllInodesByPath(client, vol, "")
	} else {
		inodes, err = getFileInodesByMp(mps, 0, mpConcurrency, 0, 0, client.MetaNodeProfPort, true)
	}
	if err != nil {
		stdout("get all inodes error: %v\n", err)
		return
	}

	metaExtentMap := make(map[uint64]map[uint64]bool)
	extents, err := getExtentsByInodes(inodes, inodeConcurrency, mps, client)
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
	var gConnPool = connpool.NewConnectPool()
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

type CheckParam struct {
	tinyOnly          bool
	tinyInUse         bool
	mpConcurrency     uint64
	inodeConcurrency  uint64
	extentConcurrency uint64
	metaPartitionId   uint64
	checkType         int
	specifyInodes     []uint64
	specifyDps        []uint64
	modifyTimeMin     time.Time
	modifyTimeMax     time.Time
}

func NewCheckParam(tinyOnly, tinyInUse bool, mpConcurrency, inodeConcurrency, extentConcurrency uint64, checkType int, modifyTimeMin, modifyTimeMax string, metaPartitionId uint64, specifyInodes, specifyDps []uint64) (cfg *CheckParam, err error) {
	var (
		modifyTimestampMin, modifyTimestampMax time.Time
	)
	if modifyTimestampMin, err = parseTime(modifyTimeMin); err != nil {
		return
	}
	if modifyTimestampMax, err = parseTime(modifyTimeMax); err != nil {
		return
	}
	cfg = &CheckParam{
		tinyOnly:          tinyOnly,
		tinyInUse:         tinyInUse,
		mpConcurrency:     mpConcurrency,
		inodeConcurrency:  inodeConcurrency,
		extentConcurrency: extentConcurrency,
		checkType:         checkType,
		modifyTimeMin:     modifyTimestampMin,
		modifyTimeMax:     modifyTimestampMax,
		metaPartitionId:   metaPartitionId,
		specifyInodes:     specifyInodes,
		specifyDps:        specifyDps,
	}
	return
}

func CheckVol(vol string, c *sdk.MasterClient, path string, param *CheckParam, rCh chan RepairExtentInfo, stopFunc func() bool) {
	var (
		checkedExtent *sync.Map
		wg            sync.WaitGroup
		inodes        []uint64
	)
	defer func() {
		msg := fmt.Sprintf("checkVol, cluster:%s, vol:%s, path%s", c.Nodes()[0], vol, path)
		if r := recover(); r != nil {
			var stack string
			stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			log.LogCritical("%s%s\n", msg, stack)
		}
	}()
	checkedExtent = new(sync.Map)
	log.LogInfof("begin check, cluster:%s, vol:%s, path:%v\n", c.Nodes()[0], vol, path)
	mps, err := c.ClientAPI().GetMetaPartitions(vol)
	if err != nil {
		return
	}
	if len(param.specifyInodes) == 0 && path != "" {
		inodes, _ = getAllInodesByPath(c, vol, path)
	} else {
		inodes = param.specifyInodes
	}
	if len(inodes) > 0 {
		checkInodes(checkedExtent, vol, mps, c, inodes, param, rCh, stopFunc)
		log.LogInfof("finish check, cluster:%s, vol:%s", c.Nodes()[0], vol)
		return
	}

	mpCh := make(chan uint64, 1000)
	wg.Add(len(mps))
	go func() {
		for _, mp := range mps {
			mpCh <- mp.PartitionID
		}
		close(mpCh)
	}()

	for i := 0; i < int(param.mpConcurrency); i++ {
		go func(checked *sync.Map) {
			for mp := range mpCh {
				if stopFunc() {
					wg.Done()
					continue
				}
				var mpInodes []uint64
				if param.metaPartitionId > 0 && mp != param.metaPartitionId {
					wg.Done()
					continue
				}
				log.LogInfof("begin check, cluster:%s, vol:%s, mpId: %d", c.Nodes()[0], vol, mp)
				if param.checkType == checkTypeInodeNlink {
					checkVolNlink(c, mps, mp, param.modifyTimeMin.Unix(), param.modifyTimeMax.Unix())
				} else {
					mpInodes, err = getFileInodesByMp(mps, mp, 1, param.modifyTimeMin.Unix(), param.modifyTimeMax.Unix(), c.MetaNodeProfPort, false)
					if err != nil {
						wg.Done()
						continue
					}
					log.LogDebugf("checkVol cluster:%s, mp:%v, inode length:%v", c.Nodes()[0], mp, len(mpInodes))
					if len(mpInodes) > 0 {
						checkInodes(checked, vol, mps, c, mpInodes, param, rCh, stopFunc)
					}
				}
				log.LogInfof("finish check, cluster:%s, vol:%s, mpId: %d", c.Nodes()[0], vol, mp)
				wg.Done()
			}
		}(checkedExtent)
	}
	wg.Wait()
	log.LogInfof("finish check volume, cluster:%s, vol:%s", c.Nodes()[0], vol)
}

func checkVolNlink(c *sdk.MasterClient, mps []*proto.MetaPartitionView, metaPartitionId uint64, modifyTimeMin int64, modifyTimeMax int64) {
	for _, mp := range mps {
		if metaPartitionId > 0 && mp.PartitionID != metaPartitionId {
			continue
		}
		var preMap map[uint64]uint32
		for i, host := range mp.Members {
			mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], c.MetaNodeProfPort), false)
			inodes, err := mtClient.GetAllInodes(mp.PartitionID)
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

func checkInodes(checkedExtent *sync.Map, vol string, mps []*proto.MetaPartitionView, c *sdk.MasterClient, inodes []uint64, param *CheckParam, rCh chan RepairExtentInfo, stopFunc func() bool) {
	var (
		wg sync.WaitGroup
	)
	inoCh := make(chan uint64, 1000*1000)
	wg.Add(len(inodes))
	go func() {
		for _, ino := range inodes {
			inoCh <- ino
		}
		close(inoCh)
	}()
	for i := 0; i < int(param.inodeConcurrency); i++ {
		go func(mc *sdk.MasterClient, extentMap *sync.Map) {
			for ino := range inoCh {
				if stopFunc() {
					wg.Done()
					continue
				}
				if param.checkType == checkTypeInodeEk {
					checkInodeEk(ino, mc, mps)
				} else {
					checkInode(vol, mc, ino, extentMap, param, mps, rCh)
				}
				wg.Done()
			}
		}(c, checkedExtent)
	}
	wg.Wait()
}
func getAllInodesByPath(c *sdk.MasterClient, vol string, path string) (inodes []uint64, err error) {
	ctx := context.Background()
	var mw *meta.MetaWrapper
	mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        vol,
		Masters:       c.Nodes(),
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

func getFileInodesByMp(mps []*proto.MetaPartitionView, metaPartitionId uint64, concurrency uint64, modifyTimeMin int64, modifyTimeMax int64, metaProf uint16, exit bool) (inodes []uint64, err error) {
	var (
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
				var inos map[uint64]*proto.MetaInode
				if mp.LeaderAddr == "" {
					stdout("mp[%v] no leader\n", mp.PartitionID)
					wg.Done()
					return
				}
				mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(mp.LeaderAddr, ":")[0], metaProf), false)
				inos, err = mtClient.GetAllInodes(mp.PartitionID)
				if err != nil {
					stdout("get inodes error: %v, mp: %d\n", err, mp.PartitionID)
					if exit {
						os.Exit(0)
					}
					wg.Done()
					return
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

func idExist(id uint64, ids []uint64) bool {
	if len(ids) == 0 {
		return false
	}
	for _, i := range ids {
		if i == id {
			return true
		}
	}
	return false
}

func checkExtentReplicaInfo(c *sdk.MasterClient, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string, checkType int, rCh chan RepairExtentInfo, checkTiny bool) (err error) {
	if checkTiny {
		if proto.IsTinyExtent(ek.ExtentId) {
			err = checkTinyExtentReplicaInfo(c, dataReplicas, ek, ino, volume, rCh)
		}
		return
	}
	err = checkNormalExtentReplicaInfo(c, dataReplicas, ek, ino, volume, checkType, rCh)
	return
}

func checkNormalExtentReplicaInfo(c *sdk.MasterClient, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string, checkType int, rCh chan RepairExtentInfo) (err error) {
	blockSize := uint64(128)
	blocksLengh := ek.Size / uint32(blockSize*unit.KB)
	if ek.Size-blocksLengh*uint32(blockSize*unit.KB) > 0 {
		blocksLengh += 1
	}
	var output string
	var same bool
	var wrongBlocks []int
	if wrongBlocks, err = checkExtentBlockCrc(dataReplicas, c, ek.PartitionId, ek.ExtentId); err != nil {
		log.LogErrorf("checkNormalExtentReplicaInfo failed, cluster:%s, partition:%v, extent:%v, err:%v", c.Nodes()[0], ek.PartitionId, ek.ExtentId, err)
		return
	}
	if len(wrongBlocks) == 0 {
		log.LogInfof("action[checkNormalExtentReplicaInfo] cluster:%s partition:%v, extent:%v, inode:%v, check same at block crc check", c.Nodes()[0], ek.PartitionId, ek.ExtentId, ino)
		return
	}
	if len(wrongBlocks) > 100 {
		for i := 0; i < 5; i++ {
			if _, output, same, err = checkExtentReplica(c, dataReplicas, ek, "md5"); err != nil {
				log.LogErrorf("checkNormalExtentReplicaInfo failed, cluster:%s, partition:%v, extent:%v, err:%v", c.Nodes()[0], ek.PartitionId, ek.ExtentId, err)
				return
			}
			if same {
				log.LogInfof("action[checkNormalExtentReplicaInfo] cluster:%s partition:%v, extent:%v, inode:%v, check same at md5 check", c.Nodes()[0], ek.PartitionId, ek.ExtentId, ino)
				return
			}
			time.Sleep(time.Second)
		}
	}
	if checkType == 2 {
		return
	}
	if wrongBlocks, err = retryCheckBlockMd5(dataReplicas, c, ek, ino, 10, blockSize, wrongBlocks); err != nil {
		return
	}
	//print bad blocks
	if len(wrongBlocks) != 0 {
		addrMap := make(map[string]int, 0)
		for _, b := range wrongBlocks {
			var badAddrs []string
			badAddrs, output, same, err = checkExtentReplicaByBlock(dataReplicas, c, ek, uint32(b), ino, blockSize)
			if err != nil {
				return
			}
			if same {
				continue
			}
			for _, addr := range badAddrs {
				if _, ok := addrMap[addr]; !ok {
					addrMap[addr] = 0
				}
				addrMap[addr] += 1
			}
			log.LogWarnf("cluster:" + c.Nodes()[0] + " " + output)
		}
		if len(addrMap) > 0 {
			repairHost := make([]string, 0)
			for k := range addrMap {
				repairHost = append(repairHost, k)
			}
			if rCh != nil {
				rCh <- RepairExtentInfo{
					ExtentID:    ek.ExtentId,
					PartitionID: ek.PartitionId,
					Hosts:       repairHost,
					Inode:       ino,
					Volume:      volume,
				}
			} else {
				if len(repairHost) == 1 {
					log.LogWarnf("cluster:%s autoRepairExtent: %v %v %v %v %v", c.Nodes()[0], ek.PartitionId, ek.ExtentId, repairHost[0], ino, volume)
				} else {
					log.LogWarnf("cluster:%s canNotAutoRepairExtent: %v %v %v %v %v", c.Nodes()[0], ek.PartitionId, ek.ExtentId, repairHost, ino, volume)
				}
			}
		}
	}
	return
}

func checkTinyExtentReplicaInfo(c *sdk.MasterClient, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, ino uint64, volume string, rCh chan RepairExtentInfo) (err error) {
	var badAddrs []string
	var output string
	var same bool
	for i := 0; i < 10; i++ {
		if badAddrs, output, same, err = checkExtentReplica(c, dataReplicas, ek, "md5"); err != nil {
			log.LogErrorf("checkTinyExtentReplicaInfo failed, partition:%v, extent:%v, err:%v\n", ek.PartitionId, ek.ExtentId, err)
			return
		}
		if same {
			log.LogInfof("action[checkTinyExtentReplicaInfo] partition:%v, extent:%v, inode:%v, check same at md5 check", ek.PartitionId, ek.ExtentId, ino)
			return
		}
		time.Sleep(time.Second)
	}
	log.LogWarnf(output)
	if len(badAddrs) > 0 {
		repairHost := make([]string, 0)
		for _, k := range badAddrs {
			repairHost = append(repairHost, k)
		}
		if rCh != nil {
			rCh <- RepairExtentInfo{
				ExtentID:    ek.ExtentId,
				PartitionID: ek.PartitionId,
				Hosts:       repairHost,
				Inode:       ino,
				Volume:      volume,
			}
		} else {
			if len(repairHost) == 1 {
				log.LogWarnf("autoRepairExtent: %v %v %v %v %v\n", ek.PartitionId, ek.ExtentId, repairHost[0], ino, volume)
			} else {
				log.LogWarnf("canNotAutoRepairExtent: %v %v %v %v %v\n", ek.PartitionId, ek.ExtentId, repairHost, ino, volume)
			}
		}
	}

	return
}

func retryCheckBlockMd5(dataReplicas []*proto.DataReplica, c *sdk.MasterClient, ek *proto.ExtentKey, ino uint64, retry int, blockSize uint64, wrongBlocks []int) (resultBlocks []int, err error) {
	var (
		same bool
	)
	for j := 0; j < retry; j++ {
		newBlk := make([]int, 0)
		for _, b := range wrongBlocks {
			if _, _, same, err = checkExtentReplicaByBlock(dataReplicas, c, ek, uint32(b), ino, blockSize); err != nil {
				return
			} else if same {
				continue
			}
			newBlk = append(newBlk, b)
		}
		wrongBlocks = newBlk
		if len(wrongBlocks) == 0 {
			break
		}
		time.Sleep(time.Second * 1)
	}
	return wrongBlocks, nil
}

func checkInode(vol string, c *sdk.MasterClient, inode uint64, checkedExtent *sync.Map, param *CheckParam, mps []*proto.MetaPartitionView, rCh chan RepairExtentInfo) {
	var (
		err         error
		extentsResp *proto.GetExtentsResponse
		errCount    int = 0
		wg          sync.WaitGroup
	)
	extentsResp, err = getExtentsByInode(inode, mps, c.MetaNodeProfPort)
	if err != nil {
		log.LogErrorf("check inode failed, cluster:%s, vol:%s, inode: %d, err:%v", c.Nodes()[0], vol, inode, err)
		return
	}

	log.LogInfof("begin check inode, cluster:%s, vol:%s, inode: %d, extent count: %d", c.Nodes()[0], vol, inode, len(extentsResp.Extents))
	ekCh := make(chan proto.ExtentKey)
	extentCount := 0
	for _, ek := range extentsResp.Extents {
		if len(param.specifyDps) > 0 && !idExist(ek.PartitionId, param.specifyDps) {
			continue
		}
		if param.tinyOnly && proto.IsTinyExtent(ek.ExtentId) {
			extentCount++
			continue
		}
		if !param.tinyOnly && !proto.IsTinyExtent(ek.ExtentId) {
			extentCount++
		}
	}
	wg.Add(extentCount)
	go func() {
		for _, ek := range extentsResp.Extents {
			if len(param.specifyDps) > 0 && !idExist(ek.PartitionId, param.specifyDps) {
				continue
			}
			if param.tinyOnly && proto.IsTinyExtent(ek.ExtentId) {
				ekCh <- ek
				continue
			}
			if !param.tinyOnly && !proto.IsTinyExtent(ek.ExtentId) {
				ekCh <- ek
			}
		}
		close(ekCh)
	}()
	//extent may be duplicated in extentsResp.Extents
	var idx int32
	for i := 0; i < int(param.extentConcurrency); i++ {
		go func(mc *sdk.MasterClient, extent *sync.Map, ino uint64, volume string) {
			for ek := range ekCh {
				switch param.checkType {
				case checkTypeExtentReplica:
					var ekStr string
					if !param.tinyOnly {
						if param.tinyInUse {
							ekStr = fmt.Sprintf("%d-%d-%d-%d", ek.PartitionId, ek.ExtentId, ek.ExtentOffset, ek.Size)
						} else {
							ekStr = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
						}
						if _, ok := extent.LoadOrStore(ekStr, true); ok {
							wg.Done()
							continue
						}
					}
					var partition *proto.DataPartitionInfo
					for j := 0; j == 0 || j < 3 && err != nil; j++ {
						partition, err = mc.AdminAPI().GetDataPartition("", ek.PartitionId)
					}
					if err != nil || partition == nil {
						log.LogErrorf("checkFailedExtent, cluster:%s, dp:%v, extent:%v, err:%v", c.Nodes()[0], ek.PartitionId, ek.ExtentId, err)
						wg.Done()
						continue
					}
					err = checkExtentReplicaInfo(c, partition.Replicas, &ek, ino, volume, 0, rCh, param.tinyOnly)
					if err != nil {
						log.LogErrorf("checkFailedExtent, cluster:%s, dp:%v, extent:%v, err:%v", c.Nodes()[0], ek.PartitionId, ek.ExtentId, err)
					}
				case checkTypeExtentLength:
					checkExtentLength(c, &ek, extent)
				}

				atomic.AddInt32(&idx, 1)
				if idx%100 == 0 {
					log.LogInfof("cluster:%s, vol:%s, inode: %d, %d extents checked", c.Nodes()[0], vol, inode, idx)
				}
				wg.Done()
			}
		}(c, checkedExtent, inode, vol)
	}
	wg.Wait()
	log.LogInfof("finish check, cluster:%s, vol:%s, inode: %d, err count: %d\n", c.Nodes()[0], vol, inode, errCount)
}

func checkInodeEk(inode uint64, c *sdk.MasterClient, mps []*proto.MetaPartitionView) {
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
		mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], c.MetaNodeProfPort), false)
		extents, err := mtClient.GetExtentsByInode(mpId, inode)
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

func checkExtentBlockCrc(dataReplicas []*proto.DataReplica, c *sdk.MasterClient, partitionId, extentId uint64) (wrongBlocks []int, err error) {
	var replicas = make([]struct {
		partitionId uint64
		extentId    uint64
		datanode    string
		blockCrc    []*proto.BlockCrc
	}, len(dataReplicas))
	var minBlockNum int
	var minBlockNumIdx int
	minBlockNum = math.MaxInt32
	wrongBlocks = make([]int, 0)
	for idx, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], c.DataNodeProfPort)
		dataClient := http_client.NewDataClient(datanode, false)

		var extentBlocks []*proto.BlockCrc
		extentBlocks, err = dataClient.GetExtentBlockCrc(partitionId, extentId)
		if err != nil {
			return
		}
		replicas[idx].partitionId = partitionId
		replicas[idx].extentId = extentId
		replicas[idx].datanode = datanode
		replicas[idx].blockCrc = extentBlocks
		if minBlockNum > len(extentBlocks) {
			minBlockNum = len(extentBlocks)
			minBlockNumIdx = idx
		}
	}
	for blkIdx, blk := range replicas[minBlockNumIdx].blockCrc {
		for idx, rp := range replicas {
			if minBlockNumIdx == idx {
				continue
			}
			if blk.Crc == 0 || rp.blockCrc[blkIdx].Crc != blk.Crc {
				wrongBlocks = append(wrongBlocks, blk.BlockNo)
				break
			}
		}
	}
	return
}

func checkExtentReplica(c *sdk.MasterClient, dataReplicas []*proto.DataReplica, ek *proto.ExtentKey, mod string) (badAddrs []string, output string, same bool, err error) {
	var (
		ok       bool
		replicas = make([]struct {
			partitionId uint64
			extentId    uint64
			datanode    string
			md5OrCrc    string
		}, len(dataReplicas))
		md5Map         = make(map[string]int)
		extentMd5orCrc string
	)
	badAddrs = make([]string, 0)
	for idx, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], c.DataNodeProfPort)
		dataClient := http_client.NewDataClient(datanode, false)
		switch mod {
		case "crc":
			var extentInfo *proto.ExtentInfoBlock
			//new version
			extentInfo, err = dataClient.GetExtentInfo(ek.PartitionId, ek.ExtentId)
			if err != nil {
				log.LogErrorf("GetExtentInfo datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
				return
			}
			extentMd5orCrc = fmt.Sprintf("%v", extentInfo[proto.ExtentInfoCrc])
		case "md5":
			var (
				size   uint32
				offset uint64
			)
			if proto.IsTinyExtent(ek.ExtentId) {
				offset = ek.ExtentOffset
				size = ek.Size
			}
			extentMd5orCrc, err = dataClient.ComputeExtentMd5(ek.PartitionId, ek.ExtentId, offset, uint64(size))
			if err != nil {
				log.LogErrorf("getExtentMd5 datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", datanode, ek.PartitionId, ek.ExtentId, err)
				return
			}
		default:
			err = fmt.Errorf("wrong mod")
			log.LogErrorf(err.Error())
			return
		}
		replicas[idx].partitionId = ek.PartitionId
		replicas[idx].extentId = ek.ExtentId
		replicas[idx].datanode = datanode
		replicas[idx].md5OrCrc = extentMd5orCrc
		if _, ok = md5Map[extentMd5orCrc]; ok {
			md5Map[extentMd5orCrc]++
		} else {
			md5Map[extentMd5orCrc] = 1
		}
	}

	if len(md5Map) == 1 {
		return badAddrs, "", true, nil
	}
	for _, r := range replicas {
		addr := strings.Split(r.datanode, ":")[0] + ":6000"
		msg := fmt.Sprintf("dp: %d, extent: %d, datanode: %s, %v: %s\n", r.partitionId, r.extentId, addr, mod, r.md5OrCrc)
		if _, ok = md5Map[r.md5OrCrc]; ok && md5Map[r.md5OrCrc] > len(dataReplicas)/2 {
			output += msg
		} else {
			output += fmt.Sprintf("ERROR Extent %s", msg)
			badAddrs = append(badAddrs, addr)
		}
	}
	return
}

type RepairExtentInfo struct {
	PartitionID uint64
	ExtentID    uint64
	Offset      uint64
	Size        uint64
	Hosts       []string
	Inode       uint64
	Volume      string
}

func checkExtentReplicaByBlock(dataReplicas []*proto.DataReplica, c *sdk.MasterClient, ek *proto.ExtentKey, blockOffset uint32, inode, blockSize uint64) (badAddrs []string, output string, same bool, err error) {
	var (
		ok       bool
		replicas = make([]struct {
			partitionId  uint64
			extentId     uint64
			datanode     string
			md5          string
			extentOffset uint64
			size         uint64
		}, len(dataReplicas))
		md5Map    = make(map[string]int)
		extentMd5 string
	)
	badAddrs = make([]string, 0)
	size := uint32(blockSize * unit.KB)
	offset := blockOffset * uint32(blockSize*unit.KB)
	if size > ek.Size-offset {
		size = ek.Size - offset
	}
	for idx, replica := range dataReplicas {
		datanode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], c.DataNodeProfPort)
		dataClient := http_client.NewDataClient(datanode, false)
		for j := 0; j == 0 || j < 3 && err != nil; j++ {
			extentMd5, err = dataClient.ComputeExtentMd5(ek.PartitionId, ek.ExtentId, uint64(offset), uint64(size))
		}
		if err != nil {
			log.LogErrorf("checkExtentReplicaByBlock, cluster:%s, getExtentMd5 datanode(%v) PartitionId(%v) ExtentId(%v) err(%v)\n", c.Nodes()[0], datanode, ek.PartitionId, ek.ExtentId, err)
			return
		}
		replicas[idx].partitionId = ek.PartitionId
		replicas[idx].extentId = ek.ExtentId
		replicas[idx].extentOffset = ek.ExtentOffset
		replicas[idx].size = uint64(ek.Size)
		replicas[idx].datanode = datanode
		replicas[idx].md5 = extentMd5
		if _, ok = md5Map[replicas[idx].md5]; ok {
			md5Map[replicas[idx].md5]++
		} else {
			md5Map[replicas[idx].md5] = 1
		}
	}
	if len(md5Map) == 1 {
		return nil, "", true, nil
	}

	for _, r := range replicas {
		msg := fmt.Sprintf("dp: %d, extent: %d, datanode: %s, inode:%v, offset:%v, size:%v, md5: %s\n", r.partitionId, r.extentId, r.datanode, inode, offset, size, r.md5)
		if _, ok = md5Map[r.md5]; ok && md5Map[r.md5] > len(dataReplicas)/2 {
			output += msg
		} else {
			output += fmt.Sprintf("ERROR ExtentBlock %s", msg)
			badAddrs = append(badAddrs, strings.Split(r.datanode, ":")[0]+":6000")
		}
	}
	return
}

func checkExtentLength(c *sdk.MasterClient, ek *proto.ExtentKey, checkedExtent *sync.Map) (same bool, err error) {
	var (
		ok        bool
		ekStr     string = fmt.Sprintf("%d-%d", ek.PartitionId, ek.ExtentId)
		partition *proto.DataPartitionInfo
		extent    *proto.ExtentInfoBlock
	)
	if _, ok = checkedExtent.LoadOrStore(ekStr, true); ok {
		return true, nil
	}
	partition, err = c.AdminAPI().GetDataPartition("", ek.PartitionId)
	if err != nil {
		stdout("GetDataPartition ERROR: %v, PartitionId: %d\n", err, ek.PartitionId)
		return
	}

	datanode := fmt.Sprintf("%s:%d", strings.Split(partition.Replicas[0].Addr, ":")[0], c.DataNodeProfPort)
	dataClient := http_client.NewDataClient(datanode, false)
	extent, err = dataClient.GetExtentInfo(ek.PartitionId, ek.ExtentId)
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

func checkVolExtentCrc(c *sdk.MasterClient, vol string, tiny bool, validateStep uint64) {
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
	log.LogInfof("begin check, vol:%s\n", vol)
	dataPartitionsView, err := c.ClientAPI().GetDataPartitions(vol)
	if err != nil {
		stdout("not exist, vol:%s\n", vol)
		return
	}
	log.LogInfof("vol:%s dp count:%v\n", vol, len(dataPartitionsView.DataPartitions))
	data.StreamConnPool = connpool.NewConnectPoolWithTimeoutAndCap(0, 10, 30, int64(1*time.Second))
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
	log.LogInfof("finish check, vol:%s\n", vol)
}

func validateDataPartitionTinyExtentCrc(dataPartition *proto.DataPartitionResponse, validateStep uint64) (dpTinyExtentCrcInfo *DataPartitionExtentCrcInfo, err error) {
	if dataPartition == nil {
		return nil, fmt.Errorf("action[validateDataPartitionTinyExtentCrc] dataPartition is nil")
	}
	if validateStep < unit.MB {
		validateStep = unit.MB
	}
	dpReplicaInfos, err := getDataPartitionReplicaInfos(dataPartition)
	if err != nil {
		return
	}
	// map[uint64]map[string]uint64 --> extentID:(host:extent size)
	extentReplicaHostSizeMap := make(map[uint64]map[string]uint64, 0)
	for replicaHost, partition := range dpReplicaInfos {
		for _, extentInfo := range partition.Files {
			if !proto.IsTinyExtent(extentInfo[storage.FileID]) {
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
	if validateStep < unit.MB {
		validateStep = unit.MB
	}
	minSize := uint64(math.MaxUint64)
	for _, size := range replicaSizeMap {
		if minSize > size {
			minSize = size
		}
	}
	offsetCrcAddrMap := make(map[uint64]map[uint32][]string) // offset:(crc:addrs)
	offset := uint64(0)
	size := uint64(unit.KB * 4)
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

func getExtentsByInodes(inodes []uint64, concurrency uint64, mps []*proto.MetaPartitionView, c *sdk.MasterClient) (extents []proto.ExtentKey, err error) {
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
				re, tmpErr := getExtentsByInode(ino, mps, c.MetaNodeProfPort)
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

func getExtentsByInode(inode uint64, mps []*proto.MetaPartitionView, metaProf uint16) (re *proto.GetExtentsResponse, err error) {
	var addr string
	var mpId uint64
	for _, mp := range mps {
		if inode >= mp.Start && inode < mp.End {
			addr = mp.LeaderAddr
			mpId = mp.PartitionID
			break
		}
	}
	mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], metaProf), false)
	return mtClient.GetExtentsByInode(mpId, inode)
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

func readExtent(dp *proto.DataPartitionResponse, addr string, extentId uint64, d []byte, offset uint64, size int) (err error) {
	ctx := context.Background()
	ek := &proto.ExtentKey{PartitionId: dp.PartitionID, ExtentId: extentId}
	dataPartition := &data.DataPartition{
		ClientWrapper:         &data.Wrapper{},
		DataPartitionResponse: *dp,
	}
	dataPartition.ClientWrapper.SetConnConfig()
	dataPartition.ClientWrapper.SetDpFollowerReadDelayConfig(false, 60)
	sc := data.NewStreamConnWithAddr(dataPartition, addr)
	reqPacket := common.NewReadPacket(ctx, ek, int(offset), size, 0, offset, true)
	req := data.NewExtentRequest(0, 0, d, 0, uint64(size), nil)
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

func getExtentInfo(dp *proto.DataPartitionInfo, client *sdk.MasterClient) (extentInfo []uint64, err error) {
	var (
		dnPartition   *proto.DNDataPartitionInfo
		extentInfoMap = make(map[uint64]bool)
		errCount      int
		errMsg        error
	)

	for _, host := range dp.Hosts {
		if errCount > len(dp.Hosts)/2 {
			break
		}
		arr := strings.Split(host, ":")
		if dnPartition, err = client.NodeAPI().DataNodeGetPartition(arr[0], dp.PartitionID); err != nil {
			errMsg = fmt.Errorf("%v DataNodeGetPartition err(%v) ", host, err)
			errCount++
			continue
		}

		for _, ei := range dnPartition.Files {
			if ei[storage.Size] == 0 {
				continue
			}
			_, ok := extentInfoMap[ei[storage.FileID]]
			if ok {
				continue
			}
			extentInfoMap[ei[storage.FileID]] = true
			extentInfo = append(extentInfo, ei[storage.FileID])
		}
	}
	if errCount > len(dp.Hosts)/2 {
		err = errMsg
	} else {
		err = nil
		sort.Slice(extentInfo, func(i, j int) bool {
			return extentInfo[i] < extentInfo[j]
		})
	}
	return
}
func newExtentCheckByIdCmd(mc *sdk.MasterClient) *cobra.Command {
	var partitionID uint64
	var extentID uint64
	var checkTiny bool
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckExtentReplicaShort,
		Run: func(cmd *cobra.Command, args []string) {
			if partitionID == 0 || extentID == 0 {
				stdout("invalid id, pid[%d], extent id[%d]\n", partitionID, extentID)
				return
			}

			dpInfo, err := mc.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				stdout("get data partitin[%d] failed :%v\n", partitionID, err.Error())
				return
			}

			dpAddr := fmt.Sprintf("%s:%d", strings.Split(dpInfo.Hosts[0], ":")[0], mc.DataNodeProfPort)
			dataClient := http_client.NewDataClient(dpAddr, false)
			ekInfo, _ := dataClient.GetExtentInfo(partitionID, extentID)
			ek := proto.ExtentKey{
				PartitionId: partitionID, ExtentId: extentID, Size: uint32(ekInfo[proto.ExtentInfoSize]),
			}
			checkExtentReplicaInfo(mc, dpInfo.Replicas, &ek, 0, dpInfo.VolName, 0, nil, checkTiny)
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(mc, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Uint64Var(&partitionID, "pid", 0, "Specify partition id")
	cmd.Flags().Uint64Var(&extentID, "eid", 0, "Specify extent id")
	cmd.Flags().BoolVar(&checkTiny, "check-tiny", false, "check tiny extent")
	return cmd
}

func parseMetaExtentDelFiles(dir string) {
	allFiles, _ := ioutil.ReadDir(dir)
	for _, file := range allFiles {
		if file.Mode().IsDir() || (!strings.Contains(file.Name(), "deleteExtentList") && !strings.Contains(file.Name(), "inodeDeleteExtentList")) {
			continue
		}
		fmt.Printf("parse file [%s] begin \n", path.Join(dir, file.Name()))

		fp, err := os.Open(path.Join(dir, file.Name()))
		if err != nil {
			fmt.Printf("parse file failed, open file [%s] failed:%v\n", path.Join(dir, file.Name()), err)
			continue
		}

		buffer, err := io.ReadAll(fp)
		if err != nil {
			fmt.Printf("parse file failed, read file [%s] failed:%v\n", path.Join(dir, file.Name()), err)
			fp.Close()
			continue
		}
		fp.Close()
		buff := bytes.NewBuffer(buffer)
		for {
			if buff.Len() == 0 {
				break
			}

			if buff.Len() < proto.ExtentDbKeyLengthWithIno {
				fmt.Printf("file parese failed, last ek len:%d < %d\n", buff.Len(), proto.ExtentDbKeyLengthWithIno)
				break
			}

			ek := proto.MetaDelExtentKey{}
			if err := ek.UnmarshalDbKeyByBuffer(buff); err != nil {
				fmt.Printf("parese failed:%v\n", err)
				break
			}
			fmt.Printf("%v\n", ek.String())
		}
		fmt.Printf("parse file [%s] success\n", path.Join(dir, file.Name()))
	}
}

func parseDataExtentDelFiles(dir string) {
	allFiles, _ := ioutil.ReadDir(dir)
	for _, file := range allFiles {
		if file.Mode().IsDir() || (!strings.Contains(file.Name(), "NORMALEXTENT_DELETE")) {
			continue
		}
		fmt.Printf("parse file [%s] begin \n", path.Join(dir, file.Name()))

		fp, err := os.Open(path.Join(dir, file.Name()))
		if err != nil {
			fmt.Printf("parse file failed, open file [%s] failed\n", path.Join(dir, file.Name()))
			continue
		}

		buffer := make([]byte, 8*1024)
		for {
			realLen, err := fp.Read(buffer)
			if err != nil {
				if err == io.EOF {
					break
				}

				fmt.Printf("parse file failed, read file [%s] failed\n", path.Join(dir, file.Name()))
				break
			}

			for off := 0; off < realLen && off+8 < realLen; off += 8 {
				ekID := binary.BigEndian.Uint64(buffer[off : off+8])
				fmt.Printf("ek: %d\n", ekID)
			}

		}
		fp.Close()
		fmt.Printf("parse file [%s] success\n", path.Join(dir, file.Name()))
	}
}

func newExtentParseCmd() *cobra.Command {
	var srcDir string
	var decoder string
	var cmd = &cobra.Command{
		Use:   cmdExtentDelParse,
		Short: cmdExtentDelParseShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			if decoder != "meta" && decoder != "data" {
				err = fmt.Errorf("invalid type param :%s", decoder)
				return
			}

			if decoder == "meta" {
				parseMetaExtentDelFiles(srcDir)
				return
			}

			if decoder == "data" {
				parseDataExtentDelFiles(srcDir)
				return
			}

			fmt.Printf("parser success")
		},
	}
	cmd.Flags().StringVar(&srcDir, "dir", ".", "src file")
	cmd.Flags().StringVar(&decoder, "type", "meta", "meta/data")
	return cmd
}
