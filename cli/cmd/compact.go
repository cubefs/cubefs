// Copyright 2018 The CubeFS Authors.
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

package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cmdCompactUse   = "compact [COMMAND]"
	cmdCompactShort = "Manage compact info"
)

func newCompactCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdCompactUse,
		Short: cmdCompactShort,
	}
	cmd.AddCommand(
		newCompactVolList(client),
		newCompactCheckVolList(client),
		newCompactBatchCloseCmd(client),
		newCompactBatchOpenCmd(client),
		newCompactCheckFragCmd(client),
	)
	return cmd
}

const (
	cmdCompactVolList = "List all compacting volumes"
)

func newCompactVolList(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdCompactVolList,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var compactVolumes []*proto.CompactVolume
			compactVolumes, err = client.AdminAPI().ListCompactVolumes()
			if err != nil {
				errout("list all compacting volumes failed case:\n%v\n", err)
			}
			sort.Slice(compactVolumes, func(i, j int) bool { return compactVolumes[i].Name < compactVolumes[j].Name })
			stdout("[compacting volumes]\n")
			stdout("%v\n", formatCompactVolViewTableHeader())
			for _, cVolume := range compactVolumes {
				if cVolume.CompactTag != proto.CompactOpen {
					continue
				}
				stdout("%v\n", formatCompactVolView(cVolume))
			}
		},
	}
	return cmd
}

const (
	cmdCompactCheckVolUse   = "checkVol"
	cmdCompactCheckVolShort = "list the volume has opened ROW"
)

func newCompactCheckVolList(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdCompactCheckVolUse,
		Short: cmdCompactCheckVolShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var vols []*proto.VolInfo
			vols, err = client.AdminAPI().ListVols("")
			if err != nil {
				errout("list vols fail:\n%v\n", err)
			}
			stdout("[the volume of has opened ROW]\n")
			stdout("%v\n", formatCompactCheckVolViewTableHeader())
			index := 1
			for _, vol := range vols {
				volInfo, _ := client.AdminAPI().GetVolumeSimpleInfo(vol.Name)
				if volInfo.CrossRegionHAType != proto.CrossRegionHATypeQuorum && !volInfo.ForceROW {
					continue
				}
				stdout("%v\n", formatCompactCheckVolView(index, volInfo))
				index++
			}
		},
	}
	return cmd
}

const (
	cmdCompactCloseUse   = "close"
	cmdCompactCloseShort = "close volume compact"
	all                  = "all"
	no                   = "no"
)

type volumeOwner struct {
	volumeName string
	owner      string
}

func newCompactBatchCloseCmd(client *master.MasterClient) *cobra.Command {
	var optVolName string
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdCompactCloseUse,
		Short: cmdCompactCloseShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			if len(optVolName) == 0 {
				errout("Please input volume name")
			}

			// ask user for confirm
			if !optYes {
				stdout("  Volume  Name        : %v\n", optVolName)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			var compactVolumes []*proto.CompactVolume
			compactVolumes, err = client.AdminAPI().ListCompactVolumes()
			if err != nil {
				errout("close compact listCompactVolumes failed case:\n%v\n", err)
			}
			var compactVolumesMap = make(map[string]*proto.CompactVolume, len(compactVolumes))
			for _, cVolume := range compactVolumes {
				compactVolumesMap[cVolume.Name] = cVolume
			}
			var setCompactMsg string
			var volumeOwners []volumeOwner
			if optVolName == all {
				for _, cVolume := range compactVolumes {
					if cVolume.CompactTag != proto.CompactOpen {
						continue
					}
					volumeOwners = append(volumeOwners, volumeOwner{
						volumeName: cVolume.Name,
						owner:      cVolume.Owner,
					})
				}
			} else {
				volNames := strings.Split(optVolName, ",")
				for _, volName := range volNames {
					var cVolume *proto.CompactVolume
					var ok bool
					if cVolume, ok = compactVolumesMap[volName]; !ok {
						setCompactMsg += fmt.Sprintf("Volume(%v) does not need to close compact.\n", volName)
						continue
					}
					if cVolume.CompactTag != proto.CompactOpen {
						setCompactMsg += fmt.Sprintf("Volume(%v) has closed compact.\n", volName)
						continue
					}
					volumeOwners = append(volumeOwners, volumeOwner{
						volumeName: cVolume.Name,
						owner:      cVolume.Owner,
					})
				}
			}
			for _, vos := range volumeOwners {
				authKey := calcAuthKey(vos.owner)
				_, cErr := client.AdminAPI().SetCompact(vos.volumeName, proto.CompactCloseName, authKey)
				if cErr != nil {
					setCompactMsg += fmt.Sprintf("Volume(%v) close compact failed, err:%v\n", vos.volumeName, cErr)
				} else {
					setCompactMsg += fmt.Sprintf("Volume(%v) close compact succeeded\n", vos.volumeName)
				}
			}
			stdout(setCompactMsg)
			stdout("close volume compact end.\n")
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name, can split by comma or use 'all' stop all volume compact")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdCompactOpenUse   = "open"
	cmdCompactOpenShort = "open volume compact"
)

func newCompactBatchOpenCmd(client *master.MasterClient) *cobra.Command {
	var optVolName string
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdCompactOpenUse,
		Short: cmdCompactOpenShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			if len(optVolName) == 0 {
				errout("Please input volume name")
			}

			// ask user for confirm
			if !optYes {
				stdout("  Volume  Name        : %v\n", optVolName)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}
			var volumeOwners []volumeOwner
			volNames := strings.Split(optVolName, ",")
			for _, volName := range volNames {
				var vv *proto.SimpleVolView
				if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volName); err != nil {
					stdout("Volume(%v) open compact failed, err:%v\n", volName, err)
					continue
				}
				volumeOwners = append(volumeOwners, volumeOwner{
					volumeName: vv.Name,
					owner:      vv.Owner,
				})
			}
			var setCompactMsg string
			for _, vos := range volumeOwners {
				authKey := calcAuthKey(vos.owner)
				_, cErr := client.AdminAPI().SetCompact(vos.volumeName, proto.CompactOpenName, authKey)
				if cErr != nil {
					setCompactMsg += fmt.Sprintf("Volume(%v) open compact failed, err:%v\n", vos.volumeName, cErr)
				} else {
					setCompactMsg += fmt.Sprintf("Volume(%v) open compact successed\n", vos.volumeName)
				}
			}
			stdout(setCompactMsg)
			stdout("open volume compact end.\n")
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name, can split by comma")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdCompactCheckFragUse   = "checkFrg"
	cmdCompactCheckFragShort = "check volume inode fragmentation"
	ekMinLength              = 10
	ekMaxAvgSize             = 64
	inodeMinSize             = 1024
	CliFlagEkMinLength       = "ek-min-length"
	CliFlagEkMaxAvgSize      = "ek-max-avg-size"
	CliFlagInodeMinSize      = "inode-min-size"
	CliFlagSaveOverlap       = "save-overlap"
	CliFlagSaveFragment      = "save-frag"
	CliVolumeConcurrency     = "volume-con"
	CliMpConcurrency         = "mp-con"
	CliInodeConcurrency      = "inode-con"
	CliFlagSizeRange         = "size-range"
)

const (
	overlapTitle    = "volume,mpId,inodeId,ct,mt,preEkEnd,nextEkOffset\n"
	overlapFileName = "overlap.csv"
	// volume名称 mp数量 inode数量 ek总数 平均ek数 平均ek大小 文件平均大小 ek链最长inodeId ek链最长的长度 ek链最长文件大小 ek链最长文件ek平均大小
	fragmentFileName = "fragment.csv"
)

type EkBaseInfo struct {
	inodeCount     uint64
	totalEk        uint64
	totalInodeSize uint64
	avgEk          uint64
	avgInodeSize   uint64
	avgEkSize      uint64
}

type EkInfo struct {
	mpCount               int
	needCompactInodeCount uint64
	needCompactEkCount    uint64
	maxEkLen              uint64
	maxEkLenInodeId       uint64
	maxEkLenInodeSize     uint64
}

var (
	ekBaseInfos       = make(map[string][]*EkBaseInfo, 0)
	ekInfos           = make(map[string]*EkInfo, 0)
	ekInfosMutex      sync.RWMutex
	overlapInode      strings.Builder
	overlapInodeMutex sync.Mutex
)

func newCompactCheckFragCmd(client *master.MasterClient) *cobra.Command {
	var (
		optVolName           string
		optEkMinLength       uint64
		optEkMaxAvgSize      uint64
		optInodeMinSize      uint64
		optSaveOverlap       bool
		optSaveFragment      bool
		optVolumeConcurrency uint64
		optMpConcurrency     uint64
		optInodeConcurrency  uint64
		optSizeRange         string
	)
	var cmd = &cobra.Command{
		Use:   cmdCompactCheckFragUse,
		Short: cmdCompactCheckFragShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			if len(optVolName) == 0 {
				errout("Please input volume name")
			}
			stdout("[check inode fragmentation]\n")
			stdout("ekMinLength:%v ekMaxAvgSize:%vMB volumeConcurrency:%v mpConcurrency:%v inodeConcurrency:%v\n", optEkMinLength, optEkMaxAvgSize, optVolumeConcurrency, optMpConcurrency, optInodeConcurrency)
			stdout("%v\n", formatCompactCheckFragViewTableHeader())
			var volNames []string
			if optVolName == all {
				var vols []*proto.VolInfo
				vols, err = client.AdminAPI().ListVols("")
				if err != nil {
					errout("list vols err:%v", err)
				}
				for _, vol := range vols {
					volNames = append(volNames, vol.Name)
				}
			} else {
				volNames = strings.Split(optVolName, ",")
			}
			var sizeRanges []int
			if optSizeRange != no {
				sizeRanges = sizeRangeStrToInt(optSizeRange)
			}
			// create result file
			createResultFile(optSaveOverlap, optSaveFragment, sizeRanges)
			var wg sync.WaitGroup
			ch := make(chan struct{}, optVolumeConcurrency)
			for i, volName := range volNames {
				var mps []*proto.MetaPartitionView
				if mps, err = client.ClientAPI().GetPhysicalMetaPartitions(volName); err != nil {
					stdout("Volume(%v) got MetaPartitions failed, err:%v\n", volName, err)
					continue
				}
				wg.Add(1)
				ch <- struct{}{}
				go func(i int, volName string, mps []*proto.MetaPartitionView) {
					defer func() {
						if optSaveFragment {
							writeVolumeFragToFile(volName)
						}
						wg.Done()
						<-ch
					}()
					checkMps(i, volName, mps, client, optEkMinLength, optEkMaxAvgSize*1024*1024, optInodeMinSize, optMpConcurrency, optInodeConcurrency, optSaveOverlap, sizeRanges)
				}(i, volName, mps)
			}
			wg.Wait()
			stdout("check volume inode fragmentation end.\n")
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name, can split by comma")
	cmd.Flags().Uint64Var(&optEkMinLength, CliFlagEkMinLength, ekMinLength, "ek min length")
	cmd.Flags().Uint64Var(&optEkMaxAvgSize, CliFlagEkMaxAvgSize, ekMaxAvgSize, "ek max avg size, uint MB")
	cmd.Flags().Uint64Var(&optInodeMinSize, CliFlagInodeMinSize, inodeMinSize, "inode min size, uint Byte")
	cmd.Flags().BoolVar(&optSaveOverlap, CliFlagSaveOverlap, false, "save overlap file")
	cmd.Flags().BoolVar(&optSaveFragment, CliFlagSaveFragment, false, "save fragment file")
	cmd.Flags().Uint64Var(&optVolumeConcurrency, CliVolumeConcurrency, 2, "volume concurrency")
	cmd.Flags().Uint64Var(&optMpConcurrency, CliMpConcurrency, 5, "mp concurrency")
	cmd.Flags().Uint64Var(&optInodeConcurrency, CliInodeConcurrency, 20, "inode concurrency")
	cmd.Flags().StringVar(&optSizeRange, CliFlagSizeRange, "1,128", "Specify range, can split by comma")
	return cmd
}

func sizeRangeStrToInt(sizeRange string) (sizeRanges []int) {
	for _, sr := range strings.Split(sizeRange, ",") {
		var v int
		var err error
		v, err = strconv.Atoi(sr)
		if err != nil {
			errout("sizeRanges str to int,err: %v", err)
		}
		if v > 0 {
			sizeRanges = append(sizeRanges, v)
		}
	}
	m := make(map[int]struct{}, 0)
	for _, v := range sizeRanges {
		m[v] = struct{}{}
	}
	sizeRanges = sizeRanges[:0]
	for k := range m {
		sizeRanges = append(sizeRanges, k)
	}
	sort.Slice(sizeRanges, func(i, j int) bool {
		return sizeRanges[i] < sizeRanges[j]
	})
	return
}

func writeVolumeFragToFile(volume string) {
	ekInfosMutex.RLock()
	defer ekInfosMutex.RUnlock()
	var (
		ekBaseInfo      []*EkBaseInfo
		ekInfo          *EkInfo
		ok              bool
		totalInodeCount uint64
		totalEk         uint64
	)
	if ekBaseInfo, ok = ekBaseInfos[volume]; ok {
		for _, baseInfo := range ekBaseInfo {
			if baseInfo.inodeCount == 0 {
				continue
			}
			totalInodeCount += baseInfo.inodeCount
			totalEk += baseInfo.totalEk
			baseInfo.avgEk = baseInfo.totalEk / baseInfo.inodeCount
			baseInfo.avgInodeSize = baseInfo.totalInodeSize / baseInfo.inodeCount
			baseInfo.avgEkSize = baseInfo.totalInodeSize / baseInfo.totalEk
		}
		if totalInodeCount == 0 {
			return
		}
	}
	if ekInfo, ok = ekInfos[volume]; !ok {
		return
	}
	if ekInfo.needCompactInodeCount == 0 {
		stdout("needCompactInodeCount:%v needCompactEkCount:%v\n", ekInfo.needCompactInodeCount, ekInfo.needCompactEkCount)
	} else {
		stdout("needCompactInodeCount:%v needCompactEkCount:%v needCompactAvgEk:%v\n", ekInfo.needCompactInodeCount, ekInfo.needCompactEkCount, ekInfo.needCompactEkCount/ekInfo.needCompactInodeCount)
	}
	var ekBaseInfoStr string
	for i, baseInfo := range ekBaseInfo {
		base := fmt.Sprintf("%v,%v,%v,%v", baseInfo.inodeCount, baseInfo.avgEk, baseInfo.avgEkSize, baseInfo.avgInodeSize)
		if i != len(ekBaseInfo)-1 {
			ekBaseInfoStr += base + ","
		} else {
			ekBaseInfoStr += base
		}
	}
	maxLenAvgEkSize := ekInfo.maxEkLenInodeSize / ekInfo.maxEkLen
	fragmentLevel := calFragmentLevel(ekInfo.maxEkLen, maxLenAvgEkSize)
	volumeFrgResult := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
		volume, ekInfo.mpCount, totalInodeCount, totalEk, totalEk/totalInodeCount, ekInfo.maxEkLenInodeId, ekInfo.maxEkLen, ekInfo.maxEkLenInodeSize, maxLenAvgEkSize, fragmentLevel, ekBaseInfoStr)
	saveCsvResult(fragmentFileName, volumeFrgResult)
}

func createResultFile(saveOverlap, saveFragment bool, sizeRanges []int) {
	if saveOverlap {
		saveCsvTitles(overlapTitle, overlapFileName)
	}
	if saveFragment {
		var groupStr string
		if len(sizeRanges) == 0 {
			groupStr += fmt.Sprintf("inodeCount,avgEk,avgEkSize,avgInodeSize")
		} else if len(sizeRanges) == 1 {
			groupStr += fmt.Sprintf("\"(0,%vMB]\" inodeCount,\"(0,%vMB]\" avgEk,\"(0,%vMB]\" avgEkSize,\"(0,%vMB]\" avgInodeSize,", sizeRanges[0], sizeRanges[0], sizeRanges[0], sizeRanges[0])
			groupStr += fmt.Sprintf("\"(%vMB,∞)\" inodeCount,\"(%vMB,∞)\" avgEk,\"(%vMB,∞)\" avgEkSize,\"(%vMB,∞)\" avgInodeSize", sizeRanges[0], sizeRanges[0], sizeRanges[0], sizeRanges[0])
		} else {
			for i, sizeRange := range sizeRanges {
				if i == 0 {
					groupStr += fmt.Sprintf("\"(0,%vMB]\" inodeCount,\"(0,%vMB]\" avgEk,\"(0,%vMB]\" avgEkSize,\"(0,%vMB]\" avgInodeSize,", sizeRange, sizeRange, sizeRange, sizeRange)
				} else if i < len(sizeRanges)-1 {
					groupStr += fmt.Sprintf("\"(%v,%vMB]\" inodeCount,\"(%v,%vMB]\" avgEk,\"(%v,%vMB]\" avgEkSize,\"(%v,%vMB]\" avgInodeSize,", sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange)
				} else {
					groupStr += fmt.Sprintf("\"(%v,%vMB]\" inodeCount,\"(%v,%vMB]\" avgEk,\"(%v,%vMB]\" avgEkSize,\"(%v,%vMB]\" avgInodeSize,", sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange)
					groupStr += fmt.Sprintf("\"(%vMB,∞)\" inodeCount,\"(%vMB,∞)\" avgEk,\"(%vMB,∞)\" avgEkSize,\"(%vMB,∞)\" avgInodeSize", sizeRange, sizeRange, sizeRange, sizeRange)
				}
			}
		}
		fragmentTitle := fmt.Sprintf("volume,mpCount,totalInodeCount,totalEk,avgEk,maxEkLenInodeId,maxEkLen,maxEkLenInodeSize,maxLenAvgEkSize,fragment level,%v\n", groupStr)
		saveCsvTitles(fragmentTitle, fragmentFileName)
	}
}

func calFragmentLevel(maxEkLen, maxLenAvgEkSize uint64) string {
	if maxEkLen > 100 && maxLenAvgEkSize < 1*1024*1024 {
		return "高"
	} else if maxEkLen > 100 && maxLenAvgEkSize < 5*1024*1024 {
		return "中"
	} else {
		return "低"
	}
}

func saveCsvTitles(titles, filePath string) {
	if filePath == "" {
		return
	}
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer fd.Close()
	buf := make([]byte, 0)
	fdContent := titles
	buf = append(buf, []byte(fdContent)...)
	fd.Write(buf)
}

func saveCsvResult(filePath, result string) {
	if filePath == "" {
		return
	}
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer fd.Close()
	buf := make([]byte, 0)
	buf = append(buf, []byte(result)...)
	fd.Write(buf)
}

func checkMps(index int, volName string, mps []*proto.MetaPartitionView, client *master.MasterClient,
	ekMinLength, ekMaxAvgSize, inodeMinSize, mpConcurrency, inodeConcurrency uint64, saveOverlapFile bool, sizeRanges []int) {
	stdout("index:%v volume:%v\n", index, volName)
	ekInfosMutex.Lock()
	ekBaseInfos[volName] = make([]*EkBaseInfo, len(sizeRanges)+1)
	for i := 0; i < len(sizeRanges)+1; i++ {
		ekBaseInfos[volName][i] = &EkBaseInfo{}
	}
	ekInfos[volName] = &EkInfo{
		mpCount: len(mps),
	}
	ekInfosMutex.Unlock()
	var wg sync.WaitGroup
	var ch = make(chan struct{}, mpConcurrency)
	for _, mp := range mps {
		wg.Add(1)
		ch <- struct{}{}
		go func(mp *proto.MetaPartitionView) {
			defer func() {
				wg.Done()
				<- ch
			}()
			var mpInfo *proto.MetaPartitionInfo
			var err error
			if mpInfo, err = client.ClientAPI().GetMetaPartition(mp.PartitionID, ""); err != nil {
				stdout("Volume(%v) mpId(%v) get MetaPartition failed, err:%v\n", volName, mp.PartitionID, err)
				return
			}
			leaderAddr := getLeaderAddr(mpInfo.Replicas)
			var leaderNodeInfo *proto.MetaNodeInfo
			if leaderNodeInfo, err = client.NodeAPI().GetMetaNode(leaderAddr); err != nil {
				stdout("Volume(%v) mpId(%v) leaderAddr(%v) get metaNode info failed:%v\n", volName, mp.PartitionID, leaderAddr, err)
				return
			}
			if leaderNodeInfo.ProfPort == "" {
				leaderNodeInfo.ProfPort = "9092"
			}
			leaderIpPort := strings.Split(leaderNodeInfo.Addr, ":")[0] + ":" + leaderNodeInfo.ProfPort
			metaAdminApi := meta.NewMetaHttpClient(leaderIpPort, false)
			var inodeIds *proto.MpAllInodesId
			if inodeIds, err = getMpInodeIds(mpInfo.PartitionID, metaAdminApi); err != nil {
				stdout("Volume(%v) mpId(%v) leaderIpPort(%v) get MpInodeIds info failed:%v\n", volName, mp.PartitionID, leaderIpPort, err)
				return
			}
			inodeInfoCheck(mp.PartitionID, inodeIds.Inodes, leaderIpPort, volName, ekMinLength, ekMaxAvgSize, inodeMinSize, inodeConcurrency, sizeRanges)
		}(mp)
	}
	wg.Wait()

	if saveOverlapFile {
		overlapInodeMutex.Lock()
		if overlapInode.String() != "" {
			saveCsvResult(overlapFileName, overlapInode.String())
		}
		overlapInode.Reset()
		overlapInodeMutex.Unlock()
	}
}

func inodeInfoCheck(mpId uint64, inodes []uint64, leaderIpPort string, volName string, ekMinLength, ekMaxAvgSize, inodeMinSize, inodeConcurrency uint64, sizeRanges []int) {
	var wg sync.WaitGroup
	var ch = make(chan struct{}, inodeConcurrency)
	for _, inode := range inodes {
		wg.Add(1)
		ch <- struct{}{}
		go func(mpId, inode uint64, leaderIpPort string, volName string, ekMinLength, ekMaxAvgSize, inodeMinSize uint64) {
			defer func() {
				wg.Done()
				<-ch
			}()
			var extentInfo *proto.GetExtentsResponse
			var err error
			if extentInfo, err = getMpInodeInfo(mpId, inode, leaderIpPort); err != nil {
				stdout("Volume(%v) mpId(%v) inode(%v) leaderIpPort(%v) get MpInodeInfo info failed:%v\n", volName, mpId, inode, leaderIpPort, err)
				return
			}
			if extentInfo == nil {
				return
			}
			extLength := len(extentInfo.Extents)
			if extLength == 0 {
				return
			}
			stdoutOverlapInode(leaderIpPort, volName, mpId, inode, extentInfo)
			addEKData(volName, uint64(extLength), extentInfo.Size, inode, sizeRanges)
			if extentInfo.Size <= inodeMinSize {
				return
			}
			ekAvgSize := extentInfo.Size / uint64(extLength)
			if uint64(extLength) > ekMinLength && ekAvgSize < ekMaxAvgSize {
				addNeedCompactInodeSizeData(volName, uint64(extLength))
				stdout("%v\n", formatCompactCheckFragView(volName, mpId, inode, extLength, ekAvgSize, extentInfo.Size))
			}
		}(mpId, inode, leaderIpPort, volName, ekMinLength, ekMaxAvgSize, inodeMinSize)
	}
	wg.Wait()
}

func stdoutOverlapInode(leaderIpPort string, volName string, mpId, inode uint64, extentInfo *proto.GetExtentsResponse) {
	if extentInfo == nil {
		return
	}
	for i, extent := range extentInfo.Extents {
		if i >= len(extentInfo.Extents)-1 {
			break
		}
		if extent.FileOffset+uint64(extent.Size) > extentInfo.Extents[i+1].FileOffset {
			var ct, mt string
			var inodeInfo *proto.InodeInfoView
			var err error
			inodeInfo, err = getInode(leaderIpPort, mpId, inode)
			if inodeInfo != nil && err == nil {
				ct = inodeInfo.Ct
				mt = inodeInfo.Mt
			}
			overlapInodeMutex.Lock()
			// volume mpId inodeId ct mt preEkEnd nextEkOffset
			overlapInode.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v\n", volName, mpId, inode, ct, mt, extent.FileOffset+uint64(extent.Size), extentInfo.Extents[i+1].FileOffset))
			overlapInodeMutex.Unlock()
			break
		}
	}
}

func addEKData(volName string, extLength uint64, inodeSize uint64, inodeId uint64, sizeRanges []int) {
	ekInfosMutex.Lock()
	defer ekInfosMutex.Unlock()
	index := calSizeRangeIndex(inodeSize, sizeRanges)
	if ekBaseInfo, ok := ekBaseInfos[volName]; ok {
		ekBaseInfo[index].inodeCount++
		ekBaseInfo[index].totalEk += extLength
		ekBaseInfo[index].totalInodeSize += inodeSize
	} else {
		ekBaseInfos[volName] = make([]*EkBaseInfo, len(sizeRanges)+1)
		for i := 0; i < len(sizeRanges)+1; i++ {
			ekBaseInfos[volName][i] = &EkBaseInfo{}
		}
		ekBaseInfos[volName][index] = &EkBaseInfo{
			inodeCount:     1,
			totalEk:        extLength,
			totalInodeSize: inodeSize,
		}
	}
	if ekInfo, ok := ekInfos[volName]; ok {
		if ekInfo.maxEkLen < extLength {
			ekInfo.maxEkLen = extLength
			ekInfo.maxEkLenInodeId = inodeId
			ekInfo.maxEkLenInodeSize = inodeSize
		}
	} else {
		ekInfos[volName] = &EkInfo{
			maxEkLen:          extLength,
			maxEkLenInodeId:   inodeId,
			maxEkLenInodeSize: inodeSize,
		}
	}
}

func calSizeRangeIndex(inodeSize uint64, sizeRanges []int) int {
	if inodeSize == 0 {
		return -1
	}
	if len(sizeRanges) == 1 {
		if inodeSize <= uint64(sizeRanges[0])*1024*1024 {
			return 0
		}
		if inodeSize > uint64(sizeRanges[0])*1024*1024 {
			return 1
		}
	}
	for i, sr := range sizeRanges {
		if i == 0 {
			if inodeSize <= uint64(sr)*1024*1024 {
				return 0
			}
			if inodeSize > uint64(sr)*1024*1024 && inodeSize <= uint64(sizeRanges[i+1])*1024*1024 {
				return 1
			}
		} else if i == len(sizeRanges)-1 {
			if inodeSize > uint64(sr)*1024*1024 {
				return i + 1
			}
			if inodeSize > uint64(sizeRanges[i-1])*1024*1024 && inodeSize <= uint64(sr)*1024*1024 {
				return i
			}
		} else {
			if inodeSize <= uint64(sr)*1024*1024 && inodeSize > uint64(sizeRanges[i-1])*1024*1024 {
				return i
			}
		}
	}
	return 0
}

func addNeedCompactInodeSizeData(volName string, needCompactEkCount uint64) {
	ekInfosMutex.Lock()
	defer ekInfosMutex.Unlock()
	if ekInfo, ok := ekInfos[volName]; ok {
		ekInfo.needCompactInodeCount++
		ekInfo.needCompactEkCount += needCompactEkCount
	} else {
		ekInfos[volName] = &EkInfo{
			needCompactInodeCount: 1,
			needCompactEkCount:    needCompactEkCount,
		}
	}
}

func getLeaderAddr(replicas []*proto.MetaReplicaInfo) (leaderAddr string) {
	for _, replica := range replicas {
		if replica.IsLeader {
			leaderAddr = replica.Addr
			break
		}
	}
	return
}

func getMpInodeIds(mpId uint64, metaAdminApi *meta.MetaHttpClient) (inodeIds *proto.MpAllInodesId, err error) {
	inodeIds, err = metaAdminApi.ListAllInodesId(mpId, 0, 0, 0)
	return
}

func getMpInodeInfo(mpId uint64, inodeId uint64, leaderIpPort string) (res *proto.GetExtentsResponse, err error) {
	res, err = getExtentsByInodeId(mpId, inodeId, leaderIpPort)
	return
}

func getExtentsByInodeId(mpId uint64, inode uint64, leaderIpPort string) (re *proto.GetExtentsResponse, err error) {
	url := fmt.Sprintf("http://%s/getExtentsByInode?pid=%d&ino=%d", leaderIpPort, mpId, inode)
	httpClient := http.Client{Timeout: 10 * time.Second}
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
	if len(data) == 0 {
		return nil, nil
	}
	re = &proto.GetExtentsResponse{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func getInode(leaderIpPort string, mpId, ino uint64) (inodeInfoView *proto.InodeInfoView, err error) {
	httpClient := http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/getInode?pid=%d&ino=%d", leaderIpPort, mpId, ino))
	if err != nil {
		return
	}
	all, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	value := make(map[string]interface{})
	err = json.Unmarshal(all, &value)
	if err != nil {
		return
	}
	if value["msg"] != "Ok" {
		return
	}
	data := value["data"].(map[string]interface{})
	dataInfo := data["info"].(map[string]interface{})
	inodeInfoView = &proto.InodeInfoView{
		Ino:         uint64(dataInfo["ino"].(float64)),
		PartitionID: mpId,
		At:          dataInfo["at"].(string),
		Ct:          dataInfo["ct"].(string),
		Mt:          dataInfo["mt"].(string),
		Nlink:       uint64(dataInfo["nlink"].(float64)),
		Size:        uint64(dataInfo["sz"].(float64)),
		Gen:         uint64(dataInfo["gen"].(float64)),
		Gid:         uint64(dataInfo["gid"].(float64)),
		Uid:         uint64(dataInfo["uid"].(float64)),
		Mode:        uint64(dataInfo["mode"].(float64)),
	}
	return
}
