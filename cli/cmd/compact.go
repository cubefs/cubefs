// Copyright 2018 The Chubao Authors.
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
	"sort"
	"strings"
	"sync"
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
			var cv *proto.ClusterView
			cv, err = client.AdminAPI().GetCluster()
			if err != nil {
				errout("Get cluster info fail:\n%v\n", err)
			}
			stdout("[the volume of has opened ROW]\n")
			stdout("%v\n", formatCompactCheckVolViewTableHeader())
			index := 1
			for _, vol := range cv.VolStatInfo {
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
	CliFlagEkMinLength       = "ek-min-length"
	CliFlagEkMaxAvgSize      = "ek-max-avg-size"
)

func newCompactCheckFragCmd(client *master.MasterClient) *cobra.Command {
	var (
		optVolName      string
		optEkMinLength  uint64
		optEkMaxAvgSize uint64
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
			stdout("ekMinLength:%v ekMaxAvgSize:%vMB\n", optEkMinLength, optEkMaxAvgSize)
			stdout("%v\n", formatCompactCheckFragViewTableHeader())
			volNames := strings.Split(optVolName, ",")
			for _, volName := range volNames {
				var mps []*proto.MetaPartitionView
				if mps, err = client.ClientAPI().GetMetaPartitions(volName); err != nil {
					stdout("Volume(%v) got MetaPartitions failed, err:%v\n", volName, err)
					continue
				}
				checkMps(volName, mps, client, optEkMinLength, optEkMaxAvgSize*1024*1024)
			}
			stdout("check volume inode fragmentation end.\n")
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name, can split by comma")
	cmd.Flags().Uint64Var(&optEkMinLength, CliFlagEkMinLength, ekMinLength, "ek min length")
	cmd.Flags().Uint64Var(&optEkMaxAvgSize, CliFlagEkMaxAvgSize, ekMaxAvgSize, "ek max avg size, uint MB")
	return cmd
}

func checkMps(volName string, mps []*proto.MetaPartitionView, client *master.MasterClient, ekMinLength, ekMaxAvgSize uint64) {
	for _, mp := range mps {
		var mpInfo *proto.MetaPartitionInfo
		var err error
		if mpInfo, err = client.ClientAPI().GetMetaPartition(mp.PartitionID); err != nil {
			stdout("Volume(%v) mpId(%v) get MetaPartition failed, err:%v\n", volName, mp.PartitionID, err)
			continue
		}
		leaderAddr := getLeaderAddr(mpInfo.Replicas)
		var leaderNodeInfo *proto.MetaNodeInfo
		if leaderNodeInfo, err = client.NodeAPI().GetMetaNode(leaderAddr); err != nil {
			stdout("Volume(%v) mpId(%v) leaderAddr(%v) get metaNode info failed:%v\n", volName, mp.PartitionID, leaderAddr, err)
			continue
		}
		if leaderNodeInfo.ProfPort == "" {
			leaderNodeInfo.ProfPort = "9092"
		}
		leaderIpPort := strings.Split(leaderNodeInfo.Addr, ":")[0] + ":" + leaderNodeInfo.ProfPort
		metaAdminApi := meta.NewMetaHttpClient(leaderIpPort, false)
		var inodeIds *proto.MpAllInodesId
		if inodeIds, err = getMpInodeIds(mpInfo.PartitionID, metaAdminApi); err != nil {
			stdout("Volume(%v) mpId(%v) leaderIpPort(%v) get MpInodeIds info failed:%v\n", volName, mp.PartitionID, leaderIpPort, err)
			continue
		}
		inodeInfoCheck(mp.PartitionID, inodeIds.Inodes, leaderIpPort, volName, ekMinLength, ekMaxAvgSize)
	}
}

func inodeInfoCheck(mpId uint64, inodes []uint64, leaderIpPort string, volName string, ekMinLength, ekMaxAvgSize uint64) {
	var wg sync.WaitGroup
	var ch = make(chan struct{}, 10)
	for _, inode := range inodes {
		wg.Add(1)
		ch <- struct{}{}
		go func(mpId, inode uint64, leaderIpPort string, volName string, ekMinLength, ekMaxAvgSize uint64) {
			defer func() {
				wg.Done()
				<- ch
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
			ekAvgSize := extentInfo.Size / uint64(extLength)
			if uint64(extLength) >= ekMinLength && ekAvgSize < ekMaxAvgSize {
				stdout("%v\n", formatCompactCheckFragView(volName, mpId, inode, extLength, ekAvgSize, extentInfo.Size))
			}
		}(mpId, inode, leaderIpPort, volName, ekMinLength, ekMaxAvgSize)
	}
	wg.Wait()
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
