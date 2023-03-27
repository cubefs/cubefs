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
	"github.com/chubaofs/chubaofs/ecstorage"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cmdEcPartitionUse   = "ecpartition [COMMAND]"
	cmdEcPartitionShort = "Manage EC partition"
)

func newEcPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdEcPartitionUse,
		Short: cmdEcPartitionShort,
	}
	cmd.AddCommand(
		newEcPartitionGetCmd(client),
		newEcPartitionDecommissionCmd(client),
		newEcPartitionDeleteReplicaCmd(client),
		newEcPartitionReplicateCmd(client),
		newEcPartitionRollBackCmd(client),
		newListUnHealthEcPartitionsCmd(client),
		newEcPartitionCheckChunkDataConsist(client),
		newEcPartitionGetExtentHosts(client),
		newEcPartitionCheckConsistency(client),
		newEcGetTinyExtentDelInfo(client),
		newEcSetPartitionSize(client),
	)
	return cmd
}

const (
	cmdEcPartitionGetShort              = "Display detail information of a ec partition"
	cmdEcPartitionDecommissionShort     = "Decommission a replication of the ec partition to a new address"
	cmdEcPartitionDeleteReplicaShort    = "Delete a replication of the ec partition on a fixed address"
	cmdEcPartitionReplicateShort        = "Add a replication of the ec partition on a new address"
	cmdEcPartitionRollBackShort         = "Will rollback from ecpartition to datapartition and delEcPartition ?"
	cmdCheckCorruptEcPartitionsShort    = "Check and list unhealthy ec partitions"
	cmdEcPartitionCheckStripeDataConsist = "Check ec partition extent stripe data consist"
	cmdGetExtentHosts                   = "get extent hosts"
	cmdGetTinyDelInfo                   = "get tiny extent del info"
	cmdEcPartitionCheckConsistency      = "Check Consistency of ec partition and data partition"
	cmdEcSetPartitionSize               = "set ecPartition size"
)

func newEcPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [EC PARTITION ID]",
		Short: cmdEcPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.EcPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetEcPartition("", partitionID); err != nil {
				return
			}
			stdout(formatEcPartitionInfo(partition))
		},
	}
	return cmd
}

func newEcPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [EC PARTITION ID]",
		Short: cmdEcPartitionDecommissionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			data, err := client.AdminAPI().DecommissionEcPartition(partitionID, address)
			if err != nil {
				stdout("%v", err)
			}else {
				stdout("%v", string(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func validEcNodes(client *master.MasterClient, toComplete string) []string {
	var (
		validEcNodes []string
		clusterView  *proto.ClusterView

		err error
	)
	if clusterView, err = client.AdminAPI().GetCluster(); err != nil {
		errout("Get ec node list failed:\n%v\n", err)
	}
	for _, en := range clusterView.EcNodes {
		validEcNodes = append(validEcNodes, en.Addr)
	}
	return validEcNodes
}

func newEcPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [EC PARTITION ID]",
		Short: cmdEcPartitionDeleteReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			data, err := client.AdminAPI().DeleteEcReplica(partitionID, address)
			if err != nil {
				stdout("%v", err)
			}else {
				stdout("%v", string(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newEcPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [ADDRESS] [EC PARTITION ID]",
		Short: cmdEcPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			data, err := client.AdminAPI().AddEcReplica(partitionID, address)
			if err != nil {
				stdout("%v", err)
			}else {
				stdout("%v", string(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newEcPartitionRollBackCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpRollBack + " [EC PARTITION ID]" + " delEc(false/true)",
		Short: cmdEcPartitionRollBackShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			needDelEc, err := strconv.ParseBool(args[1])
			if err != nil {
				stdout("%v\n", err)
				return
			}
			data, err := client.AdminAPI().SetEcRollBack(partitionID, needDelEc)
			if err != nil {
				stdout("%v", err)
			}else {
				stdout("%v", string(data))
			}
		},
	}
	return cmd
}

func newListUnHealthEcPartitionsCmd(client *master.MasterClient) *cobra.Command {
	var optCheckAll bool
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckCorruptEcPartitionsShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				diagnosis *proto.EcPartitionDiagnosis
				ecNodes   []*proto.EcNodeInfo
				err       error
			)
			if optCheckAll {
				err = checkAllEcPartitions(client)
				if err != nil {
					errout("%v\n", err)
				}
				return
			}

			if diagnosis, err = client.AdminAPI().DiagnoseEcPartition(); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("[Inactive Ec nodes]:\n")
			stdout("%v\n", formatDataNodeDetailTableHeader())
			for _, addr := range diagnosis.InactiveEcNodes {
				var ecNode *proto.EcNodeInfo
				ecNode, err = client.NodeAPI().GetEcNode(addr)
				ecNodes = append(ecNodes, ecNode)
			}
			sort.SliceStable(ecNodes, func(i, j int) bool {
				return ecNodes[i].ID < ecNodes[j].ID
			})
			for _, ecNode := range ecNodes {
				stdout("%v\n", formatEcNodeDetail(ecNode, true))
			}

			stdout("\n")
			stdout("%v\n", "[ecPartition lack replicas]:")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.LackReplicaEcPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaEcPartitionIDs[i] < diagnosis.LackReplicaEcPartitionIDs[j]
			})

			for _, pid := range diagnosis.LackReplicaEcPartitionIDs {
				var (
					partition *proto.EcPartitionInfo
				)
				if partition, err = client.AdminAPI().GetEcPartition("", pid); err != nil || partition == nil {
					stdout("get partition error, err:[%v]", err)
					return
				}
				stdout("%v", formatEcPartitionInfoRow(partition))
				sort.Strings(partition.Hosts)
				if len(partition.MissingNodes) > 0 || partition.Status == -1 {
					stdoutRed(fmt.Sprintf("partition not ready to repair"))
					continue
				}
				if len(partition.Hosts) != int(partition.ReplicaNum) {
					stdoutRed(fmt.Sprintf("replica number(expected is %v, but is %v) not match ", partition.ReplicaNum, len(partition.Hosts)))
					continue
				}
				var lackAddr []string
				for _, replica := range partition.EcReplicas {
					if !contains(partition.Hosts, replica.Addr) {
						lackAddr = append(lackAddr, replica.Addr)
					}
				}
				if len(lackAddr) != 1 {
					stdoutRed(fmt.Sprintf("Not classic partition, please check and repair it manually"))
					continue
				}
				stdoutGreen(fmt.Sprintf(" The Lack Address is: %v", lackAddr))
				stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
			}
			return
		},
	}
	cmd.Flags().BoolVar(&optCheckAll, "all", false, "true - check all ecPartitions; false - only check partitions which lack of replica")
	return cmd
}

func checkAllEcPartitions(client *master.MasterClient) (err error) {
	var (
		volInfo []*proto.VolInfo
	)
	if volInfo, err = client.AdminAPI().ListVols(""); err != nil {
		stdout("%v\n", err)
		return
	}
	stdout("\n")
	stdout("%v\n", "[Partition peer info not valid]:")
	stdout("%v\n", partitionInfoTableHeader)
	for _, vol := range volInfo {
		var (
			volView *proto.VolView
			volLock sync.Mutex
			wg      sync.WaitGroup
		)
		if volView, err = client.ClientAPI().GetVolume(vol.Name, calcAuthKey(vol.Owner)); err != nil {
			stdout("Found an invalid vol: %v\n", vol.Name)
			continue
		}
		if len(volView.EcPartitions) == 0 {
			continue
		}
		sort.SliceStable(volView.EcPartitions, func(i, j int) bool {
			return volView.EcPartitions[i].PartitionID < volView.EcPartitions[j].PartitionID
		})
		for _, ep := range volView.EcPartitions {
			wg.Add(1)
			go func(ep *proto.EcPartitionResponse) {
				defer wg.Done()
				var outPut string
				var isHealthy bool
				outPut, isHealthy, _ = checkEcPartition(ep.PartitionID, client)
				if !isHealthy {
					volLock.Lock()
					fmt.Printf(outPut)
					//stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
					fmt.Printf(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
					volLock.Unlock()
				}
			}(ep)
		}
		wg.Wait()
	}
	return
}

func checkEcPartition(pid uint64, client *master.MasterClient) (outPut string, isHealthy bool, err error) {
	var (
		partition    *proto.EcPartitionInfo
		errorReports []string
		sb           = strings.Builder{}
	)
	defer func() {
		isHealthy = true
		if len(errorReports) > 0 {
			isHealthy = false
			for i, msg := range errorReports {
				sb.WriteString(fmt.Sprintf("%-8v\n", fmt.Sprintf("error %v: %v", i+1, msg)))
			}
		}
		outPut = sb.String()
	}()
	if partition, err = client.AdminAPI().GetEcPartition("", pid); err != nil || partition == nil {
		errorReports = append(errorReports, fmt.Sprintf("get partition error, err:[%v]", err))
		return
	}
	sb.WriteString(fmt.Sprintf("%v", formatEcPartitionInfoRow(partition)))
	sort.Strings(partition.Hosts)
	if len(partition.MissingNodes) > 0 || partition.Status == -1 || len(partition.Hosts) != int(partition.ReplicaNum) {
		errorReports = append(errorReports, PartitionNotHealthyInMaster)
	}
	return
}

func newEcPartitionCheckChunkDataConsist(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheckEcData + " [EC PARTITION ID]",
		Short: cmdEcPartitionCheckStripeDataConsist,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.EcPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetEcPartition("", partitionID); err != nil {
				return
			}
			if partition.EcMigrateStatus != proto.FinishEC && partition.EcMigrateStatus != proto.OnlyEcExist {
				stdout("partition not migrate (%v)", partitionID)
				return
			}
			err = checkEcPartitionChunkData(partition)
			if err != nil {
				stdout("partition(%v) check chunk data: %v\n", partitionID, err)
			} else {
				stdout("partition(%v) all extent chunk data is consist\n", partitionID)
			}
		},
	}
	return cmd
}

func newEcPartitionGetExtentHosts(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGetEcExtentHosts + " [EC PARTITION ID]" + " [EXTENT ID]",
		Short: cmdGetExtentHosts,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.EcPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			extentID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetEcPartition("", partitionID); err != nil {
				return
			}
			nodeNum := partition.DataUnitsNum + partition.ParityUnitsNum
			hosts := proto.GetEcHostsByExtentId(uint64(nodeNum), extentID, partition.Hosts)
			stdout("partition(%v) extentId(%v) hosts(%v)\n", partitionID, extentID, hosts)
		},
	}
	return cmd
}

func getTinyDelInfo(ep *proto.EcPartitionInfo, extentId uint64) (delInfos []*proto.TinyDelInfo, err error) {
	var (
		httpPort int
		resp     *http.Response
		respData []byte
	)
	httpClient := http.Client{
		Timeout: 2 * time.Minute,
	}
	delInfos = make([]*proto.TinyDelInfo, 0)
	if extentId == 0 {
		stdout("**************partition(%v) all tinyExtents delInfos********************* \n\n",
			ep.PartitionID)
	}else {
		stdout("**************partition(%v) extentId(%v) delInfos********************* \n\n",
			ep.PartitionID, extentId)
	}
	for _, replica := range ep.EcReplicas {
		httpPort, err = strconv.Atoi(replica.HttpPort)
		if err != nil {
			continue
		}
		ecNode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], httpPort)
		url := fmt.Sprintf("http://%s/getTinyDelInfo?partitionID=%d&extentID=%d", ecNode, ep.PartitionID, extentId)
		resp, err = httpClient.Get(url)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			err = errors.NewErrorf("extent(%v) errCode[%v]", extentId, resp.StatusCode)
			if err != nil {
				continue
			}
		}
		respData, err = ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			log.LogWarnf("serveRequest: read http response body fail: err(%v)", err)
			continue
		}
		var data []byte
		if data, err = parseResp(respData); err != nil {
			return
		}
		err = json.Unmarshal(data, &delInfos)
		stdout("*****************node(%v)******************\n", replica.Addr)
		for _, delInfo := range delInfos {
			stdout("extentId(%v) offset(%v) size(%v) deleteStatus(%v) delNodeAddr(%v)\n",
				delInfo.ExtentId, delInfo.Offset, delInfo.Size, tinyDelStatusMap[delInfo.DeleteStatus], ep.Hosts[delInfo.HostIndex])
		}
		continue
	}

	return
}

func newEcGetTinyExtentDelInfo(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGetEcTinyDelInfo + " [EC PARTITION ID]" + " [EXTENT ID]" + " [Addr]",
		Short: cmdGetTinyDelInfo,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.EcPartitionInfo
				partitionID uint64
				extentID    uint64
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if len(args) > 1 {
				extentID, err = strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					return
				}
				if !ecstorage.IsTinyExtent(extentID) && extentID != 0 {
					stdout("just for get tinyDelInfo\n")
					return
				}
			}

			if partition, err = client.AdminAPI().GetEcPartition("", partitionID); err != nil {
				return
			}
			_, err = getTinyDelInfo(partition, extentID)
			if err != nil {
				stdout("partition(%v) extentId(%v) err(%v)\n", partitionID, extentID, err)
				return
			}
		},
	}
	return cmd
}

func parseRespMsg(resp []byte) (msg string, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(resp, &body); err != nil {
		return
	}
	msg = body.Msg
	return
}

func getAllEcExtents(ep *proto.EcPartitionInfo) (extents []*ecstorage.ExtentInfo, err error) {
	var (
		httpPort int
		resp     *http.Response
		respData []byte
	)
	extents = make([]*ecstorage.ExtentInfo, 0)
	httpClient := http.Client{
		Timeout: 2 * time.Minute,
	}
	for _, replica := range ep.EcReplicas {
		httpPort, err = strconv.Atoi(replica.HttpPort)
		if err != nil {
			continue
		}
		ecNode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], httpPort)
		url := fmt.Sprintf("http://%s/getAllExtents?partitionID=%d", ecNode, ep.PartitionID)
		resp, err = httpClient.Get(url)
		if err != nil {
			continue
		}
		respData, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			stdout("errMsg(%v)", resp)
			err = errors.NewErrorf("err statusCode(%v)", resp.StatusCode)
			continue
		}
		var data []byte
		if data, err = parseResp(respData); err != nil {
			return
		}
		err = json.Unmarshal(data, &extents)
		if err == nil {
			break
		}
	}
	return
}

func checkEcExtentChunkData(ep *proto.EcPartitionInfo, extentId uint64) (err error) {
	var (
		httpPort int
		resp     *http.Response
	)
	httpClient := http.Client{
		Timeout: 2 * time.Minute,
	}
	for _, replica := range ep.EcReplicas {
		httpPort, err = strconv.Atoi(replica.HttpPort)
		if err != nil {
			continue
		}
		ecNode := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], httpPort)
		url := fmt.Sprintf("http://%s/checkExtentChunkData?partitionID=%d&extentID=%d", ecNode, ep.PartitionID, extentId)
		resp, err = httpClient.Get(url)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			errMsg, _ := parseErrMsg(resp)
			err = errors.NewErrorf("extent(%v) errCode(%v) errMsg(%v)", extentId, resp.StatusCode, errMsg)
		}
		break
	}
	return
}

func checkEcPartitionChunkData(ep *proto.EcPartitionInfo) (err error) {
	extentIds, err := getAllEcExtents(ep)
	if err != nil {
		return
	}
	for _, extent := range extentIds {
		err = checkEcExtentChunkData(ep, extent.FileID)
		if err != nil {
			return
		}
	}
	return
}

func newEcPartitionCheckConsistency(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpCheckConsistency + " [EC PARTITION ID]",
		Short: cmdEcPartitionCheckConsistency,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			ep, err := client.AdminAPI().GetEcPartition("", partitionID)
			if err != nil {
				stdout("GetEcPartition(%v) err:%v\n", partitionID, err)
				return
			}
			if ep.EcMigrateStatus != proto.FinishEC {
				stdout("EcMigrateStatus(%v) != proto.FinishEC(%v)\n", ep.EcMigrateStatus, proto.FinishEC)
				return
			}
			dp, err := client.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				stdout("GetDataPartition(%v) err:%v\n", partitionID, err)
				return
			}
			if _, err := startCheckEcPartitionConsistency(dp, ep, client, true); err != nil {
				stdout("startCheckEcPartitionConsistency(%v) err:%v\n", partitionID, err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validEcNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func startCheckEcPartitionConsistency(dp *proto.DataPartitionInfo, ep *proto.EcPartitionInfo, client *master.MasterClient, printLog bool) (errExtent []uint64, err error) {
	if dp == nil || ep == nil {
		return nil, fmt.Errorf("dp == nil || ep == nil")
	}
	var (
		extentInfo = make([]uint64, 0)
		//[extentId]--->[hostAddr]:extentCrc
		dpCrc = make(map[uint64]map[string]uint32)
		//[extentId]:extentCrc
		epCrc = make(map[uint64]uint32)
		dpWg  sync.WaitGroup
		epWg  sync.WaitGroup
	)
	if extentInfo, err = getExtentInfo(dp, client); err != nil {
		return
	}

	dpWg.Add(1)
	epWg.Add(1)

	go getDataPartitionCrc(dp, dpCrc, extentInfo, &dpWg, client, printLog)
	go getEcPartitionCrc(ep, epCrc, extentInfo, &epWg, client, printLog)

	dpWg.Wait()
	epWg.Wait()

	errExtent = compareCrc(dpCrc, epCrc, extentInfo, printLog)
	return
}

func getEcPartitionCrc(ep *proto.EcPartitionInfo, epCrc map[uint64]uint32, extentInfo []uint64, epWg *sync.WaitGroup, client *master.MasterClient, printLog bool) {
	defer epWg.Done()
	nodeNum := ep.DataUnitsNum + ep.ParityUnitsNum
	for _, extentId := range extentInfo {
		if printLog {
			stdout("EcPartition:%v Extent:%v start\n", ep.PartitionID, extentId)
		}
		hosts := proto.GetEcHostsByExtentId(uint64(nodeNum), extentId, ep.Hosts)
		var (
			crc         uint32
			err         error
			crcResp     = &proto.ExtentCrcResponse{}
			stripeCount = uint64(1)
			isErr       bool
		)
	cycle:
		for {
			for idx := 0; idx < int(ep.DataUnitsNum); idx++ {
				host := hosts[idx]
				arr := strings.Split(host, ":")
				if printLog {
					stdout("  from EcNode(%v) curStripe(%v) get crc\n", host, stripeCount)
				}
				if crcResp, err = client.NodeAPI().EcNodeGetExtentCrc(arr[0], ep.PartitionID, extentId, stripeCount, crc); err != nil {
					if printLog {
						stdout("  EcNode(%v) GetExtentCrc err(%v)\n", host, err)
					}
					isErr = true
					break cycle
				}
				crc = crcResp.CRC
				if crcResp.FinishRead {
					break cycle
				}
			}
			stripeCount++
		}
		if isErr {
			continue
		}
		epCrc[extentId] = crc
	}
}

func compareCrc(dpCrc map[uint64]map[string]uint32, epCrc map[uint64]uint32, extentInfo []uint64, printLog bool) (errExtent []uint64) {
	if printLog {
		stdout("*******Start compare CRC*********\n")
	}
	errExtent = make([]uint64, 0)
	for _, extentId := range extentInfo {
		dataNodeCrc, ok := dpCrc[extentId]
		if !ok {
			stdout("DataNode not find extent(%v)\n", extentId)
			continue
		}
		ecNodeCrc, ok := epCrc[extentId]
		if !ok {
			var invalid int
			for _, crc := range dataNodeCrc {
				if crc == 0 {
					invalid++
				}
			}
			if invalid <= len(dataNodeCrc)/2 {
				stdout("EcNode not find extent(%v)\n", extentId)
			}
			continue
		}
		if printLog {
			stdout("extent[%v]:\n", extentId)
		}
		var isEqual bool
		for host, crc := range dataNodeCrc {
			if printLog {
				stdout("  DataNode:\n")
				stdout("    [%v]CRC:%v \n", host, crc)
			}
			if ecNodeCrc == crc {
				isEqual = true
			}
		}
		if printLog {
			stdout("  EcNode:\n")
			stdout("    CRC:%v ", ecNodeCrc)
		}
		if isEqual {
			if printLog {
				stdoutGreen("    PASS")
			}
		} else {
			if printLog {
				stdoutRed("    FAIL")
			}
			errExtent = append(errExtent, extentId)
		}
	}
	return
}

func newEcSetPartitionSize(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpSet + " [EC PARTITION ID]" + " [newSize]",
		Short: cmdEcSetPartitionSize,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.EcPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}

			size, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}

			if partition, err = client.AdminAPI().GetEcPartition("", partitionID); err != nil {
				return
			}
			err = setPartitionSize(partition, size)
			if err != nil {
				stdout("setPartitionSize err(%v)\n", err)
			}
		},
	}
	return cmd
}

func setPartitionSize(ep *proto.EcPartitionInfo, size uint64) (err error) {
	var (
		httpPort int
		resp     *http.Response
	)
	httpClient := http.Client{
		Timeout: 2 * time.Minute,
	}
	for _, replica := range ep.EcReplicas {
		httpPort, err = strconv.Atoi(replica.HttpPort)
		if err != nil {
			continue
		}
		ecNodeAddr := fmt.Sprintf("%s:%d", strings.Split(replica.Addr, ":")[0], httpPort)
		url := fmt.Sprintf("http://%s/setEcPartitionSize?partitionId=%v&size=%v", ecNodeAddr, ep.PartitionID, size)
		resp, err = httpClient.Get(url)
		if err != nil {
			return
		}
		if resp.StatusCode != http.StatusOK {
			errMsg, _ := parseErrMsg(resp)
			err = errors.NewErrorf("host(%v) errCode(%v) errMsg(%v)", ecNodeAddr, resp.StatusCode, errMsg)
			return
		}
	}
	return
}

func parseErrMsg(resp *http.Response) (msg string, err error) {
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(respData, &body); err != nil {
		return
	}
	msg = body.Msg
	return
}
