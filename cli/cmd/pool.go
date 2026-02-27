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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdPoolUse   = "pool [COMMAND]"
	cmdPoolShort = "Manage storage pools"
)

func newPoolCmd(client *sdk.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdPoolUse,
		Short: cmdPoolShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newPoolListCmd(client),
		newPoolInfoCmd(client),
		newPoolCreateCmd(client),
		newPoolUpdateCmd(client),
	)
	return cmd
}

const (
	cmdPoolListShort   = "List all storage pools"
	cmdPoolInfoShort   = "Show storage pool information"
	cmdPoolCreateShort = "Create a new storage pool"
	cmdPoolUpdateShort = "Update storage pool settings"
)

func newPoolListCmd(client *sdk.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:     CliOpList,
		Short:   cmdPoolListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var pools []*proto.StoragePoolInfo
			var err error
			defer func() {
				errout(err)
			}()
			if pools, err = client.AdminAPI().ListStoragePools(); err != nil {
				return
			}
			stdoutln("[Storage Pools]")
			stdoutln(formatPoolViewTableHeader())
			for _, pool := range pools {
				stdoutln(formatPoolViewTableRow(pool))
			}
		},
	}
	return cmd
}

func newPoolInfoCmd(client *sdk.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliOpInfo + " [POOL_ID]",
		Short: cmdPoolInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				poolId    uint8
				pool      *proto.StoragePoolInfo
				dataNodes []proto.NodeView
				err       error
			)
			defer func() {
				errout(err)
			}()
			id, err := strconv.ParseUint(args[0], 10, 8)
			if err != nil {
				err = fmt.Errorf("invalid pool ID: %v", args[0])
				return
			}
			poolId = uint8(id)
			if pool, err = client.AdminAPI().GetStoragePool(poolId); err != nil {
				return
			}
			// Get datanode list for replica storage pools
			if proto.IsStorageClassReplica(uint32(pool.StorageClass)) {
				if allDataNodes, err := client.AdminAPI().GetClusterDataNodes(); err == nil {
					// Filter datanodes by poolId
					for _, node := range allDataNodes {
						if node.PoolId == poolId {
							dataNodes = append(dataNodes, node)
						}
					}
				}
			}
			stdoutln("[Storage Pool Info]")
			stdoutln(formatPoolViewDetail(pool, dataNodes))
		},
	}
	return cmd
}

// formatPoolViewTableHeader returns the table header for pool list
func formatPoolViewTableHeader() string {
	pattern := "%-8v    %-20v    %-15v    %-20v    %-10v    %-30v"
	return fmt.Sprintf(pattern, "ID", "NAME", "STORAGE_CLASS", "STATUS", "CREATE_TIME", "EC_ADDR")
}

// formatPoolViewTableRow returns a table row for pool list
func formatPoolViewTableRow(pool *proto.StoragePoolInfo) string {
	pattern := "%-8v    %-20v    %-15v    %-20v    %-10v    %-30v"
	storageClassStr := proto.StorageClassString(uint32(pool.StorageClass))
	ecAddrStr := "-"
	if pool.ECAddr != "" {
		ecAddrStr = pool.ECAddr
	}
	createTime := time.Unix(pool.CreateTime, 0).Format(time.RFC3339)
	if len(createTime) > 10 {
		createTime = createTime[:10] // Show only date part
	}
	statusStr := proto.PoolStatusString(pool.Status)
	return fmt.Sprintf(pattern, pool.Id, pool.Name, storageClassStr, statusStr, createTime, ecAddrStr)
}

// formatPoolViewDetail returns detailed information of a pool
func formatPoolViewDetail(pool *proto.StoragePoolInfo, dataNodes []proto.NodeView) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("  ID           : %v\n", pool.Id))
	sb.WriteString(fmt.Sprintf("  Name         : %v\n", pool.Name))
	sb.WriteString(fmt.Sprintf("  StorageClass : %v\n", proto.StorageClassString(uint32(pool.StorageClass))))
	if pool.CId > 0 {
		sb.WriteString(fmt.Sprintf("  CId          : %v\n", pool.CId))
	}
	if pool.ECAddr != "" {
		sb.WriteString(fmt.Sprintf("  ECAddr       : %v\n", pool.ECAddr))
	}
	sb.WriteString(fmt.Sprintf("  Status       : %v\n", proto.PoolStatusString(pool.Status)))
	sb.WriteString(fmt.Sprintf("  CreateTime   : %v\n", time.Unix(pool.CreateTime, 0).Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("  UpdateTime   : %v\n", time.Unix(pool.UpdateTime, 0).Format(time.RFC3339)))

	// Show datanode list for replica storage pools
	if len(dataNodes) > 0 {
		sb.WriteString(fmt.Sprintf("  DataNodes    : %d nodes\n", len(dataNodes)))
		sb.WriteString(formatDataNodeList(dataNodes))
	}

	return sb.String()
}

// formatDataNodeList formats the datanode list for display
func formatDataNodeList(dataNodes []proto.NodeView) string {
	if len(dataNodes) == 0 {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString("\n    [Data Node List]\n")
	// Table header
	pattern := "    %-20v    %-10v    %-10v    %-15v    %-15v\n"
	sb.WriteString(fmt.Sprintf(pattern, "ADDR", "ID", "STATUS", "ZONE", "NODESET_ID"))

	// Table rows
	for _, node := range dataNodes {
		status := "Inactive"
		if node.Status {
			status = "Active"
		}
		sb.WriteString(fmt.Sprintf(pattern, node.Addr, node.ID, status, node.ZoneName, node.NodeSetID))
	}
	return sb.String()
}

func newPoolCreateCmd(client *sdk.MasterClient) *cobra.Command {
	var (
		poolId       uint8
		poolName     string
		storageClass uint8
		cId          int
		ecAddr       string
	)
	cmd := &cobra.Command{
		Use:   CliOpCreate + " --id [POOL_ID] --name [POOL_NAME]",
		Short: cmdPoolCreateShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if poolId == 0 {
				err = fmt.Errorf("pool id is required")
				return
			}
			if poolName == "" {
				err = fmt.Errorf("pool name is required")
				return
			}
			if storageClass == 0 {
				err = fmt.Errorf("storage class cannot be 0 (Unspecified), must be 1 (ReplicaSSD), 2 (ReplicaHDD), or 3 (BlobStore)")
				return
			}
			if !proto.IsValidStorageClass(uint32(storageClass)) {
				err = fmt.Errorf("invalid storage class: %d, must be 1 (ReplicaSSD), 2 (ReplicaHDD), or 3 (BlobStore)", storageClass)
				return
			}
			poolInfo := &proto.StoragePoolInfo{
				Id:           poolId,
				Name:         poolName,
				StorageClass: storageClass,
				CId:          cId,
				ECAddr:       ecAddr,
			}
			if err = client.AdminAPI().CreateStoragePool(poolInfo); err != nil {
				return
			}
			stdoutln(fmt.Sprintf("Create storage pool id[%d] name[%s] successfully", poolId, poolName))
		},
	}
	cmd.Flags().Uint8Var(&poolId, "id", 0, "Pool ID (must be greater than 3)")
	cmd.Flags().StringVar(&poolName, "name", "", "Pool name (required)")
	cmd.Flags().Uint8Var(&storageClass, "storageClass", 0, "Storage class (1=ReplicaSSD, 2=ReplicaHDD, 3=BlobStore), cannot be 0")
	// cmd.Flags().IntVar(&cId, "cId", 0, "EC cluster ID (only for EC pool)")
	cmd.Flags().StringVar(&ecAddr, "ecAddr", "", "EC cluster address (only for EC pool)")
	return cmd
}

func newPoolUpdateCmd(client *sdk.MasterClient) *cobra.Command {
	var (
		poolId   uint8
		poolName string
		cId      int
		ecAddr   string
	)
	cmd := &cobra.Command{
		Use:   CliOpUpdate + " --id [POOL_ID]",
		Short: cmdPoolUpdateShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if poolId == 0 {
				err = fmt.Errorf("pool id is required")
				return
			}
			poolInfo := &proto.StoragePoolInfo{
				Id:     poolId,
				Name:   poolName,
				CId:    cId,
				ECAddr: ecAddr,
			}
			if err = client.AdminAPI().UpdateStoragePool(poolId, poolInfo); err != nil {
				return
			}
			stdoutln(fmt.Sprintf("Update storage pool id[%d] successfully", poolId))
		},
	}
	cmd.Flags().Uint8Var(&poolId, "id", 0, "Pool ID (required)")
	cmd.Flags().StringVar(&poolName, "name", "", "Pool name (optional)")
	// cmd.Flags().IntVar(&cId, "cId", 0, "EC cluster ID (optional, only for EC pool)")
	// cmd.Flags().StringVar(&ecAddr, "ecAddr", "", "EC cluster address (optional, only for EC pool)
	return cmd
}
