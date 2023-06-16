// Copyright 2023 The CubeFS Authors.
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
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/spf13/cobra"
)

const (
	cmdQuotaUse           = "quota [COMMAND]"
	cmdQuotaShort         = "Manage cluster quota"
	cmdQuotaCreateUse     = "create [volname] [fullpath1,fullpath2]"
	cmdQuotaCreateShort   = "create paths quota"
	cmdQuotaListUse       = "list [volname]"
	cmdQuotaListShort     = "list volname all quota"
	cmdQuotaUpdateUse     = "update [volname] [quotaId]"
	cmdQuotaUpdateShort   = "update path quota"
	cmdQuotaDeleteUse     = "delete [volname] [quotaId]"
	cmdQUotaDeleteShort   = "delete path quota"
	cmdQuotaGetInodeUse   = "getInode [volname] [inode]"
	cmdQuotaGetInodeShort = "get inode quotaInfo"
	cmdQuotaListAllUse    = "listAll"
	cmdQuotaListAllShort  = "list all volname has quota"
	cmdQuotaApplyUse      = "apply [volname] [quotaId]"
	cmdQuotaApplyShort    = "apply quota"
	cmdQuotaRevokeUse     = "revoke [volname] [quotaId]"
	cmdQuotaRevokeShort   = "revoke quota"
)

const (
	cmdQuotaDefaultMaxFiles = math.MaxUint64
	cmdQuotaDefaultMaxBytes = math.MaxUint64
)

func newQuotaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdQuotaUse,
		Short:   cmdQuotaShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"quota"},
	}
	proto.InitBufferPool(32768)
	cmd.AddCommand(
		newQuotaListCmd(client),
		newQuotaCreateCmd(client),
		newQuotaUpdateCmd(client),
		newQuotaDelete(client),
		newQuotaGetInode(client),
		newQuotaListAllCmd(client),
		newQuotaApplyCmd(client),
		newQuotaRevokeCmd(client),
	)
	return cmd
}

func newQuotaCreateCmd(client *master.MasterClient) *cobra.Command {
	var maxFiles uint64
	var maxBytes uint64

	var cmd = &cobra.Command{
		Use:   cmdQuotaCreateUse,
		Short: cmdQuotaCreateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			volName := args[0]
			fullPath := args[1]

			var metaConfig = &meta.MetaConfig{
				Volume:  volName,
				Masters: client.Nodes(),
			}
			metaWrapper, err := meta.NewMetaWrapper(metaConfig)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			fullPaths := strings.Split(fullPath, ",")
			err = checkNestedDirectories(fullPaths)
			if err != nil {
				stdout("create quota failed, fullPaths %v has nested.\n", fullPaths)
				return
			}
			if len(fullPaths) > 5 {
				stdout("create quota failed, fullPath %v has more than 5 path.\n", fullPaths)
				return
			}
			quotaPathInofs := make([]proto.QuotaPathInfo, 0, 0)
			for _, path := range fullPaths {
				var quotaPathInfo proto.QuotaPathInfo
				quotaPathInfo.FullPath = path

				if strings.Count(path, "/") != 1 {
					stdout("create quota failed, path %v does not has only one / \n", path)
					return
				}
				if !strings.HasPrefix(path, "/") {
					stdout("create quota failed, path %v does not start with / \n", path)
					return
				}

				inodeId, err := metaWrapper.LookupPath(path)
				if err != nil {
					stdout("get inode by fullPath %v fail %v\n", path, err)
					return
				}
				quotaPathInfo.RootInode = inodeId
				inodeInfo, err := metaWrapper.InodeGet_ll(inodeId)
				if err != nil {
					stdout("get inode %v info fail %v\n", inodeId, err)
					return
				}

				if !proto.IsDir(inodeInfo.Mode) {
					stdout("create quota failed, inode [%v] is not dir\n", inodeId)
					return
				}

				mp := metaWrapper.GetPartitionByInodeId_ll(inodeId)
				if mp == nil {
					stdout("can not find mp by inodeId: %v\n", inodeId)
					return
				}
				quotaPathInfo.PartitionId = mp.PartitionID
				quotaPathInofs = append(quotaPathInofs, quotaPathInfo)
			}
			var quotaId uint32
			if quotaId, err = client.AdminAPI().CreateQuota(volName, quotaPathInofs, maxFiles, maxBytes); err != nil {
				stdout("volName %v path %v quota create failed(%v)\n", volName, fullPath, err)
				return
			}
			stdout("createQuota: volName %v path %v  maxFiles %v maxBytes %v quotaId %v success.\n",
				volName, fullPath, maxFiles, maxBytes, quotaId)
		},
	}
	cmd.Flags().Uint64Var(&maxFiles, CliFlagMaxFiles, cmdQuotaDefaultMaxFiles, "Specify quota max files")
	cmd.Flags().Uint64Var(&maxBytes, CliFlagMaxBytes, cmdQuotaDefaultMaxBytes, "Specify quota max bytes")
	return cmd
}

func newQuotaListCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdQuotaListUse,
		Short: cmdQuotaListShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var quotas []*proto.QuotaInfo
			var err error
			volName := args[0]
			if quotas, err = client.AdminAPI().ListQuota(volName); err != nil {
				stdout("volName %v quota list failed(%v)\n", volName, err)
				return
			}
			sort.Slice(quotas, func(i, j int) bool {
				return quotas[i].QuotaId < quotas[j].QuotaId
			})
			stdout("[quotas]\n")
			stdout("%v\n", formatQuotaTableHeader())
			for _, quotaInfo := range quotas {
				stdout("%v\n", formatQuotaInfo(quotaInfo))
			}
		},
	}
	return cmd
}

func newQuotaListAllCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdQuotaListAllUse,
		Short: cmdQuotaListAllShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var vols []*proto.VolInfo

			if vols, err = client.AdminAPI().ListQuotaAll(); err != nil {
				stdout("quota list all failed(%v)\n", err)
				return
			}
			stdout("%v\n", volumeInfoTableHeader)
			for _, vol := range vols {
				stdout("%v\n", formatVolInfoTableRow(vol))
			}
		},
	}

	return cmd
}

func newQuotaUpdateCmd(client *master.MasterClient) *cobra.Command {
	var maxFiles uint64
	var maxBytes uint64

	var cmd = &cobra.Command{
		Use:   cmdQuotaUpdateUse,
		Short: cmdQuotaUpdateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			quotaId := args[1]

			quotaInfo, err := client.AdminAPI().GetQuota(volName, quotaId)
			if err != nil {
				stdout("get quota vol: %v ,quotaId: %v failed err %v.\n", volName, quotaId, err)
				return
			}
			if maxFiles == 0 {
				maxFiles = quotaInfo.MaxFiles
			}
			if maxBytes == 0 {
				maxBytes = quotaInfo.MaxBytes
			}
			if err = client.AdminAPI().UpdateQuota(volName, quotaId, maxFiles, maxBytes); err != nil {
				stdout("volName %v quotaId %v quota update failed(%v)\n", volName, quotaId, err)
				return
			}
			stdout("updateQuota: volName %v quotaId %v maxFiles %v maxBytes %v success.\n",
				volName, quotaId, maxFiles, maxBytes)
		},
	}
	cmd.Flags().Uint64Var(&maxFiles, CliFlagMaxFiles, 0, "Specify quota max files")
	cmd.Flags().Uint64Var(&maxBytes, CliFlagMaxBytes, 0, "Specify quota max bytes")
	return cmd
}

func newQuotaDelete(client *master.MasterClient) *cobra.Command {
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdQuotaDeleteUse,
		Short: cmdQUotaDeleteShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			quotaId := args[1]
			var err error
			if !optYes {
				stdout("Before deleting the quota, please confirm that the quota of the inode in the subdirectory has been cleared\n")
				stdout("ensure that the quota list %v usedFiles is displayed as 0\n", volName)
				stdout("\nConfirm (yes/no)[yes]:")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}
			if err = client.AdminAPI().DeleteQuota(volName, quotaId); err != nil {
				stdout("volName %v quotaId %v quota delete failed(%v)\n", volName, quotaId, err)
				return
			}
			stdout("deleteQuota: volName %v quotaId %v success.\n", volName, quotaId)
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Do not prompt to clear the quota of inodes")
	return cmd
}

func newQuotaGetInode(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdQuotaGetInodeUse,
		Short: cmdQuotaGetInodeShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			inodeId, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("inodeId %v is illegal", args[2])
				return
			}

			var metaConfig = &meta.MetaConfig{
				Volume:  volName,
				Masters: client.Nodes(),
			}
			metaWrapper, err := meta.NewMetaWrapper(metaConfig)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}

			quotaInfos, err := metaWrapper.GetInodeQuota_ll(inodeId)
			if err != nil {
				stdout("get indoe quota failed %v\n", err)
				return
			}
			for quotaId, quotaInfo := range quotaInfos {
				stdout("quotaId [%v] quotaInfo [%v] \n", quotaId, quotaInfo)
			}
		},
	}
	return cmd
}

func newQuotaApplyCmd(client *master.MasterClient) *cobra.Command {
	var maxConcurrencyInode uint64
	var cmd = &cobra.Command{
		Use:   cmdQuotaApplyUse,
		Short: cmdQuotaApplyShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			quotaId := args[1]
			var err error
			var quotaInfo *proto.QuotaInfo

			if quotaInfo, err = client.AdminAPI().GetQuota(volName, quotaId); err != nil {
				stdout("volName %v get quota %v failed(%v)\n", volName, quotaId, err)
				return
			}

			var metaConfig = &meta.MetaConfig{
				Volume:  volName,
				Masters: client.Nodes(),
			}

			metaWrapper, err := meta.NewMetaWrapper(metaConfig)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			var totalNums uint64
			var quotaIdNum uint32
			tmp, err := strconv.ParseUint(quotaId, 10, 32)
			quotaIdNum = uint32(tmp)
			for _, pathInfo := range quotaInfo.PathInfos {
				inodeNums, err := metaWrapper.ApplyQuota_ll(pathInfo.RootInode, quotaIdNum, maxConcurrencyInode)
				if err != nil {
					stdout("apply quota inodeNum %v failed  %v\n", inodeNums, err)
				}
				totalNums += inodeNums
			}
			stdout("apply quota num [%v] success.\n", totalNums)
		},
	}
	cmd.Flags().Uint64Var(&maxConcurrencyInode, CliFlagMaxConcurrencyInode, 1000, "max concurrency set Inodes")
	return cmd
}

func newQuotaRevokeCmd(client *master.MasterClient) *cobra.Command {
	var maxConcurrencyInode uint64
	var forceInode uint64
	var cmd = &cobra.Command{
		Use:   cmdQuotaRevokeUse,
		Short: cmdQuotaRevokeShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			volName := args[0]
			quotaId := args[1]
			var err error
			var quotaInfo *proto.QuotaInfo
			var totalNums uint64

			var metaConfig = &meta.MetaConfig{
				Volume:  volName,
				Masters: client.Nodes(),
			}

			metaWrapper, err := meta.NewMetaWrapper(metaConfig)
			if err != nil {
				stdout("NewMetaWrapper failed: %v\n", err)
				return
			}
			var quotaIdNum uint32
			tmp, err := strconv.ParseUint(quotaId, 10, 32)
			quotaIdNum = uint32(tmp)
			if forceInode == 0 {
				if quotaInfo, err = client.AdminAPI().GetQuota(volName, quotaId); err != nil {
					stdout("volName %v get quota %v failed(%v)\n", volName, quotaId, err)
					return
				}

				for _, pathInfo := range quotaInfo.PathInfos {
					inodeNums, err := metaWrapper.RevokeQuota_ll(pathInfo.RootInode, quotaIdNum, maxConcurrencyInode)
					if err != nil {
						stdout("revoke quota inodeNums %v failed %v\n", inodeNums, err)
					}
					totalNums += inodeNums
				}
			} else {
				totalNums, err = metaWrapper.RevokeQuota_ll(forceInode, quotaIdNum, maxConcurrencyInode)
				if err != nil {
					stdout("revoke quota inodeNums %v failed %v\n", totalNums, err)
				}
			}
			stdout("revoke num [%v] success.\n", totalNums)
		},
	}
	cmd.Flags().Uint64Var(&maxConcurrencyInode, CliFlagMaxConcurrencyInode, 1000, "max concurrency delete Inodes")
	cmd.Flags().Uint64Var(&forceInode, CliFlagForceInode, 0, "force revoke quota inode")
	return cmd
}

func checkNestedDirectories(paths []string) error {
	for i, path := range paths {
		for j := i + 1; j < len(paths); j++ {
			if isAncestor(path, paths[j]) {
				return fmt.Errorf("Nested directories found: %s and %s", path, paths[j])
			}
		}
	}
	return nil
}

func isAncestor(parent, child string) bool {
	if parent == child {
		return true
	}
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return !strings.HasPrefix(rel, "..")
}

/*
func hasNestedPaths(paths []string) bool {
	for i, path1 := range paths {
		absPath1, _ := filepath.Abs(path1)

		for j, path2 := range paths {
			if i == j {
				continue
			}

			absPath2, _ := filepath.Abs(path2)

			if filepath.HasPrefix(absPath1, absPath2) ||
				filepath.HasPrefix(absPath2, absPath1) {
				return true
			}
		}
	}

	return false
}*/
