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
	"fmt"
	"github.com/chubaofs/chubaofs/convertnode"
	"github.com/chubaofs/chubaofs/sdk/convert"
	"github.com/spf13/cobra"
)

const (
	cmdConvertNodeUse         = "convert [COMMAND]"
	cmdConvertNodeShort       = "Manage convert nodes"
	defaultConvertClusterName = "spark"
)

func newConvertNodeCmd(client *convert.ConvertClient) *cobra.Command {

	var cmd = &cobra.Command{
		Use:   cmdConvertNodeUse,
		Short: cmdConvertNodeShort,
	}
	cmd.AddCommand(
		newConvertNodeList(client),
		newConvertAddCmd(client),
		newConvertDelCmd(client),
		newConvertInfoCmd(client),
		newConvertStartCmd(client),
		newConvertStopCmd(client),
		newConvertNodeDBCmd(),
	)
	return cmd
}

const (
	cmdConvertNodeList			       = "List all clusters & processors & tasks in the convert node"
)

func newConvertNodeList(client *convert.ConvertClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: cmdConvertNodeList,
		Long:  `Users are required to specify the convert node addr in config, and run this cmd`,
		Run: func(cmd *cobra.Command, args []string) {
			cv, err := client.ConvertAPI().List()
			if  err != nil {
				errout("Get cluster info fail:\n%v\n", err)
			}
			stdout("[ConvertNode]\n")
			stdout(formatConvertNodesData(cv))
			stdout("\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

const (
	cmdConvertAddUse             = "add cluster/task "
	cmdConvertAddShort           = "add cluster or convert task to convert node"
	cmdConvertDefaultClusterName = "spark"
)

func newConvertAddCmd(client *convert.ConvertClient) *cobra.Command {
	var optClusterName 	string
	var optVolName 		string
	var optnodesAddrs 	string
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdConvertAddUse,
		Short: cmdConvertAddShort,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			objType := args[0]
			if objType != "cluster" && objType != "task" {
				errout("unknown object type")
			}

			if len(optnodesAddrs) == 0 && len(optVolName) == 0 {
				errout("At least contain one param")
			}

			// ask user for confirm
			if !optYes {
				stdout("Add %s:\n", objType)
				stdout("  Cluster Name        : %v\n", optClusterName)
				stdout("  Volume  Name        : %v\n", optVolName)
				stdout("  Master Nodes        : %v\n", optnodesAddrs)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			switch objType {
			case "cluster":
				err = client.ConvertAPI().AddConvertClusterConf(optClusterName, optnodesAddrs)
			case "task":
				err = client.ConvertAPI().AddConvertTask(optClusterName, optVolName)
			}

			if err != nil {
				errout("add %s failed case:\n%v\n", objType, err)
			}

			stdout("add %s success.\n", objType)
			return
		},
	}

	cmd.Flags().StringVar(&optClusterName, CliFlagCluster, cmdConvertDefaultClusterName, "Specify cluster, default:spark")
	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name")
	cmd.Flags().StringVar(&optnodesAddrs, CliFlagNodesAddrs, "", "Specify cluster master nodes addr, split by ,")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdConvertDelUse             = "del cluster/task "
	cmdConvertDelShort           = "del cluster or convert task to convert node"
)
func newConvertDelCmd(client *convert.ConvertClient) *cobra.Command {
	var optVolName 		string
	var optClustertName string
	var optnodesAddrs 	string
	var optDelClusterFlag bool
	var optYes bool
	var cmd = &cobra.Command{
		Use:   cmdConvertDelUse,
		Short: cmdConvertDelShort,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var clusterName 	string
			objType := args[0]
			if objType != "cluster" && objType != "task" {
				errout("unknown object type")
			}

			if len(optnodesAddrs) == 0 && len(optVolName) == 0 && len(optClustertName) == 0{
				errout("At least contain one param")
			}

			// ask user for confirm
			if !optYes {
				stdout("Del %s:\n", objType)
				stdout("  Cluster Name        : %v\n", clusterName)
				stdout("  Volume  Name        : %v\n", optVolName)
				stdout("  Master Nodes        : %v\n", optnodesAddrs)
				stdout("  Delete Cluster      : %v\n", optDelClusterFlag)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					stdout("Abort by user.\n")
					return
				}
			}

			switch objType {
			case "cluster":
				err = client.ConvertAPI().DelConvertClusterConf(optClustertName, optnodesAddrs)
			case "task":
				err = client.ConvertAPI().DelConvertTask(optClustertName, optVolName)
			}

			if err != nil {
				errout("del failed case:\ndel type:%v\nerror:%v\n", objType, err)
			}

			stdout("del %s success.\n", objType)
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name")
	cmd.Flags().StringVar(&optnodesAddrs, CliFlagNodesAddrs, "", "Specify cluster master nodes addr, split by ,")
	cmd.Flags().StringVar(&optClustertName, CliFlagCluster, "", "Specify cluster name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdConvertInfoUse             = "info cluster/task/processor "
	cmdConvertInfoShort           = "info cluster/processor/task detail info"
)
func newConvertInfoCmd(client *convert.ConvertClient) *cobra.Command {
	var optVolName 		string
	var optClustertName string
	var optProcessorId	int
	var cmd = &cobra.Command{
		Use:   cmdConvertInfoUse,
		Short: cmdConvertInfoShort,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
				data interface{}
			)
			objType := args[0]
			if objType != "cluster" && objType != "task" && objType != "processor"{
				errout("unknown object type")
			}

			if len(optVolName) == 0 && len(optClustertName) == 0 && optProcessorId == -1{
				errout("At least contain one param")
			}

			// ask user for confirm

			switch objType {
			case "cluster":
				data, err = client.ConvertAPI().QueryConvertClusterConf(optClustertName)
			case "task":
				data, err = client.ConvertAPI().QueryConvertTask(optClustertName, optVolName)
			case "processor":
				data, err = client.ConvertAPI().QueryConvertProcessorInfo(optProcessorId)
			}

			if err != nil {
				errout("query %s failed case:\n%v\n", objType, err)
			}

			stdout(formatConvertDetailInfo(data, objType))
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name")
	cmd.Flags().StringVar(&optClustertName, CliFlagCluster, "", "Specify cluster name")
	cmd.Flags().IntVar(&optProcessorId, CliFlagId, -1, "Specify processor id")
	return cmd
}

const (
	cmdConvertStartUse             = "start server/task "
	cmdConvertStartShort           = "start server/task"
)
func newConvertStartCmd(client *convert.ConvertClient) *cobra.Command {
	var optVolName 		string
	var optClustertName string
	var cmd = &cobra.Command{
		Use:   cmdConvertStartUse,
		Short: cmdConvertStartShort,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
			)
			objType := args[0]
			if objType != "server" && objType != "task"{
				errout("unknown object type")
			}

			if len(optVolName) == 0 && len(optClustertName) == 0 && objType == "task"{
				errout("At least contain one param")
			}

			// ask user for confirm

			switch objType {
			case "server":
				err = client.ConvertAPI().StartConvertNode()
			case "task":
				err = client.ConvertAPI().StartConvertTask(optClustertName, optVolName)
			}

			if err != nil {
				errout("start %s failed case:\n%v\n", objType, err)
			}
			stdout("start %s success.\n", objType)
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name")
	cmd.Flags().StringVar(&optClustertName, CliFlagCluster, "", "Specify cluster name")
	return cmd
}

const (
	cmdConvertStopUse             = "stop server/task "
	cmdConvertStopShort           = "stop server/task"
)
func newConvertStopCmd(client *convert.ConvertClient) *cobra.Command {
	var optVolName 		string
	var optClustertName string
	var cmd = &cobra.Command{
		Use:   cmdConvertStopUse,
		Short: cmdConvertStopShort,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
			)
			objType := args[0]
			if objType != "server" && objType != "task"{
				errout("unknown object type")
			}

			if len(optVolName) == 0 && len(optClustertName) == 0 && objType == "task"{
				errout("At least contain one param")
			}

			// ask user for confirm

			switch objType {
			case "server":
				err = client.ConvertAPI().StopConvertNode()
			case "task":
				err = client.ConvertAPI().StopConvertTask(optClustertName, optVolName)
			}

			if err != nil {
				errout("stop %s failed case:\n%v\n", objType, err)
			}
			stdout("stop %s success.\n", objType)
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name")
	cmd.Flags().StringVar(&optClustertName, CliFlagCluster, "", "Specify cluster name")
	return cmd
}

const (
	cmdConvertNodeDBCmdUse = "db"
	cmdConvertNodeDBCmdShort = "Operation Database"
)

func newConvertNodeDBCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdConvertNodeDBCmdUse,
		Short:   cmdConvertNodeDBCmdShort,
	}
	cmd.AddCommand(
		newConvertNodeAddrCmd(),
	)
	return cmd
}

func newConvertNodeAddrCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "convertNodeAddr",
		Short: "list or clear convert nodes addr in database",
	}
	cmd.AddCommand(
		newConvertNodeAddrListCmd(),
		newConvertNodesAddrClearCmd(),
	)
	return cmd
}

func newConvertNodeAddrListCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list",
		Short: "load all convert nodes addr from database",
		Run: func(cmd *cobra.Command, args []string) {
			config, err := LoadConfig()
			if err != nil {
				stdout(fmt.Sprintf("Load config failed:%v\n", err))
				return
			}
			convertNodeDB := &convertnode.DBInfo{
				Config: config.ConvertNodeDBConfig,
			}

			if err = convertNodeDB.Open(); err != nil {
				stdout(fmt.Sprintf("Open mysql database failed:%v\n", err))
				return
			}
			defer convertNodeDB.ReleaseDBHandle()

			var convertNodesAddr []*convertnode.SingletonCheckTable
			convertNodesAddr, err = convertNodeDB.LoadIpAddrFromDataBase()
			if err != nil {
				stdout(fmt.Sprintf("Load all convert node addr info from db failed:%v\n", err))
				return
			}

			stdout("ID IPADDR\n")
			for _, convertNodeAddr := range convertNodesAddr {
				stdout(fmt.Sprintf("%v  %s\n", convertNodeAddr.ID, convertNodeAddr.IpAddr))
			}
		},
	}
	return cmd
}

func newConvertNodesAddrClearCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "clear",
		Short: "clear convert nodes addr in database",
		Run: func(cmd *cobra.Command, args []string) {
			config, err := LoadConfig()
			if err != nil {
				stdout(fmt.Sprintf("Load config failed:%v\n", err))
				return
			}
			convertNodeDB := &convertnode.DBInfo{
				Config: config.ConvertNodeDBConfig,
			}

			if err = convertNodeDB.Open(); err != nil {
				stdout(fmt.Sprintf("Open mysql data base failed:%v\n", err))
				return
			}
			defer convertNodeDB.ReleaseDBHandle()

			var convertNodesAddr []*convertnode.SingletonCheckTable
			convertNodesAddr, err = convertNodeDB.LoadIpAddrFromDataBase()
			if err != nil {
				stdout(fmt.Sprintf("Load all convert node addr info from db failed:%v\n", err))
				return
			}

			for _, convertNodeAddr := range convertNodesAddr {
				if err = convertNodeDB.ClearIpAddrInDataBase(convertNodeAddr.IpAddr); err != nil {
					stdout(fmt.Sprintf("Delete convert node addr from database failed, node addr[%s] err[%v]\n", convertNodeAddr.IpAddr, err))
					return
				}
			}

			stdout("Delete all convert node addr from database successfully\n")
		},
	}
	return cmd
}


