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
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/monitor"
	"github.com/spf13/cobra"
)

const (
	cmdMonitorNodeUse   = "monitor [COMMAND]"
	cmdMonitorNodeShort = "Manage monitor nodes"
	defaultClusterName 	= "spark"

	timeLayout = "20060102150405"
)

func newMonitorNodeCmd(client *master.MasterClient, mClient *monitor.MonitorClient) *cobra.Command {
	clusterName := defaultClusterName
	clusterView, _ := client.AdminAPI().GetCluster()
	if clusterView != nil {
		clusterName = clusterView.Name
	}

	var cmd = &cobra.Command{
		Use:   cmdMonitorNodeUse,
		Short: cmdMonitorNodeShort,
	}
	cmd.AddCommand(
		newMonitorClusterTopIPCmd(clusterName, mClient),
		newMonitorClusterTopVolCmd(clusterName, mClient),
		newMonitorClusterTopPartitionCmd(clusterName, mClient),
		newMonitorClusterTopOpCmd(clusterName, mClient),
		newMonitorOpTopIPCmd(clusterName, mClient),
		newMonitorOpTopVolCmd(clusterName, mClient),
		newMonitorOpTopPartitionCmd(clusterName, mClient),
	)
	return cmd
}

const (
	cmdMonitorClusterTopIPShort        = "List the most frequently operated ip in the cluster"
	cmdMonitorClusterTopVolShort       = "List the most frequently operated vol in the cluster"
	cmdMonitorClusterTopPartitionShort = "List the most frequently operated partition in the cluster"
	cmdMonitorClusterTopOpShort		   = "List the most frequently operated operation in the cluster"
	cmdMonitorOpTopIPShort             = "List the most frequently operated ip in the cluster by specified op"
	cmdMonitorOpTopVolShort            = "List the most frequently operated vol in the cluster by specified op"
	cmdMonitorOpTopPartitionShort      = "List the most frequently operated partition in the cluster by specified op"
)

func newMonitorClusterTopIPCmd(cluster string, client *monitor.MonitorClient) *cobra.Command {
	var (
		opTable string
		opLimit int
		opStart string
		opEnd   string
		opOrder string
		opVol   string
	)
	var cmd = &cobra.Command{
		Use:   CliOpClusterTopIP + " [MODULE]",
		Short: cmdMonitorClusterTopIPShort,
		Long:  `Users are required to specify the module: 'metanode' or 'datanode'`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			if opStart == "" || opEnd == "" {
				now := time.Now()
				opStart = now.Add(-5 * time.Minute).Format(timeLayout)
				opEnd = now.Format(timeLayout)
			}
			if opVol == "" {
				if view, err = client.MonitorAPI().GetClusterTopIP(opTable, cluster, module, opStart, opEnd, opOrder, opLimit); err != nil {
					stdout("%v\n", err)
					return
				}
			} else {
				if view, err = client.MonitorAPI().GetTopIP("", cluster, module, opStart, opEnd, opLimit, opOrder, "", opVol); err != nil {
					stdout("%v\n", err)
					return
				}
			}
			stdout("%v\n", monitorQueryDataTableHeader)
			for _, data := range view.Data {
				stdout("%v\n", formatMonitorQueryData(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opStart, "starttime", "s", "", "Specify the start time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [5 minutes ago].")
	cmd.Flags().StringVarP(&opEnd, "endtime", "e", "", "Specify the end time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [now].")
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute. Default [minute].")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 10, "Limit the number of query data. Default [10].")
	cmd.Flags().StringVar(&opOrder, "order", "count", "Specify order by 'count' or 'size'(only for 'datanode'). Default by [count].")
	cmd.Flags().StringVarP(&opVol, "vol", "v", "", "Specify volume name to query.")
	return cmd
}

func newMonitorClusterTopVolCmd(cluster string, client *monitor.MonitorClient) *cobra.Command {
	var (
		opTable string
		opLimit int
		opStart	string
		opEnd	string
		opOrder	string
		opIP	string
	)
	var cmd = &cobra.Command{
		Use:   CliOpClusterTopVol + " [MODULE]",
		Short: cmdMonitorClusterTopVolShort,
		Long:  `Users are required to specify the module: 'metanode' or 'datanode'`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			if opStart == "" || opEnd == "" {
				now := time.Now()
				opStart = now.Add(-5 * time.Minute).Format(timeLayout)
				opEnd = now.Format(timeLayout)
			}
			if opIP == "" {
				if view, err = client.MonitorAPI().GetClusterTopVol(opTable, cluster, module, opStart, opEnd, opOrder, opLimit); err != nil {
					stdout("%v\n", err)
					return
				}
			} else {
				if view, err = client.MonitorAPI().GetTopPartition("", cluster, module, opStart, opEnd, opLimit, "vol", opOrder, opIP, ""); err != nil {
					stdout("%v\n", err)
					return
				}
			}
			stdout("%v\n", monitorQueryDataTableHeader)
			for _, data := range view.Data {
				stdout("%v\n", formatMonitorQueryData(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opStart, "starttime", "s", "", "Specify the start time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [5 minutes ago].")
	cmd.Flags().StringVarP(&opEnd, "endtime", "e", "", "Specify the end time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [now].")
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute. Default [minute].")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 10, "Limit the number of query data. Default [10].")
	cmd.Flags().StringVar(&opOrder, "order", "count", "Specify order by 'count' or 'size'(only for 'datanode'). Default by [count].")
	cmd.Flags().StringVar(&opIP, "ip", "", "Specify ip to query. format[xx.xx.xx.xx].")
	return cmd
}

func newMonitorOpTopIPCmd(cluster string, client *monitor.MonitorClient) *cobra.Command {
	var (
		opLimit int
		opStart	string
		opEnd	string
		opOrder	string
		opVol	string
	)
	var cmd = &cobra.Command{
		Use:   CliOpTopIPByOp + " [MODULE] [OPERATION]",
		Short: cmdMonitorOpTopIPShort,
		Long:  `Users are required to specify the module: 'metanode' or 'datanode'.
Specify the operation, choose one of the following:
[Data operation]: read/appendWrite/overWrite/repairRead/repairWrite
[Meta operation]: createInode/evictInode/createDentry/deleteDentry/lookup/readDir/inodeGet/batchInodeGet/addExtents/listExtents/truncate/insertExtent`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			op := args[1]
			if opStart == "" || opEnd == "" {
				now := time.Now()
				opStart = now.Add(-5 * time.Minute).Format(timeLayout)
				opEnd = now.Format(timeLayout)
			}
			if view, err = client.MonitorAPI().GetTopIP("", cluster, module, opStart, opEnd, opLimit, opOrder, op, opVol); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", monitorQueryDataTableHeader)
			for _, data := range view.Data {
				stdout("%v\n", formatMonitorQueryData(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opStart, "starttime", "s", "", "Specify the start time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [5 minutes ago].")
	cmd.Flags().StringVarP(&opEnd, "endtime", "e", "", "Specify the end time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [now].")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 10, "Limit the number of query data. Default [10].")
	cmd.Flags().StringVar(&opOrder, "order", "count", "Specify order by 'count' or 'size'(only for 'datanode'). Default by [count].")
	cmd.Flags().StringVarP(&opVol, "vol", "v", "", "Specify volume name to query.")
	return cmd
}

func newMonitorOpTopVolCmd(cluster string, client *monitor.MonitorClient) *cobra.Command {
	var (
		opLimit int
		opStart	string
		opEnd	string
		opOrder	string
		opIP	string
	)
	var cmd = &cobra.Command{
		Use:   CliOpTopVolByOp + " [MODULE] [OPERATION]",
		Short: cmdMonitorOpTopVolShort,
		Long:  `Users are required to specify the module: 'metanode' or 'datanode'.
Users are required to specify the operation, choose one of the following:
[Data operation]: read/appendWrite/overWrite/repairRead/repairWrite
[Meta operation]: createInode/evictInode/createDentry/deleteDentry/lookup/readDir/inodeGet/batchInodeGet/addExtents/listExtents/truncate/insertExtent`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			op := args[1]
			if opStart == "" || opEnd == "" {
				now := time.Now()
				opStart = now.Add(-5 * time.Minute).Format(timeLayout)
				opEnd = now.Format(timeLayout)
			}
			if view, err = client.MonitorAPI().GetTopPartition("", cluster, module, opStart, opEnd, opLimit, "vol", opOrder, opIP, op); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", monitorQueryDataTableHeader)
			for _, data := range view.Data {
				stdout("%v\n", formatMonitorQueryData(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opStart, "starttime", "s", "", "Specify the start time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [5 minutes ago].")
	cmd.Flags().StringVarP(&opEnd, "endtime", "e", "", "Specify the end time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [now].")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 10, "Limit the number of query data. Default [10].")
	cmd.Flags().StringVar(&opOrder, "order", "count", "Specify order by 'count' or 'size'(only for 'datanode'). Default by [count].")
	cmd.Flags().StringVar(&opIP, "ip", "", "Specify ip to query. format[xx.xx.xx.xx].")
	return cmd
}

func newMonitorClusterTopPartitionCmd(cluster string, client *monitor.MonitorClient) *cobra.Command {
	var (
		opLimit int
		opStart	string
		opEnd	string
		opOrder	string
		opIP	string
	)
	var cmd = &cobra.Command{
		Use:   CliOpClusterTopPartition + " [MODULE]",
		Short: cmdMonitorClusterTopPartitionShort,
		Long:  `Users are required to specify the module: 'metanode' or 'datanode'`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			if opStart == "" || opEnd == "" {
				now := time.Now()
				opStart = now.Add(-5 * time.Minute).Format(timeLayout)
				opEnd = now.Format(timeLayout)
			}
			if view, err = client.MonitorAPI().GetTopPartition("", cluster, module, opStart, opEnd, opLimit, "pid", opOrder, opIP, ""); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", monitorQueryDataTableHeader)
			for _, data := range view.Data {
				stdout("%v\n", formatMonitorQueryData(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opStart, "starttime", "s", "", "Specify the start time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [5 minutes ago].")
	cmd.Flags().StringVarP(&opEnd, "endtime", "e", "", "Specify the end time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [now].")
	cmd.Flags().StringVar(&opOrder, "order", "count", "Specify order by 'count' or 'size'(only for 'datanode'). Default by [count].")
	cmd.Flags().StringVar(&opIP, "ip", "", "Specify ip to query. format[xx.xx.xx.xx].")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 10, "Limit the number of query data. Default [10].")
	return cmd
}

func newMonitorOpTopPartitionCmd(cluster string, client *monitor.MonitorClient) *cobra.Command {
	var (
		opLimit int
		opStart	string
		opEnd	string
		opOrder	string
		opIP	string
	)
	var cmd = &cobra.Command{
		Use:   CliOpTopPartitionByOp + " [MODULE] [OPERATION]",
		Short: cmdMonitorOpTopPartitionShort,
		Long:  `Users are required to specify the module: 'metanode' or 'datanode.'
Users are required to specify the operation, choose one of the following:
[Data operation]: read/appendWrite/overWrite/repairRead/repairWrite
[Meta operation]: createInode/evictInode/createDentry/deleteDentry/lookup/readDir/inodeGet/batchInodeGet/addExtents/listExtents/truncate/insertExtent`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			op := args[1]
			if opStart == "" || opEnd == "" {
				now := time.Now()
				opStart = now.Add(-5 * time.Minute).Format(timeLayout)
				opEnd = now.Format(timeLayout)
			}
			if view, err = client.MonitorAPI().GetTopPartition("", cluster, module, opStart, opEnd, opLimit, "pid", opOrder, opIP, op); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", monitorQueryDataTableHeader)
			for _, data := range view.Data {
				stdout("%v\n", formatMonitorQueryData(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opStart, "starttime", "s", "", "Specify the start time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [5 minutes ago].")
	cmd.Flags().StringVarP(&opEnd, "endtime", "e", "", "Specify the end time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default [now].")
	cmd.Flags().StringVar(&opOrder, "order", "count", "Specify order by 'count' or 'size'(only for 'datanode'). Default by [count].")
	cmd.Flags().StringVar(&opIP, "ip", "", "Specify ip to query. format[xx.xx.xx.xx].")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 10, "Limit the number of query data. Default [10].")
	return cmd
}

func newMonitorClusterTopOpCmd(cluster string, client *monitor.MonitorClient) *cobra.Command {
	var (
		opLimit int
		opStart	string
		opEnd	string
		opOrder	string
		opIP	string
		opVol	string
		opPid	string
	)
	var cmd = &cobra.Command{
		Use:   CliOpClusterTopOp + " [MODULE]",
		Short: cmdMonitorClusterTopOpShort,
		Long:  `Users are required to specify the module: 'metanode' or 'datanode'`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			if opStart == "" || opEnd == "" {
				now := time.Now()
				opStart = now.Add(-5 * time.Minute).Format(timeLayout)
				opEnd = now.Format(timeLayout)
			}
			if view, err = client.MonitorAPI().GetTopOp("", cluster, module, opStart, opEnd, opLimit, opOrder, opIP, opVol, opPid); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", monitorQueryDataTableHeader)
			for _, data := range view.Data {
				stdout("%v\n", formatMonitorQueryData(data))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opStart, "starttime", "s", "", "Specify the start time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default 5 minutes ago.")
	cmd.Flags().StringVarP(&opEnd, "endtime", "e", "", "Specify the end time to query, format[20210902120000]. 'starttime' and 'endtime' need to be specified at the same time. Default now.")
	cmd.Flags().StringVar(&opOrder, "order", "count", "Specify order by 'count' or 'size'(only for 'datanode'). Default by [count].")
	cmd.Flags().StringVar(&opIP, "ip", "", "Specify ip to query. format[xx.xx.xx.xx].")
	cmd.Flags().StringVarP(&opVol, "vol", "v", "", "Specify volume name to query.")
	cmd.Flags().StringVarP(&opPid, "pid", "p", "", "Specify partition id to query.")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 10, "Limit the number of query data. Default [10].")
	return cmd
}