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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/monitor"
	"github.com/spf13/cobra"
)

// cfs-cli monitor top-partition
//                 top-partitionOp
//                 top-vol
//                 top-volOp
const (
	cmdMonitorNodeUse   = "monitor [COMMAND]"
	cmdMonitorNodeShort = "Manage monitor nodes"
)

func newMonitorNodeCmd(client *monitor.MonitorClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdMonitorNodeUse,
		Short: cmdMonitorNodeShort,
	}
	cmd.AddCommand(
		newMonitorClusterTopIPCmd(client),
		newMonitorClusterTopVolCmd(client),
		newMonitorClusterTopPartitionCmd(client),
		newMonitorOpTopIPCmd(client),
		newMonitorOpTopVolCmd(client),
		newMonitorOpTopPartitionCmd(client),
		//newMonitorTopVolCmd(client),
		newMonitorIPTopPartitionCmd(client),
		//newMonitorTopPartitionOpCmd(client),
		//newMonitorTopVolOpCmd(client),
	)
	return cmd
}

const (
	cmdMonitorTopPartitionShort   = "List the most frequently operated partition in node"
	cmdMonitorTopPartitionOpShort = "List the most frequency operation of specific partition"
	cmdMonitorTopVolShort         = "List the most frequently operated volume in the cluster"
	cmdMonitorTopVolOpShort       = "List the most frequency operation of specific vol"

	cmdMonitorClusterTopIPShort        = "List the most frequently operated ip in the cluster"
	cmdMonitorClusterTopVolShort       = "List the most frequently operated vol in the cluster"
	cmdMonitorClusterTopPartitionShort = "List the most frequently operated partition in the cluster"
	cmdMonitorOpTopIPShort             = "List the most frequently operated ip in the cluster by specified op"
	cmdMonitorOpTopVolShort            = "List the most frequently operated vol in the cluster by specified op"
	cmdMonitorOpTopPartitionShort      = "List the most frequently operated partition in the cluster by specified op"
)

func newMonitorIPTopPartitionCmd(client *monitor.MonitorClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpIPTopPartition + " [NODE ADDR] [MODULE] [TIMESTAMP]",
		Short: cmdMonitorTopPartitionShort,
		Long:  ``,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			nodeAddr := args[0]
			module := args[1]
			timeStr := args[2]
			var (
				data *proto.MonitorView
				err  error
			)
			if data, err = client.MonitorAPI().GetIPTopPartition(nodeAddr, module, timeStr); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", MonitorDataTableHeader)
			for _, md := range data.Monitors {
				stdout("%v\n", formatMonitorData(md))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newMonitorTopPartitionOpCmd(client *monitor.MonitorClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpTopPartitionOp + " [NODE ADDR] [MODULE] [TIMESTAMP] [PID]",
		Short: cmdMonitorTopPartitionOpShort,
		Long:  ``,
		Args:  cobra.MinimumNArgs(4),
		Run: func(cmd *cobra.Command, args []string) {
			nodeAddr := args[0]
			module := args[1]
			timeStr := args[2]
			pid := args[3]
			var (
				data *proto.MonitorView
				err  error
			)
			if data, err = client.MonitorAPI().GetTopPartitionOp(nodeAddr, module, timeStr, pid); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", MonitorDataTableHeader)
			for _, md := range data.Monitors {
				stdout("%v\n", formatMonitorData(md))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newMonitorTopVolCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTimeunit string
	var cmd = &cobra.Command{
		Use:   CliOpGetTopVol + " [MODULE] [TIMESTAMP]",
		Short: cmdMonitorTopVolShort,
		Long: `Users are required to specify the module type(data/meta), statistical level (day, hour, minute and second) 
               and approximate timeStamp(20210508120000) to query`,
		Args: cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				timeunit string
				data     *proto.MonitorView
				err      error
			)
			module := args[0]
			timeStr := args[1]
			if opTimeunit != "" {
				timeunit = opTimeunit
			} else {
				timeunit = ""
			}
			if data, err = client.MonitorAPI().GetTopVol(module, timeStr, timeunit); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", MonitorDataTableHeader)
			for _, md := range data.Monitors {
				stdout("%v\n", formatMonitorData(md))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opTimeunit, "timeunit", "t", "", "Specify the table-level of query: day/hour/minute/second")
	return cmd
}

func newMonitorTopVolOpCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTimeunit string
	var cmd = &cobra.Command{
		Use:   CliOpGetTopVolOp + " [VOL NAME] [TIMESTAMP]",
		Short: cmdMonitorTopVolOpShort,
		Long:  `Users are required to specify the volName, and approximate timestamp(20210508120000) to query`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				timeunit string
				data     *proto.MonitorView
				err      error
			)
			vol := args[0]
			timeStr := args[1]
			if opTimeunit != "" {
				timeunit = opTimeunit
			} else {
				timeunit = ""
			}
			if data, err = client.MonitorAPI().GetTopVolOp(vol, timeStr, timeunit); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", MonitorDataTableHeader)
			for _, md := range data.Monitors {
				stdout("%v\n", formatMonitorData(md))
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&opTimeunit, "timeunit", "t", "", "Specify the table-level of query: day/hour/minute/second")
	return cmd
}

func newMonitorClusterTopIPCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTable string
	var opLimit int
	var cmd = &cobra.Command{
		Use:   CliOpClusterTopIP + " [MODULE] [STARTTIME] [ENDTIME]",
		Short: cmdMonitorClusterTopIPShort,
		Long:  `Users are required to specify the module('metanode' or 'datanode'), start time and end time(20210508120000) to query`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			start := args[1]
			end := args[2]
			if view, err = client.MonitorAPI().GetClusterTopIP(opTable, module, start, end, opLimit); err != nil {
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
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute/second. Default [minute]")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 0, "Limit the number of query data. Default [10]")
	return cmd
}

func newMonitorClusterTopVolCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTable string
	var opLimit int
	var cmd = &cobra.Command{
		Use:   CliOpClusterTopVol + " [MODULE] [STARTTIME] [ENDTIME]",
		Short: cmdMonitorClusterTopVolShort,
		Long:  `Users are required to specify the module('metanode' or 'datanode'), start time and end time(20210508120000) to query`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			start := args[1]
			end := args[2]
			if view, err = client.MonitorAPI().GetClusterTopVol(opTable, module, start, end, opLimit); err != nil {
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
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute/second. Default [minute]")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 0, "Limit the number of query data. Default [10]")
	return cmd
}

func newMonitorClusterTopPartitionCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTable string
	var opLimit int
	var cmd = &cobra.Command{
		Use:   CliOpClusterTopPartition + " [MODULE] [STARTTIME] [ENDTIME]",
		Short: cmdMonitorClusterTopPartitionShort,
		Long:  `Users are required to specify the module('metanode' or 'datanode'), start time and end time(20210508120000) to query`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			module := args[0]
			start := args[1]
			end := args[2]
			if view, err = client.MonitorAPI().GetClusterTopPartition(opTable, module, start, end, opLimit); err != nil {
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
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute/second. Default [minute]")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 0, "Limit the number of query data. Default [10]")
	return cmd
}

func newMonitorOpTopIPCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTable string
	var opLimit int
	var cmd = &cobra.Command{
		Use:   CliOpTopIPByOp + " [OPERATION] [STARTTIME] [ENDTIME]",
		Short: cmdMonitorOpTopIPShort,
		Long:  `Users are required to specify the operation, start time and end time(20210508120000) to query`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			op := args[0]
			start := args[1]
			end := args[2]
			if view, err = client.MonitorAPI().GetOpTopIP(opTable, op, start, end, opLimit); err != nil {
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
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute/second. Default [minute]")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 0, "Limit the number of query data. Default [10]")
	return cmd
}

func newMonitorOpTopVolCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTable string
	var opLimit int
	var cmd = &cobra.Command{
		Use:   CliOpTopVolByOp + " [OPERATION] [STARTTIME] [ENDTIME]",
		Short: cmdMonitorOpTopVolShort,
		Long:  `Users are required to specify the operation, start time and end time(20210508120000) to query`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			op := args[0]
			start := args[1]
			end := args[2]
			if view, err = client.MonitorAPI().GetOpTopVol(opTable, op, start, end, opLimit); err != nil {
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
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute/second. Default [minute]")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 0, "Limit the number of query data. Default [10]")
	return cmd
}

func newMonitorOpTopPartitionCmd(client *monitor.MonitorClient) *cobra.Command {
	var opTable string
	var opLimit int
	var cmd = &cobra.Command{
		Use:   CliOpTopPartitionByOp + " [OPERATION] [STARTTIME] [ENDTIME]",
		Short: cmdMonitorOpTopPartitionShort,
		Long:  `Users are required to specify the operation, start time and end time(20210508120000) to query`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				view *proto.QueryView
				err  error
			)
			op := args[0]
			start := args[1]
			end := args[2]
			if view, err = client.MonitorAPI().GetOpTopPartition(opTable, op, start, end, opLimit); err != nil {
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
	cmd.Flags().StringVarP(&opTable, "table", "t", "", "Specify the table-level of query: day/hour/minute/second. Default [minute]")
	cmd.Flags().IntVarP(&opLimit, "limit", "l", 0, "Limit the number of query data. Default [10]")
	return cmd
}
