// Copyright 2025 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdBalanceUse   = "mp-balance [COMMAND]"
	cmdBalanceShort = "balance memory usage ratio in meta node"
)

func newBalanceCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdBalanceUse,
		Short: cmdBalanceShort,
	}
	cmd.AddCommand(
		newCreateBalanceTaskCmd(client),
		newShowBalanceTaskCmd(client),
		newRunBalanceTaskCmd(client),
		newStopBalanceTaskCmd(client),
		newDeleteBalanceTaskCmd(client),
	)
	return cmd
}

const (
	cmdCreateBalanceTask      = "create"
	cmdCreateBalanceTaskShort = "Create meta partition balance task"
	cmdShowBalanceTask        = "show"
	cmdShowBalanceTaskShort   = "Show meta partition balance task"
	cmdRunBalanceTask         = "run"
	cmdRunBalanceTaskShort    = "Run meta partition balance task"
	cmdStopBalanceTask        = "stop"
	cmdStopBalanceTaskShort   = "Stop meta partition balance task"
	cmdDeleteBalanceTask      = "delete"
	cmdDeleteBalanceTaskShort = "Delete meta partition balance task"
)

func newCreateBalanceTaskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdCreateBalanceTask,
		Short: cmdCreateBalanceTaskShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			var task *proto.ClusterPlan
			if task, err = client.AdminAPI().CreateMetaNodeBalanceTask(); err != nil {
				return
			}
			out, err := json.MarshalIndent(task, "", "    ")
			if err != nil {
				stdout("marshal task failed: %s", err.Error())
				return
			}
			stdout("%s", string(out))
		},
	}
	return cmd
}

func newShowBalanceTaskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdShowBalanceTask,
		Short: cmdShowBalanceTaskShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			var task *proto.ClusterPlan
			if task, err = client.AdminAPI().GetMetaNodeBalanceTask(); err != nil {
				return
			}
			out, err := json.MarshalIndent(task, "", "    ")
			if err != nil {
				stdout("marshal task failed: %s", err.Error())
				return
			}
			stdout("%s", string(out))
		},
	}
	return cmd
}

func newRunBalanceTaskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdRunBalanceTask,
		Short: cmdRunBalanceTaskShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			var result string
			if result, err = client.AdminAPI().RunMetaNodeBalanceTask(); err != nil {
				return
			}
			stdout("%s", result)
		},
	}
	return cmd
}

func newStopBalanceTaskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdStopBalanceTask,
		Short: cmdStopBalanceTaskShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			var result string
			if result, err = client.AdminAPI().StopMetaNodeBalanceTask(); err != nil {
				return
			}
			stdout("%s", result)
		},
	}
	return cmd
}

func newDeleteBalanceTaskCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdDeleteBalanceTask,
		Short: cmdDeleteBalanceTaskShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			var result string
			if result, err = client.AdminAPI().DeleteMetaNodeBalanceTask(); err != nil {
				return
			}
			stdout("%s", result)
		},
	}
	return cmd
}
