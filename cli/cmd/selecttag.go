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
	cmdSelectTagUse   = "select-tag [COMMAND]"
	cmdSelectTagShort = "select tag management"
)

func newSelectTagCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdSelectTagUse,
		Short: cmdSelectTagShort,
	}
	cmd.AddCommand(
		newShowSelectTagSummaryCmd(client),
		newClearSelectTagFailedKeysCmd(client),
	)
	return cmd
}

const (
	cmdShowSelectTagSummary          = "summary"
	cmdShowSelectTagSummaryShort     = "Show select tag summary"
	cmdClearSelectTagFailedKeys      = "clear-failed-keys"
	cmdClearSelectTagFailedKeysShort = "Clear select tag failed keys"
)

func newShowSelectTagSummaryCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdShowSelectTagSummary,
		Short: cmdShowSelectTagSummaryShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			var task *proto.SelectTagSummary
			if task, err = client.AdminAPI().GetSelectTagSummary(); err != nil {
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

func newClearSelectTagFailedKeysCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdClearSelectTagFailedKeys,
		Short: cmdClearSelectTagFailedKeysShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if err = client.AdminAPI().ClearSelectTagFailedKeys(); err != nil {
				return
			}
			stdout("Clear select tag failed keys successfully.")
		},
	}
	return cmd
}
