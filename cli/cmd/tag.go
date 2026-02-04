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
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdSelectTagUse   = "tag [COMMAND]"
	cmdSelectTagShort = "tag management"
)

func newSelectTagCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdSelectTagUse,
		Short: cmdSelectTagShort,
	}
	cmd.AddCommand(
		newShowSelectTagSummaryCmd(client),
		newShowSelectTagVolSummaryCmd(client),
		newClearSelectTagFailedKeysCmd(client),
	)
	return cmd
}

const (
	cmdShowSelectTagSummary          = "summary"
	cmdShowSelectTagSummaryShort     = "Show select tag summary"
	cmdShowSelectTagVolSummary       = "vol-summary"
	cmdShowSelectTagVolSummaryShort  = "Show select tag summary for a volume"
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
			var task *proto.TagSummary
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

func newShowSelectTagVolSummaryCmd(client *master.MasterClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdShowSelectTagVolSummary,
		Short: cmdShowSelectTagVolSummaryShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			if len(args) < 1 {
				err = fmt.Errorf("volume name is required")
				return
			}
			volName := args[0]
			var task *proto.VolTagSummary
			if task, err = client.AdminAPI().GetVolTagSummary(volName); err != nil {
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
