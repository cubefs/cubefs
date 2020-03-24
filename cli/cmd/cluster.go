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
	"os"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdClusterUse   = "cluster [COMMAND]"
	cmdClusterShort = "Manage cluster components"
)

func newClusterCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdClusterUse,
		Short: cmdClusterShort,
	}
	cmd.AddCommand(
		newClusterInfoCmd(client),
	)
	return cmd
}

const (
	cmdClusterInfoUse   = "info"
	cmdClusterInfoShort = "Show cluster summary information"
)

func newClusterInfoCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdClusterInfoUse,
		Short: cmdClusterInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var cv *proto.ClusterView
			if cv, err = client.AdminAPI().GetCluster(); err != nil {
				errout("Get cluster info fail:\n%v\n", err)
				os.Exit(1)
			}
			stdout("[Cluster]\n")
			stdout(formatClusterView(cv))
			stdout("\n")
		},
	}
	return cmd
}
