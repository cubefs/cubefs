// Copyright 2020 The Chubao Authors.
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

	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/spf13/cobra"
)

const (
	cmdFSUse   = "fstools [COMMAND]"
	cmdFSShort = "filesystem tools"
)

func newFSToolsCmd(masters []string) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdFSUse,
		Short: cmdFSShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newFSrmr(masters),
	)
	return cmd
}

const (
	cmdFSrmrUse   = "rmr [volume] [file dir]"
	cmdFSrmrShort = "recursive remove a dir"
)

func newFSrmr(masters []string) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdFSrmrUse,
		Short: cmdFSrmrShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var volName = args[0]
			var dirDeleted = args[1]

			var metaConfig = &meta.MetaConfig{
				Volume:  volName,
				Masters: masters,
			}

			gMetaWrapper, err := meta.NewMetaWrapper(metaConfig)
			if err != nil {
				fmt.Printf("NewMetaWrapper failed: %v", err)
			}

			ino, err := gMetaWrapper.GetRootIno(dirDeleted)
			if err != nil {
				fmt.Printf("GetRootIno: %v, err:%v", dirDeleted, err)
			}
			err = gMetaWrapper.RecursiveRemoveDir(ino)
			if err != nil {
				fmt.Printf("RecursiveRemoveDir:%v", err)
			}
		},
	}
	return cmd
}
