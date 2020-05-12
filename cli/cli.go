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

package main

import (
	"fmt"
	"os"

	"github.com/chubaofs/chubaofs/cli/cmd"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

func runCLI() (err error) {
	var cfg *cmd.Config
	if cfg, err = cmd.LoadConfig(); err != nil {
		return
	}
	cfscli := setupCommands(cfg)
	err = cfscli.Execute()
	return
}

func setupCommands(cfg *cmd.Config) *cobra.Command {
	var mc = master.NewMasterClient(cfg.MasterAddr, false)
	cfsRootCmd := cmd.NewRootCmd(mc)
	var completionCmd = &cobra.Command{
		Use:   "completion",
		Short: "Generate completion bash file",
		Long: `To apply completion scripts:
	#!/bin/bash 
	echo 'source {path of cfs-cli.sh}' >>~/.bashrc
	source ~/.bashrc
`,
		Example: "cfs-cli completion",
		Run: func(cmd *cobra.Command, args []string) {
			cfsRootCmd.CFSCmd.GenBashCompletionFile("cfs-cli.sh")
		},
	}
	cfsRootCmd.CFSCmd.AddCommand(completionCmd)
	return cfsRootCmd.CFSCmd
}

func main() {
	var err error
	if err = runCLI(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
