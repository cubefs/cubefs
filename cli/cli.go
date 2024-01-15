// Copyright 2018 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/cli/cmd"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
)

//TODO: remove this later.
//go:generate golangci-lint run --issues-exit-code=1 -D errcheck -E bodyclose ./...

func runCLI() (err error) {
	var cfg *cmd.Config
	if cfg, err = cmd.LoadConfig(); err != nil {
		return fmt.Errorf("load config %v", err)
	}
	cfsCli := setupCommands(cfg)
	return cfsCli.Execute()
}

func setupCommands(cfg *cmd.Config) *cobra.Command {
	mc := master.NewMasterClient(cfg.MasterAddr, false)
	mc.SetTimeout(cfg.Timeout)
	mc.SetClientIDKey(cfg.ClientIDKey)
	cfsRootCmd := cmd.NewRootCmd(mc)
	//	var completionCmd = &cobra.Command{
	//		Use:   "completion",
	//		Short: "Generate completion bash file",
	//		Long: `To use the completion, tool "bash-completion" is demanded,
	//then execute the following command:
	//   $ ./cfs-cli completion
	//   # Bash completion file "cfs-cli.sh" will be generated under the present working directory
	//   $ echo 'source /usr/share/bash-completion/bash_completion' >> ~/.bashrc
	//   $ echo 'source {path of cfs-cli.sh}' >>~/.bashrc
	//   $ source ~/.bashrc
	//`,
	//		Example: "cfs-cli completion",
	//		Run: func(cmd *cobra.Command, args []string) {
	//			if err := cfsRootCmd.CFSCmd.GenBashCompletionFile("cfs-cli.sh"); err != nil {
	//				_, _ = fmt.Fprintf(os.Stdout, "generate bash file failed")
	//			}
	//			_, _ = fmt.Fprintf(os.Stdout, `File "cfs-cli.sh" has been generated successfully under the present working directory,
	//following command to execute:
	//   $ # install bash-completion
	//   $ echo 'source /usr/share/bash-completion/bash_completion' >> ~/.bashrc
	//   $ echo 'source {path of cfs-cli.sh}' >>~/.bashrc
	//   $ source ~/.bashrc
	//`)
	//		},
	//	}

	// cfsRootCmd.CFSCmd.AddCommand(completionCmd)

	cfsRootCmd.CFSCmd.AddCommand(cmd.GenClusterCfgCmd)
	return cfsRootCmd.CFSCmd
}

func main() {
	var err error
	_, err = log.InitLog("/tmp/cfs", "cli", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimit)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	if err = runCLI(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		log.LogError("Error:", err)
		log.LogFlush()
		os.Exit(1)
	}
	log.LogFlush()
}
