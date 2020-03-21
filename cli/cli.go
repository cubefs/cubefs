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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/chubaofs/chubaofs/cli/cmd"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

var (
	defaultHomeDir, _ = os.UserHomeDir()
	defaultConfigName = ".cfs-cli.json"
	defaultConfigPath = path.Join(defaultHomeDir, defaultConfigName)
	defaultConfigData = []byte(`
{
  "masterAddr": [
    "master.chubao.io"
  ]
}
`)
)

type config struct {
	MasterAddr []string `json:"masterAddr"`
}

func runCLI() (err error) {
	var cfg *config
	if cfg, err = loadConfig(); err != nil {
		return
	}
	err = setupCommands(cfg).Execute()
	return
}

func loadConfig() (*config, error) {
	var err error
	var configData []byte
	if configData, err = ioutil.ReadFile(defaultConfigPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if os.IsNotExist(err) {
		if err = ioutil.WriteFile(defaultConfigPath, defaultConfigData, 0600); err != nil {
			return nil, err
		}
		configData = defaultConfigData
	}
	var config = &config{}
	if err = json.Unmarshal(configData, config); err != nil {
		return nil, err
	}
	return config, nil
}

func setupCommands(cfg *config) *cobra.Command {
	fmt.Printf("Master address: %v\n", cfg.MasterAddr)
	var mc = master.NewMasterClient(cfg.MasterAddr, false)
	return cmd.NewRootCmd(mc)
}

func main() {
	var err error
	if err = runCLI(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
