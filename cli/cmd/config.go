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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const (
	cmdConfigShort = "Manage global config file"
)

var (
	defaultHomeDir, _ = os.UserHomeDir()
	defaultConfigName = ".cfs-cli.json"
	defaultConfigPath = path.Join(defaultHomeDir, defaultConfigName)
	defaultConfigData = []byte(`
{
  "masterAddr": [
    "master.chubao.io"
  ],
  "timeout": 60
}
`)
	defaultConfigTimeout uint16 = 60
)

type Config struct {
	MasterAddr []string `json:"masterAddr"`
	Timeout    uint16   `json:"timeout"`
}

func newConfigCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliResourceConfig,
		Short: cmdConfigShort,
	}
	cmd.AddCommand(newConfigSetCmd())
	cmd.AddCommand(newConfigInfoCmd())
	return cmd
}

const (
	cmdConfigSetShort  = "set value of config file"
	cmdConfigInfoShort = "show info of config file"
)

func newConfigSetCmd() *cobra.Command {
	var optMasterHosts string
	var optTimeout string
	var cmd = &cobra.Command{
		Use:   CliOpSet,
		Short: cmdConfigSetShort,
		Long:  `Set the config file`,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			tmp, _ := strconv.Atoi(optTimeout)
			timeOut :=uint16(tmp)
			if optMasterHosts == "" && timeOut == 0 {
				stdout(fmt.Sprintf("No change. Input 'cfs-cli config set -h' for help.\n"))
				return
			}

			if optMasterHosts != "" && timeOut == 0 {
				stdout(fmt.Sprintf("timeOut %v is invalid.\n", timeOut))
				return
			}

			if err = setConfig(optMasterHosts, timeOut); err != nil {
				return
			}
			stdout(fmt.Sprintf("Config has been set successfully!\n"))
		},
	}
	cmd.Flags().StringVar(&optMasterHosts, "addr", "",
		"Specify master address {HOST}:{PORT}[,{HOST}:{PORT}]")
	cmd.Flags().StringVar(&optTimeout, "timeout", "60", "Specify timeout for requests [Unit: s]")
	return cmd
}
func newConfigInfoCmd() *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	var cmd = &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdConfigInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			config, err := LoadConfig()
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				OsExitWithLogFlush()
			}
			printConfigInfo(config)
		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive]")
	return cmd
}

func printConfigInfo(config *Config) {
	stdout("Config info:\n")
	stdout("  Master  Address    : %v\n", config.MasterAddr)
	stdout("  Request Timeout [s]: %v\n", config.Timeout)
}

func setConfig(masterHosts string, timeout uint16) (err error) {
	var config *Config
	if config, err = LoadConfig(); err != nil {
		return
	}
	hosts := strings.Split(masterHosts, ",")
	if masterHosts != "" && len(hosts) > 0 {
		config.MasterAddr = hosts
	}
	if timeout != 0 {
		config.Timeout = timeout
	}
	var configData []byte
	if configData, err = json.Marshal(config); err != nil {
		return
	}
	if err = ioutil.WriteFile(defaultConfigPath, configData, 0600); err != nil {
		return
	}
	return nil
}

func LoadConfig() (*Config, error) {
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
	var config = &Config{}
	if err = json.Unmarshal(configData, config); err != nil {
		return nil, err
	}
	if config.Timeout == 0 {
		config.Timeout = defaultConfigTimeout
	}
	return config, nil
}
