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
  ]
}
`)
)

type Config struct {
	MasterAddr []string `json:"masterAddr"`
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
	var cmd = &cobra.Command{
		Use:   CliOpSet,
		Short: cmdConfigSetShort,
		Long: `Set the config file using command line:
	config --set-host 192.168.0.11:17010`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				count       int8
				isMasterEnd bool
				masterHosts []string
			)
			for !isMasterEnd {
				var masterHost string
				var isEnd string
				stdout(fmt.Sprintf("Please input master host %v:\n", count+1))
				_, _ = fmt.Scanln(&masterHost)
				if len(masterHost) == 0 {
					if count == 0 {
						stdout("Abort by user.\n")
						return
					}
					continue
				}
				masterHosts = append(masterHosts, masterHost)

				stdout(fmt.Sprintf("Input another master host?(y/n)[y]:\n"))
				_, _ = fmt.Scanln(&isEnd)
				if isEnd == "y" || len(isEnd) == 0 {
					count += 1
				} else {
					isMasterEnd = true
				}
			}
			config := &Config{
				MasterAddr: masterHosts,
			}
			setConfig(config)
			stdout(fmt.Sprintf("Config has been set successfully!\n"))
		},
	}
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
				os.Exit(1)
			}
			stdout(fmt.Sprintf("Config info:\n	%v", config.MasterAddr))

		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive")
	return cmd
}

func setConfig(config *Config) (*Config, error) {
	var err error
	var configData []byte
	if configData, err = json.Marshal(config); err != nil {
		return nil, err
	}
	if err = ioutil.WriteFile(defaultConfigPath, configData, 0600); err != nil {
		return nil, err
	}
	return config, nil
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
	return config, nil
}
