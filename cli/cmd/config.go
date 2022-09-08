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
	"github.com/chubaofs/chubaofs/convertnode"
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
  ],
  "monitorAddr": "monitor.chubao.io",
  "dnProf": 17320,
  "mnProf": 17220,
  "enProf": 17520,
  "convertAddr": "",
  "convertNodeDBConfig": {
    "path": "gate11.local.jed.jddb.com:3306",
    "config": "charset=utf8&parseTime=True&loc=Local",
    "dbName": "cfs_convert_node",
    "userName": "cfs_convert_node_rw",
    "password": "7ANiuzwYUuBFedhO",
    "maxIdleConns": 10,
    "maxOpenConns": 100,
    "logMode": false,
    "logZap": ""
  },
  "isDbBack": 0
}
`)
)

type Config struct {
	MasterAddr          []string             `json:"masterAddr"`
	MonitorAddr         string               `json:"monitorAddr"`
	DataNodeProfPort    uint16               `json:"dnProf"`
	MetaNodeProfPort    uint16               `json:"mnProf"`
	EcNodeProfPort      uint16               `json:"enProf"`
	ConvertAddr         string               `json:"convertAddr"`
	ConvertNodeDBConfig convertnode.DBConfig `json:"convertNodeDBConfig"`
	IsDbBack            int8                 `json:"isDbBack"`
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
	var optMasterHost string
	var optMonitorHost string
	var optDNProfPort uint16
	var optMNProfPort uint16
	var optENProfPort uint16
	var optConvertHost string
	var optConvertNodeDBAddr string
	var optConvertNodeDBName string
	var optConvertNodeDBUserName string
	var optConvertNodeDBPassword string
	var optIsDbBack int8
	var cmd = &cobra.Command{
		Use:   CliOpSet,
		Short: cmdConfigSetShort,
		Long:  `Set the config file`,
		Run: func(cmd *cobra.Command, args []string) {
			var masterHosts []string
			var monitorHosts string
			var config *Config
			var err error
			if optMasterHost == "" && optMonitorHost == "" && optDNProfPort == 0 && optMNProfPort == 0 && optENProfPort == 0 && len(optConvertHost) == 0 &&
				optConvertNodeDBAddr == "" && optConvertNodeDBUserName == "" && optConvertNodeDBPassword == "" && optIsDbBack == -1 {
				stdout(fmt.Sprintf("No changes has been set. Input 'cfs-cli config set -h' for help.\n"))
				return
			}
			if len(optMasterHost) != 0 {
				masterHosts = append(masterHosts, optMasterHost)
			}
			if optMonitorHost != "" {
				monitorHosts = optMonitorHost
			}
			if config, err = LoadConfig(); err != nil {
				stdout("load config file failed")
				return
			}
			if len(masterHosts) > 0 {
				config.MasterAddr = masterHosts
			}
			if len(monitorHosts) > 0 {
				config.MonitorAddr = monitorHosts
			}
			if optDNProfPort > 0 {
				config.DataNodeProfPort = optDNProfPort
			}
			if optMNProfPort > 0 {
				config.MetaNodeProfPort = optMNProfPort
			}
			if optENProfPort > 0 {
				config.EcNodeProfPort = optENProfPort
			}
			if len(optConvertHost) > 0 {
				config.ConvertAddr = optConvertHost
			}
			if optConvertNodeDBAddr != "" {
				config.ConvertNodeDBConfig.Path = optConvertNodeDBAddr
			}
			if optConvertNodeDBName != "" {
				config.ConvertNodeDBConfig.Dbname = optConvertNodeDBName
			}
			if optConvertNodeDBUserName != "" {
				config.ConvertNodeDBConfig.Username = optConvertNodeDBUserName
			}
			if optConvertNodeDBPassword != "" {
				config.ConvertNodeDBConfig.Password = optConvertNodeDBPassword
			}
			if optIsDbBack != -1 {
				config.IsDbBack = optIsDbBack
			}
			if _, err := setConfig(config); err != nil {
				stdout("error: %v\n", err)
				return
			}
			stdout(fmt.Sprintf("Config has been set successfully!\n"))
		},
	}
	cmd.Flags().StringVar(&optMasterHost, "addr", "", "Specify master address [{HOST}:{PORT}]")
	cmd.Flags().StringVar(&optMonitorHost, "monitorAddr", "", "Specify monitor address [{HOST}:{PORT}]")
	cmd.Flags().Uint16Var(&optDNProfPort, "dnProf", 0, "Specify prof port for DataNode")
	cmd.Flags().Uint16Var(&optMNProfPort, "mnProf", 0, "Specify prof port for MetaNode")
	cmd.Flags().StringVar(&optConvertHost, "convertAddr", "", "Specify convert address [{HOST}:{PORT}]")
	cmd.Flags().StringVar(&optConvertNodeDBAddr, "convertNodeDBAddr", "", "Specify convert node database address")
	cmd.Flags().StringVar(&optConvertNodeDBName, "convertNodeDBName", "", "Specify convert node database name")
	cmd.Flags().StringVar(&optConvertNodeDBUserName, "convertNodeDBUserName", "", "Specify convert node database user name")
	cmd.Flags().StringVar(&optConvertNodeDBPassword, "convertNodeDBPassword", "", "Specify convert node database password")
	cmd.Flags().Int8Var(&optIsDbBack, "isDbBack", -1, "If used for dbback, 0 or 1")
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
			stdout(fmt.Sprintf("Config info:\n  %v\n", config.MasterAddr))
			stdout(fmt.Sprintf("Monitor address:\n  %v\n", config.MonitorAddr))
			stdout(fmt.Sprintf("MySQL Database Addr:\n  %s\n", config.ConvertNodeDBConfig.Path))
			stdout(fmt.Sprintf("MySQL Database Name:\n  %s\n", config.ConvertNodeDBConfig.Dbname))
			stdout(fmt.Sprintf("IsDbBack:\n  %v\n", config.IsDbBack))
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
