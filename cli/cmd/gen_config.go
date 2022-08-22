// Copyright 2021 The CubeFS Authors.
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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/spf13/cobra"

	yaml "gopkg.in/yaml.v2"
)

const (
	RoleMaster           = "master"
	RoleClient           = "client"
	KeyRole              = "role"
	KeyVersion           = "version"
	KeyMasterListenPort  = "listen"
	KeyMasterAddr        = "masterAddr"
	KeyMasterClusterName = "clusterName"
	KeyMasterPeers       = "peers"
)

// config root
//
//! version: <cluster config version>
//! cluster:
//!   <cluster config>
type RootConfig struct {
	Version string         `yaml:"version,omitempty"`
	Cluster *ClusterConfig `yaml:"cluster"`
}

// cluster config
//! cluster:
//!    name: <cluster name>
//!    version:  <cluster version>
//!    config:
//!        <common config>
//!    master:
//!        <master config>
//!    nodes:
//!        - <other nodes>
type ClusterConfig struct {
	Name    string                 `yaml:"name"`
	Version string                 `yaml:"version,omitempty"`
	Config  map[string]interface{} `yaml:"config,omitempty"`
	Master  *MasterConfig          `yaml:"master"`
	Nodes   []NodeConfig           `yaml:"nodes,flow"`
}

// other node config
//! role:  <node role>
//! ignoreCommon: <ignore cluster common config>
//! config:
//!     <node specified config map>
type NodeConfig struct {
	Role         string                 `yaml:"role"`
	IgnoreCommon bool                   `yaml:"ignoreCommon,omitempty"`
	Config       map[string]interface{} `yaml:"config,omitempty"`
}

// master config
//! master:
//!     hosts:
//!         - <master host list>
//!     config:
//!         <master config map>
type MasterConfig struct {
	IgnoreCommon bool                   `yaml:"ignoreCommon,omitempty"`
	Hosts        []string               `yaml:"hosts"`
	Config       map[string]interface{} `yaml:"config,omitempty"`
}

var (
	//cluster *ClusterConfig

	optGenCfgInYaml string = "config/cluster.yaml"
	optGenCfgOutDir string = "build/config"
)

var (
	GenClusterCfgCmd = &cobra.Command{
		Use:   "gencfg",
		Short: "generate cfs cluster config",
		Long:  `Generate chubaofs cluster json config files by yaml config`,
		Run:   genCfgCmd,
	}
)

func init() {
	GenClusterCfgCmd.Flags().StringVarP(&optGenCfgInYaml, "yaml", "i", "config/cluster.yaml", "cluster config yaml input file")
	GenClusterCfgCmd.Flags().StringVarP(&optGenCfgOutDir, "out", "o", "build/config", "generated cluster config json file from yaml file")
}

// update node config use cluster common config
//! * if node ignoreCommon config is true, don't use cluster common config
//! * else if node config map exist key, use node config
//! * else add cluster common config key: value
func (cluster *ClusterConfig) updateNodeConfig(role, clusterName string, nodeConfig map[string]interface{}, ignoreCommon bool) {
	if cluster.Version != "" {
		nodeConfig[KeyVersion] = cluster.Version
	}
	nodeConfig[KeyRole] = role
	nodeConfig[KeyMasterClusterName] = clusterName

	if ignoreCommon {
		return
	}
	for k, v := range cluster.Config {
		if nodeConfig[k] == nil {
			nodeConfig[k] = v
		}
	}
}

// load yaml config file from filePath
func loadYamlConfig(filePath string) (*RootConfig, error) {
	yamlFile, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load yaml %v error: %v\n", filePath, err)
		return nil, err
	}

	yamlFile = []byte(os.ExpandEnv(string(yamlFile)))
	conf := new(RootConfig)
	if err = yaml.Unmarshal(yamlFile, conf); err != nil {
		log.Fatalf("Unmarshal %v err: %v", filePath, err)
	}

	return conf, nil
}

// load config file
func loadClusterConfig(confFile string) (cfg *ClusterConfig, err error) {
	var (
		genConfig *RootConfig
	)
	if confFile == "" {
		fmt.Fprintf(os.Stderr, "cluster yaml not specified, use -i to specified. \n")
		return nil, errors.New("cfg not set")
	}

	if genConfig, err = loadYamlConfig(confFile); err != nil {
		return nil, err
	}

	cfg = genConfig.Cluster
	if cfg == nil {
		return nil, errors.New("no cluster config")
	}
	return cfg, nil
}

// generate config files
func genCfgCmd(cmd *cobra.Command, args []string) {
	var (
		err           error
		clusterConfig *ClusterConfig
	)
	//defer func() {
	//    if err != nil {
	//        fmt.Println("Error: ", err)
	//    }
	//}()

	if clusterConfig, err = loadClusterConfig(optGenCfgInYaml); err != nil {
		return
	}

	if !validateClusterConfig(clusterConfig) {
		return
	}

	masterListen := getMasterListen(clusterConfig.Master)
	masterAddrs, masterPeers := getMasterAddrsAndPeers(clusterConfig.Master.Hosts, masterListen)

	generateMasterCfgJson(clusterConfig, masterPeers)

	// generate other node config json
	for _, node := range clusterConfig.Nodes {
		if node.Config == nil {
			node.Config = make(map[string]interface{})
		}
		if node.Role == RoleMaster {
			continue
		} else if node.Role == RoleClient {
			node.Config[KeyMasterAddr] = strings.Join(masterAddrs, ",")
		} else {
			node.Config[KeyMasterAddr] = masterAddrs
		}

		clusterConfig.updateNodeConfig(node.Role, clusterConfig.Name, node.Config, node.IgnoreCommon)

		cfgFilePath := path.Join(optGenCfgOutDir, fmt.Sprintf("%s.json", node.Role))
		if err = storeConfig2Json(cfgFilePath, node.Config); err != nil {
			return
		}
	}
}

func validateClusterConfig(cluster *ClusterConfig) bool {
	if cluster.Master == nil {
		fmt.Fprintf(os.Stderr, "yaml(%v) lack of master config.", optGenCfgInYaml)
		return false
	}
	if len(cluster.Master.Hosts) < 1 {
		fmt.Fprintf(os.Stderr, "yaml(%v) master config lack of hosts config", optGenCfgInYaml)
		return false
	}

	return true
}

func getMasterListen(masterConfig *MasterConfig) string {
	if len(masterConfig.Config) < 1 {
		return ""
	}
	return interface2String(masterConfig.Config[KeyMasterListenPort])
}

// get masterAddrs and masterPeers config from masterHosts
func getMasterAddrsAndPeers(masterHosts []string, masterListen string) (masterAddrs []string, masterPeers string) {
	masterAddrs = make([]string, 0)
	peers := make([]string, 0)

	for i, host := range masterHosts {
		masterAddr := host
		if masterListen != "" {
			masterAddr = fmt.Sprintf("%s:%s", host, masterListen)
		}
		masterAddrs = append(masterAddrs, masterAddr)
		peers = append(peers, fmt.Sprintf("%d:%s", i+1, masterAddr))
	}
	masterPeers = strings.Join(peers, ",")

	return
}

func generateMasterCfgJson(clusterConfig *ClusterConfig, masterPeers string) (err error) {
	//generate master config json
	masterConfig := clusterConfig.Master
	//masterConfig.Config[KeyMasterClusterName] = clusterConfig.Name
	masterConfig.Config[KeyMasterPeers] = masterPeers
	for i, host := range clusterConfig.Master.Hosts {
		clusterConfig.updateNodeConfig(RoleMaster, clusterConfig.Name, masterConfig.Config, masterConfig.IgnoreCommon)

		masterConfig.Config["id"] = fmt.Sprintf("%d", i+1)
		masterConfig.Config["ip"] = host

		masterCfgFile := path.Join(optGenCfgOutDir, fmt.Sprintf("%s/%s.json", RoleMaster, host))
		if err = storeConfig2Json(masterCfgFile, masterConfig.Config); err != nil {
			return
		}
	}

	return
}

// store config to json file
func storeConfig2Json(configPath string, config map[string]interface{}) error {
	configData, _ := json.MarshalIndent(config, "", "  ")

	os.MkdirAll(path.Dir(configPath), 0755)
	if err := ioutil.WriteFile(configPath, configData, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "store config %s err: %v", configPath, err)
		return err
	}
	fmt.Printf("generate config %s done\n", configPath)

	return nil
}

func interface2String(inter interface{}) string {
	switch inter.(type) {
	case string:
		return fmt.Sprintf("%s", inter.(string))
	case int:
		return fmt.Sprintf("%d", inter.(int))
	case float64:
		return fmt.Sprintf("%f", inter.(float64))
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", inter)
	}
}
