package cmd

import (
	"log"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Global          GlobalConfig          `yaml:"global"`
	Master          MasterConfig          `yaml:"master"`
	MetaNode        MetaNodeConfig        `yaml:"metanode"`
	DataNode        DataNodeConfig        `yaml:"datanode"`
	DeployHostsList DeployHostsListConfig `yaml:"deplopy_hosts_list"`
}

type GlobalConfig struct {
	ContainerImage string `yaml:"container_image"`
	DataDir        string `yaml:"data_dir"`
	Variable       struct {
		Target string `yaml:"target"`
	} `yaml:"variable"`
}

type MasterConfig struct {
	Config struct {
		Listen  string `yaml:"listen"`
		Prof    string `yaml:"prof"`
		DataDir string `yaml:"data_dir"`
	} `yaml:"config"`
}

type MetaNodeConfig struct {
	Config struct {
		Listen  string `yaml:"listen"`
		Prof    string `yaml:"prof"`
		DataDir string `yaml:"data_dir"`
	} `yaml:"config"`
}

type DataNodeConfig struct {
	Config struct {
		Listen  string `yaml:"listen"`
		Prof    string `yaml:"prof"`
		DataDir string `yaml:"data_dir"`
	} `yaml:"config"`
}

type DeployHostsListConfig struct {
	Master struct {
		Hosts []string `yaml:"hosts"`
	} `yaml:"master"`
	MetaNode struct {
		Hosts []string `yaml:"hosts"`
	} `yaml:"metanode"`
	DataNode []struct {
		Hosts string     `yaml:"hosts"`
		Disk  []DiskInfo `yaml:"disk"`
	} `yaml:"datanode"`
}

type DiskInfo struct {
	Path string `yaml:"path"`
	Size string `yaml:"size"`
}

func readConfig() (*Config, error) {
	data, err := os.ReadFile(ConfigFileName)
	if err != nil {
		log.Println("Unable to read configuration file:", err)
		return nil, err
	}

	config := &Config{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Println("Unable to parse configuration file:", err)
		return nil, err
	}
	return config, nil
}

func convertToJosn() error {
	err := createFolder(ConfDir)
	if err != nil {
		return err
	}
	config, err := readConfig()
	if err != nil {
		return err
	}

	for id, node := range config.DeployHostsList.Master.Hosts {
		peers := getMasterPeers(config)
		err := writeMaster(ClusterName, strconv.Itoa(id+1), node, config.Master.Config.Listen, config.Master.Config.Prof, peers)
		if err != nil {
			return err
		}
	}

	masterAddr, err := getMasterAddrAndPort()
	if err != nil {
		return err
	}
	for id, node := range config.DeployHostsList.MetaNode.Hosts {
		err = writeMetaNode(config.MetaNode.Config.Listen, config.MetaNode.Config.Prof, strconv.Itoa(id+1), node, masterAddr)
		if err != nil {
			return err
		}
	}

	disksInfo := []string{}
	for _, node := range config.DeployHostsList.DataNode {
		diskMap := ""
		for _, info := range node.Disk {
			diskMap += " -v " + info.Path + ":/cfs" + info.Path
			disksInfo = append(disksInfo, "/cfs"+info.Path+":"+info.Size)
		}
	}

	for id, node := range config.DeployHostsList.MetaNode.Hosts {
		err = writeDataNode(config.DataNode.Config.Listen, config.DataNode.Config.Prof, strconv.Itoa(id+1), node, masterAddr, disksInfo)
		if err != nil {
			return err
		}
	}

	return nil
}
