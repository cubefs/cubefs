package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

type DataNode struct {
	Role               string   `json:"role"`
	Listen             string   `json:"listen"`
	Prof               string   `json:"prof"`
	RaftHeartbeat      string   `json:"raftHeartbeat"`
	RaftReplica        string   `json:"raftReplica"`
	RaftDir            string   `json:"raftDir"`
	LocalIP            string   `json:"localIP"`
	ConsulAddr         string   `json:"consulAddr"`
	ExporterPort       int      `json:"exporterPort"`
	Cell               string   `json:"cell"`
	LogDir             string   `json:"logDir"`
	LogLevel           string   `json:"logLevel"`
	Disks              []string `json:"disks"`
	DiskIopsReadLimit  string   `json:"diskIopsReadLimit"`
	DiskIopsWriteLimit string   `json:"diskIopsWriteLimit"`
	DiskFlowReadLimit  string   `json:"diskFlowReadLimit"`
	DiskFlowWriteLimit string   `json:"diskFlowWriteLimit"`
	MasterAddr         []string `json:"masterAddr"`
	EnableSmuxConnPool bool     `json:"enableSmuxConnPool"`
}

func readDataNode(filename string) (*DataNode, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	dataNode := &DataNode{}
	err = json.Unmarshal(data, dataNode)
	if err != nil {
		return nil, err
	}
	return dataNode, nil
}

func writeDataNode(listen, prof, id, localIP string, masterAddrs, disks []string) error {
	// 将DataNode配置写入DataNode.json文件
	datanode := DataNode{
		Role:               "datanode",
		Listen:             listen,
		Prof:               prof,
		LocalIP:            localIP,
		RaftHeartbeat:      "17330",
		RaftReplica:        "17340",
		RaftDir:            "/cfs/log",
		ConsulAddr:         "http://192.168.0.101:8500",
		ExporterPort:       9500,
		Cell:               "cell-01",
		LogDir:             "/cfs/log",
		LogLevel:           "debug",
		Disks:              disks,
		DiskIopsReadLimit:  "20000",
		DiskIopsWriteLimit: "5000",
		DiskFlowReadLimit:  "1024000000",
		DiskFlowWriteLimit: "524288000",
		MasterAddr:         masterAddrs,
		EnableSmuxConnPool: true,
	}

	dataNodeData, err := json.MarshalIndent(datanode, "", "  ")
	if err != nil {
		log.Println("Unable to encode DataNode configuration:", err)
		return err
	}

	err = ioutil.WriteFile(ConfDir+"/datanode"+id+".json", dataNodeData, 0o644)
	if err != nil {
		log.Println("Unable to write to DataNode.json file:", err)
		return err
	}
	return nil
}

func startAllDataNode() error {
	config, err := readConfig()
	if err != nil {
		log.Println(err)
	}
	disksInfo := []string{}
	for _, node := range config.DeployHostsList.DataNode {
		diskMap := ""
		for _, info := range node.Disk {
			diskMap += " -v " + info.Path + ":/cfs" + info.Path
			// disksInfo = append(disksInfo, "/cfs"+info.Path+":"+info.Size)
		}
		disksInfo = append(disksInfo, diskMap)
	}

	var dataDir string
	if config.DataNode.Config.DataDir == "" {
		dataDir = config.Global.DataDir
	} else {
		dataDir = config.DataNode.Config.DataDir
	}
	index := 0
	files, err := ioutil.ReadDir(ConfDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "datanode") && !file.IsDir() {
			data, err := readDataNode(ConfDir + "/" + file.Name())
			if err != nil {
				fmt.Printf("Error reading file %s: %s\n", file.Name(), err)
				return nil
			}
			confFilePath := ConfDir + "/" + file.Name()
			err = transferConfigFileToRemote(confFilePath, dataDir+"/conf", RemoteUser, data.LocalIP)
			if err != nil {
				return err
			}

			err = checkAndDeleteContainerOnNode(RemoteUser, data.LocalIP, strings.Split(file.Name(), ".")[0])
			if err != nil {
				return err
			}
			status, err := startDatanodeContainerOnNode(RemoteUser, data.LocalIP, strings.Split(file.Name(), ".")[0], dataDir, disksInfo[index])
			if err != nil {
				return err
			}
			index++
			log.Println(status)
		}
	}

	log.Println("start all datanode services")
	return nil
}

func startDatanodeInSpecificNode(node string) error {
	config, err := readConfig()
	if err != nil {
		return err
	}

	var dataDir string
	if config.DataNode.Config.DataDir == "" {
		dataDir = config.Global.DataDir
	} else {
		dataDir = config.DataNode.Config.DataDir
	}
	for id, n := range config.DeployHostsList.DataNode {
		if n.Hosts == node {
			confFilePath := ConfDir + "/" + "datanode" + strconv.Itoa(id+1) + ".json"
			err = transferConfigFileToRemote(confFilePath, dataDir+"/conf", RemoteUser, node)
			if err != nil {
				return err
			}

			err = checkAndDeleteContainerOnNode(RemoteUser, node, DataNodeName+strconv.Itoa(id+1))
			if err != nil {
				return err
			}
			diskMap := ""
			for _, info := range n.Disk {
				diskMap += " -v " + info.Path + ":/cfs" + info.Path
			}
			status, err := startDatanodeContainerOnNode(RemoteUser, node, DataNodeName+strconv.Itoa(id+1), dataDir, diskMap)
			if err != nil {
				return err
			}

			log.Println(status)
			break
		}
	}
	return nil
}

func stopDatanodeInSpecificNode(node string) error {
	config, err := readConfig()
	if err != nil {
		return err
	}
	for id, n := range config.DeployHostsList.DataNode {
		if n.Hosts == node {
			status, err := stopContainerOnNode(RemoteUser, node, DataNodeName+strconv.Itoa(id+1))
			if err != nil {
				return err
			}
			log.Println(status)
			status, err = rmContainerOnNode(RemoteUser, node, DataNodeName+strconv.Itoa(id+1))
			if err != nil {
				return err
			}
			log.Println(status)
		}
	}
	return nil
}

func stopAllDataNode() error {
	files, err := ioutil.ReadDir(ConfDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "datanode") && !file.IsDir() {
			data, err := readDataNode(ConfDir + "/" + file.Name())
			if err != nil {
				fmt.Printf("Error reading file %s: %s\n", file.Name(), err)
				return nil
			}
			status, err := stopContainerOnNode(RemoteUser, data.LocalIP, strings.Split(file.Name(), ".")[0])
			if err != nil {
				return err
			}
			log.Println(status)
			status, err = rmContainerOnNode(RemoteUser, data.LocalIP, strings.Split(file.Name(), ".")[0])
			if err != nil {
				return err
			}
			log.Println(status)

		}
	}
	return nil
}
