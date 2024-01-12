package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

type Master struct {
	ClusterName         string `json:"clusterName"`
	ID                  string `json:"id"`
	Role                string `json:"role"`
	IP                  string `json:"ip"`
	Listen              string `json:"listen"`
	Prof                string `json:"prof"`
	HeartbeatPort       string `json:"heartbeatPort"`
	ReplicaPort         string `json:"replicaPort"`
	Peers               string `json:"peers"`
	RetainLogs          string `json:"retainLogs"`
	ConsulAddr          string `json:"consulAddr"`
	ExporterPort        int    `json:"exporterPort"`
	LogLevel            string `json:"logLevel"`
	LogDir              string `json:"logDir"`
	WALDir              string `json:"walDir"`
	StoreDir            string `json:"storeDir"`
	MetaNodeReservedMem string `json:"metaNodeReservedMem"`
	EBSAddr             string `json:"ebsAddr"`
	EBSServicePath      string `json:"ebsServicePath"`
}

func getMasterPeers(config *Config) string {
	peers := ""
	for id, node := range config.DeployHostsList.Master.Hosts {
		if id != len(config.DeployHostsList.Master.Hosts)-1 {
			peers = peers + strconv.Itoa(id+1) + ":" + node + ":" + config.Master.Config.Listen + ","
		} else {
			peers = peers + strconv.Itoa(id+1) + ":" + node + ":" + config.Master.Config.Listen
		}
	}
	return peers
}

func readMaster(filename string) (*Master, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	master := &Master{}
	err = json.Unmarshal(data, master)
	if err != nil {
		return nil, err
	}

	return master, nil
}

func writeMaster(clusterName, id, ip, listen, prof, peers string) error {
	master := Master{
		ClusterName:         clusterName,
		ID:                  id,
		Role:                "master",
		IP:                  ip,
		Listen:              listen,
		Prof:                prof,
		HeartbeatPort:       "5901",
		ReplicaPort:         "5902",
		Peers:               peers,
		RetainLogs:          "20000",
		ConsulAddr:          "http://192.168.0.101:8500",
		ExporterPort:        9500,
		LogLevel:            "debug",
		LogDir:              "/cfs/log",
		WALDir:              "/cfs/data/wal",
		StoreDir:            "/cfs/data/store",
		MetaNodeReservedMem: "67108864",
		EBSAddr:             "10.177.40.215:8500",
		EBSServicePath:      "access",
	}

	masterData, err := json.MarshalIndent(master, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot be resolved to master.json %v", err)
	}
	fileName := ConfDir + "/master" + id + ".json"
	err = ioutil.WriteFile(fileName, masterData, 0o644)
	if err != nil {
		return fmt.Errorf("unable to write %s  %v", fileName, err)
	}
	return nil
}

func startAllMaster() error {
	config, err := readConfig()
	if err != nil {
		return err
	}

	files, err := ioutil.ReadDir(ConfDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "master") && !file.IsDir() {
			data, err := readMaster(ConfDir + "/" + file.Name())
			if err != nil {
				fmt.Printf("Error reading file %s: %s\n", file.Name(), err)
				return nil
			}
			confFilePath := ConfDir + "/" + file.Name()
			var dataDir string
			if config.Master.Config.DataDir == "" {
				dataDir = config.Global.DataDir
			} else {
				dataDir = config.Master.Config.DataDir
			}
			err = transferConfigFileToRemote(confFilePath, dataDir+"/conf", RemoteUser, data.IP)
			if err != nil {
				return err
			}
			err = checkAndDeleteContainerOnNode(RemoteUser, data.IP, data.Role+data.ID)
			if err != nil {
				return err
			}
			status, err := startMasterContainerOnNode(RemoteUser, data.IP, data.Role+data.ID, dataDir)
			if err != nil {
				return err
			}
			log.Println(status)
		}
	}
	// Detect successful deployment
	log.Println("start all master services")
	return nil
}

func stopAllMaster() error {
	files, err := ioutil.ReadDir(ConfDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "master") && !file.IsDir() {
			data, err := readMaster(ConfDir + "/" + file.Name())
			if err != nil {
				fmt.Printf("Error reading file %s: %s\n", file.Name(), err)
				return nil
			}
			status, err := stopContainerOnNode(RemoteUser, data.IP, data.Role+data.ID)
			if err != nil {
				return err
			}
			log.Println(status)
			status, err = rmContainerOnNode(RemoteUser, data.IP, data.Role+data.ID)
			if err != nil {
				return err
			}
			log.Println(status)
		}
	}
	return nil
}
