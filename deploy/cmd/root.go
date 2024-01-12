package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	Version        bool
	CubeFSPath     string
	ConfDir        string
	ScriptDir      string
	BinDir         string
	ConfigFileName string
	ImageName      string
)

const (
	envVar      = "CUBEFS"
	ClusterName = "cubeFS"
	RemoteUser  = "root"
)

const BinVersion = "release-3.2.1"

func init() {
	CubeFSPath = os.Getenv(envVar)
	ConfDir = CubeFSPath + "/deploy/conf"
	ScriptDir = CubeFSPath + "/docker/script"
	BinDir = CubeFSPath + "/build/bin"
	ConfigFileName = CubeFSPath + "/deploy/config.yaml"
	config, _ := readConfig()
	ImageName = config.Global.ContainerImage
}

type ServerType string

const (
	MasterServer   ServerType = "master"
	MetaNodeServer ServerType = "metanode"
	DataNodeServer ServerType = "datanode"
)

const (
	MasterName   = "master"
	MetaNodeName = "metanode"
	DataNodeName = "datanode"
)

type Status string

const (
	Running Status = "running"
	Stopped Status = "stopped"
	Created Status = "created"
	Paused  Status = "paused"
)

type Service struct {
	ServerType    ServerType
	ContainerName string
	NodeIP        string
	Status        Status
}

var RootCmd = &cobra.Command{
	Use:   "deploy-cli",
	Short: "CLI for managing CubeFS server and client using Docker",
	Long:  `cubefs is a CLI application for managing CubeFS, an open-source distributed file system, using Docker containers.`,
	Run: func(cmd *cobra.Command, args []string) {
		if Version {
			fmt.Printf("deploy-cli version 0.0.1      cubefs version %s \n", BinVersion)
		} else {
			fmt.Println(cmd.UsageString())
		}
	},
}
