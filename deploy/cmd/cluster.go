package cmd

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var ClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "cluster manager",
	Long:  `This command will manager the cluster.`,

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(cmd.UsageString())
	},
}

var initCommand = &cobra.Command{
	Use:   "init",
	Short: "init the cluster from config.yaml",
	Long:  "init the cluster from config.yaml",

	Run: func(cmd *cobra.Command, args []string) {
		initCluster()
	},
}

var infoCommand = &cobra.Command{
	Use:   "info",
	Short: "Display cluster information",
	Long:  "Display cluster information",
	Run: func(cmd *cobra.Command, args []string) {
		err := infoOfCluster()
		if err != nil {
			log.Println(err)
		}
	},
}

var clearCommand = &cobra.Command{
	Use:   "clear",
	Short: "Clear cluster files and information",
	Long:  "Clear cluster files and information",
	Run: func(cmd *cobra.Command, args []string) {
		cleanContainerAndLogs()
	},
}

var configCommand = &cobra.Command{
	Use:   "config",
	Short: "Loading configuration files into the cluster",
	Long:  "Loading configuration files into the cluster",
	Run: func(cmd *cobra.Command, args []string) {
		err := convertToJosn()
		if err != nil {
			log.Println(err)
		}
	},
}

// Obtain the IP address of the current host
func getCurrentIP() (string, error) {
	// Get Host Name
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			return ipv4.String(), nil
		}
	}

	return "", fmt.Errorf("IPv4 address not found")
}

func printTable(services []Service) {
	fmt.Println("Server Type  | Container Name | Node IP         | Status")
	fmt.Println("-----------------------------------------------------")
	for _, service := range services {
		fmt.Printf("%-12s | %-14s | %-13s | %s\n", service.ServerType, service.ContainerName, service.NodeIP, service.Status)
	}
}

func infoOfCluster() error {
	config, err := readConfig()
	if err != nil {
		log.Fatal(err)
	}
	servers := []Service{}

	for id, node := range config.DeployHostsList.Master.Hosts {
		server := Service{}
		server.NodeIP = node
		server.ServerType = MasterServer
		server.Status = Stopped
		server.ContainerName = MasterName + strconv.Itoa(id+1)

		ps, _ := containerStatus(RemoteUser, node, MasterName+strconv.Itoa(id+1))

		if strings.Contains(ps, "running") {
			server.Status = Running
		}
		servers = append(servers, server)

	}

	for id, node := range config.DeployHostsList.MetaNode.Hosts {
		server := Service{}
		server.NodeIP = node
		server.ServerType = MetaNodeServer
		server.Status = Stopped
		server.ContainerName = MetaNodeName + strconv.Itoa(id+1)
		ps, _ := containerStatus(RemoteUser, node, MetaNodeName+strconv.Itoa(id+1))

		if strings.Contains(ps, "running") {
			server.Status = Running
		}
		servers = append(servers, server)
	}

	for id, node := range config.DeployHostsList.DataNode {
		server := Service{}
		server.NodeIP = node.Hosts
		server.ServerType = DataNodeServer
		server.Status = Stopped
		server.ContainerName = DataNodeName + strconv.Itoa(id+1)

		ps, _ := containerStatus(RemoteUser, node.Hosts, DataNodeName+strconv.Itoa(id+1))

		if strings.Contains(ps, "running") {
			server.Status = Running
		}
		servers = append(servers, server)
	}

	printTable(servers)
	return nil
}

func removeDuplicates(slice []string) []string {
	encountered := map[string]bool{}
	result := []string{}

	for _, item := range slice {
		if encountered[item] {
			continue
		}
		encountered[item] = true
		result = append(result, item)
	}

	return result
}

func initCluster() {
	config, err := readConfig()
	if err != nil {
		log.Fatal(err)
	}

	hosts := []string{}
	hosts = append(hosts, config.DeployHostsList.Master.Hosts...)
	hosts = append(hosts, config.DeployHostsList.MetaNode.Hosts...)
	for i := 0; i < len(config.DeployHostsList.DataNode); i++ {
		hosts = append(hosts, config.DeployHostsList.DataNode[i].Hosts)
	}

	newHosts := removeDuplicates(hosts)

	// Obtain the IP address of the current host
	currentNode, err := getCurrentIP()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("The IP address of the current host:", currentNode)

	// Establish a secure connection from the current node to other nodes
	for _, node := range newHosts {
		if node == currentNode || node == "" {
			continue
		}
		err := establishSSHConnectionWithoutPassword(currentNode, RemoteUser, node)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Password free connection establishment completed")

	for _, node := range newHosts {
		// Check if Docker is installed and installed
		if node == "" {
			continue
		}
		checkAndInstallDocker(RemoteUser, node)

		// Check if the Docker service is started and started
		err = checkAndStartDockerService(RemoteUser, node)
		if err != nil {
			log.Printf("Failed to start Docker service on node% s:% v", node, err)
		} else {
			log.Printf("The docker for node %s is ready", node)
		}

		// Pull Mirror
		err = pullImageOnNode(RemoteUser, node, config.Global.ContainerImage)
		if err != nil {
			log.Printf("Failed to pull mirror% s on node% s:% v", node, config.Global.ContainerImage, err)
		} else {
			log.Printf("Successfully pulled mirror % s on node % s", config.Global.ContainerImage, node)
		}

		// create  dir
		err = createRemoteFolder(RemoteUser, node, config.Global.DataDir)
		// check firewall
		if err != nil {
			log.Println(err)
		}
		err = transferDirectoryToRemote(BinDir, config.Global.DataDir, RemoteUser, node)
		if err != nil {
			log.Println(err)
		}
		err = transferDirectoryToRemote(ScriptDir, config.Global.DataDir, RemoteUser, node)
		if err != nil {
			log.Println(err)
		}
		// create conf dir
		err = createRemoteFolder(RemoteUser, node, config.Global.DataDir+"/conf")
		// check firewall
		if err != nil {
			log.Println(err)
		}
		stopFirewall(RemoteUser, node)

	}

	log.Println("*******Cluster environment initialization completed******")
}

func cleanContainerAndLogs() {
	config, err := readConfig()
	if err != nil {
		log.Fatal(err)
	}
	hosts := []string{}
	hosts = append(hosts, config.DeployHostsList.Master.Hosts...)
	hosts = append(hosts, config.DeployHostsList.MetaNode.Hosts...)
	for i := 0; i < len(config.DeployHostsList.DataNode); i++ {
		hosts = append(hosts, config.DeployHostsList.DataNode[i].Hosts)
	}

	newHosts := removeDuplicates(hosts)

	// only remove container image
	for _, node := range newHosts {
		if node == "" {
			continue
		}
		err := removeImageOnNode(RemoteUser, node, config.Global.ContainerImage)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func init() {
	ClusterCmd.AddCommand(initCommand)
	ClusterCmd.AddCommand(infoCommand)
	ClusterCmd.AddCommand(clearCommand)
	ClusterCmd.AddCommand(configCommand)
	configCommand.PersistentFlags().StringVarP(&ConfigFileName, "file", "f", "", "Specify the location of the configuration file relative to cfs deploy")
}
