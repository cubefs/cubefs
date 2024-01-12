package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

func stopContainerOnNode(nodeUser, node, containerName string) (string, error) {
	if ok, _ := checkContainerExistence(nodeUser, node, containerName); ok {
		cmd := exec.Command("ssh", nodeUser+"@"+node, "docker stop ", containerName)
		_, err := cmd.Output()
		if err != nil {
			return fmt.Sprintf("failed stop %s on node %s", containerName, node), err
		}
		return fmt.Sprintf("successful stop %s on node %s", containerName, node), nil
	}
	return fmt.Sprintf("%s on node %s already stopped", containerName, node), nil
}

func rmContainerOnNode(nodeUser, node, containerName string) (string, error) {
	if ok, _ := checkContainerExistence(nodeUser, node, containerName); ok {
		cmd := exec.Command("ssh", nodeUser+"@"+node, "docker rm ", containerName)
		_, err := cmd.Output()
		if err != nil {
			return fmt.Sprintf("failed rm %s on node %s", containerName, node), err
		}
		return fmt.Sprintf("successful rm %s on node %s", containerName, node), nil
	}
	return fmt.Sprintf("%s on node %s already removed", containerName, node), nil
}

func startMasterContainerOnNode(nodeUser, node, containerName, dataDir string) (string, error) {
	cmd := exec.Command("ssh", nodeUser+"@"+node,
		"docker run -d --name "+containerName+
			" -v "+dataDir+"/disk/"+containerName+"/data:/cfs/data"+
			" -v "+dataDir+"/bin"+":/cfs/bin:ro"+
			" -v "+dataDir+"/disk/"+containerName+"/log:/cfs/log"+
			" -v "+dataDir+"/conf/"+containerName+".json:/cfs/conf/master.json"+
			" -v "+dataDir+"/script/start_master.sh:/cfs/script/start.sh"+
			" --restart on-failure --privileged     --network  host   "+ImageName+
			"   /bin/sh /cfs/script/start.sh ")
	_, err := cmd.Output()
	if err != nil {
		return fmt.Sprintf("failed start %s on node %s", containerName, node), err
	}
	return fmt.Sprintf("successfully started the container %s on node %s", containerName, node), nil
}

func startMetanodeContainerOnNode(nodeUser, node, containerName, dataDir string) (string, error) {
	cmd := exec.Command("ssh", nodeUser+"@"+node,
		"docker run -d --name "+containerName+
			" -v "+dataDir+"/disk/"+containerName+"/data:/cfs/data"+
			" -v "+dataDir+"/bin"+":/cfs/bin:ro"+
			" -v "+dataDir+"/disk/"+containerName+"/log:/cfs/log"+
			" -v "+dataDir+"/conf/"+containerName+".json:/cfs/conf/metanode.json"+
			" -v "+dataDir+"/script/start_meta.sh:/cfs/script/start.sh"+
			" --restart on-failure --privileged     --network  host "+ImageName+
			"  /bin/sh /cfs/script/start.sh ")
	_, err := cmd.Output()
	if err != nil {
		return fmt.Sprintf("failed start %s on node %s", containerName, node), err
	}
	return fmt.Sprintf("successfully started the container %s on node %s", containerName, node), nil
}

func startDatanodeContainerOnNode(nodeUser, node, containerName, dataDir, diskMap string) (string, error) {
	cmd := exec.Command("ssh", nodeUser+"@"+node,
		"docker run -d --name "+containerName+
			" -v "+dataDir+"/disk/"+containerName+"/data:/cfs/data"+
			" -v "+dataDir+"/bin"+":/cfs/bin:ro"+
			" -v "+dataDir+"/disk/"+containerName+"/log:/cfs/log"+
			" -v "+dataDir+"/conf/"+containerName+".json:/cfs/conf/datanode.json"+diskMap+
			" -v "+dataDir+"/script/start_datanode.sh:/cfs/script/start.sh"+
			" --restart on-failure --privileged     --network  host  "+ImageName+
			"  /bin/sh /cfs/script/start.sh ")
	_, err := cmd.Output()
	if err != nil {
		return fmt.Sprintf("failed start %s on node %s", containerName, node), err
	}
	return fmt.Sprintf("successfully started the container %s on node %s", containerName, node), nil
}

func checkContainerExistence(nodeUser, node, containerName string) (bool, error) {
	cmd := exec.Command("ssh", nodeUser+"@"+node, "docker ps -a --format "+`{{.Names}}`+" | grep "+`"`+containerName+`"`)
	output, err := cmd.Output()
	if err != nil {
		return false, err
	}
	for _, name := range strings.Fields(string(output)) {
		if name == containerName {
			return true, nil
		}
	}
	return false, nil
}

func checkAndDeleteContainerOnNode(nodeUser, node, containerName string) error {
	if ok, _ := checkContainerExistence(nodeUser, node, containerName); ok {
		log.Printf("container %s already exists on node %s", containerName, node)
		_, err := stopContainerOnNode(nodeUser, node, containerName)
		log.Printf("stop container %s on node %s successfully", containerName, node)
		if err != nil {
			return err
		}
		_, err = rmContainerOnNode(nodeUser, node, containerName)
		log.Printf("rm container %s on node %s successfully", containerName, node)
		if err != nil {
			return err
		}
	}
	return nil
}

// Check if Docker is installed and installed
func checkAndInstallDocker(nodeUser, node string) error {
	// Check if Docker is installed
	cmd := exec.Command("ssh", nodeUser+"@"+node, "docker --version")
	output, err := cmd.Output()
	if err == nil && strings.Contains(string(output), "Docker version") {
		// log.Println("Docker installed")
		return nil
	}

	// Docker not installed, installing Docker
	cmd = exec.Command("ssh", nodeUser+"@"+node, "yum", "install", "docker", "-y")

	// Set output to standard output and standard error output
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Execute Command
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to install Docker on node %s", node)
	}

	return nil
}

// Check if the Docker service is started and started
func checkAndStartDockerService(nodeUser, node string) error {
	// Check Docker Service Status
	cmd := exec.Command("ssh", nodeUser+"@"+node, "systemctl is-active docker.service")
	output, err := cmd.Output()

	if err == nil && strings.TrimSpace(string(output)) == "active" {
		return nil
	}

	// Docker service not started, starting Docker service
	cmd = exec.Command("ssh", nodeUser+"@"+node, "systemctl start docker")

	// Set output to standard output and standard error output
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to start Docker service on node %s", node)
	}
	log.Println("docker start")

	return nil
}

// Pull image from configuration file
func pullImageOnNode(nodeUser, node, imageName string) error {
	// Remote execution of commands to pull images
	cmd := exec.Command("ssh", nodeUser+"@"+node, "docker pull "+imageName)

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to pull mirror %s on node %s", imageName, node)
	}

	return nil
}

func removeImageOnNode(nodeUser, node, imageName string) error {
	cmd := exec.Command("ssh", nodeUser+"@"+node, "docker rmi "+imageName)

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to remove mirror %s on node %s", imageName, node)
	}
	log.Printf("success to remove mirror %s on node %s \n", imageName, node)
	return nil
}

func containerStatus(nodeUser, node, containerName string) (string, error) {
	cmd := exec.Command("ssh", nodeUser+"@"+node, "docker inspect --format='{{.State.Status}}' "+containerName)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to pull mirror %s on node %s", containerName, node)
	}

	return string(output), nil
}
