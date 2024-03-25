package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

func transferConfigFileToRemote(localFilePath string, remoteFilePath string, remoteUser string, remoteHost string) error {
	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %s", err)
	}
	defer localFile.Close()

	cmd := exec.Command("scp", localFilePath, remoteUser+"@"+remoteHost+":"+remoteFilePath)
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("file %s transferred to %s@%s:%s failed", localFilePath, remoteUser, remoteHost, remoteFilePath)
	}

	log.Printf("file '%s' transferred to '%s@%s:%s' successfully.\n", localFilePath, remoteUser, remoteHost, remoteFilePath)

	return nil
}

func transferDirectoryToRemote(localFilePath string, remoteFilePath string, remoteUser string, remoteHost string) error {
	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %s", err)
	}
	defer localFile.Close()

	cmd := exec.Command("scp", "-r", localFilePath, remoteUser+"@"+remoteHost+":"+remoteFilePath)

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("file %s transferred to %s@%s:%s failed", localFilePath, remoteUser, remoteHost, remoteFilePath)
	}

	log.Printf("file '%s' transferred to '%s@%s:%s' successfully.\n", localFilePath, remoteUser, remoteHost, remoteFilePath)

	return nil
}

func createRemoteFolder(nodeUser, node, folderPath string) error {
	cmdStr := "mkdir -p " + folderPath
	cmd := exec.Command("ssh", nodeUser+"@"+node, cmdStr)
	_, err := cmd.Output()
	if err != nil {
		return err
	}
	return nil
}

func createFolder(folderPath string) error {
	_, err := os.Stat(folderPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(folderPath, 0o755)
		if err != nil {
			return err
		}

	} else if err != nil {
		return err
	} else {
		return nil
	}
	return nil
}
