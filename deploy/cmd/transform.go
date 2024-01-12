package cmd

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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

func copyFolder(sourcePath, destinationPath string) error {
	err := filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relativePath, err := filepath.Rel(sourcePath, path)
		if err != nil {
			return err
		}

		destinationFilePath := filepath.Join(destinationPath, relativePath)

		if info.IsDir() {
			err := os.MkdirAll(destinationFilePath, info.Mode())
			if err != nil {
				return err
			}
		} else {
			sourceFile, err := os.Open(path)
			if err != nil {
				return err
			}
			defer sourceFile.Close()

			destinationFile, err := os.Create(destinationFilePath)
			if err != nil {
				return err
			}
			defer destinationFile.Close()

			_, err = io.Copy(destinationFile, sourceFile)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

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
