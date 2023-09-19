package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

func transferConfigFileToRemote(localFilePath string, remoteFilePath string, remoteUser string, remoteHost string) error {
	// 打开本地文件
	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %s", err)
	}
	defer localFile.Close()

	cmd := exec.Command("scp", localFilePath, remoteUser+"@"+remoteHost+":"+remoteFilePath)
	//output, err := cmd.Output()
	// cmd.Stdout = os.Stdout
	// cmd.Stderr = os.Stderr
	// Execute Command
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("file %s transferred to %s@%s:%s failed", localFilePath, remoteUser, remoteHost, remoteFilePath)
	}

	log.Printf("file '%s' transferred to '%s@%s:%s' successfully.\n", localFilePath, remoteUser, remoteHost, remoteFilePath)

	return nil
}

func transferDirectoryToRemote(localFilePath string, remoteFilePath string, remoteUser string, remoteHost string) error {
	// 打开本地文件
	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %s", err)
	}
	defer localFile.Close()
	//递归的传输整个目录
	cmd := exec.Command("scp", "-r", localFilePath, remoteUser+"@"+remoteHost+":"+remoteFilePath)
	//output, _ := cmd.Output()
	// cmd.Stdout = os.Stdout
	// cmd.Stderr = os.Stderr
	//fmt.Println(output)
	//Execute Command
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("file %s transferred to %s@%s:%s failed", localFilePath, remoteUser, remoteHost, remoteFilePath)
	}

	log.Printf("file '%s' transferred to '%s@%s:%s' successfully.\n", localFilePath, remoteUser, remoteHost, remoteFilePath)

	return nil
}

// func main() {
//     localFilePath := "/path/to/local/file.txt"
//     remoteFilePath := "/path/to/remote/file.txt"
//     remoteUser := "your-remote-user"
//     remoteHost := "your-remote-host"

//     err := transferFileToRemote(localFilePath, remoteFilePath, remoteUser, remoteHost)
//     if err != nil {
//         fmt.Printf("Failed to transfer file: %s\n", err)
//     }
// }
