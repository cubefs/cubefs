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

	fmt.Println("文件夹复制完成！")
	return nil
}

// func main() {

// }
