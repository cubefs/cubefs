package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

func startALLFromDockerCompose(disk string) error {
	scriptPath := "../docker/run_docker.sh"
	args := []string{"-r", "-d", disk}
	err := os.Chmod(scriptPath, 0700) // 添加可执行权限
	if err != nil {
		log.Fatal(err)
	}
	err = runScript(scriptPath, args...)

	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func stopALLFromDockerCompose() error {
	//切换到xxx目录下执行down命令
	// 切换到指定目录
	err := os.Chdir("../docker")
	if err != nil {
		return err
	}

	// 执行 down 命令
	cmd := exec.Command("docker-compose", "down")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

// docker/run_docker.sh -r -d /data/disk
func runScript(scriptPath string, args ...string) error {
	cmd := exec.Command(scriptPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to run script: %w", err)
	}

	return nil
}
