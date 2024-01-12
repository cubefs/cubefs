package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

func startALLFromDockerCompose(disk string) error {
	scriptPath := CubeFSPath + "/docker/run_docker.sh"
	args := []string{"-r", "-d", disk}
	err := os.Chmod(scriptPath, 0o700)
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
	err := os.Chdir(CubeFSPath + "/docker")
	if err != nil {
		return err
	}

	cmd := exec.Command("docker-compose", "down")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

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
