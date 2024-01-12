package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

// Check if the private and public key files already exist and generate an SSH key pair
func generateSSHKey() error {
	// Check if the private and public key files already exist
	privateKeyPath := os.Getenv("HOME") + "/.ssh/id_rsa"
	// publicKeyPath := privateKeyPath + ".pub"
	if _, err := os.Stat(privateKeyPath); err == nil {
		return fmt.Errorf("SSH key already exists")
	}

	// Generate SSH key pairs
	cmd := exec.Command("ssh-keygen", "-t", "rsa", "-N", "", "-f", privateKeyPath)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to generate SSH key: %v", err)
	}

	log.Printf("SSH key generated successfully.\n")

	return nil
}

// Establishing a secure connection
func establishSSHConnectionWithoutPassword(sourceNode, targetNodeUser, targetNode string) error {
	// Check if the private and public key files already exist and generate an SSH key pairs
	generateSSHKey()

	// Check if it is possible to connect to the target node without a password
	cmd := exec.Command("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", targetNodeUser+"@"+targetNode, "echo", "connection successful")
	err := cmd.Run()
	if err != nil {
		// Copy public key to remote host
		privateKeyPath := os.Getenv("HOME") + "/.ssh/id_rsa"
		publicKeyPath := privateKeyPath + ".pub"
		cmd = exec.Command("ssh-copy-id", "-i", publicKeyPath, targetNodeUser+"@"+targetNode)
		err = cmd.Run()
		if err != nil {
			return fmt.Errorf("failed to establish passwordless SSH connection with %s@%s: %v", targetNodeUser, targetNode, err)
		}
	}
	log.Printf("Passwordless SSH connection is established with %s@%s.\n", targetNodeUser, targetNode)

	return nil
}
