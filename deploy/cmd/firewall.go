package cmd

import (
	"fmt"
	"log"
	"os/exec"
)

// func openRemotePortFirewall(hostname, username string, privateKeyPath string, port int) error {
// 	key, err := ioutil.ReadFile(privateKeyPath)
// 	if err != nil {
// 		return err
// 	}

// 	signer, err := ssh.ParsePrivateKey(key)
// 	if err != nil {
// 		return err
// 	}

// 	config := &ssh.ClientConfig{
// 		User: username,
// 		Auth: []ssh.AuthMethod{
// 			ssh.PublicKeys(signer),
// 		},
// 		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
// 	}

// 	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", hostname, 22), config)
// 	if err != nil {
// 		return err
// 	}
// 	defer conn.Close()

// 	session, err := conn.NewSession()
// 	if err != nil {
// 		return err
// 	}
// 	defer session.Close()

// 	command := fmt.Sprintf("sudo firewall-cmd --zone=public --add-port=%d/tcp --permanent", port)
// 	err = session.Run(command)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// // }

// func reloadFirewall() {

// 	cmd := exec.Command("firewall-cmd", "--reload")
// 	cmd.Run()
// }

func stopFirewall(nodeUser, node string) {
	cmd := exec.Command("ssh", nodeUser+"@"+node, "systemctl", "stop firewalld")
	err := cmd.Run()
	if err != nil {
		log.Println(err)
	}
	log.Println(cmd)
}

func checkPortStatus(nodeUser, node string, port string) (string, error) {
	cmd := exec.Command("ssh", nodeUser+"@"+node, "firewall-cmd --list-all | grep "+port)
	fmt.Println(cmd)
	_, err := cmd.Output()
	if err != nil {
		return fmt.Sprintf("Port %s %s is closed", node, port), err
	}
	return fmt.Sprintf("Port %s is open", port), nil
}
