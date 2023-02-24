package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"
)

var SignalsIgnored = []os.Signal{
	syscall.SIGUSR1,
	syscall.SIGUSR2,
	syscall.SIGPIPE,
	syscall.SIGALRM,
	syscall.SIGXCPU,
	syscall.SIGXFSZ,
	syscall.SIGVTALRM,
	syscall.SIGPROF,
	syscall.SIGIO,
	syscall.SIGPWR,
}

const (
	pidFileSeparator = ";"
)

func lockPidFile(pidFile string) error {
	var (
		err   error
		data  []byte
		exist bool
	)
	if pidFile == "" {
		return nil
	}
	exist, err = checkFileExist(pidFile)
	if err != nil {
		return fmt.Errorf("Check pidFile %s failed: %v", pidFile, err)
	}
	if !exist {
		return writePidFile(pidFile)
	}
	data, err = ioutil.ReadFile(pidFile)
	if err != nil {
		return fmt.Errorf("Read pidFile %s failed: %v", pidFile, err)
	}
	pidCmd := strings.Split(string(data), pidFileSeparator)
	if len(pidCmd) < 2 {
		return fmt.Errorf("Invalid pidFile %s. Please remove it first", pidFile)
	}
	cmdFile := fmt.Sprintf("/proc/%s/cmdline", pidCmd[0])
	data, err = ioutil.ReadFile(cmdFile)
	if err != nil {
		return writePidFile(pidFile)
	}
	cmd := string(data)
	if strings.HasPrefix(cmd, pidCmd[1]) {
		return fmt.Errorf("pidFile %s has been locked by pid %s", pidFile, pidCmd[0])
	}
	return writePidFile(pidFile)
}

func checkFileExist(file string) (bool, error) {
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func writePidFile(file string) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.WriteString(f, fmt.Sprintf("%d%s%s", os.Getpid(), pidFileSeparator, os.Args[0]))
	return err
}
