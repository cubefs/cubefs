package diskutil

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetMountPoint(t *testing.T) {
	if jenkinsTest() {
		return
	}
	mountPath, err := getMountPoint("/")
	require.NoError(t, err)
	require.Equal(t, mountPath, "/")
}

func TestLsblkByMountPoint(t *testing.T) {
	if jenkinsTest() {
		return
	}
	_, err := lsblkByMountPoint("/")
	require.NoError(t, err)
}

func backupFstab() error {
	cmd := exec.Command("cp", "/etc/fstab", "/etc/fstab.bak.98738712")
	_, err := cmd.Output()
	return err
}

func jenkinsTest() bool {
	return os.Getenv("JENKINS_TEST") != ""
}

func addMountRecord(mountPoint string) error {
	file, err := os.OpenFile("/etc/fstab", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	defer file.Close()
	write := bufio.NewWriter(file)
	mountStr := fmt.Sprintf("UUID=0fc6eb07-f31c-4c17-af1e-093b2425097b %s ext4       defaults        1 2\n", mountPoint)
	_, err = write.WriteString(mountStr)
	if err != nil {
		return err
	}
	return write.Flush()
}

func restoreFstab() error {
	cmd := exec.Command("mv", "/etc/fstab.bak.98738712", "/etc/fstab")
	_, err := cmd.Output()
	return err
}

func TestIsLostDisk(t *testing.T) {
	if jenkinsTest() {
		return
	}
	lost := IsLostDisk("/")
	require.Equal(t, lost, false)

	// Fault injection
	err := backupFstab()
	require.NoError(t, err)
	defer restoreFstab()
	mountPoint := "/TestIsLostDisk"
	err = addMountRecord(mountPoint)
	require.NoError(t, err)

	lost = IsLostDisk(mountPoint)
	require.Equal(t, lost, true)
}
