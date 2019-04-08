package fuse

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"
)

// Begin the process of mounting at the given directory, returning a connection
// to the kernel. Mounting continues in the background, and is complete when an
// error is written to the supplied channel. The file system may need to
// service the connection in order for mounting to complete.
func mount(
	dir string,
	cfg *MountConfig,
	ready chan<- error) (dev *os.File, err error) {
	// On linux, mounting is never delayed.
	ready <- nil

	// Create a socket pair.
	fds, err := syscall.Socketpair(syscall.AF_FILE, syscall.SOCK_STREAM, 0)
	if err != nil {
		err = fmt.Errorf("Socketpair: %v", err)
		return
	}

	// Wrap the sockets into os.File objects that we will pass off to fusermount.
	writeFile := os.NewFile(uintptr(fds[0]), "fusermount-child-writes")
	defer writeFile.Close()

	readFile := os.NewFile(uintptr(fds[1]), "fusermount-parent-reads")
	defer readFile.Close()

	// Start fusermount, passing it a buffer in which to write stderr.
	var stderr bytes.Buffer

	cmd := exec.Command(
		"fusermount",
		"-o", cfg.toOptionsString(),
		"--",
		dir,
	)

	cmd.Env = append(os.Environ(), "_FUSE_COMMFD=3")
	cmd.ExtraFiles = []*os.File{writeFile}
	cmd.Stderr = &stderr

	// Run the command.
	err = cmd.Run()
	if err != nil {
		err = fmt.Errorf("running fusermount: %v\n\nstderr:\n%s", err, stderr.Bytes())
		return
	}

	// Wrap the socket file in a connection.
	c, err := net.FileConn(readFile)
	if err != nil {
		err = fmt.Errorf("FileConn: %v", err)
		return
	}
	defer c.Close()

	// We expect to have a Unix domain socket.
	uc, ok := c.(*net.UnixConn)
	if !ok {
		err = fmt.Errorf("Expected UnixConn, got %T", c)
		return
	}

	// Read a message.
	buf := make([]byte, 32) // expect 1 byte
	oob := make([]byte, 32) // expect 24 bytes
	_, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		err = fmt.Errorf("ReadMsgUnix: %v", err)
		return
	}

	// Parse the message.
	scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		err = fmt.Errorf("ParseSocketControlMessage: %v", err)
		return
	}

	// We expect one message.
	if len(scms) != 1 {
		err = fmt.Errorf("expected 1 SocketControlMessage; got scms = %#v", scms)
		return
	}

	scm := scms[0]

	// Pull out the FD returned by fusermount
	gotFds, err := syscall.ParseUnixRights(&scm)
	if err != nil {
		err = fmt.Errorf("syscall.ParseUnixRights: %v", err)
		return
	}

	if len(gotFds) != 1 {
		err = fmt.Errorf("wanted 1 fd; got %#v", gotFds)
		return
	}

	// Turn the FD into an os.File.
	dev = os.NewFile(uintptr(gotFds[0]), "/dev/fuse")

	return
}
