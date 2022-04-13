// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package samples

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"sync"

	"github.com/jacobsa/ogletest"
)

var fToolPath = flag.String(
	"mount_sample",
	"",
	"Path to the mount_sample tool. If unset, we will compile it.")

var fDebug = flag.Bool("debug", false, "If true, print fuse debug info.")

// A struct that implements common behavior needed by tests in the samples/
// directory where the file system is mounted by a subprocess. Use it as an
// embedded field in your test fixture, calling its SetUp method from your
// SetUp method after setting the MountType and MountFlags fields.
type SubprocessTest struct {
	// The type of the file system to mount. Must be recognized by mount_sample.
	MountType string

	// Additional flags to be passed to the mount_sample tool.
	MountFlags []string

	// A list of files to pass to mount_sample. The given string flag will be
	// used to pass the file descriptor number.
	MountFiles map[string]*os.File

	// A context object that can be used for long-running operations.
	Ctx context.Context

	// The directory at which the file system is mounted.
	Dir string

	// Anothing non-nil in this slice will be closed by TearDown. The test will
	// fail if closing fails.
	ToClose []io.Closer

	mountSampleErr <-chan error
}

// Mount the file system and initialize the other exported fields of the
// struct. Panics on error.
func (t *SubprocessTest) SetUp(ti *ogletest.TestInfo) {
	err := t.initialize(ti.Ctx)
	if err != nil {
		panic(err)
	}
}

// Private state for getToolPath.
var getToolContents_Contents []byte
var getToolContents_Err error
var getToolContents_Once sync.Once

// Implementation detail of getToolPath.
func getToolContentsImpl() (contents []byte, err error) {
	// Fast path: has the user set the flag?
	if *fToolPath != "" {
		contents, err = ioutil.ReadFile(*fToolPath)
		if err != nil {
			err = fmt.Errorf("Reading mount_sample contents: %v", err)
			return
		}

		return
	}

	// Create a temporary directory into which we will compile the tool.
	tempDir, err := ioutil.TempDir("", "sample_test")
	if err != nil {
		err = fmt.Errorf("TempDir: %v", err)
		return
	}

	toolPath := path.Join(tempDir, "mount_sample")

	// Ensure that we kill the temporary directory when we're finished here.
	defer os.RemoveAll(tempDir)

	// Run "go build".
	cmd := exec.Command(
		"go",
		"build",
		"-o",
		toolPath,
		"github.com/jacobsa/fuse/samples/mount_sample")

	output, err := cmd.CombinedOutput()
	if err != nil {
		err = fmt.Errorf(
			"mount_sample exited with %v, output:\n%s",
			err,
			string(output))

		return
	}

	// Slurp the tool contents.
	contents, err = ioutil.ReadFile(toolPath)
	if err != nil {
		err = fmt.Errorf("ReadFile: %v", err)
		return
	}

	return
}

// Build the mount_sample tool if it has not yet been built for this process.
// Return its contents.
func getToolContents() (contents []byte, err error) {
	// Get hold of the binary contents, if we haven't yet.
	getToolContents_Once.Do(func() {
		getToolContents_Contents, getToolContents_Err = getToolContentsImpl()
	})

	contents, err = getToolContents_Contents, getToolContents_Err
	return
}

func waitForMountSample(
	cmd *exec.Cmd,
	errChan chan<- error,
	stderr *bytes.Buffer) {
	// However we exit, write the error to the channel.
	var err error
	defer func() {
		errChan <- err
	}()

	// Wait for the command.
	err = cmd.Wait()
	if err == nil {
		return
	}

	// Make exit errors nicer.
	if exitErr, ok := err.(*exec.ExitError); ok {
		err = fmt.Errorf(
			"mount_sample exited with %v. Stderr:\n%s",
			exitErr,
			stderr.String())

		return
	}

	err = fmt.Errorf("Waiting for mount_sample: %v", err)
}

func waitForReady(readyReader *os.File, c chan<- struct{}) {
	_, err := readyReader.Read(make([]byte, 1))
	if err != nil {
		log.Printf("Readying from ready pipe: %v", err)
		return
	}

	c <- struct{}{}
}

// Like SetUp, but doens't panic.
func (t *SubprocessTest) initialize(ctx context.Context) (err error) {
	// Initialize the context.
	t.Ctx = ctx

	// Set up a temporary directory.
	t.Dir, err = ioutil.TempDir("", "sample_test")
	if err != nil {
		err = fmt.Errorf("TempDir: %v", err)
		return
	}

	// Build/read the mount_sample tool.
	toolContents, err := getToolContents()
	if err != nil {
		err = fmt.Errorf("getTooltoolContents: %v", err)
		return
	}

	// Create a temporary file to hold the contents of the tool.
	toolFile, err := ioutil.TempFile("", "sample_test")
	if err != nil {
		err = fmt.Errorf("TempFile: %v", err)
		return
	}

	defer toolFile.Close()

	// Ensure that it is deleted when we leave.
	toolPath := toolFile.Name()
	defer os.Remove(toolPath)

	// Write out the tool contents and make them executable.
	if _, err = toolFile.Write(toolContents); err != nil {
		err = fmt.Errorf("toolFile.Write: %v", err)
		return
	}

	if err = toolFile.Chmod(0500); err != nil {
		err = fmt.Errorf("toolFile.Chmod: %v", err)
		return
	}

	// Close the tool file to prevent "text file busy" errors below.
	err = toolFile.Close()
	toolFile = nil
	if err != nil {
		err = fmt.Errorf("toolFile.Close: %v", err)
		return
	}

	// Set up basic args for the subprocess.
	args := []string{
		"--type",
		t.MountType,
		"--mount_point",
		t.Dir,
	}

	args = append(args, t.MountFlags...)

	// Set up a pipe for the "ready" status.
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		err = fmt.Errorf("Pipe: %v", err)
		return
	}

	defer readyReader.Close()
	defer readyWriter.Close()

	t.MountFiles["ready_file"] = readyWriter

	// Set up inherited files and appropriate flags.
	var extraFiles []*os.File
	for flag, file := range t.MountFiles {
		// Cf. os/exec.Cmd.ExtraFiles
		fd := 3 + len(extraFiles)

		extraFiles = append(extraFiles, file)
		args = append(args, "--"+flag)
		args = append(args, fmt.Sprintf("%d", fd))
	}

	// Set up a command.
	var stderr bytes.Buffer
	mountCmd := exec.Command(toolPath, args...)
	mountCmd.Stderr = &stderr
	mountCmd.ExtraFiles = extraFiles

	// Handle debug mode.
	if *fDebug {
		mountCmd.Stderr = os.Stderr
		mountCmd.Args = append(mountCmd.Args, "--debug")
	}

	// Start the command.
	if err = mountCmd.Start(); err != nil {
		err = fmt.Errorf("mountCmd.Start: %v", err)
		return
	}

	// Launch a goroutine that waits for it and returns its status.
	mountSampleErr := make(chan error, 1)
	go waitForMountSample(mountCmd, mountSampleErr, &stderr)

	// Wait for the tool to say the file system is ready. In parallel, watch for
	// the tool to fail.
	readyChan := make(chan struct{}, 1)
	go waitForReady(readyReader, readyChan)

	select {
	case <-readyChan:
	case err = <-mountSampleErr:
		return
	}

	// TearDown is no responsible for joining.
	t.mountSampleErr = mountSampleErr

	return
}

// Unmount the file system and clean up. Panics on error.
func (t *SubprocessTest) TearDown() {
	err := t.destroy()
	if err != nil {
		panic(err)
	}
}

// Like TearDown, but doesn't panic.
func (t *SubprocessTest) destroy() (err error) {
	// Make sure we clean up after ourselves after everything else below.

	// Close what is necessary.
	for _, c := range t.ToClose {
		if c == nil {
			continue
		}

		ogletest.ExpectEq(nil, c.Close())
	}

	// If we didn't try to mount the file system, there's nothing further to do.
	if t.mountSampleErr == nil {
		return
	}

	// In the background, initiate an unmount.
	unmountErrChan := make(chan error)
	go func() {
		unmountErrChan <- unmount(t.Dir)
	}()

	// Make sure we wait for the unmount, even if we've already returned early in
	// error. Return its error if we haven't seen any other error.
	defer func() {
		// Wait.
		unmountErr := <-unmountErrChan
		if unmountErr != nil {
			if err != nil {
				log.Println("unmount:", unmountErr)
				return
			}

			err = fmt.Errorf("unmount: %v", unmountErr)
			return
		}

		// Clean up.
		ogletest.ExpectEq(nil, os.Remove(t.Dir))
	}()

	// Wait for the subprocess.
	if err = <-t.mountSampleErr; err != nil {
		return
	}

	return
}
