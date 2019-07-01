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

// Helper code for starting a daemon process.
//
// This package assumes that the user invokes a tool, which invokes a daemon
// process. Though the tool starts the daemon process with stdin, stdout, and
// stderr closed (using Run), the daemon should be able to communicate status
// to the user while it starts up (using StatusWriter), causing the tool to
// exit in success or failure only when it is clear whether the daemon has
// sucessfully started (which it signal using SignalOutcome).
package daemonize

import (
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// The name of an environment variable used to communicate a file descriptor
// set up by Run to the daemon process. Gob encoding is used to communicate
// back to Run.
const envVar = "DAEMONIZE_STATUS_FD"

// A message containing logging output while starting the daemon.
type logMsg struct {
	Msg []byte
}

// A message indicating the outcome of starting the daemon. The receiver
// ignores further messages.
type outcomeMsg struct {
	Successful bool

	// Meaningful only if !Successful.
	ErrorMsg string
}

func init() {
	gob.Register(logMsg{})
	gob.Register(outcomeMsg{})
}

// The file provded to this process via the environment variable, or nil if
// none.
var gFile *os.File

// A gob encoder that writes into gFile, or nil.
var gGobEncoder *gob.Encoder

func init() {
	// Is the environment variable set?
	fdStr, ok := os.LookupEnv(envVar)
	if !ok {
		return
	}

	// Parse the file descriptor.
	fd, err := strconv.ParseUint(fdStr, 10, 32)
	if err != nil {
		log.Fatalf("Couldn't parse %s value %q: %v", envVar, fdStr, err)
	}

	// Set up the file and the encoder that wraps it.
	gFile = os.NewFile(uintptr(fd), envVar)
	gGobEncoder = gob.NewEncoder(gFile)
}

// Send the supplied message as an interface{}, matching the decoder.
func sendMsg(msg interface{}) (err error) {
	err = gGobEncoder.Encode(&msg)
	return
}

// For use by the daemon: signal an outcome back to Run in the invoking tool,
// causing it to return. Do nothing if the process wasn't invoked with Run.
func SignalOutcome(outcome error) (err error) {
	// Is there anything to do?
	if gGobEncoder == nil {
		return
	}

	// Write out the outcome.
	msg := &outcomeMsg{
		Successful: outcome == nil,
	}

	if !msg.Successful {
		msg.ErrorMsg = outcome.Error()
	}

	err = sendMsg(msg)

	return
}

// An io.Writer that sends logMsg messages over gGobEncoder.
type logMsgWriter struct {
}

func (w *logMsgWriter) Write(p []byte) (n int, err error) {
	msg := &logMsg{
		Msg: p,
	}

	err = sendMsg(msg)
	if err != nil {
		return
	}

	n = len(p)
	return
}

// For use by the daemon: the writer that should be used for logging status
// messages while in the process of starting up. The writer must not be written
// to after calling SignalOutcome.
//
// Set to a reasonable default if the process wasn't invoked by a call to Run.
var StatusWriter io.Writer

func init() {
	if gGobEncoder != nil {
		StatusWriter = &logMsgWriter{}
	} else {
		StatusWriter = os.Stderr
	}
}

// Invoke the daemon with the supplied arguments, waiting until it successfully
// starts up or reports that is has failed. Write status updates while starting
// into the supplied writer (which may be nil for silence). Return nil only if
// it starts successfully.
func Run(
	path string,
	args []string,
	env []string,
	status io.Writer) (err error) {
	if status == nil {
		status = ioutil.Discard
	}

	// Set up the pipe that we will hand to the daemon.
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		err = fmt.Errorf("Pipe: %v", err)
		return
	}

	// Attempt to start the daemon process. If we encounter an error in so doing,
	// write it to the channel.
	startProcessErr := make(chan error, 1)
	go func() {
		defer pipeW.Close()
		err := startProcess(path, args, env, pipeW)
		if err != nil {
			startProcessErr <- err
		}
	}()

	// Read communication from the daemon from the pipe, writing nil into the
	// channel only if the startup succeeds.
	readFromProcessOutcome := make(chan error, 1)
	go func() {
		defer pipeR.Close()
		readFromProcessOutcome <- readFromProcess(pipeR, status)
	}()

	// Wait for a result from one of the above.
	select {
	case err = <-startProcessErr:
		err = fmt.Errorf("startProcess: %v", err)
		return

	case err = <-readFromProcessOutcome:
		if err == nil {
			// All is good.
			return
		}

		err = fmt.Errorf("readFromProcess: %v", err)
		return
	}
}

// Start the daemon process, handing it the supplied pipe for communication. Do
// not wait for it to return.
func startProcess(
	path string,
	args []string,
	env []string,
	pipeW *os.File) (err error) {
	cmd := exec.Command(path)
	cmd.Args = append(cmd.Args, args...)
	cmd.Env = append(cmd.Env, env...)
	cmd.ExtraFiles = []*os.File{pipeW}

	// Change working directories so that we don't prevent unmounting of the
	// volume of our current working directory.
	cmd.Dir = "/"

	// Call setsid after forking in order to avoid being killed when the user
	// logs out.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	// Send along the write end of the pipe.
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=3", envVar))

	// Start. Clean up in the background, ignoring errors.
	err = cmd.Start()
	go cmd.Wait()

	return
}

// Process communication from a daemon subprocess. Write log messages to the
// supplied writer (which must be non-nil). Return nil only if the startup
// succeeds.
func readFromProcess(
	r io.Reader,
	status io.Writer) (err error) {
	decoder := gob.NewDecoder(r)

	for {
		// Read a message.
		var msg interface{}
		err = decoder.Decode(&msg)
		if err != nil {
			err = fmt.Errorf("Decode: %v", err)
			return
		}

		// Handle the message.
		switch msg := msg.(type) {
		case logMsg:
			_, err = status.Write(msg.Msg)
			if err != nil {
				err = fmt.Errorf("status.Write: %v", err)
				return
			}

		case outcomeMsg:
			if msg.Successful {
				return
			}

			err = fmt.Errorf("sub-process: %s", msg.ErrorMsg)
			return

		default:
			err = fmt.Errorf("Unhandled message type: %T", msg)
			return
		}
	}
}
