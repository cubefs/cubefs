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

package fuse

import (
	"context"
	"fmt"
	"os"
)

// Server is an interface for any type that knows how to serve ops read from a
// connection.
type Server interface {
	// Read and serve ops from the supplied connection until EOF. Do not return
	// until all operations have been responded to. Must not be called more than
	// once.
	ServeOps(*Connection)
}

// Mount attempts to mount a file system on the given directory, using the
// supplied Server to serve connection requests. It blocks until the file
// system is successfully mounted.
func Mount(
	dir string,
	server Server,
	config *MountConfig) (mfs *MountedFileSystem, err error) {
	// Sanity check: make sure the mount point exists and is a directory. This
	// saves us from some confusing errors later on OS X.
	fi, err := os.Stat(dir)
	switch {
	case os.IsNotExist(err):
		return

	case err != nil:
		err = fmt.Errorf("Statting mount point: %v", err)
		return

	case !fi.IsDir():
		err = fmt.Errorf("Mount point %s is not a directory", dir)
		return
	}

	// Initialize the struct.
	mfs = &MountedFileSystem{
		dir:                 dir,
		joinStatusAvailable: make(chan struct{}),
	}

	// Begin the mounting process, which will continue in the background.
	ready := make(chan error, 1)
	dev, err := mount(dir, config, ready)
	if err != nil {
		err = fmt.Errorf("mount: %v", err)
		return
	}

	// Choose a parent context for ops.
	cfgCopy := *config
	if cfgCopy.OpContext == nil {
		cfgCopy.OpContext = context.Background()
	}

	// Create a Connection object wrapping the device.
	connection, err := newConnection(
		cfgCopy,
		config.DebugLogger,
		config.ErrorLogger,
		dev)

	if err != nil {
		err = fmt.Errorf("newConnection: %v", err)
		return
	}

	// Serve the connection in the background. When done, set the join status.
	go func() {
		server.ServeOps(connection)
		mfs.joinStatus = connection.close()
		close(mfs.joinStatusAvailable)
	}()

	// Wait for the mount process to complete.
	if err = <-ready; err != nil {
		err = fmt.Errorf("mount (background): %v", err)
		return
	}

	return
}
