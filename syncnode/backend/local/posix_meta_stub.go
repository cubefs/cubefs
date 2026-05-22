// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//go:build !linux && !darwin

// Non-unix stub: keeps the API surface compiling on Windows / plan9 etc.
// but reports posix-meta as unsupported so callers can degrade. syncnode
// runs production on Linux, but we keep these stubs so cross-compilation
// (`GOOS=windows go build ./...`) continues to work for dev tooling.
package local

import (
	"errors"
	"os"
)

const posixMetaSupported = false

var errPosixMetaUnsupported = errors.New("posix metadata not supported on this platform")

func readPosixMeta(path string) (mode uint32, uid uint32, gid uint32, xattrs map[string][]byte, err error) {
	info, statErr := os.Lstat(path)
	if statErr != nil {
		return 0, 0, 0, nil, statErr
	}
	// Provide a best-effort mode from os.FileInfo; uid/gid/xattrs stay zero.
	return uint32(info.Mode().Perm()), 0, 0, nil, nil
}

func applyPosixMeta(_ string, mode *uint32, uid *uint32, gid *uint32, xattrs map[string][]byte) error {
	if mode == nil && uid == nil && gid == nil && len(xattrs) == 0 {
		return nil
	}
	return errPosixMetaUnsupported
}
