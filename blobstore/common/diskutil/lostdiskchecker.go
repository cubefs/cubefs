// Copyright 2022 The CubeFS Authors.
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

//go:build linux
// +build linux

package diskutil

import (
	"errors"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/deniswernert/go-fstab"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

func getMountPoint(path string) (mountPath string, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	mounts, err := fstab.ParseSystem()
	if err != nil {
		return "", err
	}
	for {
		for _, mount := range mounts {
			if path == mount.File {
				return mount.File, nil
			}
		}
		if path == "/" {
			break
		}
		path = filepath.Dir(path)
	}
	return "", errors.New("not found mount point")
}

func lsblkByMountPoint(mountPoint string) (exist bool, err error) {
	cmd := exec.Command("lsblk", "-o", "MOUNTPOINT", "-r")
	out, err := cmd.Output()
	if err != nil {
		return true, err
	}
	outStr := string(out)
	lines := strings.Split(outStr, "\n")
	if len(lines) == 0 {
		return true, errors.New("no lsblk info")
	}

	if lines[0] != "MOUNTPOINT" {
		return true, errors.New("parse lsblk info error!")
	}
	lines = lines[1:]
	for _, line := range lines {
		if line == mountPoint {
			return true, nil
		}
	}
	return false, nil
}

func IsLostDisk(path string) bool {
	mountPath, err := getMountPoint(path)
	if err != nil {
		log.Error(err)
		return false
	}
	exist, err := lsblkByMountPoint(mountPath)
	if err == nil && !exist {
		return true
	}
	log.Error(err)
	return false
}
