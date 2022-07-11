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

package sys

import (
	"errors"
	"os"
)

var (
	ErrDiskNotFound = errors.New("sys: disk not found")
	ErrPathInvalid  = errors.New("sys: path invalid")
)

var fsTypes = map[string]string{
	"137d":     "EXT",
	"ef53":     "EXT4",
	"58465342": "XFS",
	"2fc12fc1": "zfs",
	"1021994":  "TMPFS",
}

type DiskInfo struct {
	Total  uint64 // total size of the disk
	Free   uint64 // free size of the disk
	Files  uint64 // total inodes available
	Ffree  uint64 // free inodes available
	FSType string // file system type
}

func GetDiskInfo(diskPath string) (di DiskInfo, err error) {
	if len(diskPath) == 0 {
		return di, ErrPathInvalid
	}
	di, err = GetInfo(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrDiskNotFound
		}
	}
	return di, err
}
