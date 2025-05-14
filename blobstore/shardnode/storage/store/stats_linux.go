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

package store

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type Stats struct {
	Total int64
	Used  int64
	Free  int64
}

func StatFS(path string) (info Stats, err error) {
	s := syscall.Statfs_t{}
	err = syscall.Statfs(path, &s)
	if err != nil {
		return Stats{}, err
	}
	reservedBlocks := s.Bfree - s.Bavail
	info = Stats{
		Total: s.Frsize * int64(s.Blocks-reservedBlocks),
		Free:  s.Frsize * int64(s.Bavail),
	}
	info.Used = info.Total - info.Free
	return info, nil
}

func IsMountPoint(path string) bool {
	var stat1, stat2 os.FileInfo

	stat1, err := os.Lstat(path)
	if err != nil {
		return false
	}
	stat2, err = os.Lstat(filepath.Dir(strings.TrimSuffix(path, "/")))
	if err != nil {
		return false
	}
	if stat1.Mode()&os.ModeSymlink != 0 {
		return false
	}

	dev1 := stat1.Sys().(*syscall.Stat_t).Dev
	dev2 := stat2.Sys().(*syscall.Stat_t).Dev

	inode1 := stat1.Sys().(*syscall.Stat_t).Ino
	inode2 := stat2.Sys().(*syscall.Stat_t).Ino

	if dev1 != dev2 || inode1 == inode2 {
		return true
	}

	return false
}
