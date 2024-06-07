// Copyright 2018 The CubeFS Authors.
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

package tmpfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

const (
	TmpfsMagic = 0x1021994
)

func IsMountPoint(path string) (isMountPoint bool, err error) {
	parrentPath := filepath.Dir(path)
	if parrentPath == path {
		return true, nil
	}
	parrentDev, err := getDevName(parrentPath)
	if err != nil {
		return
	}
	dev, err := getDevName(path)
	if err != nil {
		return
	}
	if parrentDev != dev {
		return true, nil
	}
	return false, nil
}

func getDevName(path string) (dev uint64, err error) {
	if path == "" {
		return 0, fmt.Errorf("empty path")
	}
	statFS := &syscall.Stat_t{}
	err = syscall.Stat(path, statFS)
	if err != nil {
		return 0, err
	}
	return statFS.Dev, nil
}

func MountTmpfs(path string, size int64) error {
	if size < 0 {
		panic("MountTmpfs: size < 0")
	}
	var flags uintptr
	flags = syscall.MS_NOATIME | syscall.MS_SILENT
	flags |= syscall.MS_NODEV | syscall.MS_NOEXEC | syscall.MS_NOSUID
	options := ""
	if size >= 0 {
		options = "size=" + strconv.FormatInt(size, 10)
	}
	err := syscall.Mount("tmpfs", path, "tmpfs", flags, options)
	return os.NewSyscallError("mount", err)
}

func Umount(path string) error {
	return syscall.Unmount(path, syscall.MNT_FORCE)
}

func IsTmpfs(path string) bool {
	return isFs(path, TmpfsMagic)
}

func isFs(path string, magic int64) bool {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		panic(os.NewSyscallError("statfs", err))
	}
	return stat.Type == magic
}
