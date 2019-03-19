// Copyright 2018 The tiglabs raft Authors.
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
// +build linux

package wal

import (
	"os"
	"syscall"
)

const (
	fallocateModeDefault  uint32 = 0 // 默认模式下预分配的空间全部补0
	fallocateModeKeepSize uint32 = 1 // 预分配后保持原来的文件大小，不补0
)

func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// 预分配然后补零
func fallocate(f *os.File, sizeInBytes int64) error {
	err := syscall.Fallocate(int(f.Fd()), fallocateModeDefault, 0, sizeInBytes)
	if err != nil {
		errno, ok := err.(syscall.Errno)
		if ok && (errno == syscall.ENOTSUP || errno == syscall.EINTR) {
			return fallocDegraded(f, sizeInBytes)
		}
	}
	return err
}
