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

import "syscall"

// punch hole physize, and keep size
func PunchHole(fd uintptr, offset int64, size int64) error {
	err := Fallocate(fd, FALLOC_FL_KEEP_SIZE|FALLOC_FL_PUNCH_HOLE, offset, size)
	if err == syscall.ENOSYS || err == syscall.EOPNOTSUPP {
		return syscall.EPERM
	}
	return err
}

// pre allocate phy space, and scale size
func PreAllocate(fd uintptr, offset int64, size int64) error {
	return Fallocate(fd, FALLOC_FL_DEFAULT, offset, size)
}

func Fstat(fd uintptr, stat *syscall.Stat_t) error {
	return syscall.Fstat(int(fd), stat)
}
