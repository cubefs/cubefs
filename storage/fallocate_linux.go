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

package storage

import (
	"syscall"

	"github.com/cubefs/cubefs/util"
)

func fallocate(fd int, mode uint32, off int64, len int64) (err error) {
	var tryCnt int
	for {
		err = syscall.Fallocate(fd, mode, off, len)
		if err == syscall.EINTR {
			tryCnt++
			if tryCnt >= util.SyscallTryMaxTimes {
				return
			}
			continue
		}
		return
	}
}
