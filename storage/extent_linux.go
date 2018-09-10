// Copyright 2018 The Containerfs Authors.
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
)

const (
	FALLOC_FL_KEEP_SIZE  = 1
	FALLOC_FL_PUNCH_HOLE = 2
)

func (e *fsExtent) tryKeepSize(fd int, off int64, len int64) (err error) {
	err = syscall.Fallocate(fd, FALLOC_FL_KEEP_SIZE, off, len)
	return
}

func (e *fsExtent) tryPunchHole(fd int, off int64, len int64) (err error) {
	err = syscall.Fallocate(fd, FALLOC_FL_PUNCH_HOLE, off, len)
	return
}
