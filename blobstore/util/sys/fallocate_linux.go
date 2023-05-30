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

package sys

/*
#define _GNU_SOURCE
#include <fcntl.h>
#include <linux/falloc.h>
*/
import "C"

import "syscall"

const (
	FALLOC_FL_DEFAULT    = uint32(0x0) /* default is extend size */
	FALLOC_FL_KEEP_SIZE  = uint32(C.FALLOC_FL_KEEP_SIZE)
	FALLOC_FL_PUNCH_HOLE = uint32(C.FALLOC_FL_PUNCH_HOLE)
)

func Fallocate(fd uintptr, mode uint32, off int64, size int64) error {
	return syscall.Fallocate(int(fd), mode, off, size)
}
