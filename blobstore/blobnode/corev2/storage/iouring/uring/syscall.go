// Copyright 2024 The CubeFS Authors.
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

package uring

import (
	"syscall"
	"unsafe"
)

func io_uring_setup(entries uintptr, params *IoUringParams) (ret int, err error) {
	r1, _, e1 := syscall.Syscall(SYS_IO_URING_SETUP, entries, uintptr(unsafe.Pointer(params)), 0)
	ret = int(r1)
	if e1 < 0 {
		err = e1
	}
	return
}

func io_uring_enter(fd int, toSubmit uint32, minComplete uint32, flags uint32, sig *Sigset_t) (ret int, err error) {
	return io_uring_enter2(fd, toSubmit, minComplete, flags, sig, NSIG/8)
}

// TODO: decide to use Syscall or RawSyscall

func io_uring_enter2(fd int, toSubmit uint32, minComplete uint32, flags uint32, sig *Sigset_t, sz int32) (ret int, err error) {
	r1, _, e1 := syscall.RawSyscall6(SYS_IO_URING_ENTER,
		//r1, _, e1 := syscall.Syscall6(SYS_IO_URING_ENTER,
		uintptr(fd),
		uintptr(toSubmit), uintptr(minComplete),
		uintptr(flags), uintptr(unsafe.Pointer(sig)), uintptr(sz))
	ret = int(r1)
	if e1 != 0 {
		err = e1
	}
	return
}

func io_uring_register(fd int, opcode uint32, arg unsafe.Pointer, nrArgs uintptr) (ret int, err error) {
	r1, _, e1 := syscall.Syscall6(SYS_IO_URING_REGISTER, uintptr(fd), uintptr(opcode), uintptr(arg), uintptr(nrArgs), 0, 0)
	ret = int(r1)
	if e1 != 0 {
		err = e1
	}
	return
}

//go:linkname mmap syscall.mmap
func mmap(addr unsafe.Pointer, length uintptr, prot int, flags int, fd int, offset int64) (xaddr unsafe.Pointer, err error)

//go:linkname munmap syscall.munmap
func munmap(addr unsafe.Pointer, length uintptr) (err error)

//

func increase_rlimit_nofile(nr uint64) error {
	var rlim syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
	if err != nil {
		return err
	}
	if rlim.Cur < nr {
		rlim.Cur += nr
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
	}
	return err
}
