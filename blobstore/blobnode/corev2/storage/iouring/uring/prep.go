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

func PrepRW(op IoUringOp, sqe *IoUringSqe, fd int,
	addr unsafe.Pointer, len int, offset uint64) {
	sqe.Opcode = op
	sqe.Flags = 0
	sqe.IoPrio = 0
	sqe.Fd = int32(fd)
	sqe.IoUringSqe_Union1 = IoUringSqe_Union1(offset)                    // union1
	sqe.IoUringSqe_Union2 = *(*IoUringSqe_Union2)(unsafe.Pointer(&addr)) // union2
	sqe.Len = uint32(len)
	sqe.IoUringSqe_Union3 = 0 // sqe.SetOpFlags(0) // union3
	sqe.IoUringSqe_Union4 = 0 // sqe.SetBufIndex(0) // union4
	sqe.Personality = 0
	sqe.IoUringSqe_Union5 = 0 // sqe.SetFileIndex(0) // union5
	sqe.Addr3 = 0
	sqe.__pad2[0] = 0
}

func PrepNop(sqe *IoUringSqe) {
	PrepRW(IORING_OP_NOP, sqe, -1, nil, 0, 0)
}

func PrepTimeout(sqe *IoUringSqe, ts *syscall.Timespec, count uint32, flags uint32) {
	PrepRW(IORING_OP_TIMEOUT, sqe, -1, unsafe.Pointer(ts), 1, uint64(count))
	sqe.SetTimeoutFlags(flags)
}

func PrepTimeoutRemove(sqe *IoUringSqe, userDaata uint64, flags uint32) {
	PrepRW(IORING_OP_TIMEOUT_REMOVE, sqe, -1, nil, 0, 0)
	sqe.SetAddr_Value(userDaata)
	sqe.SetTimeoutFlags(flags)
}

func PrepTimeoutUpdate(sqe *IoUringSqe, ts *syscall.Timespec, userData uint64, flags uint32) {
	PrepRW(IORING_OP_TIMEOUT_REMOVE, sqe, -1, nil, 0, 0)
	sqe.SetAddr_Value(userData)
	sqe.SetTimeoutFlags(flags | IORING_TIMEOUT_UPDATE)
}

// ** "Syscall" OP

func PrepRead(sqe *IoUringSqe, fd int, buf *byte, nb int, offset uint64) {
	PrepRW(IORING_OP_READ, sqe, fd, unsafe.Pointer(buf), nb, offset)
}
func PrepReadFixed(sqe *IoUringSqe, fd int, buf *byte, nb int, offset uint64, bufIndex uint16) {
	addr := unsafe.Pointer(buf)
	sqe.Opcode = IORING_OP_READ_FIXED
	sqe.Flags = 0
	sqe.IoPrio = 0
	sqe.Fd = int32(fd)
	sqe.IoUringSqe_Union1 = IoUringSqe_Union1(offset)                    // union1
	sqe.IoUringSqe_Union2 = *(*IoUringSqe_Union2)(unsafe.Pointer(&addr)) // union2
	sqe.Len = uint32(nb)
	sqe.IoUringSqe_Union3 = 0                           // sqe.SetOpFlags(0) // union3
	sqe.IoUringSqe_Union4 = IoUringSqe_Union4(bufIndex) // sqe.SetBufIndex(0) // union4
	sqe.Personality = 0
	sqe.IoUringSqe_Union5 = 0 // sqe.SetFileIndex(0) // union5
	sqe.Addr3 = 0
	sqe.__pad2[0] = 0
}
func PrepReadv(sqe *IoUringSqe, fd int,
	iov *syscall.Iovec, nrVecs int,
	offset uint64) {
	PrepRW(IORING_OP_READV, sqe, fd, unsafe.Pointer(iov), nrVecs, offset)
}
func PrepReadv2(sqe *IoUringSqe, fd int,
	iov *syscall.Iovec, nrVecs int,
	offset uint64, flags uint32) {
	PrepReadv(sqe, fd, iov, nrVecs, offset)
	sqe.SetRwFlags(flags)
}

func PrepWrite(sqe *IoUringSqe, fd int, buf *byte, nb int, offset uint64) {
	PrepRW(IORING_OP_WRITE, sqe, fd, unsafe.Pointer(buf), nb, offset)
}
func PrepWriteFixed(sqe *IoUringSqe, fd int, buf *byte, nb int, offset uint64, bufIndex uint16) {
	addr := unsafe.Pointer(buf)
	sqe.Opcode = IORING_OP_WRITE_FIXED
	sqe.Flags = 0
	sqe.IoPrio = 0
	sqe.Fd = int32(fd)
	sqe.IoUringSqe_Union1 = IoUringSqe_Union1(offset)                    // union1
	sqe.IoUringSqe_Union2 = *(*IoUringSqe_Union2)(unsafe.Pointer(&addr)) // union2
	sqe.Len = uint32(nb)
	sqe.IoUringSqe_Union3 = 0                           // sqe.SetOpFlags(0) // union3
	sqe.IoUringSqe_Union4 = IoUringSqe_Union4(bufIndex) // sqe.SetBufIndex(0) // union4
	sqe.Personality = 0
	sqe.IoUringSqe_Union5 = 0 // sqe.SetFileIndex(0) // union5
	sqe.Addr3 = 0
	sqe.__pad2[0] = 0
}
func PrepWritev(sqe *IoUringSqe, fd int,
	iov *syscall.Iovec, nrVecs int,
	offset uint64) {
	PrepRW(IORING_OP_WRITEV, sqe, fd, unsafe.Pointer(iov), nrVecs, offset)
}
func PrepWritev2(sqe *IoUringSqe, fd int,
	iov *syscall.Iovec, nrVecs int,
	offset uint64, flags uint32) {
	PrepWritev(sqe, fd, iov, nrVecs, offset)
	sqe.SetRwFlags(flags)
}

func PrepAccept(sqe *IoUringSqe, fd int, rsa *syscall.RawSockaddrAny, rsaSz *uintptr, flags uint) {
	// *rsaSz = syscall.SizeofSockaddrAny // leave this out to caller?
	PrepRW(IORING_OP_ACCEPT, sqe, fd, unsafe.Pointer(rsa), 0, uint64(uintptr(unsafe.Pointer(rsaSz))))
	sqe.SetAcceptFlags(uint32(flags))
}

func PrepClose(sqe *IoUringSqe, fd int) {
	PrepRW(IORING_OP_CLOSE, sqe, fd, nil, 0, 0)
}

func PrepRecvmsg(sqe *IoUringSqe, fd int, msg *syscall.Msghdr, flags uint) {
	PrepRW(IORING_OP_RECVMSG, sqe, fd, unsafe.Pointer(msg), 1, 0)
	sqe.SetMsgFlags(uint32(flags))
}

func PrepSendmsg(sqe *IoUringSqe, fd int, msg *syscall.Msghdr, flags uint) {
	PrepRW(IORING_OP_SENDMSG, sqe, fd, unsafe.Pointer(msg), 1, 0)
	sqe.SetMsgFlags(uint32(flags))
}

// ** Multishot

func PrepMultishotAccept(sqe *IoUringSqe, fd int, rsa *syscall.RawSockaddrAny, rsaSz *uintptr, flags uint) {
	PrepAccept(sqe, fd, rsa, rsaSz, flags)
	sqe.IoPrio |= IORING_ACCEPT_MULTISHOT
}
