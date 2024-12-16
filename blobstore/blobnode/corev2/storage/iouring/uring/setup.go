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

// func io_uring_queue_init(entries uint32, ring *IoUring, flags uint32) error {
// 	p := new(IoUringParams)
// 	p.Flags = flags
// 	return io_uring_queue_init_params(entries, ring, p)
// }

func io_uring_queue_init_params(entries uint32, ring *IoUring, p *IoUringParams) error {
	fd, err := io_uring_setup(uintptr(entries), p)
	if err != nil {
		return err
	}
	err = io_uring_queue_mmap(fd, p, ring)
	if err != nil {
		return err
	}
	ring.Features = p.Features
	return nil
}

func (ring *IoUring) io_uring_queue_exit() {
	sq := &ring.Sq
	cq := &ring.Cq
	sqeSize := SizeofIoUringSqe
	if ring.Flags&IORING_SETUP_SQE128 != 0 {
		sqeSize += 64
	}
	munmap(unsafe.Pointer(sq.Sqes), sqeSize*uintptr(*sq._RingEntries()))
	io_uring_unmap_rings(sq, cq)
	/*
	 * Not strictly required, but frees up the slot we used now rather
	 * than at process exit time.
	 */
	if ring.IntFlags&INT_FLAG_REG_RING != 0 {
		ring.io_uring_unregister_ring_fd()
	}
	syscall.Close(int(ring.RingFd))
}

func io_uring_queue_mmap(fd int, p *IoUringParams, ring *IoUring) error {
	err := io_uring_mmap(fd, p, &ring.Sq, &ring.Cq)
	if err != nil {
		return err
	}
	ring.Flags = p.Flags
	ring.RingFd, ring.EnterRingFd = fd, fd
	ring.IntFlags = 0
	return nil
}

func io_uring_mmap(fd int, p *IoUringParams, sq *IoUringSq, cq *IoUringCq) (err error) {
	size := SizeofIoUringCqe
	if p.Flags&IORING_SETUP_CQE32 != 0 {
		size += SizeofIoUringCqe
	}

	sq.RingSz = p.SqOff.Array + p.SqEntries*uint32(SizeofUnsigned)
	cq.RingSz = p.CqOff.Cqes + p.CqEntries*uint32(size)

	if p.Features&IORING_FEAT_SINGLE_MMAP != 0 {
		if cq.RingSz > sq.RingSz {
			sq.RingSz = cq.RingSz
		}
		// cq.RingSz = sq.RingSz
	}
	// alloc sq ring
	sq.RingPtr, err = mmap(nil, uintptr(sq.RingSz),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE,
		fd, IORING_OFF_SQ_RING)
	if err != nil {
		return
	}

	if p.Features&IORING_FEAT_SINGLE_MMAP != 0 {
		cq.RingPtr = sq.RingPtr
	} else {
		// alloc cq ring
		cq.RingPtr, err = mmap(nil, uintptr(cq.RingSz),
			syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_SHARED|syscall.MAP_POPULATE,
			fd, IORING_OFF_CQ_RING)
		if err != nil {
			// goto errLabel
			io_uring_unmap_rings(sq, cq)
			return
		}
	}

	//sq
	sq.head = (unsafe.Pointer(uintptr(sq.RingPtr) + uintptr(p.SqOff.Head)))
	sq.tail = (unsafe.Pointer(uintptr(sq.RingPtr) + uintptr(p.SqOff.Tail)))
	sq.ringMask = (unsafe.Pointer(uintptr(sq.RingPtr) + uintptr(p.SqOff.RingMask)))
	sq.ringEntries = (unsafe.Pointer(uintptr(sq.RingPtr) + uintptr(p.SqOff.RingEntries)))
	sq.flags = (unsafe.Pointer(uintptr(sq.RingPtr) + uintptr(p.SqOff.Flags)))
	sq.dropped = (unsafe.Pointer(uintptr(sq.RingPtr) + uintptr(p.SqOff.Dropped)))
	sq.Array = (uint32Array)(unsafe.Pointer(uintptr(sq.RingPtr) + uintptr(p.SqOff.Array)))

	size = SizeofIoUringSqe
	if p.Flags&IORING_SETUP_SQE128 != 0 {
		size += 64
	}
	var sqeAddr unsafe.Pointer
	sqeAddr, err = mmap(nil, size*uintptr(p.SqEntries),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE,
		fd, IORING_OFF_SQES)
	if err != nil {
		//errLabel:
		io_uring_unmap_rings(sq, cq)
		return
	}
	sq.Sqes = (ioUringSqeArray)(sqeAddr)

	//cq
	cq.head = (unsafe.Pointer(uintptr(cq.RingPtr) + uintptr(p.CqOff.Head)))
	cq.tail = (unsafe.Pointer(uintptr(cq.RingPtr) + uintptr(p.CqOff.Tail)))
	cq.ringMask = (unsafe.Pointer(uintptr(cq.RingPtr) + uintptr(p.CqOff.RingMask)))
	cq.ringEntries = (unsafe.Pointer(uintptr(cq.RingPtr) + uintptr(p.CqOff.RingEntries)))
	cq.overflow = (unsafe.Pointer(uintptr(cq.RingPtr) + uintptr(p.CqOff.Overflow)))
	cq.Cqes = (ioUringCqeArray)(unsafe.Pointer(uintptr(cq.RingPtr) + uintptr(p.CqOff.Cqes)))
	if p.CqOff.Flags != 0 {
		cq.flags = (unsafe.Pointer(uintptr(cq.RingPtr) + uintptr(p.CqOff.Flags)))
	}
	return nil
}

func io_uring_unmap_rings(sq *IoUringSq, cq *IoUringCq) error {
	munmap(sq.RingPtr, uintptr(sq.RingSz))
	if cq.RingPtr != nil && cq.RingPtr != sq.RingPtr {
		munmap(cq.RingPtr, uintptr(cq.RingSz))
	}
	return nil
}
