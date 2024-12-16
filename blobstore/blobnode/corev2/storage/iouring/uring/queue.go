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
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const LIBURING_UDATA_TIMEOUT uint64 = ^uint64(0)

/*
 * Returns true if we're not using SQ thread (thus nobody submits but us)
 * or if IORING_SQ_NEED_WAKEUP is set, so submit thread must be explicitly
 * awakened. For the latter case, we set the thread wakeup flag.
 */
func (ring *IoUring) sq_ring_needs_enter(flags *uint32) bool {
	if ring.Flags&IORING_SETUP_SQPOLL == 0 {
		return true
	}

	// FIXME: io_uring_smp_mb

	if atomic.LoadUint32(ring.Sq._Flags())&IORING_SQ_NEED_WAKEUP != 0 {
		*flags |= IORING_ENTER_SQ_WAKEUP
		return true
	}
	return false
}

func (ring *IoUring) cq_ring_needs_flush() bool {
	return atomic.LoadUint32(ring.Sq._Flags())&(IORING_SQ_CQ_OVERFLOW|IORING_SQ_TASKRUN) != 0
}

func (ring *IoUring) cq_ring_needs_enter() bool {
	return (ring.Flags&IORING_SETUP_IOPOLL != 0) || ring.cq_ring_needs_flush()
}

type get_data struct {
	submit   uint32
	waitNr   uint32
	getFlags uint32
	sz       int32
	arg      unsafe.Pointer
}

func (ring *IoUring) _io_uring_get_cqe(cqePtr **IoUringCqe, data *get_data) (err error) {
	var cqe *IoUringCqe
	var looped = false
	var ret int
	for {
		var needEnter = false
		var flags uint32 = 0
		var nrAvail uint32 = 0
		err = ring.__io_uring_peek_cqe(&cqe, &nrAvail)
		if err != nil {
			break
		}
		if cqe != nil && data.waitNr == 0 && data.submit == 0 {
			if looped || !ring.cq_ring_needs_enter() {
				err = syscall.EAGAIN
				break
			}
			needEnter = true
		}
		if data.waitNr > nrAvail || needEnter {
			flags = IORING_ENTER_GETEVENTS | data.getFlags
			needEnter = true
		}
		if data.submit > 0 && ring.sq_ring_needs_enter(&flags) {
			needEnter = true
		}
		if !needEnter {
			break
		}

		if ring.IntFlags&INT_FLAG_REG_RING != 0 {
			flags |= IORING_ENTER_REGISTERED_RING
		}
		ret, err = io_uring_enter2(ring.EnterRingFd, data.submit, data.waitNr, flags, (*Sigset_t)(data.arg), data.sz)
		if err != nil {
			break
		}
		data.submit -= uint32(ret)
		if cqe != nil {
			break
		}
		looped = true
	}

	*cqePtr = cqe
	return
}

func (ring *IoUring) __io_uring_get_cqe(cqePtr **IoUringCqe, submit uint32, waitNr uint32, sigmask *Sigset_t) error {
	data := &get_data{
		submit:   submit,
		waitNr:   waitNr,
		getFlags: 0,
		sz:       NSIG / 8,
		arg:      unsafe.Pointer(sigmask),
	}
	return ring._io_uring_get_cqe(cqePtr, data)
}

/*
 * Fill in an array of IO completions up to count, if any are available.
 * Returns the amount of IO completions filled.
 */
func (ring *IoUring) io_uring_peek_batch_cqe(cqes []*IoUringCqe, count uint32) uint32 {
	var ready uint32
	var overflowChecked = false
	var shift = 0
	if ring.Flags&IORING_SETUP_CQE32 != 0 {
		shift = 1
	}

again:
	ready = ring.io_uring_cq_ready()
	if ready > 0 {
		var head = *ring.Cq._Head()
		var mask = *ring.Cq._RingMask()
		var last uint32
		if count > ready {
			count = ready
		}
		last = head + count
		var i uintptr = 0
		for head != last {
			cqes[i] = ioUringCqeArray_Index(ring.Cq.Cqes, uintptr((head&mask)<<uint32(shift)))
			i++
			head++
		}
		return count
	}

	if overflowChecked {
		goto done
	}

	if ring.cq_ring_needs_flush() {
		var flags uint32 = IORING_ENTER_GETEVENTS
		if ring.IntFlags&INT_FLAG_REG_RING != 0 {
			flags |= IORING_ENTER_REGISTERED_RING
		}
		io_uring_enter(ring.EnterRingFd, 0, 0, flags, nil)
		overflowChecked = true
		goto again
	}

done:
	return 0
}

/*
 * Sync internal state with kernel ring state on the SQ side. Returns the
 * number of pending items in the SQ ring, for the shared ring.
 */
func (ring *IoUring) __io_uring_flush_sq() uint32 {
	sq := &ring.Sq
	var mask = *sq._RingMask()
	var ktail = *sq._Tail()
	var toSubmit = sq.SqeTail - sq.SqeHead

	if toSubmit < 1 {
		goto out
	}

	/*
	 * Fill in sqes that we have queued up, adding them to the kernel ring
	 */
	for ; toSubmit > 0; toSubmit-- {
		*uint32Array_Index(sq.Array, uintptr(ktail&mask)) = sq.SqeHead & mask
		ktail++
		sq.SqeHead++
	}

	/*
	 * Ensure that the kernel sees the SQE updates before it sees the tail
	 * update.
	 */
	atomic.StoreUint32(sq._Tail(), ktail)

out:
	/*
	 * This _may_ look problematic, as we're not supposed to be reading
	 * SQ->head without acquire semantics. When we're in SQPOLL mode, the
	 * kernel submitter could be updating this right now. For non-SQPOLL,
	 * task itself does it, and there's no potential race. But even for
	 * SQPOLL, the load is going to be potentially out-of-date the very
	 * instant it's done, regardless or whether or not it's done
	 * atomically. Worst case, we're going to be over-estimating what
	 * we can submit. The point is, we need to be able to deal with this
	 * situation regardless of any perceived atomicity.
	 */
	return ktail - *sq._Head()
}

/*
 * If we have kernel support for IORING_ENTER_EXT_ARG, then we can use that
 * more efficiently than queueing an internal timeout command.
 */
func (ring *IoUring) io_uring_wait_cqes_new(cqePtr **IoUringCqe, waitNtr uint32, ts *syscall.Timespec, sigmask *Sigset_t) error {
	arg := &IoUringGeteventsArg{
		Sigmask:   uint64(uintptr(unsafe.Pointer(sigmask))),
		SigmaskSz: NSIG / 8,
		Ts:        uint64(uintptr(unsafe.Pointer(ts))),
	}
	data := &get_data{
		waitNr:   waitNtr,
		getFlags: IORING_ENTER_EXT_ARG,
		sz:       int32(unsafe.Sizeof(arg)),
	}
	return ring._io_uring_get_cqe(cqePtr, data)
}

/*
 * Like io_uring_wait_cqe(), except it accepts a timeout value as well. Note
 * that an sqe is used internally to handle the timeout. For kernel doesn't
 * support IORING_FEAT_EXT_ARG, applications using this function must never
 * set sqe->user_data to LIBURING_UDATA_TIMEOUT!
 *
 * For kernels without IORING_FEAT_EXT_ARG (5.10 and older), if 'ts' is
 * specified, the application need not call io_uring_submit() before
 * calling this function, as we will do that on its behalf. From this it also
 * follows that this function isn't safe to use for applications that split SQ
 * and CQ handling between two threads and expect that to work without
 * synchronization, as this function manipulates both the SQ and CQ side.
 *
 * For kernels with IORING_FEAT_EXT_ARG, no implicit submission is done and
 * hence this function is safe to use for applications that split SQ and CQ
 * handling between two threads.
 */
func (ring *IoUring) __io_uring_submit_timeout(waitNr uint32, ts *syscall.Timespec) (ret int, err error) {
	sqe := ring.io_uring_get_sqe()
	if sqe == nil {
		ret, err = ring.io_uring_submit()
		if err != nil {
			return
		}
		sqe = ring.io_uring_get_sqe()
		if sqe == nil {
			err = syscall.EAGAIN
			return
		}
	}

	PrepTimeout(sqe, ts, waitNr, 0)
	sqe.UserData.SetUint64(LIBURING_UDATA_TIMEOUT)
	ret = int(ring.__io_uring_flush_sq())
	return
}

func (ring *IoUring) io_uring_wait_cqes(cqePtr **IoUringCqe, waitNtr uint32, ts *syscall.Timespec, sigmask *Sigset_t) (err error) {
	var toSubmit = 0
	if ts != nil {
		if ring.Features&IORING_FEAT_EXT_ARG != 0 {
			err = ring.io_uring_wait_cqes_new(cqePtr, waitNtr, ts, sigmask)
			return
		}
		toSubmit, err = ring.__io_uring_submit_timeout(waitNtr, ts)
		if err != nil {
			return
		}
	}
	err = ring.__io_uring_get_cqe(cqePtr, uint32(toSubmit), waitNtr, sigmask)
	return
}

func (ring *IoUring) io_uring_submit_and_wait_timeout(cqePtr **IoUringCqe, waitNtr uint32, ts *syscall.Timespec, sigmask *Sigset_t) (err error) {
	var toSubmit int
	if ts != nil {
		if ring.Features&IORING_FEAT_EXT_ARG != 0 {
			arg := IoUringGeteventsArg{
				Sigmask:   uint64(uintptr(unsafe.Pointer(sigmask))),
				SigmaskSz: NSIG / 8,
				Ts:        uint64(uintptr(unsafe.Pointer(ts))),
			}
			data := &get_data{
				submit:   ring.__io_uring_flush_sq(),
				waitNr:   waitNtr,
				getFlags: IORING_ENTER_EXT_ARG,
				sz:       int32(unsafe.Sizeof(arg)),
				arg:      unsafe.Pointer(&arg),
			}
			return ring._io_uring_get_cqe(cqePtr, data)
		}
		toSubmit, err = ring.__io_uring_submit_timeout(waitNtr, ts)
		if err != nil {
			return
		}
	} else {
		toSubmit = int(ring.__io_uring_flush_sq())
	}
	err = ring.__io_uring_get_cqe(cqePtr, uint32(toSubmit), waitNtr, sigmask)
	return
}

/*
 * See io_uring_wait_cqes() - this function is the same, it just always uses
 * '1' as the wait_nr.
 */
func (ring *IoUring) io_uring_wait_cqe_timeout(cqePtr **IoUringCqe, ts *syscall.Timespec) error {
	return ring.io_uring_wait_cqes(cqePtr, 1, ts, nil)
}

/*
 * Submit sqes acquired from io_uring_get_sqe() to the kernel.
 *
 * Returns number of sqes submitted
 */
func (ring *IoUring) io_uring_submit() (int, error) {
	return ring.__io_uring_submit_and_wait(0)
}

/*
 * Like io_uring_submit(), but allows waiting for events as well.
 *
 * Returns number of sqes submitted
 */
func (ring *IoUring) io_uring_submit_and_wait(waitNtr uint32) (int, error) {
	return ring.__io_uring_submit_and_wait(waitNtr)
}

func (ring *IoUring) __io_uring_submit_and_wait(waitNr uint32) (int, error) {
	return ring.__io_uring_submit(ring.__io_uring_flush_sq(), waitNr)
}

func (ring *IoUring) __io_uring_submit(submitted uint32, waitNr uint32) (ret int, err error) {
	var flags uint32 = 0

	if ring.sq_ring_needs_enter(&flags) || waitNr != 0 {
		if waitNr != 0 || ring.Flags&IORING_SETUP_IOPOLL != 0 {
			flags |= IORING_ENTER_GETEVENTS
		}
		if ring.IntFlags&INT_FLAG_REG_RING != 0 {
			flags |= IORING_ENTER_REGISTERED_RING
		}
		ret, err = io_uring_enter(ring.EnterRingFd, submitted, waitNr, flags, nil)
	} else {
		ret = int(submitted)
	}
	return
}

func (ring *IoUring) io_uring_get_sqe() *IoUringSqe {
	return ring._io_uring_get_sqe()
}

/*
 * Return an sqe to fill. Application must later call io_uring_submit()
 * when it's ready to tell the kernel about it. The caller may call this
 * function multiple times before calling io_uring_submit().
 *
 * Returns a vacant sqe, or NULL if we're full.
 */
func (ring *IoUring) _io_uring_get_sqe() (sqe *IoUringSqe) {
	sq := &ring.Sq
	var head = atomic.LoadUint32(sq._Head())
	var next = sq.SqeTail + 1
	var shift uint32 = 0

	if ring.Flags&IORING_SETUP_SQE128 != 0 {
		shift = 1
	}

	if next-head <= *sq._RingEntries() {
		sqe = ioUringSqeArray_Index(sq.Sqes, uintptr((sq.SqeTail&*sq._RingMask())<<shift))
		sq.SqeTail = next
		return
	}

	sqe = nil
	return
}

func (ring *IoUring) io_uring_cq_ready() uint32 {
	return atomic.LoadUint32(ring.Cq._Tail()) - *ring.Cq._Head()
}

func (ring *IoUring) __io_uring_peek_cqe(cqePtr **IoUringCqe, nrAvail *uint32) error {
	var cqe *IoUringCqe
	var err int32 = 0
	var avail int

	var mask = *ring.Cq._RingMask()
	var shift uint32 = 0

	if ring.Flags&IORING_SETUP_CQE32 != 0 {
		shift = 1
	}

	for {
		var tail = atomic.LoadUint32(ring.Cq._Tail())
		var head = *ring.Cq._Head()

		cqe = nil
		avail = int(tail - head)
		if avail < 1 {
			break
		}

		cqe = ioUringCqeArray_Index(ring.Cq.Cqes, uintptr((head&mask)<<shift))
		if ring.Features&IORING_FEAT_EXT_ARG == 0 &&
			cqe.UserData.GetUint64() == LIBURING_UDATA_TIMEOUT {
			if cqe.Res < 0 {
				err = cqe.Res
			}
			ring.io_uring_cq_advance(1)
			if err == 0 {
				// yields G
				runtime.Gosched()
				continue
			}
			cqe = nil
		}

		break
	}

	*cqePtr = cqe
	if nrAvail != nil {
		*nrAvail = uint32(avail)
	}
	if err == 0 {
		return nil
	}
	return syscall.Errno(-err)
}

func (ring *IoUring) io_uring_cq_advance(nr uint32) {
	if nr > 0 {
		atomic.StoreUint32(ring.Cq._Head(), *ring.Cq._Head()+nr)
	}
}

/*
 * Return an IO completion, waiting for 'wait_nr' completions if one isn't
 * readily available. Returns 0 with cqe_ptr filled in on success, -errno on
 * failure.
 */
func (ring *IoUring) io_uring_wait_cqe_nr(cqePtr **IoUringCqe, waitNr uint32) error {
	return ring.__io_uring_get_cqe(cqePtr, 0, waitNr, nil)
}

/*
 * Return an IO completion, if one is readily available. Returns 0 with
 * cqe_ptr filled in on success, -errno on failure.
 */
func (ring *IoUring) io_uring_peek_cqe(cqePtr **IoUringCqe) error {
	err := ring.__io_uring_peek_cqe(cqePtr, nil)
	if err == nil && *cqePtr != nil {
		return nil
	}
	return ring.io_uring_wait_cqe_nr(cqePtr, 0)
}

/*
 * Return an IO completion, waiting for it if necessary. Returns 0 with
 * cqe_ptr filled in on success, -errno on failure.
 */
func (ring *IoUring) io_uring_wait_cqe(cqePtr **IoUringCqe) error {
	err := ring.__io_uring_peek_cqe(cqePtr, nil)
	if err == nil && *cqePtr != nil {
		return nil
	}
	return ring.io_uring_wait_cqe_nr(cqePtr, 1)
}

/*
 * Must be called after io_uring_{peek,wait}_cqe() after the cqe has
 * been processed by the application.
 */
func (ring *IoUring) io_uring_cqe_seen(cqe *IoUringCqe) {
	if cqe != nil {
		ring.io_uring_cq_advance(1)
	}
}
