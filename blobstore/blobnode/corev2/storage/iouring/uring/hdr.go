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

/* SPDX-License-Identifier: (GPL-2.0 WITH Linux-syscall-note) OR MIT */
package uring

import "unsafe"

/*
 * IO submission data structure (Submission Queue Entry)
 */
type IoUringSqe_Union1 uint64

func (u *IoUringSqe_Union1) SetOffset(v uint64) { *u = IoUringSqe_Union1(v) }
func (u *IoUringSqe_Union1) SetAddr2(v uint64)  { *u = IoUringSqe_Union1(v) }

type IoUringSqe_Union2 uint64

func (u *IoUringSqe_Union2) SetAddr_Value(v uint64)     { *u = IoUringSqe_Union2(v) }
func (u *IoUringSqe_Union2) SetAddr(v unsafe.Pointer)   { *u = IoUringSqe_Union2((uintptr)(v)) }
func (u *IoUringSqe_Union2) SetSpliceOffsetIn(v uint64) { *u = IoUringSqe_Union2(v) }

type IoUringSqe_Union3 uint32

func (u *IoUringSqe_Union3) SetRwFlags(v uint32)        { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetPollEvents(v uint16)     { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetPoll32Events(v uint32)   { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetSyncRangeFlags(v uint32) { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetMsgFlags(v uint32)       { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetTimeoutFlags(v uint32)   { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetAcceptFlags(v uint32)    { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetCancelFlags(v uint32)    { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetOpenFlags(v uint32)      { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetStatxFlags(v uint32)     { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetFadviseAdvice(v uint32)  { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetSpliceFlags(v uint32)    { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetRenameFlags(v uint32)    { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetUnlinkFlags(v uint32)    { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetHardlinkFlags(v uint32)  { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetXattrFlags(v uint32)     { *u = IoUringSqe_Union3(v) }
func (u *IoUringSqe_Union3) SetOpFlags(v uint32)        { *u = IoUringSqe_Union3(v) } //generic
func (u IoUringSqe_Union3) GetOpFlags() uint32          { return uint32(u) }          //generic

type IoUringSqe_Union4 uint16

func (u *IoUringSqe_Union4) SetBufIndex(v uint16) { *u = IoUringSqe_Union4(v) }
func (u *IoUringSqe_Union4) SetBufGroup(v uint16) { *u = IoUringSqe_Union4(v) }

type IoUringSqe_Union5 uint32

func (u *IoUringSqe_Union5) SetSpliceFdIn(v int32) { *u = IoUringSqe_Union5(v) }
func (u *IoUringSqe_Union5) SetFileIndex(v uint32) { *u = IoUringSqe_Union5(v) }

type IoUringSqe struct {
	Opcode IoUringOp /* type of operation for this sqe */
	Flags  uint8     /* IOSQE_ flags */
	IoPrio uint16    /* ioprio for the request */
	Fd     int32     /* file descriptor to do IO on */

	//  union {
	// 	 __u64	off;	/* offset into file */
	// 	 __u64	addr2;
	//  };
	IoUringSqe_Union1

	//  union {
	// 	 __u64	addr;	/* pointer to buffer or iovecs */
	// 	 __u64	splice_off_in;
	//  };
	IoUringSqe_Union2

	Len uint32 /* buffer size or number of iovecs */

	//  union {
	// 	 __kernel_rwf_t	rw_flags;
	// 	 __u32		fsync_flags;
	// 	 __u16		poll_events;	/* compatibility */
	// 	 __u32		poll32_events;	/* word-reversed for BE */
	// 	 __u32		sync_range_flags;
	// 	 __u32		msg_flags;
	// 	 __u32		timeout_flags;
	// 	 __u32		accept_flags;
	// 	 __u32		cancel_flags;
	// 	 __u32		open_flags;
	// 	 __u32		statx_flags;
	// 	 __u32		fadvise_advice;
	// 	 __u32		splice_flags;
	// 	 __u32		rename_flags;
	// 	 __u32		unlink_flags;
	// 	 __u32		hardlink_flags;
	// 	 __u32		xattr_flags;
	//  };
	IoUringSqe_Union3

	UserData UserData /* data to be passed back at completion time */

	/* pack this to avoid bogus arm OABI complaints */
	//  union {
	// 	 /* index into fixed buffers, if used */
	// 	 __u16	buf_index;
	// 	 /* for grouped buffer selection */
	// 	 __u16	buf_group;
	//  } __attribute__((packed));
	IoUringSqe_Union4

	/* personality to use, if used */
	Personality uint16

	//  union {
	// 	 __s32	splice_fd_in;
	// 	 __u32	file_index;
	//  };
	IoUringSqe_Union5

	Addr3  uint64
	__pad2 [1]uint64
}

/*
 * If sqe->file_index is set to this for opcodes that instantiate a new
 * direct descriptor (like openat/openat2/accept), then io_uring will allocate
 * an available direct descriptor instead of having the application pass one
 * in. The picked direct descriptor will be returned in cqe->res, or -ENFILE
 * if the space is full.
 */
const IORING_FILE_INDEX_ALLOC = ^uint32(0)

const (
	IOSQE_FIXED_FILE_BIT = iota
	IOSQE_IO_DRAIN_BIT
	IOSQE_IO_LINK_BIT
	IOSQE_IO_HARDLINK_BIT
	IOSQE_ASYNC_BIT
	IOSQE_BUFFER_SELECT_BIT
	IOSQE_CQE_SKIP_SUCCESS_BIT
)

/*
 * sqe->flags
 */
const (
	/* use fixed fileset */
	IOSQE_FIXED_FILE = (1 << IOSQE_FIXED_FILE_BIT)
	/* issue after inflight IO */
	IOSQE_IO_DRAIN = (1 << IOSQE_IO_DRAIN_BIT)
	/* links next sqe */
	IOSQE_IO_LINK = (1 << IOSQE_IO_LINK_BIT)
	/* like LINK, but stronger */
	IOSQE_IO_HARDLINK = (1 << IOSQE_IO_HARDLINK_BIT)
	/* always go async */
	IOSQE_ASYNC = (1 << IOSQE_ASYNC_BIT)
	/* select buffer from sqe->buf_group */
	IOSQE_BUFFER_SELECT = (1 << IOSQE_BUFFER_SELECT_BIT)
	/* don't post CQE if request succeeded */
	IOSQE_CQE_SKIP_SUCCESS = (1 << IOSQE_CQE_SKIP_SUCCESS_BIT)
)

/*
 * io_uring_setup() flags
 */
const (
	IORING_SETUP_IOPOLL     = (1 << 0) /* io_context is polled */
	IORING_SETUP_SQPOLL     = (1 << 1) /* SQ poll thread */
	IORING_SETUP_SQ_AFF     = (1 << 2) /* sq_thread_cpu is valid */
	IORING_SETUP_CQSIZE     = (1 << 3) /* app defines CQ size */
	IORING_SETUP_CLAMP      = (1 << 4) /* clamp SQ/CQ ring sizes */
	IORING_SETUP_ATTACH_WQ  = (1 << 5) /* attach to existing wq */
	IORING_SETUP_R_DISABLED = (1 << 6) /* start with ring disabled */
	IORING_SETUP_SUBMIT_ALL = (1 << 7) /* continue submit on error */
)

/*
 * Cooperative task running. When requests complete, they often require
 * forcing the submitter to transition to the kernel to complete. If this
 * flag is set, work will be done when the task transitions anyway, rather
 * than force an inter-processor interrupt reschedule. This avoids interrupting
 * a task running in userspace, and saves an IPI.
 */
const IORING_SETUP_COOP_TASKRUN = (1 << 8)

/*
 * If COOP_TASKRUN is set, get notified if task work is available for
 * running and a kernel transition would be needed to run it. This sets
 * IORING_SQ_TASKRUN in the sq ring flags. Not valid with COOP_TASKRUN.
 */
const IORING_SETUP_TASKRUN_FLAG = (1 << 9)

const IORING_SETUP_SQE128 = (1 << 10) /* SQEs are 128 byte */
const IORING_SETUP_CQE32 = (1 << 11)  /* CQEs are 32 byte */

type IoUringOp = uint8

//go:generate stringerx -type=IoUringOp

const (
	IORING_OP_NOP IoUringOp = iota
	IORING_OP_READV
	IORING_OP_WRITEV
	IORING_OP_FSYNC
	IORING_OP_READ_FIXED
	IORING_OP_WRITE_FIXED
	IORING_OP_POLL_ADD
	IORING_OP_POLL_REMOVE
	IORING_OP_SYNC_FILE_RANGE
	IORING_OP_SENDMSG
	IORING_OP_RECVMSG
	IORING_OP_TIMEOUT
	IORING_OP_TIMEOUT_REMOVE
	IORING_OP_ACCEPT
	IORING_OP_ASYNC_CANCEL
	IORING_OP_LINK_TIMEOUT
	IORING_OP_CONNECT
	IORING_OP_FALLOCATE
	IORING_OP_OPENAT
	IORING_OP_CLOSE
	IORING_OP_FILES_UPDATE
	IORING_OP_STATX
	IORING_OP_READ
	IORING_OP_WRITE
	IORING_OP_FADVISE
	IORING_OP_MADVISE
	IORING_OP_SEND
	IORING_OP_RECV
	IORING_OP_OPENAT2
	IORING_OP_EPOLL_CTL
	IORING_OP_SPLICE
	IORING_OP_PROVIDE_BUFFERS
	IORING_OP_REMOVE_BUFFERS
	IORING_OP_TEE
	IORING_OP_SHUTDOWN
	IORING_OP_RENAMEAT
	IORING_OP_UNLINKAT
	IORING_OP_MKDIRAT
	IORING_OP_SYMLINKAT
	IORING_OP_LINKAT
	IORING_OP_MSG_RING
	IORING_OP_FSETXATTR
	IORING_OP_SETXATTR
	IORING_OP_FGETXATTR
	IORING_OP_GETXATTR
	IORING_OP_SOCKET
	IORING_OP_URING_CMD

	/* this goes last, obviously */
	IORING_OP_LAST
)

/*
 * sqe->fsync_flags
 */
const IORING_FSYNC_DATASYNC = (1 << 0)

/*
 * sqe->timeout_flags
 */
const (
	IORING_TIMEOUT_ABS           = (1 << 0)
	IORING_TIMEOUT_UPDATE        = (1 << 1)
	IORING_TIMEOUT_BOOTTIME      = (1 << 2)
	IORING_TIMEOUT_REALTIME      = (1 << 3)
	IORING_LINK_TIMEOUT_UPDATE   = (1 << 4)
	IORING_TIMEOUT_ETIME_SUCCESS = (1 << 5)
	IORING_TIMEOUT_CLOCK_MASK    = (IORING_TIMEOUT_BOOTTIME | IORING_TIMEOUT_REALTIME)
	IORING_TIMEOUT_UPDATE_MASK   = (IORING_TIMEOUT_UPDATE | IORING_LINK_TIMEOUT_UPDATE)
)

/*
 * sqe->splice_flags
 * extends splice(2) flags
 */
const SPLICE_F_FD_IN_FIXED = (1 << 31) /* the last bit of __u32 */

/*
 * POLL_ADD flags. Note that since sqe->poll_events is the flag space, the
 * command flags for POLL_ADD are stored in sqe->len.
 *
 * IORING_POLL_ADD_MULTI	Multishot poll. Sets IORING_CQE_F_MORE if
 *				the poll handler will continue to report
 *				CQEs on behalf of the same SQE.
 *
 * IORING_POLL_UPDATE		Update existing poll request, matching
 *				sqe->addr as the old user_data field.
 */
const (
	IORING_POLL_ADD_MULTI        = (1 << 0)
	IORING_POLL_UPDATE_EVENTS    = (1 << 1)
	IORING_POLL_UPDATE_USER_DATA = (1 << 2)
)

/*
 * ASYNC_CANCEL flags.
 *
 * IORING_ASYNC_CANCEL_ALL	Cancel all requests that match the given key
 * IORING_ASYNC_CANCEL_FD	Key off 'fd' for cancelation rather than the
 *				request 'user_data'
 * IORING_ASYNC_CANCEL_ANY	Match any request
 */
const (
	IORING_ASYNC_CANCEL_ALL = (1 << 0)
	IORING_ASYNC_CANCEL_FD  = (1 << 1)
	IORING_ASYNC_CANCEL_ANY = (1 << 2)
)

/*
 * send/sendmsg and recv/recvmsg flags (sqe->addr2)
 *
 * IORING_RECVSEND_POLL_FIRST	If set, instead of first attempting to send
 *				or receive and arm poll if that yields an
 *				-EAGAIN result, arm poll upfront and skip
 *				the initial transfer attempt.
 */
const IORING_RECVSEND_POLL_FIRST = (1 << 0)

/*
 * accept flags stored in sqe->ioprio
 */
const IORING_ACCEPT_MULTISHOT = (1 << 0)

/*
 * IO completion data structure (Completion Queue Entry)
 */
type IoUringCqe struct {
	UserData UserData /* sqe->data submission passed back */
	Res      int32    /* result code for this event */
	Flags    uint32

	/*
	 * If the ring is initialized with IORING_SETUP_CQE32, then this field
	 * contains 16-bytes of padding, doubling the size of the CQE.
	 */
	//  __u64 big_cqe[];

	// 8+4+4 == 16 , correct
}

/*
 * cqe->flags
 *
 * IORING_CQE_F_BUFFER	If set, the upper 16 bits are the buffer ID
 * IORING_CQE_F_MORE	If set, parent SQE will generate more CQE entries
 * IORING_CQE_F_SOCK_NONEMPTY	If set, more data to read after socket recv
 */

const (
	IORING_CQE_F_BUFFER        = (1 << 0)
	IORING_CQE_F_MORE          = (1 << 1)
	IORING_CQE_F_SOCK_NONEMPTY = (1 << 2)
)

const (
	IORING_CQE_BUFFER_SHIFT = 16
)

/*
 * Magic offsets for the application to mmap the data it needs
 */
const (
	IORING_OFF_SQ_RING = 0
	IORING_OFF_CQ_RING = 0x8000000
	IORING_OFF_SQES    = 0x10000000
)

/*
 * Filled with the offset for mmap(2)
 */

type IoSqringOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	resv1       uint32
	resv2       uint64
}

/*
 * sq_ring->flags
 */
const (
	IORING_SQ_NEED_WAKEUP = (1 << 0) /* needs io_uring_enter wakeup */
	IORING_SQ_CQ_OVERFLOW = (1 << 1) /* CQ ring is overflown */
	IORING_SQ_TASKRUN     = (1 << 2) /* task should enter the kernel */
)

type IoCqringOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	Cqes        uint32
	Flags       uint32
	resv1       uint32
	resv2       uint64
}

/*
 * cq_ring->flags
 */

/* disable eventfd notifications */
const IORING_CQ_EVENTFD_DISABLED = (1 << 0)

/*
 * io_uring_enter(2) flags
 */
const (
	IORING_ENTER_GETEVENTS       = (1 << 0)
	IORING_ENTER_SQ_WAKEUP       = (1 << 1)
	IORING_ENTER_SQ_WAIT         = (1 << 2)
	IORING_ENTER_EXT_ARG         = (1 << 3)
	IORING_ENTER_REGISTERED_RING = (1 << 4)
)

/*
 * Passed in for io_uring_setup(2). Copied back with updated info on success
 */

type IoUringParams struct {
	SqEntries    uint32
	CqEntries    uint32
	Flags        uint32
	SqThreadCpu  uint32
	SqThreadIdle uint32
	Features     uint32
	WqFd         uint32
	resv         [3]uint32
	SqOff        IoSqringOffsets
	CqOff        IoCqringOffsets
}

/*
 * io_uring_params->features flags
 */
const (
	IORING_FEAT_SINGLE_MMAP     = (1 << 0)
	IORING_FEAT_NODROP          = (1 << 1)
	IORING_FEAT_SUBMIT_STABLE   = (1 << 2)
	IORING_FEAT_RW_CUR_POS      = (1 << 3)
	IORING_FEAT_CUR_PERSONALITY = (1 << 4)
	IORING_FEAT_FAST_POLL       = (1 << 5)
	IORING_FEAT_POLL_32BITS     = (1 << 6)
	IORING_FEAT_SQPOLL_NONFIXED = (1 << 7)
	IORING_FEAT_EXT_ARG         = (1 << 8)
	IORING_FEAT_NATIVE_WORKERS  = (1 << 9)
	IORING_FEAT_RSRC_TAGS       = (1 << 10)
	IORING_FEAT_CQE_SKIP        = (1 << 11)
	IORING_FEAT_LINKED_FILE     = (1 << 12)
)

/*
 * io_uring_register(2) opcodes and arguments
 */
const (
	IORING_REGISTER_BUFFERS       = 0
	IORING_UNREGISTER_BUFFERS     = 1
	IORING_REGISTER_FILES         = 2
	IORING_UNREGISTER_FILES       = 3
	IORING_REGISTER_EVENTFD       = 4
	IORING_UNREGISTER_EVENTFD     = 5
	IORING_REGISTER_FILES_UPDATE  = 6
	IORING_REGISTER_EVENTFD_ASYNC = 7
	IORING_REGISTER_PROBE         = 8
	IORING_REGISTER_PERSONALITY   = 9
	IORING_UNREGISTER_PERSONALITY = 10
	IORING_REGISTER_RESTRICTIONS  = 11
	IORING_REGISTER_ENABLE_RINGS  = 12

	/* extended with tagging */
	IORING_REGISTER_FILES2         = 13
	IORING_REGISTER_FILES_UPDATE2  = 14
	IORING_REGISTER_BUFFERS2       = 15
	IORING_REGISTER_BUFFERS_UPDATE = 16

	/* set/clear io-wq thread affinities */
	IORING_REGISTER_IOWQ_AFF   = 17
	IORING_UNREGISTER_IOWQ_AFF = 18

	/* set/get max number of io-wq workers */
	IORING_REGISTER_IOWQ_MAX_WORKERS = 19

	/* register/unregister io_uring fd with the ring */
	IORING_REGISTER_RING_FDS   = 20
	IORING_UNREGISTER_RING_FDS = 21

	/* register ring based provide buffer group */
	IORING_REGISTER_PBUF_RING   = 22
	IORING_UNREGISTER_PBUF_RING = 23

	/* this goes last */
	IORING_REGISTER_LAST
)

/* io-wq worker categories */
const (
	IO_WQ_BOUND = iota
	IO_WQ_UNBOUND
)

/* deprecated, see struct IoUringRsrcUpdate */
type IoUringFilesUpdate struct {
	Offset uint32
	resv   uint32
	Fds    uint64 // __aligned_u64/* __s32 * */
}

/*
 * Register a fully sparse file space, rather than pass in an array of all
 * -1 file descriptors.
 */
const IORING_RSRC_REGISTER_SPARSE = (1 << 0)

type IoUringRsrcRegister struct {
	Nr    uint32
	Flags uint32
	resv2 uint64
	Data  uint64 // __aligned_u64
	Tags  uint64 // __aligned_u64
}

type IoUringRsrcUpdate struct {
	Offset uint32
	resv   uint32
	Data   uint64 // __aligned_u64
}

type IoUringRsrcUpdate2 struct {
	Offset uint32
	resv   uint32
	Data   uint64 // __aligned_u64
	Tags   uint64 // __aligned_u64
	Nr     uint32
	resv2  uint32
}

/* Skip updating fd indexes set to this value in the fd table */
const IORING_REGISTER_FILES_SKIP = (-2)

const IO_URING_OP_SUPPORTED = (1 << 0)

type IoUringProbeOp struct {
	op    uint8
	resv  uint8
	flags uint16 /* IO_URING_OP_* flags */
	resv2 uint32
}

type IoUringProbe struct {
	last_op uint8 /* last opcode supported */
	uint8         /* length of ops[] array below */
	resv    uint16
	resv2   [3]uint32
	ops     [0]IoUringProbeOp
}

type IoUringRestriction struct {
	opcode uint16
	//  union {
	// 	 __u8 register_op; /* IORING_RESTRICTION_REGISTER_OP */
	// 	 __u8 sqe_op;      /* IORING_RESTRICTION_SQE_OP */
	// 	 __u8 sqe_flags;   /* IORING_RESTRICTION_SQE_FLAGS_* */
	//  };
	Union1 uint8
	resv   uint8
	resv2  [3]uint32
}

type IoUringBuf struct {
	Addr uint64
	Len  uint32
	Bid  uint16
	resv uint16
}

type IoUringBufRing struct {
	//  union {
	/*
	 * To avoid spilling into more pages than we need to, the
	 * ring tail is overlaid with the IoUringBuf->resv field.
	 */
	Anon0 struct {
		resv1 uint64
		resv2 uint32
		resv3 uint16
		Tail  uint16
	}
	// bufs [0]IoUringBuf
	//  };
}

/* argument for IORING_(UN)REGISTER_PBUF_RING */
type IoUringBufReg struct {
	RingAddr    uint64
	RingEntries uint32
	Bgid        uint16
	Pad         uint16
	resv        [3]uint64
}

/*
 * IoUringRestriction->opcode values
 */
const (
	/* Allow an io_uring_register(2) opcode */
	IORING_RESTRICTION_REGISTER_OP = 0

	/* Allow an sqe opcode */
	IORING_RESTRICTION_SQE_OP = 1

	/* Allow sqe flags */
	IORING_RESTRICTION_SQE_FLAGS_ALLOWED = 2

	/* Require sqe flags (these flags must be set on each submission) */
	IORING_RESTRICTION_SQE_FLAGS_REQUIRED = 3

	IORING_RESTRICTION_LAST
)

type IoUringGeteventsArg struct {
	Sigmask   uint64
	SigmaskSz uint32
	Pad       uint32
	Ts        uint64
}

/*
 * accept flags stored in sqe->ioprio
 */
// const IORING_ACCEPT_MULTISHOT = (1 << 0)
