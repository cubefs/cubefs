// Copyright 2018 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build ignore
// +build ignore

package sysconf

/*
#include <limits.h>
#include <paths.h>
#include <stdio.h>
#include <sys/param.h>
#include <time.h>
#include <unistd.h>

#ifndef _PATH_ZONEINFO
# define _PATH_ZONEINFO	"/usr/share/zoneinfo" // TZDATA from tzcode/stdtime/tzfile.h
#endif
*/
import "C"

// sysconf variables
const (
	SC_AIO_LISTIO_MAX               = C._SC_AIO_LISTIO_MAX
	SC_AIO_MAX                      = C._SC_AIO_MAX
	SC_ARG_MAX                      = C._SC_ARG_MAX
	SC_ATEXIT_MAX                   = C._SC_ATEXIT_MAX
	SC_BC_BASE_MAX                  = C._SC_BC_BASE_MAX
	SC_BC_DIM_MAX                   = C._SC_BC_DIM_MAX
	SC_BC_SCALE_MAX                 = C._SC_BC_SCALE_MAX
	SC_BC_STRING_MAX                = C._SC_BC_STRING_MAX
	SC_CHILD_MAX                    = C._SC_CHILD_MAX
	SC_CLK_TCK                      = C._SC_CLK_TCK
	SC_COLL_WEIGHTS_MAX             = C._SC_COLL_WEIGHTS_MAX
	SC_EXPR_NEST_MAX                = C._SC_EXPR_NEST_MAX
	SC_HOST_NAME_MAX                = C._SC_HOST_NAME_MAX
	SC_IOV_MAX                      = C._SC_IOV_MAX
	SC_LINE_MAX                     = C._SC_LINE_MAX
	SC_LOGIN_NAME_MAX               = C._SC_LOGIN_NAME_MAX
	SC_MQ_OPEN_MAX                  = C._SC_MQ_OPEN_MAX
	SC_MQ_PRIO_MAX                  = C._SC_MQ_PRIO_MAX
	SC_NGROUPS_MAX                  = C._SC_NGROUPS_MAX
	SC_OPEN_MAX                     = C._SC_OPEN_MAX
	SC_PAGE_SIZE                    = C._SC_PAGE_SIZE
	SC_PAGESIZE                     = C._SC_PAGESIZE
	SC_THREAD_DESTRUCTOR_ITERATIONS = C._SC_THREAD_DESTRUCTOR_ITERATIONS
	SC_THREAD_KEYS_MAX              = C._SC_THREAD_KEYS_MAX
	SC_THREAD_STACK_MIN             = C._SC_THREAD_STACK_MIN
	SC_THREAD_THREADS_MAX           = C._SC_THREAD_THREADS_MAX
	SC_RE_DUP_MAX                   = C._SC_RE_DUP_MAX
	SC_STREAM_MAX                   = C._SC_STREAM_MAX
	SC_SYMLOOP_MAX                  = C._SC_SYMLOOP_MAX
	SC_TTY_NAME_MAX                 = C._SC_TTY_NAME_MAX
	SC_TZNAME_MAX                   = C._SC_TZNAME_MAX

	SC_ASYNCHRONOUS_IO = C._SC_ASYNCHRONOUS_IO
	SC_BARRIERS        = C._SC_BARRIERS
	SC_FSYNC           = C._SC_FSYNC
	SC_JOB_CONTROL     = C._SC_JOB_CONTROL
	SC_MAPPED_FILES    = C._SC_MAPPED_FILES
	SC_SEMAPHORES      = C._SC_SEMAPHORES
	SC_SHELL           = C._SC_SHELL
	SC_THREADS         = C._SC_THREADS
	SC_TIMERS          = C._SC_TIMERS
	SC_VERSION         = C._SC_VERSION

	SC_2_VERSION   = C._SC_2_VERSION
	SC_2_C_DEV     = C._SC_2_C_DEV
	SC_2_FORT_DEV  = C._SC_2_FORT_DEV
	SC_2_FORT_RUN  = C._SC_2_FORT_RUN
	SC_2_LOCALEDEF = C._SC_2_LOCALEDEF
	SC_2_SW_DEV    = C._SC_2_SW_DEV
	SC_2_UPE       = C._SC_2_UPE

	// non-standard variables
	SC_PHYS_PAGES       = C._SC_PHYS_PAGES
	SC_MONOTONIC_CLOCK  = C._SC_MONOTONIC_CLOCK
	SC_NPROCESSORS_CONF = C._SC_NPROCESSORS_CONF
	SC_NPROCESSORS_ONLN = C._SC_NPROCESSORS_ONLN
)

// sysconf values
const (
	_MAXHOSTNAMELEN = C.MAXHOSTNAMELEN
	_MAXLOGNAME     = C.MAXLOGNAME
	_MAXSYMLINKS    = C.MAXSYMLINKS

	_POSIX_ARG_MAX                      = C._POSIX_ARG_MAX
	_POSIX_CHILD_MAX                    = C._POSIX_CHILD_MAX
	_POSIX_SHELL                        = C._POSIX_SHELL
	_POSIX_THREAD_DESTRUCTOR_ITERATIONS = C._POSIX_THREAD_DESTRUCTOR_ITERATIONS
	_POSIX_THREAD_KEYS_MAX              = C._POSIX_THREAD_KEYS_MAX
	_POSIX_VERSION                      = C._POSIX_VERSION

	_POSIX2_VERSION = C._POSIX2_VERSION

	_FOPEN_MAX  = C.FOPEN_MAX
	_NAME_MAX   = C.NAME_MAX
	_RE_DUP_MAX = C.RE_DUP_MAX

	_BC_BASE_MAX      = C.BC_BASE_MAX
	_BC_DIM_MAX       = C.BC_DIM_MAX
	_BC_SCALE_MAX     = C.BC_SCALE_MAX
	_BC_STRING_MAX    = C.BC_STRING_MAX
	_COLL_WEIGHTS_MAX = C.COLL_WEIGHTS_MAX
	_EXPR_NEST_MAX    = C.EXPR_NEST_MAX
	_LINE_MAX         = C.LINE_MAX

	_PATH_DEV      = C._PATH_DEV
	_PATH_ZONEINFO = C._PATH_ZONEINFO
)

// pathconf variables

const _PC_NAME_MAX = C._PC_NAME_MAX
