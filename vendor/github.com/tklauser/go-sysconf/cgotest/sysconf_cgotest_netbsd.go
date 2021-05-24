// Copyright 2018 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sysconf_cgotest

/*
#include <unistd.h>
*/
import "C"

import (
	"testing"

	"github.com/tklauser/go-sysconf"
)

func testSysconfCgoMatch(t *testing.T) {
	testCases := []struct {
		goVar int
		cVar  C.int
		name  string
	}{
		{sysconf.SC_AIO_LISTIO_MAX, C._SC_AIO_LISTIO_MAX, "AIO_LISTIO_MAX"},
		{sysconf.SC_AIO_MAX, C._SC_AIO_MAX, "AIO_MAX"},
		{sysconf.SC_ARG_MAX, C._SC_ARG_MAX, "ARG_MAX"},
		{sysconf.SC_ATEXIT_MAX, C._SC_ATEXIT_MAX, "ATEXIT_MAX"},
		{sysconf.SC_BC_BASE_MAX, C._SC_BC_BASE_MAX, "BC_BASE_MAX"},
		{sysconf.SC_BC_DIM_MAX, C._SC_BC_DIM_MAX, "BC_DIM_MAX"},
		{sysconf.SC_BC_SCALE_MAX, C._SC_BC_SCALE_MAX, "BC_SCALE_MAX"},
		{sysconf.SC_BC_STRING_MAX, C._SC_BC_STRING_MAX, "BC_STRING_MAX"},
		{sysconf.SC_CHILD_MAX, C._SC_CHILD_MAX, "CHILD_MAX"},
		{sysconf.SC_CLK_TCK, C._SC_CLK_TCK, "CLK_TCK"},
		{sysconf.SC_COLL_WEIGHTS_MAX, C._SC_COLL_WEIGHTS_MAX, "COLL_WEIGHTS_MAX"},
		{sysconf.SC_EXPR_NEST_MAX, C._SC_EXPR_NEST_MAX, "EXPR_NEST_MAX"},
		{sysconf.SC_HOST_NAME_MAX, C._SC_HOST_NAME_MAX, "HOST_NAME_MAX"},
		{sysconf.SC_IOV_MAX, C._SC_IOV_MAX, "IOV_MAX"},
		{sysconf.SC_LINE_MAX, C._SC_LINE_MAX, "LINE_MAX"},
		{sysconf.SC_LOGIN_NAME_MAX, C._SC_LOGIN_NAME_MAX, "LOGIN_NAME_MAX"},
		{sysconf.SC_MQ_OPEN_MAX, C._SC_MQ_OPEN_MAX, "MQ_OPEN_MAX"},
		{sysconf.SC_MQ_PRIO_MAX, C._SC_MQ_PRIO_MAX, "MQ_PRIO_MAX"},
		{sysconf.SC_NGROUPS_MAX, C._SC_NGROUPS_MAX, "NGROUPS_MAX"},
		{sysconf.SC_OPEN_MAX, C._SC_OPEN_MAX, "OPEN_MAX"},
		{sysconf.SC_PAGE_SIZE, C._SC_PAGE_SIZE, "PAGE_SIZE"},
		{sysconf.SC_PAGESIZE, C._SC_PAGESIZE, "PAGESIZE"},
		{sysconf.SC_THREAD_DESTRUCTOR_ITERATIONS, C._SC_THREAD_DESTRUCTOR_ITERATIONS, "PTHREAD_DESTRUCTOR_ITERATIONS"},
		{sysconf.SC_THREAD_KEYS_MAX, C._SC_THREAD_KEYS_MAX, "PTHREAD_KEYS_MAX"},
		{sysconf.SC_THREAD_STACK_MIN, C._SC_THREAD_STACK_MIN, "PTHREAD_STACK_MIN"},
		{sysconf.SC_THREAD_THREADS_MAX, C._SC_THREAD_THREADS_MAX, "PTHREAD_THREADS_MAX"},
		{sysconf.SC_RE_DUP_MAX, C._SC_RE_DUP_MAX, "RE_DUP_MAX"},
		{sysconf.SC_STREAM_MAX, C._SC_STREAM_MAX, "STREAM_MAX"},
		{sysconf.SC_SYMLOOP_MAX, C._SC_SYMLOOP_MAX, "SYMLOOP_MAX"},
		{sysconf.SC_TTY_NAME_MAX, C._SC_TTY_NAME_MAX, "TTY_NAME_MAX"},
		{sysconf.SC_TZNAME_MAX, C._SC_TZNAME_MAX, "TZNAME_MAX"},

		{sysconf.SC_ASYNCHRONOUS_IO, C._SC_ASYNCHRONOUS_IO, "_POSIX_ASYNCHRONOUS_IO"},
		{sysconf.SC_BARRIERS, C._SC_BARRIERS, "_POSIX_BARRIERS"},
		{sysconf.SC_FSYNC, C._SC_FSYNC, "_POSIX_FSYNC"},
		{sysconf.SC_JOB_CONTROL, C._SC_JOB_CONTROL, "_POSIX_JOB_CONTROL"},
		{sysconf.SC_MAPPED_FILES, C._SC_MAPPED_FILES, "_POSIX_MAPPED_FILES"},
		{sysconf.SC_SEMAPHORES, C._SC_SEMAPHORES, "_POSIX_SEMAPHORES"},
		{sysconf.SC_SHELL, C._SC_SHELL, "_POSIX_SHELL"},
		{sysconf.SC_THREADS, C._SC_THREADS, "_POSIX_THREADS"},
		{sysconf.SC_TIMERS, C._SC_TIMERS, "_POSIX_TIMERS"},
		{sysconf.SC_VERSION, C._SC_VERSION, "_POSIX_VERSION"},

		{sysconf.SC_2_UPE, C._SC_2_UPE, "_POSIX2_UPE"},
		{sysconf.SC_2_VERSION, C._SC_2_VERSION, "_POSIX2_VERSION"},

		// non-standard
		{sysconf.SC_PHYS_PAGES, C._SC_PHYS_PAGES, "_PHYS_PAGES"},
		// AV_PHYS_PAGES might change between calling Go and C version
		// of sysconf. Don't test it for now.
		{sysconf.SC_MONOTONIC_CLOCK, C._SC_MONOTONIC_CLOCK, "MONOTONIC_CLOCK"},
		{sysconf.SC_NPROCESSORS_CONF, C._SC_NPROCESSORS_CONF, "_NPROCESSORS_CONF"},
		{sysconf.SC_NPROCESSORS_ONLN, C._SC_NPROCESSORS_ONLN, "_NPROCESSORS_ONLN"},
	}

	for _, tc := range testCases {
		testSysconfGoCgo(t, tc)
	}
}
