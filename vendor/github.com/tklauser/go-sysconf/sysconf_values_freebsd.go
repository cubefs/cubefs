// Copyright 2021 Tobias Klauser. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build ignore
// +build ignore

package sysconf

/*
#include <limits.h>
*/
import "C"

const (
	_LONG_MAX = C.LONG_MAX
	_SHRT_MAX = C.SHRT_MAX
)
