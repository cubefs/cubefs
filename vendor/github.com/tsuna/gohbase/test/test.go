// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package test

import (
	"fmt"

	"github.com/golang/mock/gomock"
)

type reporter struct {
	gomock.TestReporter
}

func (r reporter) Errorf(format string, args ...interface{}) {
	r.TestReporter.Errorf(format, args...)
}

func (r reporter) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// NewController returns a gomock controller that panics when there's no match
// to handle goroutines
func NewController(t gomock.TestReporter) *gomock.Controller {
	return gomock.NewController(reporter{t})
}

// ErrEqual returns true if errors messages are equal
func ErrEqual(a, b error) bool {
	aMsg, bMsg := "", ""
	if a != nil {
		aMsg = a.Error()
	}
	if b != nil {
		bMsg = b.Error()
	}
	return aMsg == bMsg
}
