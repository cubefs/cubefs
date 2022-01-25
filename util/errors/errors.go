// Copyright 2018 The Cubefs Authors.
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

package errors

import (
	"fmt"
	"path"
	"runtime"
	"strings"
)

type ErrorTrace struct {
	msg string
}

func New(msg string) error {
	return &ErrorTrace{msg: msg}
}

func NewError(err error) error {
	if err == nil {
		return nil
	}

	_, file, line, _ := runtime.Caller(1)
	_, fileName := path.Split(file)

	return &ErrorTrace{
		msg: fmt.Sprintf("[%v %v] %v", fileName, line, err.Error()),
	}
}

func NewErrorf(format string, a ...interface{}) error {
	msg := fmt.Sprintf(format, a...)
	_, file, line, _ := runtime.Caller(1)
	_, fileName := path.Split(file)

	return &ErrorTrace{
		msg: fmt.Sprintf("[%v %v] %v", fileName, line, msg),
	}
}

func (e *ErrorTrace) Error() string {
	return e.msg
}

func Trace(err error, format string, a ...interface{}) error {
	msg := fmt.Sprintf(format, a...)
	_, file, line, _ := runtime.Caller(1)
	_, fileName := path.Split(file)

	if err == nil {
		return &ErrorTrace{
			msg: fmt.Sprintf("[%v %v] %v", fileName, line, msg),
		}
	}

	return &ErrorTrace{
		msg: fmt.Sprintf("[%v %v] %v :: %v", fileName, line, msg, err),
	}
}

func Stack(err error) string {
	e, ok := err.(*ErrorTrace)
	if !ok {
		return err.Error()
	}

	var msg string

	stack := strings.Split(e.msg, "::")
	for _, s := range stack {
		msg = fmt.Sprintf("%v\n%v", msg, strings.TrimPrefix(s, " "))
	}
	return msg
}
