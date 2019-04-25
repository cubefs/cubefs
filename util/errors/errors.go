// Copyright 2018 The Chubao Authors.
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
	"go/build"
	"path"
	"runtime"
	"strings"
)

type ErrorTrace struct {
	msg string
}

var trimPath string
var projectPath string = "github.com/chubaofs/cfs"

func init() {
	trimPath = path.Join(build.Default.GOPATH, "src", projectPath)
}

func New(msg string) error {
	return &ErrorTrace{msg: msg}
}

func NewError(err error) error {
	if err == nil {
		return nil
	}

	_, file, line, _ := runtime.Caller(1)
	relativePath := strings.TrimPrefix(strings.TrimPrefix(file, trimPath), "/")

	return &ErrorTrace{
		msg: fmt.Sprintf("[%v %v] %v", relativePath, line, err.Error()),
	}
}

func (e *ErrorTrace) Error() string {
	return e.msg
}

func Trace(err error, format string, a ...interface{}) error {
	msg := fmt.Sprintf(format, a...)
	_, file, line, _ := runtime.Caller(1)
	relativePath := strings.TrimPrefix(strings.TrimPrefix(file, trimPath), "/")

	if err == nil {
		return &ErrorTrace{
			msg: fmt.Sprintf("[%v %v] %v", relativePath, line, msg),
		}
	}

	return &ErrorTrace{
		msg: fmt.Sprintf("[%v %v] %v :: %v", relativePath, line, msg, err),
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
