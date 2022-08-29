// Copyright 2022 The CubeFS Authors.
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
	"errors"
	"fmt"
)

// New alias of errors.New
func New(msg string) error {
	return errors.New(msg)
}

// Newf alias of fmt.Errorf
func Newf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

// Newx returns error with multi message
func Newx(v ...interface{}) error {
	return errors.New(stringJoin(v...))
}

// As alias of errors.As
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

// Is alias of errors.Is
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// Unwrap alias of errors.Unwrap
func Unwrap(err error) error {
	return errors.Unwrap(err)
}
