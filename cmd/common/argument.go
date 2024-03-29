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

package common

import (
	"fmt"
	"net/http"

	"github.com/cubefs/cubefs/util"
)

// Argument http argument parser.
type Argument struct {
	key       string       // key in form
	val       interface{}  // pointer to argument
	omitEmpty bool         // omit empty value
	omitError bool         // omit parse error
	onEmpty   func() error // run if empty
	onError   func() error // run on error happens
	onValue   func() error // run after parsed if no error
}

// NewArgument returns Argument.
func NewArgument(key string, val interface{}) *Argument {
	return &Argument{key: key, val: val}
}

// OmitEmpty value omits empty.
func (a *Argument) OmitEmpty() *Argument {
	a.omitEmpty = true
	return a
}

// OmitError convert val omits error.
func (a *Argument) OmitError() *Argument {
	a.omitError = true
	return a
}

// OnEmpty run only if value omits empty.
func (a *Argument) OnEmpty(f func() error) *Argument {
	a.onEmpty = f
	return a
}

// OnEmpty run only if value omits error.
func (a *Argument) OnError(f func() error) *Argument {
	a.onError = f
	return a
}

// OnValue run only convert not-empty value has no error.
func (a *Argument) OnValue(f func() error) *Argument {
	a.onValue = f
	return a
}

func ParseArguments(r *http.Request, args ...*Argument) error {
	if err := r.ParseForm(); err != nil {
		return err
	}

	for _, arg := range args {
		str := r.FormValue(arg.key)
		if str == "" {
			if !arg.omitEmpty {
				return fmt.Errorf("key(%s) can not omit empty", arg.key)
			}
			if arg.onEmpty != nil {
				if err := arg.onEmpty(); err != nil {
					return err
				}
			}
			continue
		}

		if err := util.String2Any(str, arg.val); err != nil {
			if !arg.omitError {
				return fmt.Errorf("key(%s) can not omit error(%s)", arg.key, err.Error())
			}
			if arg.onError != nil {
				if err = arg.onError(); err != nil {
					return err
				}
			}
			continue
		}

		if arg.onValue != nil {
			if err := arg.onValue(); err != nil {
				return err
			}
		}
	}

	return nil
}
