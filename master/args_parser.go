// Copyright 2023 The CubeFS Authors.
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

package master

import (
	"fmt"
	"net/http"
	"strconv"
)

type httpArg struct {
	key       string       // key in form
	val       interface{}  // pointer to argument
	omitEmpty bool         // omit empty value
	omitError bool         // omit parse error
	convert   func() error // run after parsed if no err
}

func newHTTPArg(key string, val interface{}) *httpArg {
	return &httpArg{key: key, val: val}
}

func (a *httpArg) OmitEmpty() *httpArg {
	a.omitEmpty = true
	return a
}

func (a *httpArg) OmitError() *httpArg {
	a.omitError = true
	return a
}

func (a *httpArg) Convert(f func() error) *httpArg {
	a.convert = f
	return a
}

func (a *httpArg) done() error {
	if a.convert != nil {
		return a.convert()
	}
	return nil
}

func parseHTTPArgs(r *http.Request, args ...*httpArg) error {
	if err := r.ParseForm(); err != nil {
		return err
	}

	for _, arg := range args {
		str := r.FormValue(arg.key)
		if str == "" {
			if !arg.omitEmpty {
				return keyNotFound(arg.key)
			}
			if err := arg.done(); err != nil {
				return err
			}
			continue
		}

		var val interface{}
		var err error

		switch v := arg.val.(type) {
		case *string:
			val = str
		case *bool:
			val, err = strconv.ParseBool(str)

		case *int:
			val, err = strconv.ParseInt(str, 10, 0)
		case *int8:
			val, err = strconv.ParseInt(str, 10, 8)
		case *int16:
			val, err = strconv.ParseInt(str, 10, 16)
		case *int32:
			val, err = strconv.ParseInt(str, 10, 32)
		case *int64:
			val, err = strconv.ParseInt(str, 10, 64)

		case *uint:
			val, err = strconv.ParseUint(str, 10, 0)
		case *uint8:
			val, err = strconv.ParseUint(str, 10, 8)
		case *uint16:
			val, err = strconv.ParseUint(str, 10, 16)
		case *uint32:
			val, err = strconv.ParseUint(str, 10, 32)
		case *uint64:
			val, err = strconv.ParseUint(str, 10, 64)

		case *float32:
			val, err = strconv.ParseFloat(str, 32)
		case *float64:
			val, err = strconv.ParseFloat(str, 64)

		default:
			return fmt.Errorf("unknown type %v of %s %v", v, arg.key, arg.val)
		}

		if err != nil {
			if !arg.omitError {
				return unmatchedKey(arg.key)
			}
			if err = arg.done(); err != nil {
				return err
			}
			continue
		}

		switch v := arg.val.(type) {
		case *string:
			*v = val.(string)
		case *bool:
			*v = val.(bool)

		case *int:
			*v = int(val.(int64))
		case *int8:
			*v = int8(val.(int64))
		case *int16:
			*v = int16(val.(int64))
		case *int32:
			*v = int32(val.(int64))
		case *int64:
			*v = int64(val.(int64))

		case *uint:
			*v = uint(val.(uint64))
		case *uint8:
			*v = uint8(val.(uint64))
		case *uint16:
			*v = uint16(val.(uint64))
		case *uint32:
			*v = uint32(val.(uint64))
		case *uint64:
			*v = uint64(val.(uint64))

		case *float32:
			*v = float32(val.(float64))
		case *float64:
			*v = float64(val.(float64))
		}

		if err := arg.done(); err != nil {
			return err
		}
	}

	return nil
}
