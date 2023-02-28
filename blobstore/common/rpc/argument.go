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

package rpc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type (
	parserKey struct {
		PkgPath   string
		Name      string
		FieldName string
	}
	parserVal struct {
		Name string
		Opt  struct {
			Ignore    bool // "-"
			Omitempty bool // ",omitempty"
			Base64    bool // ",base64"
		}
	}
)

var registeredParsers map[parserKey]parserVal

func init() {
	registeredParsers = make(map[parserKey]parserVal)
}

// RegisterArgsParser regist your argument need parse in
// uri, query, form, or postform.
// the tags is sorted.
// NOTE: the function is thread-unsafe.
func RegisterArgsParser(args interface{}, tags ...string) {
	if args == nil {
		return
	}

	if _, ok := args.(Parser); ok {
		return
	}

	typ := reflect.TypeOf(args)
	val := reflect.ValueOf(args)
	if typ.Kind() != reflect.Ptr {
		log.Panicf("args(%s) must be pointer", typ.Name())
	}

	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		log.Panicf("args(%s) reference must be struct", typ.Name())
	}

	val = val.Elem()
	t := val.Type()
	for i := 0; i < val.NumField(); i++ {
		ft := t.Field(i)

		pVal := parserVal{
			Name: strings.ToLower(ft.Name),
		}
		for _, tag := range tags {
			tagStr := ft.Tag.Get(tag)
			if tagStr != "" {
				ts := strings.Split(tagStr, ",")
				if ts[0] == "-" {
					pVal.Opt.Ignore = true
					break
				}

				if ts[0] != "" {
					pVal.Name = ts[0]
				}
				for _, t := range ts[1:] {
					switch t {
					case "omitempty":
						pVal.Opt.Omitempty = true
					case "base64":
						pVal.Opt.Base64 = true
					}
				}
				break
			}
		}

		pKey := parserKey{
			PkgPath:   typ.PkgPath(),
			Name:      typ.Name(),
			FieldName: ft.Name,
		}
		registeredParsers[pKey] = pVal

		log.Infof("register args field:%+v val:%+v", pKey, pVal)
	}
}

func parseArgs(c *Context, args interface{}, opts ...ServerOption) error {
	if args == nil {
		return nil
	}
	opt := c.opts
	if len(opts) > 0 {
		opt = c.opts.copy()
		for _, o := range opts {
			o.apply(opt)
		}
	}

	if opt.argsBody {
		size, err := c.RequestLength()
		if err != nil {
			return err
		}

		if arg, ok := args.(UnmarshalerFrom); ok {
			return arg.UnmarshalFrom(io.LimitReader(c.Request.Body, int64(size)))
		}

		buf := bytespool.Alloc(size)
		defer bytespool.Free(buf)
		if _, err = io.ReadFull(c.Request.Body, buf); err != nil {
			return err
		}

		if arg, ok := args.(Unmarshaler); ok {
			return arg.Unmarshal(buf)
		}
		return json.Unmarshal(buf, args)
	}

	if !opt.hasArgs() {
		return nil
	}

	getter := func(fKey string) string {
		if opt.argsURI {
			if val := c.Param.ByName(fKey); val != "" {
				return val
			}
		}
		if opt.argsQuery {
			if val := c.Request.URL.Query().Get(fKey); val != "" {
				return val
			}
		}
		if opt.argsForm {
			if val := c.Request.Form.Get(fKey); val != "" {
				return val
			}
		}
		if opt.argsPostForm {
			if val := c.Request.PostForm.Get(fKey); val != "" {
				return val
			}
		}
		return ""
	}

	if arg, ok := args.(Parser); ok {
		return arg.Parse(getter)
	}

	typ := reflect.TypeOf(args)
	val := reflect.ValueOf(args)

	if typ.Kind() != reflect.Ptr {
		return fmt.Errorf("args(%s) must be pointer", typ.Name())
	}

	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("args(%s) reference must be struct", typ.Name())
	}

	val = val.Elem()
	t := val.Type()
	for i := 0; i < val.NumField(); i++ {
		ft := t.Field(i)

		pVal, ok := registeredParsers[parserKey{
			PkgPath:   typ.PkgPath(),
			Name:      typ.Name(),
			FieldName: ft.Name,
		}]
		if !ok {
			pVal = parserVal{Name: strings.ToLower(ft.Name)}
		}
		if pVal.Opt.Ignore {
			continue
		}

		fVal := getter(pVal.Name)
		if fVal == "" {
			if pVal.Opt.Omitempty {
				continue
			}
			return fmt.Errorf("args(%s) field(%s) do not omit", typ.Name(), ft.Name)
		}

		if pVal.Opt.Base64 {
			switch len(fVal) & 3 {
			case 2:
				fVal += "=="
			case 3:
				fVal += "="
			}
			b, err := base64.URLEncoding.DecodeString(fVal)
			if err != nil {
				return fmt.Errorf("args(%s) field(%s) invalid base64(%s)", typ.Name(), ft.Name, fVal)
			}
			fVal = string(b)
		}

		fv := val.Field(i)
		if err := parseValue(fv, fVal); err != nil {
			return err
		}
	}

	return nil
}

func parseValue(val reflect.Value, str string) (err error) {
	var (
		bv bool
		iv int64
		uv uint64
		fv float64
	)

RETRY:
	switch val.Kind() {
	case reflect.Bool:
		bv, err = strconv.ParseBool(str)
		val.SetBool(bv)

	case reflect.Int:
		iv, err = strconv.ParseInt(str, 10, 0)
		val.SetInt(iv)
	case reflect.Int8:
		iv, err = strconv.ParseInt(str, 10, 8)
		val.SetInt(iv)
	case reflect.Int16:
		iv, err = strconv.ParseInt(str, 10, 16)
		val.SetInt(iv)
	case reflect.Int32:
		iv, err = strconv.ParseInt(str, 10, 32)
		val.SetInt(iv)
	case reflect.Int64:
		iv, err = strconv.ParseInt(str, 10, 64)
		val.SetInt(iv)

	case reflect.Uint:
		uv, err = strconv.ParseUint(str, 10, 0)
		val.SetUint(uv)
	case reflect.Uint8:
		uv, err = strconv.ParseUint(str, 10, 8)
		val.SetUint(uv)
	case reflect.Uint16:
		uv, err = strconv.ParseUint(str, 10, 16)
		val.SetUint(uv)
	case reflect.Uint32:
		uv, err = strconv.ParseUint(str, 10, 32)
		val.SetUint(uv)
	case reflect.Uint64:
		uv, err = strconv.ParseUint(str, 10, 64)
		val.SetUint(uv)

	case reflect.Float32:
		fv, err = strconv.ParseFloat(str, 32)
		val.SetFloat(fv)
	case reflect.Float64:
		fv, err = strconv.ParseFloat(str, 64)
		val.SetFloat(fv)

	case reflect.String:
		val.SetString(str)
	case reflect.Uintptr:
		uv, err = strconv.ParseUint(str, 10, 64)
		val.SetUint(uv)
	case reflect.Ptr:
		elem := reflect.New(val.Type().Elem())
		val.Set(elem)
		val = elem.Elem()
		goto RETRY

	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			val.SetBytes([]byte(str))
		} else {
			return fmt.Errorf("unsupported type(%s) of slice", val.Type().Elem().Kind().String())
		}

	default:
		return fmt.Errorf("unsupported type(%s)", val.Kind().String())
	}
	return
}
