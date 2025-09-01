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

package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/google/uuid"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

// GenTmpPath create a temporary path
func GenTmpPath() (string, error) {
	id := uuid.NewString()
	path := os.TempDir() + "/" + id
	if err := os.RemoveAll(path); err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	return path, nil
}

func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func GetLocalIP() (string, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range addresses {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", errors.New("can not find the local ip address")
}

func GenUnusedPort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("invalid duration: %s", string(b))
	}
}

func parseBytes(b []byte) (uint64, error) {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return 0, err
	}
	switch value := v.(type) {
	case float64:
		return uint64(value), nil
	case string:
		var val uint64
		var err error
		val, err = humanize.ParseBytes(value)
		if err != nil {
			return 0, err
		}
		return val, nil
	default:
		return 0, fmt.Errorf("invalid bytes: %s", string(b))
	}
}

// ISize json IEC Sizes binary (base-2) 1024.
type ISize uint64

func (s ISize) MarshalJSON() ([]byte, error) {
	return json.Marshal(humanize.IBytes(uint64(s)))
}

func (s *ISize) UnmarshalJSON(b []byte) error {
	val, err := parseBytes(b)
	if err != nil {
		return err
	}
	*s = ISize(val)
	return nil
}

// Size json SI Sizes decimal (base-10) 1000.
type Size uint64

func (s Size) MarshalJSON() ([]byte, error) {
	return json.Marshal(humanize.Bytes(uint64(s)))
}

func (s *Size) UnmarshalJSON(b []byte) error {
	val, err := parseBytes(b)
	if err != nil {
		return err
	}
	*s = Size(val)
	return nil
}

func DiscardReader(n int) io.Reader { return &discardReader{n: n} }
func ZeroReader(n int) io.Reader    { return &discardReader{n: n, zero: true} }

type discardReader struct {
	n    int
	zero bool
}

func (r *discardReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.n {
		n = r.n
	}
	if r.zero && n > 0 {
		bytespool.Zero(p[:n])
	}
	r.n -= n
	return n, nil
}

var (
	FormatBool    = strconv.FormatBool
	FormatComplex = strconv.FormatComplex
	FormatFloat   = strconv.FormatFloat

	HexDigits = [16]byte{
		'0', '1', '2', '3',
		'4', '5', '6', '7',
		'8', '9', 'a', 'b',
		'c', 'd', 'e', 'f',
	}
)

// FormatInt returns the string representation of i in the given base,
// for 10 <= base <= 16. The result uses the lower-case letters 'a' to 'f'.
// This is inlinable to take advantage of "function outlining".
// Thus, the caller can decide whether a string must be heap allocated.
func FormatInt(ii int64, base int) string {
	if ii == 0 {
		return "0"
	}
	negative := ii < 0
	if negative {
		ii = -ii
	}
	var arr [32]byte
	idx := 32
	for ii > 0 {
		idx--
		arr[idx] = HexDigits[ii%int64(base)]
		ii = ii / int64(base)
	}
	if negative {
		idx--
		arr[idx] = '-'
	}
	return string(arr[idx:])
}

// FormatUint returns the string representation of i in the given base,
// for 10 <= base <= 16. The result uses the lower-case letters 'a' to 'f'.
func FormatUint(ii uint64, base int) string {
	if ii == 0 {
		return "0"
	}
	var arr [32]byte
	idx := 32
	for ii > 0 {
		idx--
		arr[idx] = HexDigits[ii%uint64(base)]
		ii = ii / uint64(base)
	}
	return string(arr[idx:])
}

// Any2String formats any value to base-10 string.
func Any2String(value interface{}) string {
	var val string
	switch v := value.(type) {
	case string:
		val = v
	case *string:
		val = *v
	case bool:
		val = strconv.FormatBool(v)
	case *bool:
		val = strconv.FormatBool(*v)

	case int:
		val = strconv.FormatInt(int64(v), 10)
	case int8:
		val = strconv.FormatInt(int64(v), 10)
	case int16:
		val = strconv.FormatInt(int64(v), 10)
	case int32:
		val = strconv.FormatInt(int64(v), 10)
	case int64:
		val = strconv.FormatInt(int64(v), 10)
	case *int:
		val = strconv.FormatInt(int64(*v), 10)
	case *int8:
		val = strconv.FormatInt(int64(*v), 10)
	case *int16:
		val = strconv.FormatInt(int64(*v), 10)
	case *int32:
		val = strconv.FormatInt(int64(*v), 10)
	case *int64:
		val = strconv.FormatInt(int64(*v), 10)

	case uint:
		val = strconv.FormatUint(uint64(v), 10)
	case uint8:
		val = strconv.FormatUint(uint64(v), 10)
	case uint16:
		val = strconv.FormatUint(uint64(v), 10)
	case uint32:
		val = strconv.FormatUint(uint64(v), 10)
	case uint64:
		val = strconv.FormatUint(uint64(v), 10)
	case *uint:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint8:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint16:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint32:
		val = strconv.FormatUint(uint64(*v), 10)
	case *uint64:
		val = strconv.FormatUint(uint64(*v), 10)

	case float32:
		val = strconv.FormatFloat(float64(v), 'f', 6, 64)
	case float64:
		val = strconv.FormatFloat(float64(v), 'f', 6, 64)
	case *float32:
		val = strconv.FormatFloat(float64(*v), 'f', 6, 64)
	case *float64:
		val = strconv.FormatFloat(float64(*v), 'f', 6, 64)
	case complex64:
		val = strconv.FormatComplex(complex128(v), 'f', 6, 64)
	case complex128:
		val = strconv.FormatComplex(complex128(v), 'f', 6, 64)
	case *complex64:
		val = strconv.FormatComplex(complex128(*v), 'f', 6, 64)
	case *complex128:
		val = strconv.FormatComplex(complex128(*v), 'f', 6, 64)

	default:
		val = fmt.Sprintf("%v", value)
	}
	return val
}

// String2Any parses base-10 string to pointer of value.
func String2Any(str string, pvalue interface{}) error {
	var (
		valBool       bool
		valInt64      int64
		valUint64     uint64
		valFloat64    float64
		valComplex128 complex128

		err error
	)
	switch v := pvalue.(type) {
	case *string:
		*v = str
	case *bool:
		if valBool, err = strconv.ParseBool(str); err == nil {
			*v = valBool
		}

	case *int:
		if valInt64, err = strconv.ParseInt(str, 10, strconv.IntSize); err == nil {
			*v = int(valInt64)
		}
	case *int8:
		if valInt64, err = strconv.ParseInt(str, 10, 8); err == nil {
			*v = int8(valInt64)
		}
	case *int16:
		if valInt64, err = strconv.ParseInt(str, 10, 16); err == nil {
			*v = int16(valInt64)
		}
	case *int32:
		if valInt64, err = strconv.ParseInt(str, 10, 32); err == nil {
			*v = int32(valInt64)
		}
	case *int64:
		if valInt64, err = strconv.ParseInt(str, 10, 64); err == nil {
			*v = valInt64
		}

	case *uint:
		if valUint64, err = strconv.ParseUint(str, 10, strconv.IntSize); err == nil {
			*v = uint(valUint64)
		}
	case *uint8:
		if valUint64, err = strconv.ParseUint(str, 10, 8); err == nil {
			*v = uint8(valUint64)
		}
	case *uint16:
		if valUint64, err = strconv.ParseUint(str, 10, 16); err == nil {
			*v = uint16(valUint64)
		}
	case *uint32:
		if valUint64, err = strconv.ParseUint(str, 10, 32); err == nil {
			*v = uint32(valUint64)
		}
	case *uint64:
		if valUint64, err = strconv.ParseUint(str, 10, 64); err == nil {
			*v = valUint64
		}

	case *float32:
		if valFloat64, err = strconv.ParseFloat(str, 32); err == nil {
			*v = float32(valFloat64)
		}
	case *float64:
		if valFloat64, err = strconv.ParseFloat(str, 64); err == nil {
			*v = valFloat64
		}
	case *complex64:
		if valComplex128, err = strconv.ParseComplex(str, 64); err == nil {
			*v = complex64(valComplex128)
		}
	case *complex128:
		if valComplex128, err = strconv.ParseComplex(str, 128); err == nil {
			*v = valComplex128
		}

	default:
		return fmt.Errorf("unknown type %v of %s %v", v, str, pvalue)
	}
	return err
}
