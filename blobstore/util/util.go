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
	"io"
	"net"
	"os"
	"strconv"
	"time"
	"unsafe"

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
		return errors.New("invalid duration")
	}
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
