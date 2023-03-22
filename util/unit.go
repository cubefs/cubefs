// Copyright 2018 The CubeFS Authors.
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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	DefaultDataPartitionSize = 120 * GB
	TaskWorkerInterval       = 1
)

const (
	BlockCount         = 1024
	BlockSize          = 65536 * 2
	ReadBlockSize      = BlockSize
	PerBlockCrcSize    = 4
	ExtentSize         = BlockCount * BlockSize
	PacketHeaderSize   = 57
	BlockHeaderSize    = 4096
	SyscallTryMaxTimes = 3
)

const (
	AclListIP  = 0
	AclAddIP   = 1
	AclDelIP   = 2
	AclCheckIP = 3
)

const (
	UidLimitList = 0
	UidAddLimit  = 1
	UidDel       = 2
	UidGetLimit  = 3
)

const (
	DefaultTinySizeLimit = 1 * MB // TODO explain tiny extent?
)

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// IsIPV4 returns if it is IPV4 address.
func IsIPV4(val interface{}) bool {
	ip4Pattern := `((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)`
	ip4 := regexpCompile(ip4Pattern)
	return isMatch(ip4, val)
}

func GetIp(addr string) (ip string) {
	var arr []string
	if arr = strings.Split(addr, ":"); len(arr) < 2 {
		return
	}
	ip = strings.Trim(arr[0], " ")
	return ip
}

func regexpCompile(str string) *regexp.Regexp {
	return regexp.MustCompile("^" + str + "$")
}

func isMatch(exp *regexp.Regexp, val interface{}) bool {
	switch v := val.(type) {
	case []rune:
		return exp.MatchString(string(v))
	case []byte:
		return exp.Match(v)
	case string:
		return exp.MatchString(v)
	default:
		return false
	}
}

func GenerateKey(volName string, ino uint64, offset uint64) string {
	return fmt.Sprintf("%v_%v_%016x", volName, ino, offset)
}

func GenerateRepVolKey(volName string, ino uint64, dpId uint64, extentId uint64, offset uint64) string {
	return fmt.Sprintf("%v_%v_%v_%v_%016x", volName, ino, dpId, extentId, offset)
}

func OneDaySec() int64 {
	return 60 * 60 * 24
}

func CalcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
