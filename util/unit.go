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
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/cubefs/cubefs/util/log"
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

func getIpAndPort(ipAddr string) (ip string, port string, success bool) {
	success = false
	arr := strings.Split(ipAddr, ":")
	if len(arr) != 2 {
		log.LogWarnf("action[GetIpAndPort] ipAddr[%v] invalid", ipAddr)
		return
	}
	ip = strings.Trim(arr[0], " ")
	port = strings.Trim(arr[1], " ")
	success = true
	return
}

func getDomainAndPort(domainAddr string) (domain string, port string, success bool) {
	success = false
	arr := strings.Split(domainAddr, ":")
	if len(arr) != 2 {
		log.LogWarnf("action[GetDomainAndPort] domainAddr[%v] invalid", domainAddr)
		return
	}
	domain = strings.Trim(arr[0], " ")
	port = strings.Trim(arr[1], " ")
	success = true
	return
}

func IsIPV4Addr(ipAddr string) bool {
	ip, _, ok := getIpAndPort(ipAddr)
	if !ok {
		return false
	}
	return IsIPV4(ip)
}

func ParseIpAddrToDomainAddr(ipAddr string) (domainAddr string) {
	ip, port, ok := getIpAndPort(ipAddr)
	if !ok {
		return
	}
	domains, err := net.LookupAddr(ip)
	if err != nil {
		log.LogWarnf("action[ParseIpAddrToDomainAddr] failed, ipAddr[%v], ip[%v], err[%v]", ipAddr, ip, err)
		return
	}
	for _, v := range domains {
		domain := v
		if domain[len(domain)-1] == '.' {
			domain = domain[0 : len(domain)-1]
		}
		if len(domainAddr) != 0 {
			domainAddr += ","
		}
		domainAddr += fmt.Sprintf("%s:%v", domain, port)
	}
	return
}

func ParseAddrToIpAddr(addr string) (ipAddr string, success bool) {
	success = true
	if IsIPV4Addr(addr) {
		ipAddr = addr
		return
	}
	if parsedAddr, ok := ParseDomainAddrToIpAddr(addr); ok {
		ipAddr = parsedAddr
		return
	}
	success = false
	return
}

func ParseDomainAddrToIpAddr(domainAddr string) (ipAddr string, success bool) {
	success = false
	domain, port, ok := getDomainAndPort(domainAddr)
	if !ok {
		return
	}
	ips, err := net.LookupHost(domain)
	if err != nil {
		log.LogWarnf("action[ParseDomainAddrToIpAddr] failed, domainAddr[%v], domain[%v], err[%v]",
			domainAddr, domain, err)
		return
	}
	if len(ips) == 0 {
		log.LogWarnf("action[ParseDomainAddrToIpAddr] ips is null, domainAddr[%v], domain[%v]",
			domainAddr, domain)
		return
	}
	for i := 0; i < len(ips); i += 1 {
		if ips[i] != ips[0] {
			log.LogWarnf("action[ParseDomainAddrToIpAddr] the number of ips is not one,"+
				"domainAddr[%v], domain[%v], ips[%v], err[%v]", domainAddr, domain, ips, err)
			return
		}
	}
	ipAddr = fmt.Sprintf("%s:%v", ips[0], port)
	success = true
	return
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
