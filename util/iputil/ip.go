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

package iputil

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-ping/ping"
)

const (
	GetLocalIPTimeout = 3 * time.Second
)

var cidrs []*net.IPNet

func init() {
	maxCidrBlocks := []string{
		"127.0.0.1/8",    // localhost
		"10.0.0.0/8",     // 24-bit block
		"172.16.0.0/12",  // 20-bit block
		"192.168.0.0/16", // 16-bit block
		"169.254.0.0/16", // link local address
		"::1/128",        // localhost IPv6
		"fc00::/7",       // unique local address IPv6
		"fe80::/10",      // link local address IPv6
	}

	cidrs = make([]*net.IPNet, len(maxCidrBlocks))
	for i, maxCidrBlock := range maxCidrBlocks {
		_, cidr, _ := net.ParseCIDR(maxCidrBlock)
		cidrs[i] = cidr
	}
}

// isLocalAddress works by checking if the address is under private CIDR blocks.
// List of private CIDR blocks can be seen on :
//
// https://en.wikipedia.org/wiki/Private_network
//
// https://en.wikipedia.org/wiki/Link-local_address
func isPrivateAddress(address string) (bool, error) {
	ipAddress := net.ParseIP(address)
	if ipAddress == nil {
		return false, errors.New("address is not valid")
	}

	for i := range cidrs {
		if cidrs[i].Contains(ipAddress) {
			return true, nil
		}
	}

	return false, nil
}

// FromRequest return client's real public IP address from http request headers.
func FromRequest(r *http.Request) string {
	// Fetch header value
	xRealIP := r.Header.Get("X-Real-Ip")
	xForwardedFor := r.Header.Get("X-Forwarded-For")

	// If both empty, return IP from remote address
	if xRealIP == "" && xForwardedFor == "" {
		var remoteIP string

		// If there are colon in remote address, remove the port number
		// otherwise, return remote address as is
		if strings.ContainsRune(r.RemoteAddr, ':') {
			remoteIP, _, _ = net.SplitHostPort(r.RemoteAddr)
		} else {
			remoteIP = r.RemoteAddr
		}

		return remoteIP
	}

	// Check list of IP in X-Forwarded-For and return the first global address
	for _, address := range strings.Split(xForwardedFor, ",") {
		address = strings.TrimSpace(address)
		isPrivate, err := isPrivateAddress(address)
		if !isPrivate && err == nil {
			return address
		}
	}

	// If nothing succeed, return X-Real-IP
	return xRealIP
}

// RealIP is depreciated, use FromRequest instead
func RealIP(r *http.Request) string {
	return FromRequest(r)
}

// set default max distance from two ips to length of ipv6
const DEFAULT_MAX_DISTANCE = 128

func GetDistance(a, b net.IP) int {
	return DEFAULT_MAX_DISTANCE - commonPrefixLen(a, b)
}

func GetLocalIPByDial() (ip string, err error) {
	var conn net.Conn
	conn, err = net.Dial("tcp", "cn.chubaofs.jd.local:80")
	if err != nil {
		return
	}
	defer conn.Close()
	ip = strings.Split(conn.LocalAddr().String(), ":")[0]
	return
}

func GetLocalIPByDialWithMaster(masters []string, timeout time.Duration) (ip string, err error) {
	var conn net.Conn
	defaultAddr := "cn.chubaofs.jd.local"
	defaultPort := 80
	if len(masters) == 0 {
		masters = append(masters, fmt.Sprintf("%v:%v", defaultAddr, defaultPort))
	}
	for _, master := range masters {
		ipPort := strings.Split(master, ":")
		if len(ipPort) == 1 {
			master = fmt.Sprintf("%v:%v", ipPort[0], defaultPort)
		}
		conn, err = net.DialTimeout("tcp", master, timeout)
		if err == nil {
			break
		}
	}
	if conn == nil {
		return
	}
	ip = strings.Split(conn.LocalAddr().String(), ":")[0]
	conn.Close()
	return
}

func PingWithTimeout(addr string, count int, timeout time.Duration) (avgTime time.Duration, err error) {
	pinger, err := ping.NewPinger(addr)
	if err != nil {
		return
	}
	pinger.Timeout = timeout
	pinger.Count = count
	pinger.Interval = 50 * time.Microsecond
	pinger.SetPrivileged(true)
	// Blocks until finished.
	err = pinger.Run()
	if err != nil {
		return
	}
	stats := pinger.Statistics()
	return stats.AvgRtt, nil
}

// GetRemoteRealIP will not ignore private ip
func GetRemoteRealIP(r *http.Request) (ip string) {
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if ip = strings.TrimSpace(strings.Split(xForwardedFor, ",")[0]); ip != "" {
		return
	}
	if ip = strings.TrimSpace(r.Header.Get("X-Real-Ip")); ip != "" {
		return
	}
	ip = strings.Split(r.RemoteAddr, ":")[0]
	return
}
