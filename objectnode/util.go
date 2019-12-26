// Copyright 2018 The ChubaoFS Authors.
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

package objectnode

import (
	"regexp"
	"strings"

	"net"
	"net/http"
	"time"

	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	pathSep         = "/"
	tempFileNameSep = "_"
)

func splitPath(path string) (dirs []string, filename string) {
	pathParts := strings.Split(path, pathSep)
	if len(pathParts) > 1 {
		dirs = pathParts[:len(pathParts)-1]
	}
	filename = pathParts[len(pathParts)-1]
	return
}

func tempFileName(origin string) string {
	return "." + origin + tempFileNameSep + util.RandomString(16, util.LowerLetter|util.UpperLetter)
}

func formatSimpleTime(time time.Time) string {
	return time.UTC().Format("2006-01-02T15:04:05")
}

func formatTimeISO(time time.Time) string {
	return time.UTC().Format("2006-01-02T15:04:05.000Z")
}

func formatTimeRFC1123(time time.Time) string {
	return time.UTC().Format(http.TimeFormat)
}

func parseTimeRFC1123(timeStr string) (time.Time, error) {
	t, err := time.Parse(http.TimeFormat, timeStr)
	if err != nil {
		return t, err
	}
	return t, err
}

func transferError(key string, err error) Error {
	// TODO: complete sys error transfer
	ossError := Error{
		Key:     key,
		Message: err.Error(),
	}
	return ossError
}

// get request remote IP
func getRequestIP(r *http.Request) string {
	IPAddress := r.Header.Get("X-Real-Ip")
	if IPAddress == "" {
		IPAddress = r.Header.Get("X-Forwarded-For")
	}
	if IPAddress == "" {
		IPAddress = r.RemoteAddr
	}
	if ok := strings.Contains(IPAddress, ":"); ok {
		IPAddress = strings.Split(IPAddress, ":")[0]
	}

	return IPAddress
}

// check ipnet contains ip
// ip: 172.17.0.2
// ipnet: 172.17.0.0/16
func isIPNetContainsIP(ipStr, ipnetStr string) (bool, error) {
	if !strings.Contains(ipnetStr, "/") {
		if ipStr == ipnetStr {
			return true, nil
		} else {
			return false, nil
		}
	}
	_, ipnet, err := net.ParseCIDR(ipnetStr)
	if err != nil {
		log.LogInfof("parse ipnet error ipnet   %v", ipnetStr)
		return false, err
	}

	ip := net.ParseIP(ipStr)
	if ipnet.Contains(ip) {
		return true, nil
	}

	return false, nil
}

func patternMatch(pattern, key string) bool {
	if pattern == "" {
		return key == pattern
	}
	if pattern == "*" {
		return true
	}
	matched, err := regexp.MatchString(pattern, key)
	if err != nil {
		log.LogErrorf("patternMatch error %v", err)
		return false
	}

	return matched
}
