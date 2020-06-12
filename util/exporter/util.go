// Copyright 2018 The Chubao Authors.
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

package exporter

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/chubaofs/chubaofs/util/log"
)

type NULL struct{}

var (
	null = NULL{}
)

func stringMD5(str string) string {
	h := md5.New()
	_, err := io.WriteString(h, str)
	if err != nil {
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func stringMapToString(m map[string]string) string {
	mjson, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}

	return string(mjson)
}

// GetLocalIpAddr returns the local IP address.
func GetLocalIpAddr() (ipaddr string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.LogError("consul register get local ip failed, ", err)
		return
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("cannot get local ip")
}
