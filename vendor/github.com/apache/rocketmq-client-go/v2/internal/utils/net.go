/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"bytes"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"net"
	"strconv"
	"time"
)

var (
	LocalIP string
)

func init() {
	ip, err := ClientIP4()
	if err != nil {
		LocalIP = ""
	} else {
		LocalIP = fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3])
	}
}

func ClientIP4() ([]byte, error) {
	if ifaces, err := net.Interfaces(); err == nil && ifaces != nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
				continue
			}
			if addrs, err := iface.Addrs(); err == nil && addrs != nil {
				for _, addr := range addrs {
					if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
						if ip4 := ipnet.IP.To4(); ip4 != nil {
							return ip4, nil
						}
					}
				}
			}
		}
	}
	return nil, errors.ErrUnknownIP
}

func FakeIP() []byte {
	buf := bytes.NewBufferString("")
	buf.WriteString(strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10))
	return buf.Bytes()[4:8]
}

func GetAddressByBytes(data []byte) string {
	return net.IPv4(data[0], data[1], data[2], data[3]).String()
}
