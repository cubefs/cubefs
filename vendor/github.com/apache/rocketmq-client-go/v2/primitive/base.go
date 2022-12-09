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

package primitive

import (
	"github.com/apache/rocketmq-client-go/v2/errors"
	"regexp"
	"strings"
)

var (
	ipv4Regex, _ = regexp.Compile(`^((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))`)
	ipv6Regex, _ = regexp.Compile(`(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))`)
)

type NamesrvAddr []string

func NewNamesrvAddr(s ...string) (NamesrvAddr, error) {
	if len(s) == 0 {
		return nil, errors.ErrNoNameserver
	}

	ss := s
	if len(ss) == 1 {
		// compatible with multi server env string: "a;b;c"
		ss = strings.Split(s[0], ";")
	}

	for _, srv := range ss {
		if err := verifyIP(srv); err != nil {
			return nil, err
		}
	}

	addrs := make(NamesrvAddr, 0)
	addrs = append(addrs, ss...)
	return addrs, nil
}

func (addr NamesrvAddr) Check() error {
	for _, srv := range addr {
		if err := verifyIP(srv); err != nil {
			return err
		}
	}
	return nil
}

var (
	httpPrefixRegex, _ = regexp.Compile("^(http|https)://")
)

func verifyIP(ip string) error {
	if httpPrefixRegex.MatchString(ip) {
		return nil
	}
	if strings.Contains(ip, ";") {
		return errors.ErrMultiIP
	}
	ipV4s := ipv4Regex.FindAllString(ip, -1)
	ipV6s := ipv6Regex.FindAllString(ip, -1)

	if len(ipV4s) == 0 && len(ipV6s) == 0 {
		return errors.ErrIllegalIP
	}

	if len(ipV4s) > 1 || len(ipV6s) > 1 {
		return errors.ErrMultiIP
	}
	return nil
}

var PanicHandler func(interface{})

func WithRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}

func Diff(origin, latest []string) bool {
	if len(origin) != len(latest) {
		return true
	}

	// check added
	originFilter := make(map[string]struct{}, len(origin))
	for _, srv := range origin {
		originFilter[srv] = struct{}{}
	}

	latestFilter := make(map[string]struct{}, len(latest))
	for _, srv := range latest {
		if _, ok := originFilter[srv]; !ok {
			return true // added
		}
		latestFilter[srv] = struct{}{}
	}

	// check delete
	for _, srv := range origin {
		if _, ok := latestFilter[srv]; !ok {
			return true // deleted
		}
	}
	return false
}
