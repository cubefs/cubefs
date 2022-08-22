// Copyright 2019 The CubeFS Authors.
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
)

type Wildcard struct {
	domain string
	r      *regexp.Regexp
}

func (w *Wildcard) Parse(host string) (bucket string, is bool) {
	if is = w.r.MatchString(host); is {
		var index int
		if index = strings.Index(host, "."+w.domain); index == -1 {
			is = false
			return
		}
		bucket = host[:index]
		return
	}
	return
}

func NewWildcard(domain string) (*Wildcard, error) {
	var regexpString = "^(([a-zA-Z0-9]|-|_)+.)+" + domain + "(:(\\d)+)*$"
	r, err := regexp.Compile(regexpString)
	if err != nil {
		return nil, err
	}
	var wc = &Wildcard{
		domain: domain,
		r:      r,
	}
	return wc, nil
}

type Wildcards []*Wildcard

func (ws Wildcards) Parse(host string) (bucket string, is bool) {
	for _, w := range ws {
		if bucket, is = w.Parse(host); is {
			return
		}
	}
	return
}

func NewWildcards(domains []string) (Wildcards, error) {
	var err error
	var ws = make([]*Wildcard, len(domains))
	for i := 0; i < len(domains); i++ {
		if ws[i], err = NewWildcard(domains[i]); err != nil {
			return nil, err
		}
	}
	return ws, nil
}
