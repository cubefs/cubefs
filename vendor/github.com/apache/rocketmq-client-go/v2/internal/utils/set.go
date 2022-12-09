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
	"encoding/json"
	"sort"
)

type UniqueItem interface {
	UniqueID() string
}

type StringUnique string

func (str StringUnique) UniqueID() string {
	return string(str)
}

type Set struct {
	items map[string]UniqueItem
}

func NewSet() Set {
	return Set{
		items: make(map[string]UniqueItem, 0),
	}
}

func (s *Set) Add(v UniqueItem) {
	s.items[v.UniqueID()] = v
}

func (s *Set) AddKV(k, v string) {
	s.items[k] = StringUnique(v)
}

func (s *Set) Contains(k string) (UniqueItem, bool) {
	v, ok := s.items[k]
	return v, ok
}

func (s *Set) Len() int {
	return len(s.items)
}

var _ json.Marshaler = &Set{}

func (s *Set) MarshalJSON() ([]byte, error) {
	if len(s.items) == 0 {
		return []byte("[]"), nil
	}

	buffer := new(bytes.Buffer)
	buffer.WriteByte('[')
	keys := make([]string, 0)
	for _, k := range s.items {
		var key string
		switch kval := k.(type) {
		case StringUnique:
			key = "\"" + string(kval) + "\""
		default:
			v, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			key = string(v)
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for i, key := range keys {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(key)
	}

	buffer.WriteByte(']')

	return buffer.Bytes(), nil
}

func (s Set) UnmarshalJSON(data []byte) (err error) {
	return nil
}
