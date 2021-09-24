// Copyright 2019 The ChubaoFS Authors.
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
	"os"
	"sort"
	"time"
)

type FSFileInfo struct {
	Path         string
	Size         int64
	Mode         os.FileMode
	ModifyTime   time.Time
	CreateTime   time.Time
	ETag         string
	Inode        uint64
	MIMEType     string
	Disposition  string
	CacheControl string
	Expires      string
	Metadata     map[string]string `graphql:"-"` // User-defined metadata
}

type Prefixes []string

type PrefixMap map[string]struct{}

func (m PrefixMap) AddPrefix(prefix string) {
	m[prefix] = struct{}{}
}

func (m PrefixMap) Prefixes() Prefixes {
	s := make([]string, 0, len(m))
	for prefix := range m {
		s = append(s, prefix)
	}
	sort.Strings(s)
	return s
}

func (m PrefixMap) contain(prefix string) bool {
	if _, ok := m[prefix]; ok {
		return true
	}
	return false
}

type FSUpload struct {
	Key          string
	UploadId     string
	StorageClass string
	Initiated    string
}

type FSPart struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int
}
