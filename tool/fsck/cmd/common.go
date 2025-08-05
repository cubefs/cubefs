// Copyright 2020 The CubeFS Authors.
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

package cmd

import (
	"encoding/json"
)

var (
	MasterAddr     string
	VolName        string
	MpId           uint64
	InodesFile     string
	DensFile       string
	MetaPort       string
	InodeID        uint64
	DataPort       string
	CleanS         bool
	CleanFlag      string
	isCheckApplyId bool
	forceClean     bool
)

const (
	inodeDumpFileName          string = "inode.dump"
	dentryDumpFileName         string = "dentry.dump"
	obsoleteInodeDumpFileName  string = "inode.dump.obsolete"
	obsoleteDentryDumpFileName string = "dentry.dump.obsolete"
	pathDumpFileName           string = "path.dump"
	normalDir                  string = "normal"
	migrateDir                 string = "migrate"
	beforeTimeFile             string = "before_time"
	verifyInfoFile             string = "verify_info"
)

type Inode struct {
	Inode      uint64
	Type       uint32
	Size       uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	NLink      uint32

	Dens  []*Dentry
	Valid bool
	Files uint64
	Dirs  uint64
	Bytes uint64
	Path  string
}

func (i *Inode) String() string {
	data, err := json.Marshal(i)
	if err != nil {
		return ""
	}
	return string(data)
}

type Dentry struct {
	ParentId uint64
	Name     string
	Inode    uint64
	Type     uint32

	Valid bool
}

func (d *Dentry) String() string {
	data, err := json.Marshal(d)
	if err != nil {
		return ""
	}
	return string(data)
}
