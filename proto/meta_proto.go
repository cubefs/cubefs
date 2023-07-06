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

package proto

import "sync"

// CreateNameSpaceRequest defines the request to create a name space.
type CreateNameSpaceRequest struct {
	Name string
}

// CreateNameSpaceResponse defines the response to the request of creating a name space.
type CreateNameSpaceResponse struct {
	Status int
	Result string
}

// Peer defines the peer of the node id and address.
type Peer struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

// CreateMetaPartitionRequest defines the request to create a meta partition.
type CreateMetaPartitionRequest struct {
	MetaId      string
	VolName     string
	Start       uint64
	End         uint64
	PartitionID uint64
	Members     []Peer
}

// CreateMetaPartitionResponse defines the response to the request of creating a meta partition.
type CreateMetaPartitionResponse struct {
	VolName     string
	PartitionID uint64
	Status      uint8
	Result      string
}

type UidSpaceInfo struct {
	VolName   string
	Uid       uint32
	CTime     int64
	Enabled   bool
	Limited   bool
	UsedSize  uint64
	LimitSize uint64
	Rsv       string
}

type UidReportSpaceInfo struct {
	Uid   uint32
	Size  uint64
	Rsv   string
	MTime int64
}

type QuotaUsedInfo struct {
	UsedFiles int64
	UsedBytes int64
}

type QuotaLimitedInfo struct {
	LimitedFiles bool
	LimitedBytes bool
}

type QuotaReportInfo struct {
	QuotaId  uint32
	UsedInfo QuotaUsedInfo
}

type QuotaInfo struct {
	VolName     string
	QuotaId     uint32
	CTime       int64
	PathInfos   []QuotaPathInfo
	LimitedInfo QuotaLimitedInfo
	UsedInfo    QuotaUsedInfo
	MaxFiles    uint64
	MaxBytes    uint64
	Rsv         string
}

type QuotaHeartBeatInfo struct {
	VolName     string
	QuotaId     uint32
	LimitedInfo QuotaLimitedInfo
	Enable      bool
}

type MetaQuotaInfos struct {
	QuotaInfoMap map[uint32]*MetaQuotaInfo
	sync.RWMutex
}

type MetaQuotaInfo struct {
	RootInode bool
}

type QuotaPathInfo struct {
	FullPath    string
	RootInode   uint64
	PartitionId uint64
}

func (usedInfo *QuotaUsedInfo) Add(info *QuotaUsedInfo) {
	usedInfo.UsedFiles += info.UsedFiles
	usedInfo.UsedBytes += info.UsedBytes
}

func (quotaInfo *QuotaInfo) IsOverQuotaFiles() (isOver bool) {
	if uint64(quotaInfo.UsedInfo.UsedFiles) > quotaInfo.MaxFiles {
		isOver = true
	} else {
		isOver = false
	}
	return
}

func (quotaInfo *QuotaInfo) IsOverQuotaBytes() (isOver bool) {
	if uint64(quotaInfo.UsedInfo.UsedBytes) > quotaInfo.MaxBytes {
		isOver = true
	} else {
		isOver = false
	}
	return
}
