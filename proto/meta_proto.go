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

package proto

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

// Learner defines the learner of the node id and address.
type Learner struct {
	ID       uint64         `json:"id"`
	Addr     string         `json:"addr"`
	PmConfig *PromoteConfig `json:"promote_config"`
}

type PromoteConfig struct {
	AutoProm      bool  `json:"auto_prom"`
	PromThreshold uint8 `json:"prom_threshold"`
}

// CreateMetaPartitionRequest defines the request to create a meta partition.
type CreateMetaPartitionRequest struct {
	MetaId      string
	VolName     string
	Start       uint64
	End         uint64
	PartitionID uint64
	Members     []Peer
	Learners    []Learner
	StoreMode   StoreMode
	TrashDays   uint32
	CreationType int
}

// CreateMetaPartitionResponse defines the response to the request of creating a meta partition.
type CreateMetaPartitionResponse struct {
	VolName     string
	PartitionID uint64
	Status      uint8
	Result      string
}

type MNMetaPartitionInfo struct {
	LeaderAddr string    `json:"leaderAddr"`
	Peers      []Peer    `json:"peers"`
	Learners   []Learner `json:"learners"`
	NodeId     uint64    `json:"nodeId"`
	Cursor     uint64    `json:"cursor"`
}

type MetaDataCRCSumInfo struct {
	PartitionID uint64   `json:"pid"`
	ApplyID     uint64   `json:"applyID"`
	CntSet      []uint64 `json:"cntSet"`
	CRCSumSet   []uint32 `json:"crcSumSet"`
}

type InodesCRCSumInfo struct {
	PartitionID     uint64   `json:"pid"`
	ApplyID         uint64   `json:"applyID"`
	AllInodesCRCSum uint32   `json:"allInodesCRCSum"`
	InodesID        []uint64 `json:"inodeIDSet"`
	CRCSumSet       []uint32 `json:"crcSumSet"`
}