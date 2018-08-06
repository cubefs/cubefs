// Copyright 2018 The ChuBao Authors.
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

type CreateNameSpaceRequest struct {
	Name string
}

type CreateNameSpaceResponse struct {
	Status int
	Result string
}

type Peer struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}
type CreateMetaPartitionRequest struct {
	MetaId      string
	VolName     string
	Start       uint64
	End         uint64
	PartitionID uint64
	Members     []Peer
}

type CreateMetaPartitionResponse struct {
	VolName     string
	PartitionID uint64
	Status      uint8
	Result      string
}
