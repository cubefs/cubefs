// Copyright 2018 The Containerfs Authors.
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

package master

import (
	"fmt"

	"github.com/juju/errors"
)

var (
	errNoAvailDataPartition                = errors.New("no avail data partition")
	errDisOrderArray                       = errors.New("dis order array is nil")
	errDataReplicaExcess                   = errors.New("data replica Excess error")
	errDataReplicaLack                     = errors.New("data replica Lack error")
	errDataReplicaHasMissOne               = errors.New("data replica has miss one ,cannot miss any one")
	errNoHaveAnyDataNodeToWrite            = errors.New("No have any data node for create data partition")
	errNoHaveAnyMetaNodeToWrite            = errors.New("No have any meta node for create meta partition")
	errCannotOffLine                       = errors.New("cannot offline because avail data replica <0")
	errNoAnyDataNodeForCreateDataPartition = errors.New("no have enough data server for create data partition")
	errNoRackForCreateDataPartition        = errors.New("no rack for create data partition")
	errNoNodeSetForCreateDataPartition     = errors.New("no node set for create data partition")
	errNoNodeSetForCreateMetaPartition     = errors.New("no node set for create meta partition")
	errNoAnyMetaNodeForCreateMetaPartition = errors.New("no have enough meta server for create meta partition")
	errMetaReplicaExcess                   = errors.New("meta partition Replication Excess error")
	errNoHaveMajorityReplica               = errors.New("no have majority replica error")
	errNoLeader                            = errors.New("no leader")
	errBadConfFile                         = errors.New("BadConfFile")
)

func paraNotFound(name string) (err error) {
	return errors.Errorf("parameter %v not found", name)
}

func paraUnmatch(name string) (err error) {
	return errors.Errorf("parameter %v not match", name)
}

func elementNotFound(name string) (err error) {
	return errors.Errorf("%v not found", name)
}

func metaPartitionNotFound(id uint64) (err error) {
	return elementNotFound(fmt.Sprintf("meta partition[%v]", id))
}

func metaReplicaNotFound(addr string) (err error) {
	return elementNotFound(fmt.Sprintf("meta replica[%v]", addr))
}

func dataPartitionNotFound(id uint64) (err error) {
	return elementNotFound(fmt.Sprintf("data partition[%v]", id))
}

func dataReplicaNotFound(addr string) (err error) {
	return elementNotFound(fmt.Sprintf("data replica[%v]", addr))
}

func rackNotFound(name string) (err error) {
	return elementNotFound(fmt.Sprintf("rack[%v]", name))
}

func dataNodeNotFound(addr string) (err error) {
	return elementNotFound(fmt.Sprintf("data node[%v]", addr))
}

func metaNodeNotFound(addr string) (err error) {
	return elementNotFound(fmt.Sprintf("meta node[%v]", addr))
}

func volNotFound(name string) (err error) {
	return elementNotFound(fmt.Sprintf("vol[%v]", name))
}

func hasExist(name string) (err error) {
	err = errors.Errorf("%v has exist", name)
	return
}
