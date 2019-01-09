// Copyright 2018 The Container File System Authors.
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

//err
var (
	ErrNoAvailDataPartition = errors.New("no available data partition")
	ErrReshuffleArray       = errors.New("the array to be reshuffled is nil")

	ErrIllegalDataReplica = errors.New("data replica is illegal")

	ErrMissingReplica       = errors.New("a missing data replica is found")
	ErrHasOneMissingReplica = errors.New("there is a missing data replica")

	ErrNoDataNodeToWrite = errors.New("No data node available for creating a data partition")
	ErrNoMetaNodeToWrite = errors.New("No meta node available for creating a meta partition")

	ErrCannotBeOffLine                 = errors.New("cannot take the data replica offline")
	ErrNoDataNodeToCreateDataPartition = errors.New("no enough data nodes for creating a data partition")
	ErrNoRackToCreateDataPartition     = errors.New("no rack available for creating a data partition")
	ErrNoNodeSetToCreateDataPartition  = errors.New("no node set available for creating a data partition")
	ErrNoNodeSetToCreateMetaPartition  = errors.New("no node set available for creating a meta partition")
	ErrNoMetaNodeToCreateMetaPartition = errors.New("no enough meta nodes for creating a meta partition")
	ErrIllegalMetaReplica              = errors.New("illegal meta replica")
	ErrNoEnoughReplica                 = errors.New("no enough replicas")
	ErrNoLeader                        = errors.New("no leader")
	ErrBadConf                         = errors.New("bad configuration file")
)

func keyNotFound(name string) (err error) {
	return errors.Errorf("parameter %v not found", name)
}

func unmatchedKey(name string) (err error) {
	return errors.Errorf("parameter %v not match", name)
}

func notFoundMsg(name string) (err error) {
	return errors.Errorf("%v not found", name)
}

func metaPartitionNotFound(id uint64) (err error) {
	return notFoundMsg(fmt.Sprintf("meta partition[%v]", id))
}

func metaReplicaNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("meta replica[%v]", addr))
}

func dataPartitionNotFound(id uint64) (err error) {
	return notFoundMsg(fmt.Sprintf("data partition[%v]", id))
}

func dataReplicaNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("data replica[%v]", addr))
}

func rackNotFound(name string) (err error) {
	return notFoundMsg(fmt.Sprintf("rack[%v]", name))
}

func dataNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("data node[%v]", addr))
}

func metaNodeNotFound(addr string) (err error) {
	return notFoundMsg(fmt.Sprintf("meta node[%v]", addr))
}

func volNotFound(name string) (err error) {
	return notFoundMsg(fmt.Sprintf("vol[%v]", name))
}

func exists(name string) (err error) {
	err = errors.Errorf("%v exists", name)
	return
}
