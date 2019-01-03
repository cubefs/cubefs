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
	noAvailDataPartitionErr = errors.New("no available data partition")
	reshuffleArrayErr       = errors.New("the array to be reshuffled is nil")

	// TODO rename meta replica and data replica : we do not have these two concepts
	illegalDataReplicaErr   = errors.New("data replica is illegal")

	//  TODO what is the difference between the following two errs?
	missingReplicaErr       = errors.New("a missing data replica is found")
	hasOneMissingReplicaErr = errors.New("there is a missing data replica")

	noDataNodeToWriteErr    = errors.New("No data node available for creating a data partition")
	noMetaNodeToWriteErr    = errors.New("No meta node available for creating a meta partition")

	// TODO verify this is correct: "the number of available data replicas is less than 0" ?
	cannotBeOffLineErr      = errors.New("cannot take the data replica offline because the number of available data replicas is less than 0")
	noDataNodeToCreateDataPartitionErr = errors.New("no enough data nodes for creating a data partition")
	noRackToCreateDataPartitionErr     = errors.New("no rack available for creating a data partition")
	noNodeSetToCreateDataPartitionErr  = errors.New("no node set available for creating a data partition")
	noNodeSetToCreateMetaPartitionErr  = errors.New("no node set available for creating a meta partition")
	noMetaNodeToCreateMetaPartitionErr = errors.New("no enough meta nodes for creating a meta partition")
	illegalMetaReplicaErr              = errors.New("illegal meta replica")
	noEnoughReplicaErr                 = errors.New("no enough replicas")
	noLeaderErr                        = errors.New("no leader")
	badConfErr                         = errors.New("bad configuration file")
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
