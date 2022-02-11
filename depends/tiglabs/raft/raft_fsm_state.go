// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

type (
	fsmState     byte
	replicaState byte
)

const (
	stateFollower    fsmState = 0
	stateCandidate            = 1
	stateLeader               = 2
	stateElectionACK          = 3

	replicaStateProbe     replicaState = 0
	replicaStateReplicate              = 1
	replicaStateSnapshot               = 2
)

func (st fsmState) String() string {
	switch st {
	case 0:
		return "StateFollower"
	case 1:
		return "StateCandidate"
	case 2:
		return "StateLeader"
	case 3:
		return "StateElectionACK"
	}
	return ""
}

func (st replicaState) String() string {
	switch st {
	case 1:
		return "ReplicaStateReplicate"
	case 2:
		return "ReplicaStateSnapshot"
	default:
		return "ReplicaStateProbe"
	}
}
