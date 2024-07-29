// Copyright 2022 The CubeFS Authors.
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

package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestProposalQueue(t *testing.T) {
	// Create a new proposal queue with a buffer size of 2
	queue := newProposalQueue(2)

	// Push two proposals onto the queue
	pdata1 := &ProposalData{Module: []byte("test 1")}
	pdata2 := &ProposalData{Module: []byte("test 2")}
	rawData1, err := pdata1.Marshal()
	require.NoError(t, err)
	rawData2, err := pdata2.Marshal()
	require.NoError(t, err)

	proposal1 := proposalRequest{entryType: raftpb.EntryNormal, data: rawData1}
	proposal2 := proposalRequest{entryType: raftpb.EntryConfChange, data: rawData2}
	err = queue.Push(context.Background(), proposal1)
	require.NoError(t, err)
	err = queue.Push(context.Background(), proposal2)
	require.NoError(t, err)

	// Define a function for the `Iter` method to collect all the proposals into a slice
	var proposals []proposalRequest
	iterFunc := func(p proposalRequest) bool {
		proposals = append(proposals, p)
		return true
	}

	// Iterate over the queue and ensure all the proposals are received
	queue.Iter(iterFunc)
	expectedProposals := []proposalRequest{proposal1, proposal2}
	require.EqualValues(t, proposals, expectedProposals)

	// Test context cancellation
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = queue.Push(cancelCtx, proposalRequest{})
	if err != nil {
		require.Equal(t, err, context.Canceled)
	}
}
