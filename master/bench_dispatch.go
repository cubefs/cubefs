// Copyright 2026 The CubeFS Authors.
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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/util/log"
)

// dispatchBenchTask picks an active syncnode and sends an OpSyncNodeRunTask
// packet with the BenchRule embedded in the request payload. Returns the
// chosen syncnode addr on success so the caller can record it as the task's
// Owner in the ledger (used by the orphan scan to detect a dead owner).
// The BenchRule is dispatched as-is; BackendEndpoint should be
// pre-populated by the caller for S3/SDK storage types.
func (c *Cluster) dispatchBenchTask(taskID string, rule *spec.BenchRule) (string, error) {
	sn, err := c.pickActiveSyncNode()
	if err != nil {
		return "", err
	}
	payload := &SyncRunTaskRequest{
		TaskID:    taskID,
		BenchRule: rule,
	}
	task := proto.NewAdminTaskEx(proto.OpSyncNodeRunTask, sn.Addr, payload, taskID)
	sn.TaskManager.AddTask(task)
	log.LogInfof("dispatchBenchTask: task=%q rule=%q dispatched to syncnode=%q", taskID, rule.ID, sn.Addr)
	return sn.Addr, nil
}

// dispatchBenchShards dispatches n shard tasks for a single bench rule,
// spreading across available synconodes (round-robin over the active set).
// Returns the shard task IDs + a parallel slice of owner addrs (so the
// caller can record per-shard ownership in the ledger). Partial results
// are returned on error along with the error.
//
// Shard IDs follow the "<parentID>/<idx>" convention used by SyncFanout.
// Each shard is dispatched with ShardIdx set in the SyncRunTaskRequest so
// the executor can include the correct index in its result.
func (c *Cluster) dispatchBenchShards(parentID string, rule *spec.BenchRule, n int) ([]string, []string, error) {
	// Collect all active synconodes.
	var nodes []*SyncNode
	c.syncNodes.Range(func(_, v interface{}) bool {
		sn, ok := v.(*SyncNode)
		if !ok || sn == nil || sn.TaskManager == nil {
			return true
		}
		if sn.State != SyncNodeStateActive || !sn.IsActive {
			return true
		}
		nodes = append(nodes, sn)
		return true
	})
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no active syncnode available for bench shard dispatch")
	}

	shardIDs := make([]string, 0, n)
	shardOwners := make([]string, 0, n)
	for i := 0; i < n; i++ {
		shardID := fmt.Sprintf("%s/%d", parentID, i)
		target := nodes[i%len(nodes)]
		payload := &SyncRunTaskRequest{
			TaskID:    shardID,
			BenchRule: rule,
			SubTask: &SyncRunSubTaskInfo{
				ParentTaskID: parentID,
				ShardIndex:   i,
				ShardTotal:   n,
			},
		}
		task := proto.NewAdminTaskEx(proto.OpSyncNodeRunTask, target.Addr, payload, shardID)
		target.TaskManager.AddTask(task)
		shardIDs = append(shardIDs, shardID)
		shardOwners = append(shardOwners, target.Addr)
		log.LogInfof("dispatchBenchShards: shard=%q rule=%q dispatched to syncnode=%q (shard %d/%d)",
			shardID, rule.ID, target.Addr, i+1, n)
	}
	return shardIDs, shardOwners, nil
}

// pickActiveSyncNode returns the first active syncnode it finds in the
// syncNodes registry. Used for single-node dispatch paths.
func (c *Cluster) pickActiveSyncNode() (*SyncNode, error) {
	var target *SyncNode
	c.syncNodes.Range(func(_, v interface{}) bool {
		sn, ok := v.(*SyncNode)
		if !ok || sn == nil || sn.TaskManager == nil {
			return true
		}
		if sn.State != SyncNodeStateActive || !sn.IsActive {
			return true
		}
		target = sn
		return false
	})
	if target == nil {
		return nil, fmt.Errorf("no active syncnode available")
	}
	return target, nil
}
