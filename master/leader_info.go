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

package master

import "sync/atomic"

type LeaderTermInfo struct {
	id        uint64
	term      uint64
	lastIndex uint64
}

type LeaderInfo struct {
	addr          string //host:port
	isLeader      atomic.Bool
	metaReady     atomic.Bool
	leaderVersion uint64
}

func (c *Cluster) setIsLeader(value bool) {
	c.leaderInfo.isLeader.Store(value)
}

func (c *Cluster) IsLeader() bool {
	return c.leaderInfo.isLeader.Load()
}

func (c *Cluster) setMetaReady(value bool) {
	c.leaderInfo.metaReady.Store(value)
}

func (c *Cluster) isMetaReady() bool {
	return c.leaderInfo.metaReady.Load()
}

func (c *Cluster) incrLeaderVersion() {
	atomic.AddUint64(&c.leaderInfo.leaderVersion, 1)
}

func (c *Cluster) getLeaderVersion() uint64 {
	return atomic.LoadUint64(&c.leaderInfo.leaderVersion)
}

func (m *Server) preProcessOnLeaderChange() {
	m.cluster.setMetaReady(false)
	m.cluster.setIsLeader(false)
	m.cluster.incrLeaderVersion()
	close(m.cluster.taskExitC)
}

func (m *Server) postProcessOnLeaderChange() {
	m.cluster.setMetaReady(true)
	m.cluster.setIsLeader(true)
}
