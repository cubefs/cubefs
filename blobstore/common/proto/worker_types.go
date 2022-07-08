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

package proto

// stats task has done in worker
type TaskStatistics struct {
	MigDataSizeByte   uint64 `json:"mig_data_size_byte"`
	MigShardCnt       uint64 `json:"mig_shard_cnt"`
	TotalDataSizeByte uint64 `json:"total_data_size_byte"`
	TotalShardCnt     uint64 `json:"total_shard_cnt"`
	Progress          uint64 `json:"progress"`
}

func (self *TaskStatistics) Add(dataSize, shardCnt uint64) {
	self.MigDataSizeByte += dataSize
	self.MigShardCnt += shardCnt
	if self.TotalDataSizeByte == 0 {
		self.Progress = 100
	} else {
		self.Progress = (self.MigDataSizeByte * 100) / self.TotalDataSizeByte
	}
}

func (self *TaskStatistics) InitTotal(totalDataSize, totalShardCnt uint64) {
	self.TotalDataSizeByte = totalDataSize
	self.TotalShardCnt = totalShardCnt
}

func (self *TaskStatistics) Completed() bool {
	return self.Progress == 100
}
