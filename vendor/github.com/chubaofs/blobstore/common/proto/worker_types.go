package proto

const WorkerSvrName = "worker"

//stats task has done in worker
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
