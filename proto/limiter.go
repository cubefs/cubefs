package proto

const (
	OpExtentRepairWrite_ = iota + 512
	OpExtentRepairWriteToApplyTempFile_
	OpExtentRepairWriteByPolicy_
	OpExtentRepairReadToRollback_
	OpExtentRepairReadToComputeCrc_
	OpExtentReadToGetCrc_
	OpFlushDelete_
	OpFetchDataPartitionView
)
