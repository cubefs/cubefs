package proto

const (
	OpExtentRepairWrite_ = iota + 512
	OpExtentRepairReadToRollback_
	OpExtentRepairWriteToApplyTempFile_
	OpExtentRepairReadToComputeCrc_
	OpExtentRepairReadByPolicy_
	OpExtentReadToGetCrc_
	OpFlushDelete_
)
