package errors

// 2xx
var (
	ErrShardNodeNotLeader          = newError(1001, "shard node is not leader")
	ErrShardRangeMismatch          = newError(1002, "shard range mismatch")
	ErrShardDoesNotExist           = newError(1003, "shard doest not exist")
	ErrShardNodeDiskNotFound       = newError(1004, "shard disk not found")
	ErrUnknownField                = newError(1005, "unknown field")
	ErrShardRouteVersionNeedUpdate = newError(1006, "shard route version need update")
)
