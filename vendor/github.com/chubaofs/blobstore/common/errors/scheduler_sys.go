package errors

import (
	"errors"
)

const (
	CodeNotingTodo        = 700
	CodeDestReplicaBad    = 702
	CodeOrphanShard       = 703
	CodeIllegalTask       = 704
	CodeNoInspect         = 705
	CodeClusterIDNotMatch = 706
)

//scheduler
var (
	ErrNoSuchService   = errors.New("no such service")
	ErrIllegalTaskType = errors.New("illegal task type")
	ErrCanNotDropped   = errors.New("disk can not dropped")

	//error code
	ErrNothingTodo = Error(CodeNotingTodo)
	ErrNoInspect   = Error(CodeNoInspect)
)

//worker
var (
	ErrShardMayBeLost = errors.New("shard may be lost")
	//error code
	ErrOrphanShard    = Error(CodeOrphanShard)
	ErrIllegalTask    = Error(CodeIllegalTask)
	ErrDestReplicaBad = Error(CodeDestReplicaBad)
)

//
var (
	ErrClusterIDNotMatch = Error(CodeClusterIDNotMatch)
)
