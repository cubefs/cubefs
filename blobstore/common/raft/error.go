package raft

const (
	ErrCodeReplicaTooOld = 601 + iota
	ErrCodeRaftGroupDeleted
	ErrCodeGroupHandleRaftMessage
	ErrCodeGroupNotFound
)

var (
	ErrReplicaTooOld          = newError(ErrCodeReplicaTooOld, "replica too old")
	ErrRaftGroupDeleted       = newError(ErrCodeRaftGroupDeleted, "raft group has been deleted")
	ErrGroupHandleRaftMessage = newError(ErrCodeGroupHandleRaftMessage, "group handle raft message failed")
	ErrGroupNotFound          = newError(ErrCodeGroupNotFound, "group not found")
)

func newError(code uint32, err string) *Error {
	return &Error{
		ErrorCode: code,
		ErrorMsg:  err,
	}
}

func (m *Error) Error() string {
	return m.ErrorMsg
}
