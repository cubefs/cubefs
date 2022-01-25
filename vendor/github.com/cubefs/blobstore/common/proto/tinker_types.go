package proto

import (
	"github.com/cubefs/blobstore/common/codemode"
)

const (
	TinkerModule = "TINKER"
)

type ShardRepairTask struct {
	Bid      BlobID            `json:"bid"` //blobId
	CodeMode codemode.CodeMode `json:"code_mode"`
	Sources  []VunitLocation   `json:"sources"`
	BadIdxs  []uint8           `json:"bad_idxs"`
	Reason   string            `json:"reason"`
}

func (task *ShardRepairTask) IsValid() bool {
	if !codemode.IsValid(task.CodeMode) {
		return false
	}
	if !CheckVunitLocations(task.Sources) {
		return false
	}
	return true
}
