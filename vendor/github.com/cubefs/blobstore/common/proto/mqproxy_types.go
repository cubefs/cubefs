package proto

import (
	"github.com/cubefs/blobstore/util/errors"
)

const MqProxySvrName = "MQPROXY"

var ErrInvalidMsg = errors.New("msg is invalid")

type DeleteStage byte

const (
	InitStage DeleteStage = iota
	MarkDelStage
	DelStage
)

type BlobDeleteStage struct {
	Stages map[uint8]DeleteStage `json:"stages"`
}

func (s *BlobDeleteStage) SetStage(vuidIdx uint8, stage DeleteStage) {
	if s.Stages == nil {
		s.Stages = make(map[uint8]DeleteStage)
	}
	s.Stages[vuidIdx] = stage
}

func (s *BlobDeleteStage) Stage(vuid Vuid) (DeleteStage, bool) {
	stage, exist := s.Stages[vuid.Index()]
	return stage, exist
}

func (s *BlobDeleteStage) Copy() BlobDeleteStage {
	myCopy := BlobDeleteStage{}
	myCopy.Stages = make(map[uint8]DeleteStage)
	for k, v := range s.Stages {
		myCopy.Stages[k] = v
	}
	return myCopy
}

type DeleteMsg struct {
	ClusterID     ClusterID       `json:"cluster_id"`
	Bid           BlobID          `json:"bid"`
	Vid           Vid             `json:"vid"`
	Retry         int             `json:"retry"`
	Time          int64           `json:"time"`
	ReqId         string          `json:"req_id"`
	BlobDelStages BlobDeleteStage `json:"blob_del_stages"`
}

func (msg *DeleteMsg) IsValid() bool {
	if msg.Bid == InValidBlobID {
		return false
	}
	if msg.Vid == InvalidVid {
		return false
	}
	return true
}

func (msg *DeleteMsg) SetDeleteStage(stage BlobDeleteStage) {
	for idx, s := range stage.Stages {
		msg.BlobDelStages.SetStage(idx, s)
	}
}

type ShardRepairMsg struct {
	ClusterID ClusterID `json:"cluster_id"`
	Bid       BlobID    `json:"bid"`
	Vid       Vid       `json:"vid"`
	BadIdx    []uint8   `json:"bad_idx"`
	Retry     int       `json:"retry"`
	Reason    string    `json:"reason"`
	ReqId     string    `json:"req_id"`
}

func (msg *ShardRepairMsg) IsValid() bool {
	if msg.Bid == InValidBlobID {
		return false
	}
	if msg.Vid == InvalidVid {
		return false
	}
	if len(msg.BadIdx) == 0 {
		return false
	}
	return true
}
