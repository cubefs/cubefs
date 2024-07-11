package clustermgr

import "github.com/cubefs/cubefs/blobstore/common/proto"

type Space struct {
	Sid    proto.SpaceID     `json:"sid"`
	Name   string            `json:"name"`
	Status proto.SpaceStatus `json:"status"`
}
