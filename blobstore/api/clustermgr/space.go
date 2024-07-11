package clustermgr

import "github.com/cubefs/cubefs/blobstore/common/proto"

type Space struct {
	SpaceID    proto.SpaceID     `json:"sid"`
	Name       string            `json:"name"`
	Status     proto.SpaceStatus `json:"status"`
	FieldMetas []FieldMeta       `json:"field_metas"`
}

type FieldMeta struct {
	ID          proto.FieldID     `json:"id"`
	Name        string            `json:"name"`
	FieldType   proto.FieldType   `json:"field_type"`
	IndexOption proto.IndexOption `json:"index_option"`
}
