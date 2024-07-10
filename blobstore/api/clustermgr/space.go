// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package clustermgr

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

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

type CreateSpaceArgs struct {
	Name       string      `json:"name"`
	FieldMetas []FieldMeta `json:"field_metas"`
}

func (c *Client) CreateSpace(ctx context.Context, args *CreateSpaceArgs) (err error) {
	err = c.PostWith(ctx, "/space/create", nil, args)
	return
}

type GetSpaceArgs struct {
	Name string `json:"name"`
}

func (c *Client) GetSpace(ctx context.Context, args *GetSpaceArgs) (ret *Space, err error) {
	ret = &Space{}
	err = c.GetWith(ctx, "/space/get?name="+args.Name, ret)
	return
}
