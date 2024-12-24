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

package store

import (
	"context"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type MetaHandler interface {
	ID() clustermgr.ChunkID
	//InnerDB() db.MetaHandler
	SupportInline() bool
	//Write(ctx context.Context, id proto.BlobID, value core.ShardMeta) (err error)
	Get(ctx context.Context, id proto.BlobID) (meta core.ShardMeta, err error)
	Update(ctx context.Context, id proto.BlobID, meta core.ShardMeta) error
	//Delete(ctx context.Context, id proto.BlobID) (err error)
	Scan(ctx context.Context, id proto.BlobID, limit int,
		fn func(id proto.BlobID, meta core.ShardMeta) error) (err error)
	//Destroy(ctx context.Context) (err error)
	Flush(ctx context.Context) error
	Close(ctx context.Context)
}

type (
	ChunkMeta struct {
		// chunk index in the physical device layout
		Index chunkIndex
		// chunk epoch, every chunk reuse will increase this field to avoid slice meta reuse or conflicts
		Epoch uint32
		core.VuidMeta
	}
)

func (c *ChunkMeta) Marshal() ([]byte, error) {
	return nil, nil
}

func (c *ChunkMeta) Unmarshal(raw []byte) error {
	return nil
}

type chunkMeta chunk

func (c *chunkMeta) ID() clustermgr.ChunkID {
	return c.meta.ChunkID
}

func (c *chunkMeta) SupportInline() bool {
	return false
}

func (c *chunkMeta) Get(ctx context.Context, id proto.BlobID) (core.ShardMeta, error) {
	slice, err := (*chunk)(c).getSlice(id)
	if err != nil {
		return core.ShardMeta{}, err
	}
	return slice.GetShardMeta().ShardMeta, nil
}

func (c *chunkMeta) Update(ctx context.Context, id proto.BlobID, meta core.ShardMeta) error {
	slice, err := (*chunk)(c).getSlice(id)
	if err != nil {
		return err
	}

	sm := slice.GetShardMeta()
	_sm := *sm
	_sm.ShardMeta = meta
	if err := c.sliceHandler.UpdateSlice(&_sm); err != nil {
		return err
	}
	return nil
}

func (c *chunkMeta) Scan(ctx context.Context, id proto.BlobID, limit int,
	fn func(id proto.BlobID, sm core.ShardMeta) error) (err error) {
	// todo
	return nil
}

func (c *chunkMeta) Destroy(ctx context.Context) (err error) {
	return nil
}

func (c *chunkMeta) Flush(ctx context.Context) error {
	return nil
}

func (c *chunkMeta) Close(ctx context.Context) {
	return
}
