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
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
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
	//Delete(ctx context.Context, id proto.BlobID) (err error)
	Scan(ctx context.Context, id proto.BlobID, limit int,
		fn func(id proto.BlobID, meta core.ShardMeta) error) (err error)
	Destroy(ctx context.Context) (err error)
	Flush(ctx context.Context) error
	Close()
}

type (
	ChunkMeta struct {
		// chunk index in the physical device layout
		Index chunkIndex
		// chunk epoch, every chunk reuse will increase this field to avoid slice meta reuse or conflicts
		Epoch uint32
		core.VuidMeta
	}
	SliceMeta struct {
		// slice index in the physical device layout, as ShardMeta record offset, this filed can be removed
		Index sliceIndex
		ID    proto.BlobID
		// Vuid means which chunk manage this slice
		Vuid proto.Vuid
		// record chunk's epoch,
		// when chunk delete and open reuse, the slice epoch is mismatch with chunk's epoch and add into slice free list
		ChunkEpoch uint32
		// LastBlockCrc hold last block increment checksum raw, it'll flush into the tail of slice data as block write full
		LastBlockCrc uint32
		// LastSectorCrc hold last device sector increment checksum raw
		//LastSectorCrc []byte
		core.ShardMeta
	}
)

func (c *ChunkMeta) Marshal() ([]byte, error) {
	return nil, nil
}

func (c *ChunkMeta) Unmarshal(raw []byte) error {
	return nil
}

// Marshal Slice into 512 byte
func (s *SliceMeta) Marshal() ([]byte, error) {
	return nil, nil
}

func (s *SliceMeta) MarshalTo(dest []byte) (n int64, err error) {
	return 0, nil
}

func (s *SliceMeta) Reset() {
	*s = SliceMeta{
		Index:     s.Index,
		ShardMeta: core.ShardMeta{Flag: blobnode.ShardStatusMarkDelete},
	}
}

/*func (s *SliceMeta) Size() int {
	return int(unsafe.Sizeof(s))
}*/

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

func (c *chunkMeta) Close() {
	return
}

func newSlice(sm *SliceMeta) *slice {
	return &slice{sm: sm}
}

type slice struct {
	// sm is the same pointer to the store's slice meta to save memory cost
	sm         *SliceMeta
	lastSector [deviceSectorSize]byte
}

func (s *slice) GetShardMeta() *SliceMeta {
	return s.sm
}
