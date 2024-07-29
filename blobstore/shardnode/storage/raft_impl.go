// Copyright 2022 The CubeFS Authors.
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

package storage

import (
	"context"
	"io"

	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
)

const (
	raftWalCF = "raft-wal"
)

type RaftSnapshotTransmitConfig struct {
	BatchInflightNum  int `json:"batch_inflight_num"`
	BatchInflightSize int `json:"batch_inflight_size"`
}

type raftSnapshot struct {
	*RaftSnapshotTransmitConfig

	appliedIndex uint64
	iterIndex    int
	st           kvstore.Snapshot
	ro           kvstore.ReadOption
	lrs          []kvstore.ListReader
	kvStore      kvstore.Store
}

// ReadBatch read batch data for snapshot transmit
// An io.EOF error should be return when the read end of snapshot
// TODO: limit the snapshot transmitting speed
func (r *raftSnapshot) ReadBatch() (raft.Batch, error) {
	var (
		size  = 0
		batch raft.Batch
	)

	for i := 0; i < r.BatchInflightNum; i++ {
		if size >= r.BatchInflightSize {
			return batch, nil
		}

		kg, vg, err := r.lrs[r.iterIndex].ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			if r.iterIndex == len(r.lrs)-1 {
				return batch, io.EOF
			}
			r.iterIndex++
			return batch, nil
		}

		if batch == nil {
			batch = raftBatch{batch: r.kvStore.NewWriteBatch()}
		}
		batch.Put(kg.Key(), vg.Value())
		size += vg.Size()
	}

	return batch, nil
}

func (r *raftSnapshot) Index() uint64 {
	return r.appliedIndex
}

func (r *raftSnapshot) Close() error {
	for i := range r.lrs {
		r.lrs[i].Close()
	}
	r.st.Close()
	r.ro.Close()
	return nil
}

type raftStorage struct {
	kvStore kvstore.Store
}

func (w *raftStorage) Get(key []byte) (raft.ValGetter, error) {
	return w.kvStore.Get(context.TODO(), raftWalCF, key, nil)
}

func (w *raftStorage) Iter(prefix []byte) raft.Iterator {
	return raftIterator{lr: w.kvStore.List(context.TODO(), raftWalCF, prefix, nil, nil)}
}

func (w *raftStorage) NewBatch() raft.Batch {
	return raftBatch{cf: raftWalCF, batch: w.kvStore.NewWriteBatch()}
}

func (w *raftStorage) Write(b raft.Batch) error {
	return w.kvStore.Write(context.TODO(), b.(raftBatch).batch, nil)
}

type raftIterator struct {
	lr kvstore.ListReader
}

func (i raftIterator) SeekTo(key []byte) {
	i.lr.Seek(key)
}

func (i raftIterator) SeekForPrev(prev []byte) error {
	return i.lr.SeekForPrev(prev)
}

func (i raftIterator) ReadNext() (key raft.KeyGetter, val raft.ValGetter, err error) {
	return i.lr.ReadNext()
}

func (i raftIterator) ReadPrev() (key raft.KeyGetter, val raft.ValGetter, err error) {
	return i.lr.ReadPrev()
}

func (i raftIterator) Close() {
	i.lr.Close()
}

type raftBatch struct {
	cf    kvstore.CF
	batch kvstore.WriteBatch
}

func (t raftBatch) Put(key, value []byte) { t.batch.Put(t.cf, key, value) }

func (t raftBatch) Delete(key []byte) { t.batch.Delete(t.cf, key) }

func (t raftBatch) DeleteRange(start []byte, end []byte) { t.batch.DeleteRange(t.cf, start, end) }

func (t raftBatch) Data() []byte { return t.batch.Data() }

func (t raftBatch) From(data []byte) { t.batch.From(data) }

func (t raftBatch) Close() { t.batch.Close() }

type addressResolver struct {
	t base.Transport
}

func (a *addressResolver) Resolve(ctx context.Context, diskID uint64) (raft.Addr, error) {
	disk, err := a.t.GetDisk(ctx, proto.DiskID(diskID))
	if err != nil {
		return nil, err
	}
	node, err := a.t.GetNode(ctx, disk.NodeID)
	if err != nil {
		return nil, err
	}
	return nodeAddr{addr: node.RaftHost}, nil
}

type nodeAddr struct {
	addr string
}

func (n nodeAddr) String() string {
	return n.addr
}
