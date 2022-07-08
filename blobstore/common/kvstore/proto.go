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

package kvstore

type KVStorage interface {
	Put(kv KV, opts ...OpOption) error
	Get(key []byte, opts ...OpOption) ([]byte, error)
	Delete(key []byte, opts ...OpOption) error
	NewWriteBatch() *WriteBatch
	DeleteBatch(keys [][]byte, safe bool) error
	WriteBatch(kvs []KV, safe bool) error
	DoBatch(batch *WriteBatch) error
	Flush() error
	NewSnapshot() *Snapshot
	ReleaseSnapshot(snapshot *Snapshot)
	NewIterator(snapshot *Snapshot, opts ...OpOption) Iterator
}

type Op struct {
	// for put
	PutKeys []KV
	// for delete
	DelKeys []KV
	// for iterator
	Ro   *ReadOptions
	Snap *Snapshot
}

type OpOption func(*Op)

func (op *Op) applyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}
