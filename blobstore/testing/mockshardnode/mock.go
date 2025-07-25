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

package mock

//go:generate mockgen -source=../../shardnode/catalog/catalog.go -destination=./catalog.go -package=mock -mock_names ShardGetter=MockShardGetter
//go:generate mockgen -source=../../shardnode/storage/shard.go -destination=./shard.go -package=mock -mock_names ShardHandler=MockSpaceShardHandler
//go:generate mockgen -source=../../shardnode/base/transport.go -destination=../mocks/shardnode_transport.go -package=mocks -mock_names Transport=MockTransport
//go:generate mockgen -source=../../shardnode/catalog/allocator/allocator.go -destination=./allocator.go -package=mock -mock_names Allocator=MockAllocator
//go:generate mockgen -source=../../shardnode/base/volume_cache.go -destination=../mocks/shardnode_volume_cache.go -package=mocks -mock_names IVolumeCache=MockIVolumeCache
//go:generate mockgen -source=../../shardnode/blobdeleter/blob_deleter.go -destination=./blob_deleter.go -package=mock -mock_names ShardGetter=MockDelMgrShardGetter
