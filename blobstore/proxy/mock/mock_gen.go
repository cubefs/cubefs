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

package mock

// github.com/cubefs/cubefs/blobstore/proxy/... module proxy interfaces

//go:generate mockgen -destination=./mq_mock.go -package=mock -mock_names BlobDeleteHandler=MockBlobDeleteHandler,ShardRepairHandler=MockShardRepairHandler,Producer=MockProducer github.com/cubefs/cubefs/blobstore/proxy/mq BlobDeleteHandler,ShardRepairHandler,Producer
//go:generate mockgen -destination=./allocator_mock.go -package=mock -mock_names BlobDeleteHandler=MockBlobDeleteHandler,ShardRepairHandler=MockShardRepairHandler,Producer=MockProducer github.com/cubefs/cubefs/blobstore/proxy/allocator VolumeMgr
//go:generate mockgen -destination=./cacher_mock.go -package=mock -mock_names Cacher=MockCacher github.com/cubefs/cubefs/blobstore/proxy/cacher Cacher
