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

// github.com/cubefs/cubefs/blobstore/clustermgr/... module clustermgr interfaces
//go:generate mockgen -destination=../base/raftproto_mock_test.go -package=base -mock_names RaftApplier=MockRaftApplier github.com/cubefs/cubefs/blobstore/clustermgr/base RaftApplier
//go:generate mockgen -destination=./kvmgr.go -package=mock -mock_names KvMgrAPI=MockKvMgrAPI github.com/cubefs/cubefs/blobstore/clustermgr/kvmgr KvMgrAPI
//go:generate mockgen -destination=./configmgr.go -package=mock -mock_names ConfigMgrAPI=MockConfigMgrAPI github.com/cubefs/cubefs/blobstore/clustermgr/configmgr ConfigMgrAPI
//go:generate mockgen -destination=./scopemgr.go -package=mock -mock_names ScopeMgrAPI=MockScopeMgrAPI github.com/cubefs/cubefs/blobstore/clustermgr/scopemgr ScopeMgrAPI
//go:generate mockgen -destination=../cluster/blobnodemgr_mock.go -package=cluster -mock_names DiskMgrAPI=MockDiskMgrAPI github.com/cubefs/cubefs/blobstore/clustermgr/cluster BlobNodeManagerAPI
