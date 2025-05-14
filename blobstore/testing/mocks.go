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

// Package testing for mocking interfaces with `go generate`
package testing

// github.com/cubefs/cubefs/blobstore/util/... util interfaces
//go:generate mockgen -destination=./mocks/util_selector.go -package=mocks -mock_names Selector=MockSelector github.com/cubefs/cubefs/blobstore/util/selector Selector
//go:generate mockgen -destination=./mocks/util_iopool.go -package=mocks -mock_names IoPool=MockIoPool github.com/cubefs/cubefs/blobstore/util/taskpool IoPool

// github.com/cubefs/cubefs/blobstore/common/... common interfaces
//go:generate mockgen -destination=./mocks/common_raftserver.go -package=mocks -mock_names RaftServer=MockRaftServer github.com/cubefs/cubefs/blobstore/common/raftserver RaftServer
//go:generate mockgen -destination=./mocks/common_rpc.go -package=mocks -mock_names Client=MockRPCClient github.com/cubefs/cubefs/blobstore/common/rpc Client
//go:generate mockgen -destination=./mocks/common_recordlog.go -package=mocks -mock_names Encoder=MockRecordLogEncoder github.com/cubefs/cubefs/blobstore/common/recordlog Encoder
//go:generate mockgen -destination=./mocks/common_taskswitch.go -package=mocks -mock_names ISwitcher=MockSwitcher github.com/cubefs/cubefs/blobstore/common/taskswitch ISwitcher,Accessor

// github.com/cubefs/cubefs/blobstore/api/... api interfaces
//go:generate mockgen -destination=./mocks/api_access.go -package=mocks -mock_names API=MockAccessAPI github.com/cubefs/cubefs/blobstore/api/access API
//go:generate mockgen -destination=./mocks/api_clustermgr.go -package=mocks -mock_names ClientAPI=MockClientAPI github.com/cubefs/cubefs/blobstore/api/clustermgr ClientAPI
//go:generate mockgen -destination=./mocks/api_blobnode.go -package=mocks -mock_names StorageAPI=MockStorageAPI github.com/cubefs/cubefs/blobstore/api/blobnode StorageAPI
//go:generate mockgen -destination=./mocks/api_scheduler.go -package=mocks -mock_names IScheduler=MockIScheduler github.com/cubefs/cubefs/blobstore/api/scheduler IScheduler
//go:generate mockgen -destination=./mocks/api_proxy.go -package=mocks -mock_names Client=MockProxyClient,LbMsgSender=MockProxyLbRpcClient github.com/cubefs/cubefs/blobstore/api/proxy Client,LbMsgSender
//go:generate mockgen -destination=./mocks/api_shardnode.go -package=mocks -mock_names AccessAPI=MockShardnodeAccess github.com/cubefs/cubefs/blobstore/api/shardnode AccessAPI

// github.com/cubefs/cubefs/blobstore/access/... access interfaces
//go:generate mockgen -destination=./mocks/access_stream.go -package=mocks -mock_names StreamHandler=MockStreamHandler github.com/cubefs/cubefs/blobstore/access/stream StreamHandler

import (
	// add package to go.mod for `go generate`
	_ "github.com/golang/mock/mockgen/model"
)
