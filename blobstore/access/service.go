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

package access

import (
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

var (
	gConfig  Config
	gService *Service
)

func init() {
	mod := &cmd.Module{
		Name:       "ACCESS",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterGracefulModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "access.conf")
	if err := config.Load(&gConfig); err != nil {
		return nil, err
	}
	return &gConfig.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	gService = New(gConfig)
	// register all self functions of service
	gService.RegisterService()
	gService.RegisterAdminHandler()
	return NewHandler(gService), nil
}

func tearDown() {
	gService.Close()
}

// NewHandler returns app server handler
func NewHandler(service *Service) *rpc.Router {
	rpc.RegisterArgsParser(&access.PutArgs{}, "json")
	rpc.RegisterArgsParser(&access.PutAtArgs{}, "json")
	rpc.RegisterArgsParser(&access.DeleteBlobArgs{}, "json")

	rpc.Use(service.Limit)

	// POST /put?size={size}&hashes={hashes}
	// request  body:  DataStream
	// response body:  json
	rpc.POST("/put", service.Put, rpc.OptArgsQuery())
	// PUT /put?size={size}&hashes={hashes}
	rpc.PUT("/put", service.Put, rpc.OptArgsQuery())

	// POST /putat?clusterid={clusterid}&volumeid={volumeid}&blobid={blobid}&size={size}&hashes={hashes}&token={token}
	// request  body:  DataStream
	// response body:  json
	rpc.POST("/putat", service.PutAt, rpc.OptArgsQuery())
	// PUT /putat?clusterid={clusterid}&volumeid={volumeid}&blobid={blobid}&size={size}&hashes={hashes}&token={token}
	rpc.PUT("/putat", service.PutAt, rpc.OptArgsQuery())

	// POST /alloc
	// request  body:  json
	// response body:  json
	rpc.POST("/alloc", service.Alloc, rpc.OptArgsBody())

	// POST /get
	// request  body:  json
	// response body:  DataStream
	rpc.POST("/get", service.Get, rpc.OptArgsBody())

	// POST /delete
	// request  body:  json
	// response body:  json
	rpc.POST("/delete", service.Delete, rpc.OptArgsBody())
	// DELETE /deleteblob
	rpc.DELETE("/deleteblob", service.DeleteBlob, rpc.OptArgsQuery())

	// POST /sign
	// request  body:  json
	// response body:  json
	rpc.POST("/sign", service.Sign, rpc.OptArgsBody())

	return rpc.DefaultRouter
}
