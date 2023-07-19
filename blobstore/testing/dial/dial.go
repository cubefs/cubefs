// Copyright 2023 The CubeFS Authors.
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

package dial

import (
	"net/http"

	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

var (
	gService *serviceDial
	gConfig  serviceConfig
)

func init() {
	cmd.RegisterModule(&cmd.Module{
		Name:       "DialTest",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	})
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "dial.conf")
	if err := config.Load(&gConfig); err != nil {
		return nil, err
	}
	return &gConfig.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	gService = newService(gConfig)
	if err := gService.Start(); err != nil {
		panic(err)
	}
	return newHandler(gService), nil
}

func tearDown() {
	gService.Close()
}

func newHandler(service *serviceDial) *rpc.Router {
	router := rpc.New()
	router.Handle(http.MethodGet, "/status", service.Status)
	return router
}
