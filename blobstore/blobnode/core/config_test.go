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

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func init() {
	log.SetOutputLevel(log.Lfatal)
}

func TestInitConfig(t *testing.T) {
	conf := &Config{
		BaseConfig: BaseConfig{
			Path: "",
		},
	}

	err := InitConfig(conf)
	require.Error(t, err)

	conf.Path = "/home"
	err = InitConfig(conf)
	require.Error(t, err)

	conf.HandleIOError = func(ctx context.Context, diskID proto.DiskID, diskErr error) {}
	err = InitConfig(conf)
	require.Error(t, err)
}
