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
	"context"
	"fmt"
	"io"

	"github.com/cubefs/cubefs/blobstore/api/access"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

// SDK stream sdk handler, request in client.
type SDK interface {
	// Put put one object
	//     required: size, file size
	//     optional: hasher map to calculate hash.Hash
	Put(ctx context.Context, rc io.Reader, size int64, hasherMap access.HasherMap) (*access.Location, error)
	// Get read file
	//     required: location, readSize
	//     optional: offset(default is 0)
	Get(ctx context.Context, w io.Writer, location *access.Location, readSize, offset uint64) error
	// Delete delete all blobs in this location
	Delete(ctx context.Context, location *access.Location) error
	// Close free all.
	Close()
}

type sdkStream struct {
	closer.Closer
	handler StreamHandler
}

// NewSDK return stream handler.
func NewSDK(config StreamConfig) (SDK, error) {
	// add region magic checksum to the secret keys
	initWithRegionMagic(config.ClusterConfig.RegionMagic)

	if config.IDC == "" {
		return nil, fmt.Errorf("idc of config is empty")
	}
	if config.ClusterConfig.ConsulAgentAddr == "" && len(config.ClusterConfig.Clusters) == 0 {
		return nil, fmt.Errorf("consul and clusters are empty")
	}
	confCheck(&config)

	cl := closer.New()
	handler, err := newStreamHandler(&config, cl.Done())
	if err != nil {
		return nil, err
	}
	return &sdkStream{
		Closer:  cl,
		handler: handler,
	}, nil
}

func (sdk *sdkStream) Put(ctx context.Context, rc io.Reader, size int64, hasherMap access.HasherMap) (*access.Location, error) {
	location, err := sdk.handler.Put(ctx, rc, size, hasherMap)
	if err != nil {
		return nil, httpError(err)
	}
	if err := fillCrc(location); err != nil {
		return nil, httpError(err)
	}
	return location, nil
}

func (sdk *sdkStream) Get(ctx context.Context, w io.Writer, location *access.Location, readSize, offset uint64) error {
	if !verifyCrc(location) {
		return errcode.ErrIllegalArguments
	}
	transfer, err := sdk.handler.Get(ctx, w, *location, readSize, offset)
	if err != nil {
		return httpError(err)
	}
	if err = transfer(); err != nil {
		return rpc.NewError(errcode.CodeAccessUnexpect, "", err)
	}
	return nil
}

func (sdk *sdkStream) Delete(ctx context.Context, location *access.Location) error {
	if !verifyCrc(location) {
		return errcode.ErrIllegalArguments
	}
	if err := sdk.handler.Delete(ctx, location); err != nil {
		return httpError(err)
	}
	return nil
}
