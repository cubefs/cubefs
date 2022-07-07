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

package example

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// Client read and write client
type Client interface {
	Write(context.Context, int, io.Reader) error
	Read(context.Context, int) (io.ReadCloser, error)
}

// SimpleConfig simple client config
type SimpleConfig struct {
	Host   string     `json:"host"`
	Config rpc.Config `json:"rpc_config"`
}

// NewFileClient returns file client
func NewFileClient(conf *SimpleConfig) Client {
	return &fileClient{conf.Host, rpc.NewClient(&conf.Config)}
}

type fileClient struct {
	host string
	rpc.Client
}

func (fc *fileClient) Write(ctx context.Context, size int, body io.Reader) (err error) {
	request, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/write/%d", fc.host, size), body)
	if err != nil {
		return
	}
	return fc.DoWith(ctx, request, nil, rpc.WithCrcEncode())
}

func (fc *fileClient) Read(ctx context.Context, size int) (io.ReadCloser, error) {
	resp, err := fc.Get(ctx, fmt.Sprintf("%s/read/%d", fc.host, size))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid httpcode %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// LBConfig lb client config
type LBConfig struct {
	Config rpc.LbConfig `json:"rpc_lb_config"`
}

// NewMetaClient returns meta client
func NewMetaClient(conf *LBConfig, selector rpc.Selector) Client {
	return &metaClient{rpc.NewLbClient(&conf.Config, selector)}
}

type metaClient struct {
	rpc.Client
}

func (fc *metaClient) Write(ctx context.Context, size int, body io.Reader) (err error) {
	request, err := http.NewRequest(http.MethodPut, fmt.Sprintf("/write/%d", size), body)
	if err != nil {
		return
	}
	resp, err := fc.Do(ctx, request)
	if err != nil {
		return
	}
	resp.Body.Close()
	return
}

func (fc *metaClient) Read(ctx context.Context, size int) (io.ReadCloser, error) {
	resp, err := fc.Get(ctx, fmt.Sprintf("/read/%d", size))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid httpcode %d", resp.StatusCode)
	}
	return resp.Body, nil
}
